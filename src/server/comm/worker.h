#pragma once

#include <boost/asio.hpp>
#include <spdlog/spdlog.h>
#include <algorithm>
#include <cstring>
#include <memory>
#include <optional>
#include <vector>

#include "outbox.h"
#include "session.h"
#include "shard.h"
#include "type.h"
#include "../../protocol/memcached_parser.h"
#include "../../cache/shard_cache.h"

namespace fastest {
namespace asio = boost::asio;

using asio::awaitable;
using asio::use_awaitable;

inline MultiKeyCommand rebuild(const MultiKeyCommand &command, std::vector<ItemKey> keys) {
	return std::visit(
		[&keys](const auto &value) -> MultiKeyCommand {
			using T = std::remove_cvref_t<decltype(value)>;
			if constexpr (std::is_same_v<T, MultiGetCommand>) {
				return MultiKeyCommand{MultiGetCommand{std::move(keys)}};
			} else if constexpr (std::is_same_v<T, MultiGetsCommand>) {
				return MultiKeyCommand{MultiGetsCommand{std::move(keys)}};
			} else if constexpr (std::is_same_v<T, MultiGatCommand>) {
				return MultiKeyCommand{MultiGatCommand{value.expired_at, std::move(keys)}};
			} else if constexpr (std::is_same_v<T, MultiGatsCommand>) {
				return MultiKeyCommand{MultiGatsCommand{value.expired_at, std::move(keys)}};
			}
		},
		command.command);
}

// 统计计数：只在测试构建（定义了 FASTEST_TESTING）里存在。生产构建里是空类型，配合 [[no_unique_address]] 不占空间，调用也不产生指令。
// 发件侧（敲门、信封）的计数在 OutboxStats，写次数在 SessionStats。
struct WorkerStats {
#if defined(FASTEST_TESTING)
	uint64_t mailbox_messages{0}; // 收件协程处理的消息数
	uint64_t mailbox_sleeps{0};   // 收件协程在 fd 上睡下的次数

	void handled(const std::size_t n) noexcept {
		mailbox_messages += n;
	}

	void slept() noexcept {
		++mailbox_sleeps;
	}
#else
	void handled(std::size_t) noexcept {
	}

	void slept() noexcept {
	}
#endif
};

class Worker {
public:
	using Sc = ShardComm;

	explicit Worker(Sc shard_comm, std::shared_ptr<std::atomic_bool> stop)
		: shard_comm_(std::move(shard_comm)), data_(static_cast<uint16_t>(shard_comm_.current_shard_id())),
		  outbox_(shard_comm_), sessions_(shard_comm_.current_shard_id()), multi_key_index_(shard_comm_.shard_num()),
		  stop_(std::move(stop)) {
	}

	Worker(const Worker &) = delete;

	Worker &operator=(const Worker &) = delete;

	/*
	 * 批次作用域：合并跨分片投递、敲门和写。
	 * 作用域内发往别的分片的请求交给 Outbox，装进每个目标分片一个的信封（装满 Envelope::kCapacity 条就先发出去）；
	 * 响应入环（SessionTable::deliver）只记下连接，不发起写。离开作用域时先 Outbox::flush()：把还开着的信封发出去、
	 * 每个目标分片敲一次门铃（让对端尽早开始处理），再 SessionTable::flush_writes()：给每个连接 kick_write 一次，
	 * 一次写带上这一批里该连接所有就绪的响应。
	 * 一个作用域里已发出的消息攒到 PendingDoorbells::kFlushEvery 条也先敲一次门铃，避免对端在睡、长时间听不到。
	 * 作用域内不能 co_await：挂起前必须把信封发出去、门铃敲出去、写发起，否则对端收不到请求，响应也一直不发。
	 * 所有跨分片发送和响应入环都必须在作用域内：解析批次（Handler::session）和收件协程的一轮（drain_inbox）。
	 * 作用域外装进信封的请求要等下一个作用域结束才发出，入环的响应要等下一个作用域结束才写。
	 * 嵌套时内层退出也会发出信封、敲门、发起写，只是提前，不影响正确性。
	 */
	class BatchScope {
	public:
		explicit BatchScope(Worker &worker) noexcept : worker_(worker) {
		}

		~BatchScope() {
			worker_.outbox_.flush();
			worker_.sessions_.flush_writes();
		}

		BatchScope(const BatchScope &) = delete;

		BatchScope &operator=(const BatchScope &) = delete;

	private:
		Worker &worker_;
	};

	/*
	 * 本分片唯一的收件协程：一个收件队列（请求信封和回程信封都在里面）加一个门铃。
	 *
	 * 醒着时门铃保持抬起，生产者 push 后只读一下标志就返回，不写 eventfd。每轮把收件队列清空（不设上限），
	 * 这一轮处理过消息就让出 CPU 给 socket，稍后再来一轮；某一轮收件队列是空的，才放下门铃、
	 * fence 后再查一遍，确实为空才去等 fd。所以有流量时几乎不写 eventfd，只有从空闲转忙时敲一次门。
	 *
	 * 从不等 socket：响应只入环形数组，一轮结束时每个连接发起一次写，写被阻塞的连接不影响其他连接和请求。
	 */
	awaitable<void> mailbox_loop() {
		const std::size_t shard_id = shard_comm_.current_shard_id();
		const auto executor = co_await asio::this_coro::executor;
		Doorbell &doorbell = shard_comm_.current_inbox().doorbell;
		const int wait_fd = doorbell.duplicate_event_fd();
		if (wait_fd < 0) {
			spdlog::error("[shard {}] failed to dup doorbell eventfd: {}", shard_id, std::strerror(errno));
			co_return;
		}
		asio::posix::stream_descriptor descriptor(executor, wait_fd);
#if defined(BOOST_ASIO_HAS_IO_URING_AS_DEFAULT)
		uint64_t doorbell_count = 0; // async_read 门铃的目标；在协程帧里，读在途期间一直有效
#endif
		while (!stop_->load(std::memory_order_relaxed)) {
			std::size_t handled = drain_inbox();
			if (handled == 0) {
				// 一整轮都是空的：放下门铃，再复查一遍。复查接住的是放下之前 push、看到门铃仍抬起而没敲门的消息。
				doorbell.lower();
				handled = drain_inbox();
				if (handled == 0) {
					// 睡：门铃已放下，之后第一个 push 的生产者会敲门。
					stats_.slept();
					boost::system::error_code ec;
#if defined(BOOST_ASIO_HAS_IO_URING_AS_DEFAULT)
					// 读门铃本身就把计数清零，醒来不用再 read 一次。
					co_await descriptor.async_read_some(asio::buffer(&doorbell_count, sizeof(doorbell_count)),
					                                    redirect_error(use_awaitable, ec));
#else
					co_await descriptor.async_wait(asio::posix::stream_descriptor::wait_read,
					                               redirect_error(use_awaitable, ec));
					if (!ec) {
						doorbell.clear(); // 先清 fd 再处理；放下门铃一定在清 fd 之后，所以不会吞掉之后的敲门
					}
#endif
					if (ec) {
						if (ec != asio::error::operation_aborted && !stop_->load(std::memory_order_relaxed)) {
							spdlog::error("[shard {}] doorbell wait failed: {}", shard_id, ec.message());
						}
						break;
					}
					// 被敲醒。通常门铃已由敲门的生产者抬起；但 fd 里也可能是放下门铃后、复查前留下的计数，
					// 这时门铃是放下的。醒着就该抬起，否则这一轮里生产者还会再敲。
					if (!doorbell.is_rung()) {
						doorbell.raise();
					}
					continue;
				}
				doorbell.raise();
			}
			// 有流量：让出给 socket，门铃保持抬起。defer 进本线程私有队列，不触发 reactor 的 interrupt。
			co_await asio::defer(executor, use_awaitable);
		}
	}

#if defined(FASTEST_TESTING)
	// 只给测试用：统计计数和直接访问门铃。
	[[nodiscard]] uint64_t doorbell_knocks() const noexcept {
		return outbox_.doorbell_knocks();
	}

	[[nodiscard]] uint64_t mailbox_messages() const noexcept {
		return stats_.mailbox_messages;
	}

	[[nodiscard]] uint64_t mailbox_sleeps() const noexcept {
		return stats_.mailbox_sleeps;
	}

	[[nodiscard]] uint64_t socket_writes() const noexcept {
		return sessions_.socket_writes();
	}

	[[nodiscard]] uint64_t envelopes_sent() const noexcept {
		return outbox_.envelopes_sent();
	}

	[[nodiscard]] Doorbell &doorbell(const std::size_t shard) const noexcept {
		return shard_comm_.doorbell(shard);
	}
#endif

	[[nodiscard]] std::size_t current_shard_id() const {
		return shard_comm_.current_shard_id();
	}

	// 本分片的连接表和写路径：Handler 用它登记连接、放行丢弃完的数据块、回收退出的连接。
	SessionTable &sessions() noexcept {
		return sessions_;
	}

	// key 一律移动，不复制：按分片拆开的 key 最后会移动进响应里的 ValueItem。
	void send_multi_command(MultiKeyCommand &&command, const uint64_t tcp_id) {
		TCPSession *session = sessions_.find_open(tcp_id);
		if (session == nullptr) {
			return;
		}
		std::vector<ItemKey> &item_keys = command.item_keys();
		const uint32_t seq = session->seq++;

		// 所有 key 落在同一个分片时整条命令原样转交，不拆分、不重新分配 key 数组。
		const std::size_t first_shard =
			item_keys.empty() ? current_shard_id() : shard_comm_.get_shard(item_keys[0].hash);
		const bool single_shard = std::all_of(item_keys.begin(), item_keys.end(), [&](const ItemKey &key) {
			return shard_comm_.get_shard(key.hash) == first_shard;
		});
		if (single_shard) {
			dispatch_multi_command(std::move(command), first_shard, 1, tcp_id, seq);
			return;
		}

		uint16_t counter = 0;
		try {
			for (ItemKey &key : item_keys) {
				auto &bucket = multi_key_index_[shard_comm_.get_shard(key.hash)];
				if (!bucket) {
					++counter;
					bucket.emplace();
				}
				bucket->push_back(std::move(key));
			}
			for (std::size_t i = 0; i < multi_key_index_.size(); i++) {
				if (!multi_key_index_[i]) {
					continue;
				}
				MultiKeyCommand part = rebuild(command, std::move(*multi_key_index_[i]));
				multi_key_index_[i].reset();
				dispatch_multi_command(std::move(part), i, counter, tcp_id, seq);
			}
		} catch (...) {
			// 下一条命令不能看到本次残留的 key。
			for (auto &bucket : multi_key_index_) {
				bucket.reset();
			}
			throw;
		}
	}

	void send_single_command(SingleKeyCommand command, const uint64_t tcp_id) {
		TCPSession *session = sessions_.find_open(tcp_id);
		if (session == nullptr) {
			return;
		}
		const std::size_t shard = shard_comm_.get_shard(command.item_key().hash);
		const uint32_t seq = session->seq++;
		if (shard_comm_.is_current_shard(shard)) {
			auto response = data_.handle_single_command(std::move(command));
			// 在当前分片 那么直接处理
			sessions_.deliver(ResponseMessage{.tcp_id = tcp_id,
			                                  .seq = seq,
			                                  .shard_id = static_cast<uint16_t>(current_shard_id()),
			                                  .data = ResponseData{std::move(response)}});
			return;
		}
		outbox_.post(shard, tcp_id, seq, RequestData{SingleKeyRequest{std::move(command)}});
	}

	// 过大value 需要把同名值删掉，否则旧数据会一直留着。
	void send_error_command(ErrorResponse response, const uint64_t tcp_id,
	                        std::optional<ItemKey> rejected_set_key = std::nullopt,
	                        const bool wait_for_swallow = false) {
		TCPSession *session = sessions_.find_open(tcp_id);
		if (session == nullptr) {
			return;
		}
		const uint32_t seq = session->seq++;
		if (wait_for_swallow) {
			session->swallow_seq = seq;
		}
		if (rejected_set_key) {
			const std::size_t shard = shard_comm_.get_shard(rejected_set_key->hash);
			if (!shard_comm_.is_current_shard(shard)) {
				outbox_.post(shard, tcp_id, seq,
				             RequestData{RejectedSetRequest{std::move(*rejected_set_key), std::move(response)}});
				return;
			}
			data_.erase_rejected_set(*rejected_set_key);
		}
		// 错误走同一条有序响应路径，在连接所属分片上直接入环，不经过信封。
		sessions_.deliver(ResponseMessage{.tcp_id = tcp_id,
		                                  .seq = seq,
		                                  .shard_id = static_cast<uint16_t>(current_shard_id()),
		                                  .data = ResponseData{std::move(response)}});
	}

private:
	// 执行一条别的分片发来的请求，返回它的响应。请求里的数据会被移走。
	ResponseData execute_request(RequestData &request) {
		return std::visit(
			[this](auto &c) -> ResponseData {
				using T = std::decay_t<decltype(c)>;
				if constexpr (std::is_same_v<T, SingleKeyRequest>) {
					return data_.handle_single_command(std::move(c.command));
				} else if constexpr (std::is_same_v<T, MultiKeyRequest>) {
					return ShardResponse{.count = c.count,
					                     .command = data_.handle_multi_command(std::move(c.command))};
				} else if constexpr (std::is_same_v<T, RejectedSetRequest>) {
					data_.erase_rejected_set(c.key);
					return std::move(c.response);
				}
			},
			request);
	}

	// 别的分片发来的信封：逐条执行，响应原地写回同一个位置，然后把信封推回发起分片。
	void execute_envelope(Envelope &envelope) {
		for (std::size_t i = 0; i < envelope.size(); ++i) {
			HopEntry &entry = envelope[i];
			try {
				ResponseData response = execute_request(std::get<RequestData>(entry.data));
				entry.data.emplace<ResponseData>(std::move(response));
			} catch (const std::bad_alloc &) {
				spdlog::error("[shard {}] request failed: out of memory", current_shard_id());
				fail_entry(entry, "out of memory");
			} catch (const std::exception &e) {
				spdlog::error("[shard {}] request failed: {}", current_shard_id(), e.what());
				fail_entry(entry, "internal error");
			} catch (...) {
				spdlog::error("[shard {}] request failed: unknown exception", current_shard_id());
				fail_entry(entry, "internal error");
			}
		}
		outbox_.send_back(envelope);
	}

	// 每条请求都必须有回复，否则连接的有序响应会一直等这个序号；message 必须是字面量。
	static void fail_entry(HopEntry &entry, const std::string_view message) noexcept {
		entry.data.emplace<ResponseData>(ErrorResponse{.kind = ResponseErrorKind::ServerError, .message = message});
	}

	// 自己发出去、带着响应回来的信封：响应放进各连接的环形数组（只入环，批次结束统一发起写），然后信封放回池里。
	void receive_envelope(Envelope &envelope) noexcept {
		const uint16_t from = envelope.target();
		for (std::size_t i = 0; i < envelope.size(); ++i) {
			HopEntry &entry = envelope[i];
			try {
				sessions_.deliver(ResponseMessage{.tcp_id = entry.tcp_id,
				                                  .seq = entry.seq,
				                                  .shard_id = from,
				                                  .data = std::move(std::get<ResponseData>(entry.data))});
			} catch (const std::exception &e) {
				spdlog::error("[shard {}] response handler exception: {}", current_shard_id(), e.what());
			} catch (...) {
				spdlog::error("[shard {}] response handler unknown exception", current_shard_id());
			}
		}
		outbox_.release(&envelope);
	}

	// count 是这条多 key 请求一共拆成了几个分片。
	void dispatch_multi_command(MultiKeyCommand &&command, const std::size_t shard, const uint16_t count,
	                            const uint64_t tcp_id, const uint32_t seq) {
		if (shard_comm_.is_current_shard(shard)) {
			// Handle the local shard exactly once; sending it through the
			// mailbox would duplicate the work and add avoidable latency.
			MultiKeyResponse response = data_.handle_multi_command(std::move(command));
			sessions_.deliver(ResponseMessage{
				.tcp_id = tcp_id,
				.seq = seq,
				.shard_id = static_cast<uint16_t>(current_shard_id()),
				.data = ResponseData{ShardResponse{.count = count, .command = std::move(response)}}});
			return;
		}
		outbox_.post(shard, tcp_id, seq, RequestData{MultiKeyRequest{.count = count, .command = std::move(command)}});
	}

	// 一轮：按到达顺序取空收件队列（不设上限）。别人的请求信封执行完立刻推回发起分片；
	// 遇到自己的回程信封时，先把已推回信封的门铃敲出去，再把响应放进环形数组，不让发起分片等本线程处理响应。
	// 写在一轮结束时（BatchScope 析构）统一发起。返回处理的消息数。
	std::size_t drain_inbox() {
		auto &mailbox = shard_comm_.current_inbox().mailbox;
		BatchScope batch(*this);
		std::size_t handled = 0;
		while (Envelope *envelope = mailbox.pop()) {
			handled += envelope->size();
			if (!envelope->returned()) {
				execute_envelope(*envelope);
				continue;
			}
			outbox_.ring_doorbells();
			receive_envelope(*envelope);
		}
		stats_.handled(handled);
		return handled;
	}

	Sc shard_comm_;
	ShardCache data_;
	// 跨分片发件：信封装填、信封池和门铃抑制。依赖 shard_comm_，必须声明在它之后。
	Outbox outbox_;
	// 连接表和有序写路径：响应入环、批次结束统一发起写、关闭和回收连接。
	SessionTable sessions_;
	// 长度和CPU核心数一样
	std::vector<std::optional<std::vector<ItemKey> > > multi_key_index_;
	std::shared_ptr<std::atomic_bool> stop_;

	// 统计计数，只在测试构建里存在。
	[[no_unique_address]] WorkerStats stats_;
};
} // namespace fastest