#pragma once

#include <boost/asio.hpp>
#include <spdlog/spdlog.h>
#include <algorithm>
#include <cstring>
#include <memory>
#include <optional>
#include <unordered_map>
#include <vector>

#include "ring_array.h"
#include "shard.h"
#include "type.h"
#include "../../protocol/memcached_parser.h"
#include "../../cache/shard_cache.h"

namespace fastest {
namespace asio = boost::asio;

using asio::awaitable;
using asio::use_awaitable;
using asio::ip::tcp;

struct TCPSession {
	// 背压：已分配序号、但还没交给写操作的请求数（backlog）超过高水位时暂停读，降到低水位以下再恢复。
	static constexpr uint32_t kPauseReadBacklog = 2048;
	static constexpr uint32_t kResumeReadBacklog = 256;

	// 注意：resume_read 的初始化依赖 socket，socket 必须声明在 resume_read 之前；
	//      out 引用 ring_array 已发送槽位里的数据，ring_array 需声明在 out 之前（析构时 out 先于 ring_array 销毁）。

	tcp::socket socket;
	asio::steady_timer resume_read; // 背压：当条件变量用，永不到期，cancel() 唤醒
	RingArray ring_array;           // 有序响应；[head, next_id) 同时是在途写引用的响应，写完成前不能释放
	GatherSink out;                 // 写路径：在途写的 buffer 描述和短字段 arena

	uint32_t seq{0};
	std::optional<uint32_t> swallow_seq; // 数据块丢弃完成前，不能发出该序号的错误

	bool closing{false};                // 已决定关闭：不再发送，也不再执行新命令
	bool protocol_error_waiting{false}; // 致命解析错误已入队，等待有序响应排空后关闭
	bool session_exited{false};         // session 协程已退出，允许 writer 回收对象
	bool writing{false};                // 写路径：有 async_write 在途；同一时刻最多一个，期间不能回收 session
	bool read_paused{false};            // 背压：读协程因背压在 resume_read 上等待
	bool kick_pending{false};           // 写路径：已记进 Worker::dirty_sessions_，等批次结束统一 kick_write；期间不能回收 session

	explicit TCPSession(tcp::socket &&s)
		: socket(std::move(s)), resume_read(socket.get_executor(), asio::steady_timer::time_point::max()) {
	}

	[[nodiscard]] uint32_t backlog() const noexcept {
		return seq - ring_array.next_id(); // 序号允许回绕，无符号减法即可
	}

	// 写完成（或放弃这次写）后调用：先丢掉对槽位的引用，再释放已发送的槽位。
	void release_sent() noexcept {
		out.clear();
		ring_array.release_sent();
	}
};

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
struct WorkerStats {
#if defined(FASTEST_TESTING)
	uint64_t doorbell_knocks{0};  // 本线程写 eventfd 的次数
	uint64_t mailbox_messages{0}; // 收件协程处理的消息数
	uint64_t mailbox_sleeps{0};   // 收件协程在 fd 上睡下的次数
	uint64_t socket_writes{0};    // 发起的 async_write 次数

	void knocked(const bool wrote) noexcept {
		doorbell_knocks += wrote;
	}

	void wrote() noexcept {
		++socket_writes;
	}

	void handled(const std::size_t n) noexcept {
		mailbox_messages += n;
	}

	void slept() noexcept {
		++mailbox_sleeps;
	}
#else
	void knocked(bool) noexcept {
	}

	void wrote() noexcept {
	}

	void handled(std::size_t) noexcept {
	}

	void slept() noexcept {
	}
#endif
};

class Worker {
public:
	std::unordered_map<uint64_t, std::unique_ptr<TCPSession> > tcp_session_;
	using Sc = ShardComm<RequestMessage, ResponseMessage>;

	explicit Worker(Sc shard_comm, std::shared_ptr<std::atomic_bool> stop)
		: shard_comm_(std::move(shard_comm)), data_(static_cast<uint16_t>(shard_comm_.current_shard_id())),
		  multi_key_index_(shard_comm_.shard_num()), stop_(std::move(stop)),
		  doorbell_pending_mark_(shard_comm_.shard_num(), 0) {
		doorbell_pending_.reserve(shard_comm_.shard_num());
	}

	/*
	 * 批次作用域：合并敲门和写。
	 * 作用域内发出的请求/响应只入队、记下目标分片；响应入环只记下连接，不发起写。离开作用域时先给每个目标分片敲一次门铃
	 * （让对端尽早开始处理），再给每个连接 kick_write 一次，一次写带上这一批里该连接所有就绪的响应。
	 * 一个作用域里攒到 kDoorbellFlushEvery 条消息也先敲一次门铃，避免对端在睡、长时间听不到。
	 * 作用域内不能 co_await：挂起前必须把门铃敲出去、把写发起，否则对端可能一直睡着，响应也一直不发。
	 * 所有跨分片发送和响应入环都必须在作用域内：解析批次（Handler::session）和收件协程的一轮（drain_inbox）。
	 * 作用域外发出的消息没人敲门，入环的响应要等下一个作用域结束才写。嵌套时内层退出也会敲门、发起写，只是提前，不影响正确性。
	 */
	class BatchScope {
	public:
		explicit BatchScope(Worker &worker) noexcept : worker_(worker) {
		}

		~BatchScope() {
			worker_.ring_doorbells();
			worker_.kick_dirty_sessions();
		}

		BatchScope(const BatchScope &) = delete;

		BatchScope &operator=(const BatchScope &) = delete;

	private:
		Worker &worker_;
	};

	/*
	 * 本分片唯一的收件协程：请求、响应两个 mailbox 共用一个门铃。
	 *
	 * 醒着时门铃保持抬起，生产者 push 后只读一下标志就返回，不写 eventfd。每轮把两个 mailbox 都清空（不设上限），
	 * 这一轮处理过消息就让出 CPU 给 socket，稍后再来一轮；某一轮两个 mailbox 都是空的，才放下门铃、
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
		while (!stop_->load(std::memory_order_relaxed)) {
			//核心代码 消费俩个mailbox
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
					co_await descriptor.async_read_some(asio::buffer(&doorbell_count_, sizeof(doorbell_count_)),
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
		return stats_.doorbell_knocks;
	}

	[[nodiscard]] uint64_t mailbox_messages() const noexcept {
		return stats_.mailbox_messages;
	}

	[[nodiscard]] uint64_t mailbox_sleeps() const noexcept {
		return stats_.mailbox_sleeps;
	}

	[[nodiscard]] uint64_t socket_writes() const noexcept {
		return stats_.socket_writes;
	}

	[[nodiscard]] Doorbell &doorbell(const std::size_t shard) const noexcept {
		return shard_comm_.doorbell(shard);
	}
#endif

	[[nodiscard]] std::size_t current_shard_id() const {
		return shard_comm_.current_shard_id();
	}

	// key 一律移动，不复制：按分片拆开的 key 最后会移动进响应里的 ValueItem。
	void send_multi_command(MultiKeyCommand &&command, const uint64_t tcp_id) {
		auto session_it = tcp_session_.find(tcp_id);
		if (session_it == tcp_session_.end() || !session_it->second || session_it->second->closing) {
			return;
		}
		std::vector<ItemKey> &item_keys = command.item_keys();
		const uint32_t seq = session_it->second->seq++;

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
		auto session_it = tcp_session_.find(tcp_id);
		if (session_it == tcp_session_.end() || !session_it->second || session_it->second->closing) {
			return;
		}
		const std::size_t shard = shard_comm_.get_shard(command.item_key().hash);
		const uint32_t seq = session_it->second->seq++;
		if (shard_comm_.is_current_shard(shard)) {
			auto response = data_.handle_single_command(std::move(command));
			// 在当前分片 那么直接处理
			handle_response_message(ResponseMessage{.tcp_id = tcp_id,
			                                        .seq = seq,
			                                        .shard_id = static_cast<uint16_t>(current_shard_id()),
			                                        .data = ResponseData{std::move(response)}});
			return;
		}
		RequestMessage request_message = {.tcp_id = tcp_id,
		                                  .seq = seq,
		                                  .shard_id = static_cast<uint16_t>(current_shard_id()),
		                                  .data = RequestData{SingleKeyRequest{std::move(command)}}};
		post_request(shard, std::move(request_message));
	}

	// 过大value 需要把同名值删掉，否则旧数据会一直留着。
	void send_error_command(ErrorResponse response, const uint64_t tcp_id,
	                        std::optional<ItemKey> rejected_set_key = std::nullopt,
	                        const bool wait_for_swallow = false) {
		const auto session_it = tcp_session_.find(tcp_id);
		if (session_it == tcp_session_.end() || !session_it->second || session_it->second->closing) {
			return;
		}
		const uint32_t seq = session_it->second->seq++;
		if (wait_for_swallow) {
			session_it->second->swallow_seq = seq;
		}
		if (rejected_set_key) {
			const std::size_t shard = shard_comm_.get_shard(rejected_set_key->hash);
			if (!shard_comm_.is_current_shard(shard)) {
				RequestMessage request{.tcp_id = tcp_id,
				                       .seq = seq,
				                       .shard_id = static_cast<uint16_t>(current_shard_id()),
				                       .data = RejectedSetRequest{std::move(*rejected_set_key), std::move(response)}};
				post_request(shard, std::move(request));
				return;
			}
			data_.erase_rejected_set(*rejected_set_key);
		}
		// Errors use the same ordered response path, without a mailbox hop or
		// a heap-allocated message on the connection's owning shard.
		handle_response_message(ResponseMessage{.tcp_id = tcp_id,
		                                        .seq = seq,
		                                        .shard_id = static_cast<uint16_t>(current_shard_id()),
		                                        .data = ResponseData{std::move(response)}});
	}

	void finish_swallow(const uint64_t tcp_id) {
		auto it = tcp_session_.find(tcp_id);
		if (it == tcp_session_.end() || !it->second || it->second->closing) {
			return;
		}
		it->second->swallow_seq.reset();
		kick_write(tcp_id, *it->second);
	}

	// session 协程已退出、没有写在途、也不在本批待写列表里时才回收；致命解析错误要等有序响应发完（closing）才回收。
	// 调用后 session 可能已被销毁，调用方不能再访问它。
	void reclaim_session_if_idle(const uint64_t tcp_id, const TCPSession &session) {
		if (session.session_exited && !session.writing && !session.kick_pending &&
		    (session.closing || !session.protocol_error_waiting)) {
			tcp_session_.erase(tcp_id);
		}
	}

private:
	void handle_request_message(const std::unique_ptr<RequestMessage> &message) {
		auto &cmd = message->data;
		ResponseData res = std::visit(
			[&, this](auto &c) -> ResponseData {
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
			cmd);
		// 响应发回请求的发送方；响应里的 shard_id 填自己（响应的发送方）
		ResponseMessage response_message = {.tcp_id = message->tcp_id,
		                                    .seq = message->seq,
		                                    .shard_id = static_cast<uint16_t>(shard_comm_.current_shard_id()),
		                                    .data = std::move(res)};
		post_response(message->shard_id, std::move(response_message));
	}

	// count 是这条多 key 请求一共拆成了几个分片。
	void dispatch_multi_command(MultiKeyCommand &&command, const std::size_t shard, const uint16_t count,
	                            const uint64_t tcp_id, const uint32_t seq) {
		if (shard_comm_.is_current_shard(shard)) {
			// Handle the local shard exactly once; sending it through the
			// mailbox would duplicate the work and add avoidable latency.
			MultiKeyResponse response = data_.handle_multi_command(std::move(command));
			handle_response_message(ResponseMessage{
				.tcp_id = tcp_id,
				.seq = seq,
				.shard_id = static_cast<uint16_t>(current_shard_id()),
				.data = ResponseData{ShardResponse{.count = count, .command = std::move(response)}}});
			return;
		}
		RequestMessage request_message = {
			.tcp_id = tcp_id,
			.seq = seq,
			.shard_id = static_cast<uint16_t>(current_shard_id()),
			.data = RequestData{MultiKeyRequest{.count = count, .command = std::move(command)}}};
		post_request(shard, std::move(request_message));
	}

	// ---- 门铃 ----
	// 一个作用域里攒到这么多条消息就先敲一次已攒下的门铃，不中断消费，只限制对端听到门铃前的延迟。
	static constexpr uint32_t kDoorbellFlushEvery = 64;

	void post_request(const std::size_t shard, RequestMessage &&message) {
		shard_comm_.push_request(shard, std::make_unique<RequestMessage>(std::move(message)));
		note_doorbell(shard);
	}

	void post_response(const std::size_t shard, ResponseMessage &&message) {
		shard_comm_.push_response(shard, std::make_unique<ResponseMessage>(std::move(message)));
		note_doorbell(shard);
	}

	// 只记下目标分片，由 BatchScope 退出时（或攒满 kDoorbellFlushEvery 条时）统一敲门。
	void note_doorbell(const std::size_t shard) {
		if (!doorbell_pending_mark_[shard]) {
			doorbell_pending_mark_[shard] = 1;
			doorbell_pending_.push_back(static_cast<uint16_t>(shard)); // 预留了 shard_num 个，不会扩容
		}
		if (++doorbell_pending_messages_ >= kDoorbellFlushEvery) {
			ring_doorbells();
		}
	}

	// 一次 fence，然后每个目标分片敲一次；对端醒着时只是一次只读的标志检查。
	void ring_doorbells() noexcept {
		doorbell_pending_messages_ = 0;
		if (doorbell_pending_.empty()) {
			return;
		}
		Doorbell::fence();
		for (const uint16_t shard : doorbell_pending_) {
			stats_.knocked(shard_comm_.doorbell(shard).knock());
			doorbell_pending_mark_[shard] = 0;
		}
		doorbell_pending_.clear();
	}

	// 一轮：先清请求并把响应的门铃敲出去，再清响应（只入环）；响应处理期间又来了请求就立刻再清一遍，
	// 不让别的分片发来的请求排到本线程所有 socket 后面。一轮结束时（BatchScope 析构）才统一发起写。返回处理的消息数。
	std::size_t drain_inbox() {
		auto &inbox = shard_comm_.current_inbox();
		BatchScope batch(*this);
		std::size_t handled = drain_requests(inbox.requests);
		ring_doorbells();
		const std::size_t responses = drain_responses(inbox.responses);
		handled += responses;
		if (responses != 0) {
			handled += drain_requests(inbox.requests);
		}
		stats_.handled(handled);
		return handled;
	}

	std::size_t drain_requests(Mailbox<RequestMessage> &mailbox) {
		std::size_t handled = 0;
		std::unique_ptr<RequestMessage> message;
		while (mailbox.pop(message)) {
			++handled;
			try {
				handle_request_message(message);
			} catch (const std::exception &e) {
				spdlog::error("[shard {}] mailbox handler exception: {}", current_shard_id(), e.what());
			} catch (...) {
				spdlog::error("[shard {}] mailbox handler unknown exception", current_shard_id());
			}
			message.reset();
		}
		return handled;
	}

	std::size_t drain_responses(Mailbox<ResponseMessage> &mailbox) {
		std::size_t handled = 0;
		std::unique_ptr<ResponseMessage> message;
		while (mailbox.pop(message)) {
			++handled;
			try {
				handle_response_message(std::move(*message)); // 只入环，一轮结束时统一发起写
			} catch (const std::exception &e) {
				spdlog::error("[shard {}] mailbox handler exception: {}", current_shard_id(), e.what());
			} catch (...) {
				spdlog::error("[shard {}] mailbox handler unknown exception", current_shard_id());
			}
			message.reset();
		}
		return handled;
	}

	// ---- Worker private ----
	// 一批最多编码这么多就发起写；超大 value 只引用不复制，这里限制的是单次写的时长和 iovec 数量。
	static constexpr std::size_t kMaxBatchBytes = 1 << 20;
	static constexpr std::size_t kMaxBatchResponses = 256;

	void handle_response_message(ResponseMessage message) {
		const uint64_t tcp_id = message.tcp_id;
		auto it = tcp_session_.find(tcp_id);
		if (it == tcp_session_.end() || !it->second)
			return;
		auto &session = *it->second;
		if (session.closing)
			return;                                    // 丢弃迟到响应
		session.ring_array.append(std::move(message)); // 顺便省一次拷贝
		// 不在这里发起写：记下连接，批次结束时统一 kick_write，一次写带上这一批所有就绪的响应。
		// 写在途时不用记，写完成回调会把期间就绪的响应作为下一批发出。
		if (!session.writing && !session.kick_pending) {
			session.kick_pending = true;
			dirty_sessions_.emplace_back(tcp_id, &session);
		}
	}

	// 批次结束（BatchScope 析构）时调用：本批入过环的连接各 kick_write 一次。
	// kick_write 可能回收当前 session；列表里其他 session 的 kick_pending 仍为 true，不会被回收，指针一直有效。
	void kick_dirty_sessions() noexcept {
		for (std::size_t i = 0; i < dirty_sessions_.size(); ++i) {
			const auto [tcp_id, session] = dirty_sessions_[i];
			session->kick_pending = false;
			try {
				if (session->closing) {
					// 防御：作用域内被关闭的连接，kick_write 会直接返回，这里补上回收。
					reclaim_session_if_idle(tcp_id, *session);
					continue;
				}
				kick_write(tcp_id, *session);
			} catch (const std::exception &e) {
				spdlog::error("[shard {}] kick write failed: {}", current_shard_id(), e.what());
				close_session(tcp_id, *session);
			}
		}
		dirty_sessions_.clear();
	}

	// 只有当前的线程会调用这个方法，因为能放进ring的只有连接所属的线程。分别是当前分片命中和跨分片响应经 mailbox_loop 放入的。
	// 写路径只入队、不等待：由批次结束（kick_dirty_sessions）、写完成回调和 finish_swallow 调用。
	// 同一连接同一时刻最多一个 async_write 在途；在途期间新就绪的响应留在环形数组，写完成后作为下一批一起发出。
	// 调用后 session 可能已被回收，调用方不能再访问它。
	void kick_write(const uint64_t tcp_id, TCPSession &session) {
		if (session.writing || session.closing) {
			return;
		}
		bool close_after = false;
		try {
			while (session.out.size_bytes() < kMaxBatchBytes &&
			       session.ring_array.sent_count() < kMaxBatchResponses) {
				// memcached queues an oversized object's error while swallowing its
				// data block. Earlier ready responses may still leave in order.
				if (session.swallow_seq && *session.swallow_seq == session.ring_array.next_id()) {
					break;
				}
				// 直接从槽位编码，不搬出：编码引用的数据留在槽里，写完成后 release_sent() 才释放。
				const RingResponseMessage *message = session.ring_array.front();
				if (!message) {
					break;
				}
				close_after = encode_response(session.out, message->data);
				session.ring_array.advance();
				if (close_after) {
					break; // Quit / 致命错误：之后的响应不再发送
				}
			}
		} catch (const std::exception &e) {
			spdlog::error("[shard {}] response batch failed: {}", current_shard_id(), e.what());
			session.release_sent();
			close_session(tcp_id, session);
			return;
		}

		if (session.out.empty()) {
			// 没有就绪的响应，或者本批只有 noreply 这类不产生字节的响应。
			session.release_sent();
			if (close_after) {
				close_session(tcp_id, session);
				return;
			}
			maybe_resume_read(session);
			reclaim_session_if_idle(tcp_id, session);
			return;
		}

		session.writing = true;
		stats_.wrote();
		maybe_resume_read(session);
		// 把buffer里的数据异步写完，然后完成回调。
		asio::async_write(session.socket, session.out.buffers(),
		                  [this, tcp_id, close_after](const boost::system::error_code &ec, std::size_t) {
			                  on_write_complete(tcp_id, ec, close_after);
		                  });
	}

	void on_write_complete(const uint64_t tcp_id, const boost::system::error_code &ec, const bool close_after) {
		const auto it = tcp_session_.find(tcp_id);
		if (it == tcp_session_.end() || !it->second) {
			return; // writing 期间不会回收 session，正常不会走到这里
		}
		TCPSession &session = *it->second;
		session.writing = false;
		session.release_sent();
		if (ec) {
			if (ec != asio::error::broken_pipe && ec != asio::error::connection_reset && ec != asio::error::eof &&
			    ec != asio::error::bad_descriptor && ec != asio::error::operation_aborted) {
				spdlog::warn("[shard {}] send failed: {}", current_shard_id(), ec.message());
			}
			close_session(tcp_id, session);
			return;
		}
		if (close_after || session.closing) {
			close_session(tcp_id, session);
			return;
		}
		kick_write(tcp_id, session); // 写的期间新就绪的响应，作为下一批发出
	}

	// 编码一条响应；字段非法时撤销这条已追加的部分，改发 SERVER_ERROR 占住这个 seq。
	bool encode_response(GatherSink &out, const RingResponseData &data) const {
		const GatherSink::Mark mark = out.mark();
		try {
			return std::visit(
				[&out](const auto &value) -> bool {
					using T = std::remove_cvref_t<decltype(value)>;
					if constexpr (std::is_same_v<T, std::vector<ShardResponse> >) {
						// 多分片：按到达顺序依次输出各分片的 items，不先合并。
						return encode_multi_parts(
							out, value, [](const ShardResponse &part) -> const MultiKeyResponse & {
								return part.command;
							});
					} else {
						return encode_to(out, value);
					}
				},
				data);
		} catch (const std::logic_error &e) {
			// invalid_argument 是它的子类
			spdlog::error("[shard {}] encode failed: {}", current_shard_id(), e.what());
			out.rollback(mark);
			const ErrorResponse err{.kind = ResponseErrorKind::ServerError, .message = "response encoding failed"};
			return encode_to(out, err);
		}
	}

	static void maybe_resume_read(TCPSession &session) {
		if (session.read_paused && session.backlog() <= TCPSession::kResumeReadBacklog) {
			session.resume_read.cancel();
		}
	}

	// 可重复调用；调用后 session 可能已被回收，调用方不能再访问它。
	void close_session(const uint64_t tcp_id, TCPSession &session) {
		if (!session.closing) {
			session.closing = true;
			boost::system::error_code ec;
			session.socket.shutdown(tcp::socket::shutdown_both, ec);
			session.socket.close(ec);     // 唤醒 session() 中的 async_read_some，并取消在途写
			session.resume_read.cancel(); // 唤醒因背压暂停的读协程
		}
		// 写在途时不回收：取消也会回调 on_write_complete，届时再回收；在那之前 ring_array 的已发送槽位仍可能被引用。
		reclaim_session_if_idle(tcp_id, session);
	}

	Sc shard_comm_;
	ShardCache data_;
	// 长度和CPU核心数一样
	std::vector<std::optional<std::vector<ItemKey> > > multi_key_index_;
	std::shared_ptr<std::atomic_bool> stop_;

	// 门铃（都只由本线程访问，不需要 atomic）：待敲门的目标分片和去重标记，长度都是分片数。
	std::vector<uint16_t> doorbell_pending_;
	//每个分片一个字节的去重标记。同一个分片在一批里只进一次 doorbell_pending_，敲完门铃后清零。用 uint8_t 而不是 vector<bool>，判断时不用做位运算。
	std::vector<uint8_t> doorbell_pending_mark_;
	//上次敲门铃之后又攒了多少条消息。到 64 条就先把攒下的门铃敲出去，也就是你确认的「每 64 条顺手敲一次」，不中断消费。
	uint32_t doorbell_pending_messages_{0};
	uint64_t doorbell_count_{0}; // 只在io_uring下使用io_uring：async_read 门铃的目标
	// 本批入过环、等批次结束统一 kick_write 的连接（会话的 kick_pending 去重）。只由本线程访问；批次之间总是空的。
	std::vector<std::pair<uint64_t, TCPSession *> > dirty_sessions_;

	// 统计计数，只在测试构建里存在。
	[[no_unique_address]] WorkerStats stats_;
};
} // namespace fastest