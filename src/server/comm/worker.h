#pragma once

#include <boost/asio.hpp>
#include <spdlog/spdlog.h>
#include <cstring>
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
	static constexpr uint32_t kPauseReadBacklog = 1024;
	static constexpr uint32_t kResumeReadBacklog = 256;

	// 注意：resume_read 的初始化依赖 socket，socket 必须声明在 resume_read 之前；
	//      out 引用 inflight 中的数据，inflight 需声明在 out 之前（析构时 out 先于 inflight 销毁）。


	tcp::socket socket;
	asio::steady_timer resume_read;       // 背压：当条件变量用，永不到期，cancel() 唤醒
	RingArray ring_array;
	std::vector<ResponseMessage> inflight; // 写路径：在途写引用的响应，写完成前不能释放
	GatherSink out;                        // 写路径：在途写的 buffer 描述和短字段 arena

	uint32_t seq{0};
	std::optional<uint32_t> swallow_seq; // 数据块丢弃完成前，不能发出该序号的错误

	bool closing{false};                // 已决定关闭：不再发送，也不再执行新命令
	bool protocol_error_waiting{false}; // 致命解析错误已入队，等待有序响应排空后关闭
	bool session_exited{false};         // session 协程已退出，允许 writer 回收对象
	bool writing{false};                // 写路径：有 async_write 在途；同一时刻最多一个，期间不能回收 session
	bool read_paused{false};            // 背压：读协程因背压在 resume_read 上等待

	explicit TCPSession(tcp::socket &&s)
		: socket(std::move(s)), resume_read(socket.get_executor(), asio::steady_timer::time_point::max()) {
	}

	[[nodiscard]] uint32_t backlog() const noexcept {
		return seq - ring_array.next_id(); // 序号允许回绕，无符号减法即可
	}

	void release_inflight() noexcept {
		inflight.clear();
		out.clear();
		if (inflight.capacity() > 64) {
			std::vector<ResponseMessage>().swap(inflight);
		}
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
				return MultiKeyCommand{MultiGatCommand{value.exptime, std::move(keys)}};
			} else if constexpr (std::is_same_v<T, MultiGatsCommand>) {
				return MultiKeyCommand{MultiGatsCommand{value.exptime, std::move(keys)}};
			}
		},
		command.command);
}

class Worker {
public:
	std::unordered_map<uint64_t, std::unique_ptr<TCPSession> > tcp_session_;
	using Sc = ShardComm<RequestMessage, ResponseMessage>;

	explicit Worker(Sc shard_comm, std::shared_ptr<std::atomic_bool> stop)
		: shard_comm_(std::move(shard_comm)), multi_key_index_(shard_comm_.shard_num()), stop_(std::move(stop)) {
	}

	// 接收他人请求
	awaitable<void> mailbox_request_loop() {
		std::size_t shard_id = shard_comm_.current_shard_id();
		const auto executor = co_await asio::this_coro::executor;
		Mailbox<RequestMessage> &mailbox = shard_comm_.current_request_mailbox();
		const auto wait_fd = mailbox.duplicate_event_fd();
		if (wait_fd < 0) {
			spdlog::error("[shard {}] failed to dup mailbox eventfd: {}", shard_id, std::strerror(errno));
			co_return;
		}
		asio::posix::stream_descriptor descriptor(executor, wait_fd);
		while (!stop_->load(std::memory_order_relaxed)) {
			boost::system::error_code ec;
			co_await descriptor.async_wait(asio::posix::stream_descriptor::wait_read,
			                               redirect_error(use_awaitable, ec));
			if (ec) {
				if (ec != asio::error::operation_aborted && !stop_->load(std::memory_order_relaxed)) {
					spdlog::error("[shard {}] mailbox wait failed: {}", shard_id, ec.message());
				}
				break;
			}
			mailbox.clear_notify();
			std::unique_ptr<RequestMessage> message;
			// 一次可以消费掉多个元素
			while (mailbox.pop(message)) {
				try {
					handle_request_message(message);
				} catch (const std::exception &e) {
					spdlog::error("[shard {}] mailbox handler exception: {}", shard_id, e.what());
				} catch (...) {
					spdlog::error("[shard {}] mailbox handler unknown exception", shard_id);
				}
				message.reset();
			}
		}
	}

	// 接收他人响应
	awaitable<void> mailbox_response_loop() {
		std::size_t shard_id = shard_comm_.current_shard_id();
		const auto executor = co_await asio::this_coro::executor;
		Mailbox<ResponseMessage> &mailbox = shard_comm_.current_response_mailbox();
		const auto wait_fd = mailbox.duplicate_event_fd();
		if (wait_fd < 0) {
			spdlog::error("[shard {}] failed to dup mailbox eventfd: {}", shard_id, std::strerror(errno));
			co_return;
		}
		asio::posix::stream_descriptor descriptor(executor, wait_fd);
		while (!stop_->load(std::memory_order_relaxed)) {
			boost::system::error_code ec;
			co_await descriptor.async_wait(asio::posix::stream_descriptor::wait_read,
			                               redirect_error(use_awaitable, ec));
			if (ec) {
				if (ec != asio::error::operation_aborted && !stop_->load(std::memory_order_relaxed)) {
					spdlog::error("[shard {}] mailbox wait failed: {}", shard_id, ec.message());
				}
				break;
			}
			mailbox.clear_notify();
			std::unique_ptr<ResponseMessage> message;
			// 一次可以消费掉多个元素
			while (mailbox.pop(message)) {
				try {
					handle_response_message(std::move(*message)); // 只入队并发起写，不等 socket
				} catch (const std::exception &e) {
					spdlog::error("[shard {}] mailbox handler exception: {}", shard_id, e.what());
				} catch (...) {
					spdlog::error("[shard {}] mailbox handler unknown exception", shard_id);
				}
				message.reset();
			}
		}
	}

	[[nodiscard]] std::size_t current_shard_id() const {
		return shard_comm_.current_shard_id();
	}

	void send_multi_command(const MultiKeyCommand &command, const uint64_t tcp_id) {
		auto session_it = tcp_session_.find(tcp_id);
		if (session_it == tcp_session_.end() || !session_it->second || session_it->second->closing) {
			return;
		}
		const std::vector<ItemKey> &item_keys = command.item_keys();
		uint16_t counter = 0;
		for (size_t i = 0; i < item_keys.size(); i++) {
			std::size_t shard = shard_comm_.get_shard(item_keys[i].hash);
			if (!multi_key_index_[shard].has_value()) {
				counter++;
				multi_key_index_[shard] = std::vector<ItemKey>{item_keys[i]};
			} else {
				multi_key_index_[shard].value().emplace_back(item_keys[i]);
			}
		}
		const uint32_t seq = session_it->second->seq++;

		for (size_t i = 0; i < multi_key_index_.size(); i++) {
			if (!multi_key_index_[i].has_value()) {
				continue;
			}
			MultiKeyCommand multi_key_command = rebuild(command, std::move(*multi_key_index_[i]));
			multi_key_index_[i].reset();

			if (shard_comm_.is_current_shard(i)) {
				// Handle the local shard exactly once; sending it through the
				// mailbox would duplicate the work and add avoidable latency.
				MultiKeyResponse response = data_.handle_multi_command(multi_key_command);
				ResponseMessage response_message = {
					.tcp_id = tcp_id,
					.seq = seq,
					.shard_id = static_cast<uint16_t>(shard_comm_.current_shard_id()),
					.data = ResponseData{ShardResponse{.count = counter, .command = std::move(response)}}};
				handle_response_message(std::move(response_message));
				continue;
			}

			MultiKeyRequest request = {.count = counter, .command = std::move(multi_key_command)};
			RequestMessage request_message = {.tcp_id = tcp_id,
			                                  .seq = seq,
			                                  .shard_id = static_cast<uint16_t>(shard_comm_.current_shard_id()),
			                                  .data = RequestData{std::move(request)}};
			shard_comm_.send_request_to_shard(i, std::make_unique<RequestMessage>(std::move(request_message)));
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
			//在当前分片 那么直接处理
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
		shard_comm_.send_request_to_shard(shard, std::make_unique<RequestMessage>(std::move(request_message)));
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
				shard_comm_.send_request_to_shard(shard, std::make_unique<RequestMessage>(std::move(request)));
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

	// session 协程已退出、且没有写在途时才回收；致命解析错误要等有序响应发完（closing）才回收。
	// 调用后 session 可能已被销毁，调用方不能再访问它。
	void reclaim_session_if_idle(const uint64_t tcp_id, const TCPSession &session) {
		if (session.session_exited && !session.writing && (session.closing || !session.protocol_error_waiting)) {
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
					return ShardResponse{.count = c.count, .command = data_.handle_multi_command(c.command)};
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
		shard_comm_.send_response_shard(message->shard_id,
		                                std::make_unique<ResponseMessage>(std::move(response_message)));
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
		kick_write(tcp_id, session);
	}

	// 只有当前的线程会调用这个方法。
	// 写路径只入队、不等待：本分片 inline 路径、mailbox、错误和 finish_swallow 都调用这里。
	// 同一连接同一时刻最多一个 async_write 在途；在途期间新就绪的响应留在环形数组，写完成后作为下一批一起发出。
	// 调用后 session 可能已被回收，调用方不能再访问它。
	void kick_write(const uint64_t tcp_id, TCPSession &session) {
		if (session.writing || session.closing) {
			return;
		}
		bool close_after = false;
		try {
			while (session.out.size_bytes() < kMaxBatchBytes && session.inflight.size() < kMaxBatchResponses) {
				// memcached queues an oversized object's error while swallowing its
				// data block. Earlier ready responses may still leave in order.
				if (session.swallow_seq && *session.swallow_seq == session.ring_array.next_id()) {
					break;
				}
				auto popped = session.ring_array.try_pop();
				if (!popped) {
					break;
				}
				// 先放进 inflight 再编码：编码引用的是 inflight 里的字符串，它们要活到这次写完成。
				const ResponseMessage &message = session.inflight.emplace_back(std::move(*popped));
				close_after = encode_response(session.out, message.data);
				if (close_after) {
					break; // Quit / 致命错误：之后的响应不再发送
				}
			}
		} catch (const std::exception &e) {
			spdlog::error("[shard {}] response batch failed: {}", current_shard_id(), e.what());
			session.release_inflight();
			close_session(tcp_id, session);
			return;
		}

		if (session.out.empty()) {
			// 没有就绪的响应，或者本批只有 noreply 这类不产生字节的响应。
			session.release_inflight();
			if (close_after) {
				close_session(tcp_id, session);
				return;
			}
			maybe_resume_read(session);
			reclaim_session_if_idle(tcp_id, session);
			return;
		}

		session.writing = true;
		maybe_resume_read(session);
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
		session.release_inflight();
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
	bool encode_response(GatherSink &out, const ResponseData &data) const {
		const GatherSink::Mark mark = out.mark();
		try {
			return std::visit(
				[&out](const auto &value) -> bool {
					using T = std::remove_cvref_t<decltype(value)>;
					if constexpr (std::is_same_v<T, ShardResponse>) {
						return encode_to(out, value.command);
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
		// 写在途时不回收：取消也会回调 on_write_complete，届时再回收；在那之前 inflight 仍可能被引用。
		reclaim_session_if_idle(tcp_id, session);
	}

	Sc shard_comm_;
	ShardCache data_;
	// 长度和CPU核心数一样
	std::vector<std::optional<std::vector<ItemKey> > > multi_key_index_;
	std::shared_ptr<std::atomic_bool> stop_;
};
} // namespace fastest