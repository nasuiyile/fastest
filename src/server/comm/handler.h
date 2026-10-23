#pragma once

#include "config.h"

#include <thread>
#include <algorithm>
#include <utility>
#include <boost/asio.hpp>
#include <spdlog/spdlog.h>
#include <atomic>
#include <sys/socket.h>
#include "input_buffer.h"
#include "worker.h"
#include "type.h"
#include "../../util/hardware_info.h"
#include "../../protocol/memcached_parser.h"

namespace fastest {
using asio::awaitable;
using asio::use_awaitable;
using asio::ip::tcp;

class Handler {
public:
	void start() {
		thread_ = std::thread([this] { run(); });
	}

	static std::vector<std::unique_ptr<Handler>> create_handlers(const Config &cfg) {
		const std::size_t core_num = util::core_num();
		const auto stop = std::make_shared<std::atomic_bool>(false);
		auto shard_comms = Worker::Sc::create_shard_comm(core_num);
		std::vector<std::unique_ptr<Handler>> handlers;
		handlers.reserve(shard_comms.size());
		for (auto &comm : shard_comms) {
			handlers.push_back(std::unique_ptr<Handler>(new Handler(cfg, std::move(comm), stop)));
		}
		return handlers;
	}

	void join() {
		if (thread_.joinable()) {
			thread_.join();
		}
	}

private:
	// Construct the nonmovable Worker directly at its final address.
	explicit Handler(Config config, Worker::Sc comm, std::shared_ptr<std::atomic_bool> stop)
	    : config_(std::move(config)), worker_(std::move(comm), stop), stop_(std::move(stop)) {
	}

	static constexpr std::size_t kReadChunkBytes = 4096;

	bool handle_parse_error(const uint64_t tcp_id, ParseError &error, const bool wait_for_swallow) {
		worker_.send_error_command(to_error_response(error), tcp_id, std::move(error.rejected_set_key),
		                           wait_for_swallow);
		return error.recovery == ErrorRecovery::Continue;
	}

	void execute_batch(std::vector<Command> &commands, const uint64_t tcp_id) {
		// Commands own all data that may outlive this parse call. Referencing the
		// vector avoids copying multi-key command vectors on the hot path.
		for (Command &command : commands) {
			std::visit(
			    [this, tcp_id](auto &arg) {
				    using T = std::decay_t<decltype(arg)>;
				    if constexpr (std::is_same_v<T, SingleKeyCommand>) {
					    worker_.send_single_command(std::move(arg), tcp_id);
				    } else if constexpr (std::is_same_v<T, MultiKeyCommand>) {
				    	worker_.send_multi_command(std::move(arg), tcp_id);
				    } else if constexpr (std::is_same_v<T, NoKeyCommand>) {
					    // No-key commands are not routed through key shards.
				    }
			    },
			    command);
		}
	};

	awaitable<void> session(const uint64_t tcp_id) {
		TCPSession *const found = worker_.sessions().find(tcp_id);
		if (found == nullptr) {
			co_return;
		}
		TCPSession &tcp_session = *found;
		tcp::socket &socket = tcp_session.socket;
		std::size_t shard_id = worker_.current_shard_id();
		try {
			const MemcachedParser parser;
			// 保留上一次没有解析完的数据；socket 直接读进它的尾部，不经过中转 buffer。
			InputBuffer input;
			// 需要丢弃的字节
			std::size_t discard_remaining = 0;
			while (!tcp_session.closing && !stop_->load(std::memory_order_relaxed)) {
				if (discard_remaining != 0) {
					const std::size_t dropped = std::min(discard_remaining, input.size());
					input.consume(dropped);
					discard_remaining -= dropped;
					if (discard_remaining == 0) {
						worker_.sessions().finish_swallow(tcp_id);
						continue;
					}
					// 缓冲区已空：读一块，下一轮循环再丢弃。
					const std::span<char> buffer = input.prepare(kReadChunkBytes);
					input.commit(co_await socket.async_read_some(asio::buffer(buffer.data(), buffer.size()),
					                                             use_awaitable));
					continue;
				}

				// 背压：读和写互不等待，客户端只发不读时积压会一直增长；超过高水位先暂停解析和读取，
				// 等写回调把积压降到低水位以下。丢弃数据块的路径不受影响，否则 swallow 永远完不成。
				if (tcp_session.backlog() >= TCPSession::kPauseReadBacklog) {
					co_await wait_for_backlog(tcp_session);
					continue;
				}

				auto result = parser.parse(input.readable(), 128);
				bool keep_connection = true;
				{
					// 这一批发往同一分片的请求只敲一次门铃，同一连接的响应只发起一次写；作用域内没有 co_await，离开时敲门、发起写。
					Worker::BatchScope batch(worker_);
					// 先执行成功解析的命令，确保响应顺序正确
					execute_batch(result.commands, tcp_id);
					if (result.stop == StopReason::Error) {
						keep_connection = handle_parse_error(tcp_id, *result.error, result.discard_bytes != 0);
					}
				}
				// 解析结果已投递后才能消费未解析输入
				input.consume(result.consumed);
				discard_remaining = result.discard_bytes;
				if (!keep_connection) {
					// The error response already occupies its sequence number. Stop
					// reading, but let the worker drain earlier responses first; the
					// ordered response path then closes the socket after this error.
					tcp_session.protocol_error_waiting = true;
					break;
				}
				if (result.stop == StopReason::PendingValue) {
					// 大块数据直接从 socket 读进最终的 CacheValue，不经过 input，也不撑大 input。
					// 前面的命令已执行；之后的命令在这块数据后面才到达，顺序不变。
					PendingValue &pending = *result.pending;
					const std::span<std::byte> rest = pending.unfilled();
					co_await asio::async_read(socket, asio::buffer(rest.data(), rest.size()), use_awaitable);
					pending.filled = pending.frame.size();
					auto completed = MemcachedParser::complete(std::move(pending));
					{
						Worker::BatchScope batch(worker_);
						execute_batch(completed.commands, tcp_id);
						if (completed.error) {
							handle_parse_error(tcp_id, *completed.error, false); // BadDataChunk，可恢复
						}
					}
					continue;
				}
				if (result.stop == StopReason::Error || result.stop == StopReason::BatchLimit) {
					// 继续解析剩余缓冲区
					continue;
				}
				if (tcp_session.closing) {
					break; // 写路径已关闭连接（发送失败或 Quit），不再读
				}
				// NeedMoreData 或 EndOfInput
				const std::span<char> buffer = input.prepare(kReadChunkBytes);
				input.commit(
					co_await socket.async_read_some(asio::buffer(buffer.data(), buffer.size()), use_awaitable));
			}
		} catch (const boost::system::system_error &e) {
			if (e.code() != asio::error::eof && e.code() != asio::error::connection_reset &&
			    e.code() != asio::error::operation_aborted) {
				spdlog::error("[shard {}] session error: {}", shard_id, e.what());
			}
		} catch (const std::exception &e) {
			spdlog::error("[shard {}] session exception: {}", shard_id, e.what());

		} catch (...) {
			spdlog::error("[shard {}] session unknown exception", shard_id);
		}
		// A fatal parse error may still have earlier cross-shard responses in the
		// ring. Keep the session object until the ordered writer closes it.
		tcp_session.session_exited = true;
		// 有写在途时不能回收（ring_array 的已发送槽位还被引用），由写回调结束时回收。
		worker_.sessions().reclaim_if_idle(tcp_id, tcp_session);
	}

	awaitable<void> wait_for_backlog(TCPSession &session) {
		session.read_paused = true;
		while (!session.closing && !stop_->load(std::memory_order_relaxed) &&
		       session.backlog() > TCPSession::kResumeReadBacklog) {
			boost::system::error_code ec; // 被 cancel() 唤醒时是 operation_aborted
			co_await session.resume_read.async_wait(asio::redirect_error(use_awaitable, ec));
		}
		session.read_paused = false;
	}

	void run() {
		asio::co_spawn(io_, worker_.mailbox_loop(), asio::detached);
		asio::co_spawn(io_, accept_loop(), asio::detached);
		io_.run();
	}

	awaitable<void> accept_loop() {
		const std::size_t shard_id = worker_.current_shard_id();
		const auto executor = co_await asio::this_coro::executor;
		boost::system::error_code ec;
		const auto address = asio::ip::make_address(config_.addr, ec);
		if (ec) {
			spdlog::error("[shard {}] invalid address {}: {}", shard_id, config_.addr, ec.message());
			co_return;
		}
		const tcp::endpoint endpoint(address, config_.port);
		tcp::acceptor acceptor(executor);
		try {
			acceptor.open(endpoint.protocol());
			acceptor.set_option(asio::socket_base::reuse_address(true));
#ifdef SO_REUSEPORT
			constexpr int reuse_port = 1;
			if (::setsockopt(acceptor.native_handle(), SOL_SOCKET, SO_REUSEPORT, &reuse_port, sizeof(reuse_port)) ==
			    -1) {
				spdlog::error("[shard {}] SO_REUSEPORT failed: {}", shard_id, std::strerror(errno));
				co_return;
			}
#endif
			acceptor.bind(endpoint);
			acceptor.listen(asio::socket_base::max_listen_connections);
		} catch (const boost::system::system_error &e) {
			spdlog::error("[shard {}] acceptor setup failed: {}", shard_id, e.what());
			co_return;
		}
		spdlog::info("[shard {}] listening on {}:{}", shard_id, address.to_string(), config_.port);
		while (!stop_->load(std::memory_order_relaxed)) {
			tcp::socket socket = co_await acceptor.async_accept(asio::redirect_error(use_awaitable, ec));
			if (ec) {
				if (ec == asio::error::operation_aborted || stop_->load(std::memory_order_relaxed)) {
					break;
				}
				spdlog::warn("[shard {}] accept failed: {}", shard_id, ec.message());
				continue;
			}
			// 响应已经在 kick_write 里按批合并，不需要 Nagle；否则批次末尾的小段会和客户端的延迟 ACK 互相等待约 40ms。
			socket.set_option(tcp::no_delay(true), ec);
			uint64_t id = tcp_id_++;
			worker_.sessions().add(id, std::move(socket));
			asio::co_spawn(executor, session(id), asio::detached);
		}
	}

	// 每次收到TCP都分配一个新版本号
	uint64_t tcp_id_ {0};

	std::thread thread_;

	asio::io_context io_;

	Config config_;

	Worker worker_;

	std::shared_ptr<std::atomic_bool> stop_;
};
} // namespace fastest
