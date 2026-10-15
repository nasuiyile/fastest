#pragma once

#include "config.h"

#include <thread>
#include <algorithm>
#include <utility>
#include <boost/asio.hpp>
#include <spdlog/spdlog.h>
#include <atomic>
#include <sys/socket.h>
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
		auto workers = Worker::create_workers(core_num, stop);
		std::vector<std::unique_ptr<Handler>> handlers;
		handlers.reserve(workers.size());
		for (auto &worker : workers) {
			handlers.push_back(
			    std::unique_ptr<Handler>(new Handler(cfg, std::make_unique<Worker>(std::move(worker)), stop)));
		}
		return handlers;
	}

	void join() {
		if (thread_.joinable()) {
			thread_.join();
		}
	}

private:
	explicit Handler(Config config, std::unique_ptr<Worker> worker, std::shared_ptr<std::atomic_bool> stop)
	    : config_(std::move(config)), worker_(std::move(worker)), stop_(std::move(stop)) {
	}

	static constexpr std::size_t kReadChunkBytes = 4096;

	bool handle_parse_error(const uint64_t tcp_id, ParseError &error, const bool wait_for_swallow) const {
		worker_->send_error_command(to_error_response(error), tcp_id, std::move(error.rejected_set_key),
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
					    worker_->send_single_command(std::move(arg), tcp_id);
				    } else if constexpr (std::is_same_v<T, MultiKeyCommand>) {
					    worker_->send_multi_command(arg, tcp_id);
				    } else if constexpr (std::is_same_v<T, NoKeyCommand>) {
					    // No-key commands are not routed through key shards.
				    }
			    },
			    command);
		}
	};

	awaitable<void> session(const uint64_t tcp_id) {
		auto it = worker_->tcp_session_.find(tcp_id);
		if (it == worker_->tcp_session_.end()) {
			co_return;
		}
		TCPSession &tcp_session = *it->second;
		tcp::socket &socket = tcp_session.socket;
		std::size_t shard_id = worker_->current_shard_id();
		try {
			const MemcachedParser parser;
			// 保留上一次没有解析完的数据
			std::string input;
			std::array<char, kReadChunkBytes> recv_buffer {};
			std::size_t discard_remaining = 0;
			while (!tcp_session.closing && !stop_->load(std::memory_order_relaxed)) {
				if (discard_remaining != 0) {
					const std::size_t buffered = std::min(discard_remaining, input.size());
					input.erase(0, buffered);
					discard_remaining -= buffered;
					if (discard_remaining == 0) {
						worker_->finish_swallow(tcp_id);
						continue;
					}
					const std::size_t n = co_await socket.async_read_some(asio::buffer(recv_buffer), use_awaitable);
					const std::size_t dropped = std::min(discard_remaining, n);
					discard_remaining -= dropped;
					if (n > dropped) {
						input.append(recv_buffer.data() + dropped, n - dropped);
					}
					if (discard_remaining == 0) {
						worker_->finish_swallow(tcp_id);
					}
					continue;
				}

				auto result = parser.parse(std::span(input.data(), input.size()), 128);
				// 先执行成功解析的命令，确保响应顺序正确
				execute_batch(result.commands, tcp_id);
				bool keep_connection = true;
				if (result.stop == StopReason::Error) {
					keep_connection = handle_parse_error(tcp_id, *result.error, result.discard_bytes != 0);
				}
				// 解析结果已投递后才能消费未解析输入
				input.erase(0, result.consumed);
				discard_remaining = result.discard_bytes;
				if (!keep_connection) {
					// The error response already occupies its sequence number. Stop
					// reading, but let the worker drain earlier responses first; the
					// ordered response path then closes the socket after this error.
					tcp_session.protocol_error_waiting = true;
					break;
				}
				if (result.stop == StopReason::Error || result.stop == StopReason::BatchLimit) {
					// 继续解析剩余缓冲区
					continue;
				}
				// NeedMoreData 或 EndOfInput
				const std::size_t n = co_await socket.async_read_some(asio::buffer(recv_buffer), use_awaitable);
				input.append(recv_buffer.data(), n);
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
		if (!tcp_session.protocol_error_waiting || tcp_session.closing) {
			worker_->tcp_session_.erase(tcp_id);
		}
	}

	void run() {
		/*
		 * 如果项目其他代码依赖 thread_local shard id，
		 * 继续保留。
		 */
		asio::co_spawn(io_, worker_->mailbox_request_loop(), asio::detached);
		asio::co_spawn(io_, worker_->mailbox_response_loop(), asio::detached);
		asio::co_spawn(io_, accept_loop(), asio::detached);
		io_.run();
	}

	awaitable<void> accept_loop() {
		const std::size_t shard_id = worker_->current_shard_id();
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
			auto ptr = std::make_unique<TCPSession>(std::move(socket));
			uint64_t id = tcp_id_++;
			worker_->tcp_session_.emplace(id, std::move(ptr));
			asio::co_spawn(executor, session(id), asio::detached);
		}
	}

	// 每次收到TCP都分配一个新版本号
	uint64_t tcp_id_ {0};

	std::thread thread_;

	asio::io_context io_;

	Config config_;

	std::unique_ptr<Worker> worker_;

	std::shared_ptr<std::atomic_bool> stop_;
};
} // namespace fastest