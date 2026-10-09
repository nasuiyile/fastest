#pragma once

#include "config.h"

#include <thread>

#include <utility>
#include <boost/asio.hpp>
#include <spdlog/spdlog.h>
#include <atomic>

#include "worker.h"
#include "../../protocol/memcached_parser.h"
#include "type.h"
#include <sys/socket.h>

namespace comm {
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
		auto stop = std::make_shared<std::atomic_bool>(false);
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

	// 最大读取命令行的限制，命令行是\r\n结尾的
	static constexpr std::size_t kMaxCommandLineBytes = 8 * 1024;
	// 单次最多读取字节数
	static constexpr std::size_t kReadChunkBytes = 4096;

	static awaitable<void> write_text(tcp::socket &socket, const std::string_view text) {
		co_await asio::async_write(socket, asio::buffer(text.data(), text.size()), use_awaitable);
	}

	static std::string_view parse_error_message(protocol::ParseErrorCode code) noexcept {
		using enum protocol::ParseErrorCode;
		switch (code) {
		case UnknownCommand:
			return "ERROR\r\n";
		case InvalidKey:
			return "CLIENT_ERROR bad key\r\n";
		case ValueTooLarge:
			return "SERVER_ERROR object too large\r\n";
		case BadDataChunk:
			return "CLIENT_ERROR bad data chunk\r\n";
		case CommandLineTooLong:
			return "CLIENT_ERROR command line too long\r\n";
		default:
			return "CLIENT_ERROR bad command line format\r\n";
		}
	}

	static awaitable<bool> handle_parse_error(tcp::socket &socket, const protocol::ParseError &error) {
		co_await write_text(socket, parse_error_message(error.code));
		co_return error.recovery == protocol::ErrorRecovery::Continue;
	}

	awaitable<void> execute_batch(const std::vector<protocol::Command> &vector) {
		for (protocol::Command variant : vector) {
			std::visit(
			    [this](auto &&arg) {
				    using T = std::decay_t<decltype(arg)>;
				    if constexpr (std::is_same_v<T, protocol::SingleKeyCommand>) {
					    // 直接发送
					    worker_->handle_single_command(arg);
				    } else if constexpr (std::is_same_v<T, protocol::MultiKeyCommand>) {
					    // 确定有多少个分片的数据需要聚合，然后发送
				    } else if constexpr (std::is_same_v<T, protocol::NoKeyCommand>) {
					    // 直接本地处理
				    }
			    },
			    variant);
		}
	};

	awaitable<void> session(tcp::socket socket) {
		std::size_t shard_id = worker_->current_shard_id();
		try {
			protocol::MemcachedParser parser;
			// 保留上一次没有解析完的数据
			std::string input;
			std::array<char, 4096> recv_buffer {};
			while (!stop_->load(std::memory_order_relaxed)) {
				auto result = parser.parse(std::span(input.data(), input.size()), 128);
				// 先执行成功解析的命令，确保响应顺序正确
				co_await execute_batch(result.commands);
				bool keep_connection = true;
				if (result.stop == protocol::StopReason::Error) {
					keep_connection = co_await handle_parse_error(socket, *result.error);
				}
				// 所有 CommandView 使用完毕后才能消费
				input.erase(0, result.consumed);
				if (!keep_connection) {
					co_return;
				}
				if (result.stop == protocol::StopReason::Error || result.stop == protocol::StopReason::BatchLimit) {
					// 继续解析剩余缓冲区
					continue;
				}
				// NeedMoreData 或 EndOfInput
				std::size_t n = co_await socket.async_read_some(asio::buffer(recv_buffer), use_awaitable);
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
			int reuse_port = 1;
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
			asio::co_spawn(executor, session(std::move(socket)), asio::detached);
		}
	}

	std::thread thread_;

	asio::io_context io_;

	Config config_;

	std::unique_ptr<Worker> worker_;

	std::shared_ptr<std::atomic_bool> stop_;
};
} // namespace comm