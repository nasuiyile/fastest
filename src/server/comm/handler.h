#pragma once

#include "config.h"

#include <thread>

#include <utility>
#include <boost/asio.hpp>
#include <spdlog/spdlog.h>
#include <atomic>

#include "worker.h"
#include "../../protocol/memcahed_request.h"
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

	class ConnectionBuffer : public asio::streambuf {
	public:
		// 在缓存中找到行位
		[[nodiscard]] std::size_t find_crlf() const noexcept {
			const auto buffers = data();
			auto it = asio::buffers_begin(buffers);
			const auto end = asio::buffers_end(buffers);
			std::size_t offset = 0;
			while (it != end) {
				if (*it == '\r') {
					auto next = it;
					++next;
					if (next != end && *next == '\n') {
						return offset;
					}
				}
				++it;
				++offset;
			}

			return std::string::npos;
		}
	};

	// 多读到的 value 数据仍然保存在 ConnectionBuffer 中。 后续 read_exact() 会首先消费这些数据。

	static awaitable<bool> read_command_line(tcp::socket &socket, ConnectionBuffer &input, std::string &line) {
		while (true) {
			const std::size_t crlf = input.find_crlf();
			if (crlf != std::string::npos) {
				if (crlf > kMaxCommandLineBytes) {
					co_return false;
				}
				line.resize(crlf);
				if (crlf != 0) {
					asio::buffer_copy(asio::buffer(line.data(), line.size()), input.data(), crlf);
				}
				input.consume(crlf + 2);
				co_return true;
			}
			constexpr std::size_t max_buffered = kMaxCommandLineBytes + 2;
			if (input.size() >= max_buffered) {
				co_return false;
			}
			const std::size_t room = max_buffered - input.size();
			const std::size_t want = std::min(room, kReadChunkBytes);
			if (want == 0) {
				co_return false;
			}
			auto buffer = input.prepare(want);
			// 至少读一个字节，如果缓冲区里有足够的数据，最多读4096
			const std::size_t n = co_await socket.async_read_some(buffer, use_awaitable);
			input.commit(n);
		}
	}
	static awaitable<void> write_text(tcp::socket &socket, const std::string_view text) {
		co_await asio::async_write(socket, asio::buffer(text.data(), text.size()), use_awaitable);
	}

	awaitable<void> session(tcp::socket socket) const {
		std::size_t shard_id = worker_->current_shard_id();
		try {
			ConnectionBuffer input;
			std::string line;
			line.reserve(256);
			while (!stop_->load(std::memory_order_relaxed)) {
				const bool valid_line = co_await read_command_line(socket, input, line);
				if (!valid_line) {
					co_await write_text(socket, "CLIENT_ERROR command line too long\r\n");
					co_return;
				}
				if (line.empty()) {
					continue;
				}
				auto tokens = protocol::split_ws(line);
				if (tokens.empty()) {
					continue;
				}
				// const bool keep_connection = co_await parse(socket, input, std::move(tokens));
				// if (!keep_connection) {
				// co_return;
				// }
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
