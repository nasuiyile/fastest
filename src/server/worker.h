#pragma once

#include "config.h"
#include "mailbox.h"
#include "shard_registry.h"

#include "../protocol/memcahed_request.h"

#include <boost/asio.hpp>
#include <boost/asio/posix/stream_descriptor.hpp>

#include <spdlog/spdlog.h>

#include <sys/socket.h>

#include <algorithm>
#include <array>
#include <atomic>
#include <charconv>
#include <chrono>
#include <cstdint>
#include <cstring>
#include <memory>
#include <string>
#include <string_view>
#include <thread>
#include <unordered_map>
#include <utility>
#include <vector>

namespace fast_server {
namespace asio = boost::asio;

using asio::awaitable;
using asio::ip::tcp;
using asio::use_awaitable;

class Worker {
public:
	Worker(std::size_t shard_id, const Config &cfg) : shard_id_(shard_id), config_(cfg) {
	}

	void start() {
		thread_ = std::thread([this] { run(); });
	}

	void request_stop() {
		stop_.store(true, std::memory_order_relaxed);

		/*
		 * io_context::stop() 是 thread-safe 的。
		 *
		 * 它会让 mailbox wait / accept / timers 等事件循环停止。
		 */
		io_.stop();
	}

	void join() {
		if (thread_.joinable()) {
			thread_.join();
		}
	}

	std::size_t shard_id() const noexcept { return shard_id_; }

private:
	/*
	 * 一个 connection 上已经从 socket 读取、但是 parser
	 * 尚未消费的数据。
	 *
	 * 这里解决了原来的核心问题：
	 *
	 * async_read_until() 可能把 value 一起读进 streambuf，
	 * 随后又直接从 raw socket 读取 value，导致永久等待。
	 *
	 * 当前方案：
	 *
	 * socket -> ConnectionBuffer -> parser
	 *
	 * read_exact() 会优先消费 ConnectionBuffer。
	 * 只有 buffer 不够的时候，才从 socket 精确读取剩余字节。
	 *
	 * 精确读取不会 over-read，因此不会偷走下一条 command。
	 */
	class ConnectionBuffer : public asio::streambuf {
	public:
		//在缓存中找到行位
		std::size_t find_crlf() const noexcept {
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

	struct Item {
		uint32_t flags{0};
		std::string value;
	};

	struct PendingRequest {
		explicit PendingRequest(const asio::any_io_executor &executor) : timer(executor) {
		}

		asio::steady_timer timer;

		bool completed{false};
		ShardStatus status{ShardStatus::Error};

		uint32_t item_flags{0};
		std::string value;
	};

	struct RemoteResult {
		bool transport_ok{false};
		ShardStatus status{ShardStatus::Error};

		uint32_t item_flags{0};
		std::string value;
	};

private:
	//最大读取命令行的限制，命令行是\r\n结尾的
	static constexpr std::size_t kMaxCommandLineBytes = 8 * 1024;
	static constexpr std::size_t kMaxValueBytes = 16 * 1024 * 1024;
	static constexpr std::size_t kMaxKeyBytes = 250;

	static constexpr std::size_t kReadChunkBytes = 4096;

	static constexpr auto kMailboxRetryDelay = std::chrono::microseconds(100);

	static constexpr auto kMailboxEnqueueTimeout = std::chrono::milliseconds(500);

	static constexpr auto kCrossShardResponseTimeout = std::chrono::seconds(2);

private:
	void run() {
		/*
		 * 如果项目其他代码依赖 thread_local shard id，
		 * 继续保留。
		 */
		ShardRegistry::local_shard = shard_id_;
		asio::co_spawn(io_, mailbox_loop(), asio::detached);
		asio::co_spawn(io_, accept_loop(), asio::detached);
		io_.run();
	}

	/*
	 * 有界 mailbox 的 coroutine backpressure。
	 *
	 * 这里不 spin。
	 *
	 * queue 满时：
	 *
	 *   suspend coroutine
	 *   ->
	 *   io_context 可以继续运行 mailbox_loop / session
	 *   ->
	 *   100 us 后重试
	 *
	 * 因此不会出现：
	 *
	 * shard A 等 shard B drain
	 * shard B 又等 shard A drain
	 * 两边都卡在 while(push == false)
	 */
	awaitable<bool> send_to_shard(std::size_t target, std::unique_ptr<ShardMessage> message,
	                              std::chrono::steady_clock::duration timeout) {
		auto executor = co_await asio::this_coro::executor;

		asio::steady_timer retry_timer(executor);

		auto &mailbox = ShardRegistry::inst().mailbox(target);

		const auto deadline = std::chrono::steady_clock::now() + timeout;

		while (!stop_.load(std::memory_order_relaxed)) {
			if (mailbox.try_push(message)) {
				co_return true;
			}

			if (std::chrono::steady_clock::now() >= deadline) {
				co_return false;
			}

			retry_timer.expires_after(kMailboxRetryDelay);

			boost::system::error_code ec;

			co_await retry_timer.async_wait(asio::redirect_error(use_awaitable, ec));

			if (ec && ec != asio::error::operation_aborted) {
				co_return false;
			}
		}

		co_return false;
	}

	/*
	 * target shard 处理完请求之后，response 由独立 coroutine
	 * 负责投递。
	 *
	 * mailbox_loop 本身绝对不能等待另一个 mailbox 腾空间，
	 * 否则多个 shard 的 mailbox_loop 之间仍然可能形成等待环。
	 */
	awaitable<void> deliver_response(std::size_t target, std::unique_ptr<ShardMessage> response) {
		const bool sent = co_await send_to_shard(target, std::move(response), kMailboxEnqueueTimeout);

		if (!sent && !stop_.load(std::memory_order_relaxed)) {
			spdlog::warn("[shard {}] response mailbox {} is full", shard_id_, target);
		}
	}

	awaitable<void> mailbox_loop() {
		auto executor = co_await asio::this_coro::executor;
		auto &mailbox = ShardRegistry::inst().mailbox(shard_id_);
		const int wait_fd = mailbox.duplicate_event_fd();
		if (wait_fd < 0) {
			spdlog::error("[shard {}] failed to dup mailbox eventfd: {}", shard_id_, std::strerror(errno));
			co_return;
		}
		/*
		 * stream_descriptor 只拥有 duplicate fd。
		 *
		 * Mailbox 仍然拥有原始 eventfd。
		 */
		asio::posix::stream_descriptor descriptor(executor, wait_fd);
		while (!stop_.load(std::memory_order_relaxed)) {
			boost::system::error_code ec;
			co_await descriptor.async_wait(asio::posix::stream_descriptor::wait_read,
			                               asio::redirect_error(use_awaitable, ec));
			if (ec) {
				if (ec != asio::error::operation_aborted && !stop_.load(std::memory_order_relaxed)) {
					spdlog::error("[shard {}] mailbox wait failed: {}", shard_id_, ec.message());
				}

				break;
			}
			mailbox.clear_notify();
			std::unique_ptr<ShardMessage> message;
			// 一次可以消费掉多个元素
			while (mailbox.pop(message)) {
				try {
					handle_shard_message(std::move(message));
				} catch (const std::exception &e) {
					spdlog::error("[shard {}] mailbox handler exception: {}", shard_id_, e.what());
				} catch (...) {
					spdlog::error("[shard {}] mailbox handler unknown exception", shard_id_);
				}

				message.reset();
			}
		}
	}

	void handle_shard_message(std::unique_ptr<ShardMessage> message) {
		if (!message) {
			return;
		}

		if (message->type == ShardMessageType::Response) {
			complete_pending(std::move(*message));

			return;
		}

		handle_shard_request(std::move(message));
	}

	void handle_shard_request(std::unique_ptr<ShardMessage> request) {
		auto response = std::make_unique<ShardMessage>();

		response->type = ShardMessageType::Response;

		response->op = request->op;

		response->req_id = request->req_id;

		response->origin_shard = request->origin_shard;

		response->status = ShardStatus::Error;

		try {
			switch (request->op) {
			case ShardOp::Set: {
				Item item{};
				item.flags = request->item_flags;
				item.value = std::move(request->value);

				data_.insert_or_assign(std::move(request->key), std::move(item));

				response->status = ShardStatus::Ok;

				break;
			}

			case ShardOp::Del: {
				const std::size_t erased = data_.erase(request->key);

				response->status = erased != 0 ? ShardStatus::Ok : ShardStatus::NotFound;

				break;
			}

			case ShardOp::Get: {
				const auto it = data_.find(request->key);

				if (it == data_.end()) {
					response->status = ShardStatus::NotFound;

					break;
				}

				response->status = ShardStatus::Ok;

				response->item_flags = it->second.flags;

				response->value = it->second.value;

				break;
			}
			}
		} catch (const std::exception &e) {
			response->status = ShardStatus::Error;

			spdlog::error("[shard {}] request execution failed: {}", shard_id_, e.what());
		} catch (...) {
			response->status = ShardStatus::Error;

			spdlog::error("[shard {}] request execution failed", shard_id_);
		}

		const std::size_t origin = static_cast<std::size_t>(request->origin_shard);

		/*
		 * 不在当前 mailbox_loop 里面等待 response queue。
		 */
		asio::co_spawn(io_, deliver_response(origin, std::move(response)), asio::detached);
	}

	void complete_pending(ShardMessage &&response) {
		const auto it = pending_.find(response.req_id);

		/*
		 * 可能请求已经 timeout。
		 *
		 * late response 直接丢掉即可。
		 */
		if (it == pending_.end()) {
			return;
		}

		std::shared_ptr<PendingRequest> pending = it->second;

		pending_.erase(it);

		pending->status = response.status;

		pending->item_flags = response.item_flags;

		pending->value = std::move(response.value);

		pending->completed = true;

		pending->timer.cancel();
	}

	uint64_t allocate_req_id() noexcept {
		const uint64_t result = next_req_id_++;

		if (next_req_id_ == 0) {
			next_req_id_ = 1;
		}

		return result;
	}

	awaitable<RemoteResult> request_remote(std::size_t target, ShardOp op, std::string key, std::string value = {},
	                                       uint32_t item_flags = 0) {
		auto executor = co_await asio::this_coro::executor;

		const uint64_t req_id = allocate_req_id();

		auto pending = std::make_shared<PendingRequest>(executor);

		pending_.emplace(req_id, pending);

		auto message = std::make_unique<ShardMessage>();

		message->type = ShardMessageType::Request;

		message->op = op;

		message->req_id = req_id;

		message->origin_shard = shard_id_;

		message->item_flags = item_flags;

		message->key = std::move(key);

		message->value = std::move(value);

		const bool sent = co_await send_to_shard(target, std::move(message), kMailboxEnqueueTimeout);

		if (!sent) {
			pending_.erase(req_id);

			co_return RemoteResult{};
		}

		pending->timer.expires_after(kCrossShardResponseTimeout);

		boost::system::error_code ec;

		co_await pending->timer.async_wait(asio::redirect_error(use_awaitable, ec));

		/*
		 * response 到达时：
		 *
		 *   complete_pending()
		 *   ->
		 *   completed = true
		 *   ->
		 *   timer.cancel()
		 *
		 * timer 自然到期时 completed 仍然为 false。
		 */
		if (!pending->completed) {
			pending_.erase(req_id);

			co_return RemoteResult{};
		}

		RemoteResult result{};

		result.transport_ok = true;
		result.status = pending->status;
		result.item_flags = pending->item_flags;
		result.value = std::move(pending->value);

		co_return result;
	}

	awaitable<bool> route_set(std::string key, std::string value, uint32_t item_flags) {
		auto &registry = ShardRegistry::inst();
		const std::size_t target = registry.shard_of(key);
		if (target == shard_id_) {
			Item item{};
			item.flags = item_flags;
			item.value = std::move(value);
			data_.insert_or_assign(std::move(key), std::move(item));
			co_return true;
		}
		RemoteResult result = co_await request_remote(target, ShardOp::Set, std::move(key), std::move(value),
		                                              item_flags);
		co_return result.transport_ok && result.status == ShardStatus::Ok;
	}

	awaitable<RemoteResult> route_get(const std::string &key) {
		auto &registry = ShardRegistry::inst();

		const std::size_t target = registry.shard_of(key);

		if (target == shard_id_) {
			RemoteResult result{};
			result.transport_ok = true;

			const auto it = data_.find(key);

			if (it == data_.end()) {
				result.status = ShardStatus::NotFound;

				co_return result;
			}

			result.status = ShardStatus::Ok;

			result.item_flags = it->second.flags;

			result.value = it->second.value;

			co_return result;
		}

		co_return co_await request_remote(target, ShardOp::Get, key);
	}

	awaitable<RemoteResult> route_delete(std::string key) {
		auto &registry = ShardRegistry::inst();

		const std::size_t target = registry.shard_of(key);

		if (target == shard_id_) {
			RemoteResult result{};
			result.transport_ok = true;

			const std::size_t erased = data_.erase(key);

			result.status = erased != 0 ? ShardStatus::Ok : ShardStatus::NotFound;

			co_return result;
		}

		co_return co_await request_remote(target, ShardOp::Del, std::move(key));
	}

	awaitable<void> accept_loop() {
		//获取当前协程的executor
		const auto executor = co_await asio::this_coro::executor;
		boost::system::error_code ec;
		const auto address = asio::ip::make_address(config_.addr, ec);
		if (ec) {
			spdlog::error("[shard {}] invalid address {}: {}", shard_id_, config_.addr, ec.message());
			co_return;
		}
		const tcp::endpoint endpoint(address, config_.port);
		tcp::acceptor acceptor(executor);
		acceptor.open(endpoint.protocol(), ec);
		if (ec) {
			spdlog::error("[shard {}] acceptor open failed: {}", shard_id_, ec.message());
			co_return;
		}
		acceptor.set_option(asio::socket_base::reuse_address(true), ec);
		if (ec) {
			spdlog::error("[shard {}] SO_REUSEADDR failed: {}", shard_id_, ec.message());
			co_return;
		}
#ifdef SO_REUSEPORT
		int one = 1;
		if (::setsockopt(acceptor.native_handle(), SOL_SOCKET, SO_REUSEPORT, &one, sizeof(one)) != 0) {
			spdlog::error("[shard {}] SO_REUSEPORT failed: {}", shard_id_, std::strerror(errno));
			co_return;
		}
#endif
		acceptor.bind(endpoint, ec);
		if (ec) {
			spdlog::error("[shard {}] bind failed: {}", shard_id_, ec.message());
			co_return;
		}
		acceptor.listen(asio::socket_base::max_listen_connections, ec);
		if (ec) {
			spdlog::error("[shard {}] listen failed: {}", shard_id_, ec.message());
			co_return;
		}
		spdlog::info("[shard {}] listening on {}:{}", shard_id_, address.to_string(), config_.port);
		while (!stop_.load(std::memory_order_relaxed)) {
			tcp::socket socket = co_await acceptor.async_accept(asio::redirect_error(use_awaitable, ec));
			if (ec) {
				if (ec == asio::error::operation_aborted || stop_.load(std::memory_order_relaxed)) {
					break;
				}
				spdlog::warn("[shard {}] accept failed: {}", shard_id_, ec.message());
				continue;
			}
			//在当前 executor 所属的 io_context 上启动一个协程
			asio::co_spawn(executor, session(std::move(socket)), asio::detached);
		}
	}

	/*
	 * 只读取 command line。不会读取memcached中的value
	 *
	 * 与 async_read_until 最大的区别是：
	 *
	 * 1. line 有明确的最大长度。
	 * 2. 多读到的 value 数据仍然保存在 ConnectionBuffer 中。
	 * 3. 后续 read_exact() 会首先消费这些数据。
	 */
	awaitable<bool> read_command_line(tcp::socket &socket, ConnectionBuffer &input, std::string &line) {
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
			const std::size_t max_buffered = kMaxCommandLineBytes + 2;
			if (input.size() >= max_buffered) {
				co_return false;
			}
			const std::size_t room = max_buffered - input.size();
			const std::size_t want = std::min(room, kReadChunkBytes);
			if (want == 0) {
				co_return false;
			}
			auto buffer = input.prepare(want);
			const std::size_t n = co_await socket.async_read_some(buffer, use_awaitable);
			input.commit(n);
		}
	}

	/*
	 * 精确读取 n 字节。
	 *
	 * 必须首先消费 input 中已经预读的数据。
	 *
	 * buffer 不够时再从 socket 精确读取剩余部分。
	 *
	 * asio::async_read() 目标 buffer 只有 remaining 字节，
	 * 所以不会 over-read 下一条 command。
	 */
	awaitable<void> read_exact(tcp::socket &socket, ConnectionBuffer &input, char *output, std::size_t n) {
		if (n == 0) {
			co_return;
		}

		const std::size_t buffered = std::min(input.size(), n);

		if (buffered != 0) {
			asio::buffer_copy(asio::buffer(output, buffered), input.data(), buffered);

			input.consume(buffered);
		}

		const std::size_t remaining = n - buffered;

		if (remaining == 0) {
			co_return;
		}

		co_await asio::async_read(socket, asio::buffer(output + buffered, remaining), use_awaitable);
	}

	//把命令行里的字符串token安全转换为无符号整数。
	template <typename UInt>
	static bool parse_unsigned(std::string_view text, UInt &result) {
		if (text.empty()) {
			return false;
		}
		const char *begin = text.data();
		const char *end = text.data() + text.size();
		const auto [p, ec] = std::from_chars(begin, end, result);
		return ec == std::errc{} && p == end;
	}

	static bool valid_key(std::string_view key) noexcept { return !key.empty() && key.size() <= kMaxKeyBytes; }

	awaitable<void> write_text(tcp::socket &socket, std::string_view text) {
		co_await asio::async_write(socket, asio::buffer(text.data(), text.size()), use_awaitable);
	}

	awaitable<void> write_value(tcp::socket &socket, std::string_view key, uint32_t flags, std::string_view value) {
		std::string response;

		response.reserve(key.size() + value.size() + 48);

		response += "VALUE ";
		response.append(key.data(), key.size());

		response += " ";
		response += std::to_string(flags);
		response += " ";
		response += std::to_string(value.size());
		response += "\r\n";

		response.append(value.data(), value.size());

		response += "\r\n";

		co_await write_text(socket, response);
	}

	awaitable<void> session(tcp::socket socket) {
		try {
			ConnectionBuffer input;
			while (!stop_.load(std::memory_order_relaxed)) {
				std::string line;
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
				/*
				 * 注意：
				 * socket 现在通过引用传给 parse。
				 * 绝对不能再：
				 *   parse(std::move(socket), ...)
				 */
				const bool keep_connection = co_await parse(socket, input, std::move(tokens));
				if (!keep_connection) {
					co_return;
				}
			}
		} catch (const boost::system::system_error &e) {
			if (e.code() != asio::error::eof && e.code() != asio::error::connection_reset && e.code() !=
			    asio::error::operation_aborted) {
				spdlog::error("[shard {}] session error: {}", shard_id_, e.what());
			}
		} catch (const std::exception &e) {
			spdlog::error("[shard {}] session exception: {}", shard_id_, e.what());
		} catch (...) {
			spdlog::error("[shard {}] session unknown exception", shard_id_);
		}
	}

	awaitable<bool> parse(tcp::socket &socket, ConnectionBuffer &input, std::vector<std::string_view> tokens) {
		protocol::MemcachedRequest req;
		req.command = std::string(tokens[0]);

		/*
		 * ---------------- SET / storage ----------------
		 */
		if (protocol::is_storage_command(req.command)) {
			if (tokens.size() < 5) {
				co_await write_text(socket, "ERROR\r\n");
				co_return true;
			}

			if (!valid_key(tokens[1])) {
				co_await write_text(socket, "CLIENT_ERROR bad key\r\n");
				co_return true;
			}

			req.key = std::string(tokens[1]);
			req.flags = std::string(tokens[2]);
			req.exptime = std::string(tokens[3]);
			uint32_t item_flags = 0;
			if (!parse_unsigned(tokens[2], item_flags)) {
				co_await write_text(socket, "CLIENT_ERROR bad command line format\r\n");
				co_return true;
			}
			std::size_t bytes = 0;
			if (!parse_unsigned(tokens[4], bytes)) {
				co_await write_text(socket, "CLIENT_ERROR bad command line format\r\n");
				co_return true;
			}

			if (bytes > kMaxValueBytes) {
				co_await write_text(socket, "SERVER_ERROR object too large\r\n");

				/*
				 * bytes 后面的数据长度来自不可信客户端。
				 *
				 * 既然拒绝这个 value，就不能可靠地恢复 framing。
				 * 最安全的方法是发送错误后关闭连接。
				 */
				co_return false;
			}
			req.bytes = bytes;
			req.noreply = tokens.size() >= 6 && tokens[5] == "noreply";
			req.has_value = true;
			req.value.resize(req.bytes);
			/*
			 * 先消费 ConnectionBuffer 里可能已经预读的 value。
			 */
			co_await read_exact(socket, input, req.value.data(), req.bytes);

			std::array<char, 2> crlf{};

			co_await read_exact(socket, input, crlf.data(), crlf.size());

			if (crlf[0] != '\r' || crlf[1] != '\n') {
				co_await write_text(socket, "CLIENT_ERROR bad data chunk\r\n");

				/*
				 * framing 已经无法可信恢复。
				 */
				co_return false;
			}

			const bool stored = co_await route_set(std::move(req.key), std::move(req.value), item_flags);

			if (!req.noreply) {
				if (stored) {
					co_await write_text(socket, "STORED\r\n");
				} else {
					co_await write_text(socket, "SERVER_ERROR cross-shard SET failed\r\n");
				}
			}

			co_return true;
		}

		/*
		 * ---------------- GET ----------------
		 */
		if (protocol::is_read_command(req.command)) {
			if (tokens.size() < 2) {
				co_await write_text(socket, "END\r\n");

				co_return true;
			}

			for (std::size_t i = 1; i < tokens.size(); ++i) {
				if (!valid_key(tokens[i])) {
					co_await write_text(socket, "CLIENT_ERROR bad key\r\n");

					co_return true;
				}

				std::string key(tokens[i]);

				RemoteResult result = co_await route_get(key);

				if (!result.transport_ok) {
					co_await write_text(socket, "SERVER_ERROR cross-shard GET failed\r\n");

					co_return true;
				}

				if (result.status == ShardStatus::NotFound) {
					continue;
				}

				if (result.status != ShardStatus::Ok) {
					co_await write_text(socket, "SERVER_ERROR GET failed\r\n");

					co_return true;
				}

				co_await write_value(socket, key, result.item_flags, result.value);
			}

			co_await write_text(socket, "END\r\n");

			co_return true;
		}

		/*
		 * ---------------- DELETE ----------------
		 */
		if (protocol::is_delete_command(req.command)) {
			if (tokens.size() < 2) {
				co_await write_text(socket, "CLIENT_ERROR bad command line format\r\n");

				co_return true;
			}

			if (!valid_key(tokens[1])) {
				co_await write_text(socket, "CLIENT_ERROR bad key\r\n");

				co_return true;
			}

			req.key = std::string(tokens[1]);

			const bool noreply = tokens.size() >= 3 && tokens.back() == "noreply";

			RemoteResult result = co_await route_delete(std::move(req.key));

			if (noreply) {
				co_return true;
			}

			if (!result.transport_ok) {
				co_await write_text(socket, "SERVER_ERROR cross-shard DELETE failed\r\n");

				co_return true;
			}

			if (result.status == ShardStatus::Ok) {
				co_await write_text(socket, "DELETED\r\n");

				co_return true;
			}

			if (result.status == ShardStatus::NotFound) {
				co_await write_text(socket, "NOT_FOUND\r\n");

				co_return true;
			}

			co_await write_text(socket, "SERVER_ERROR DELETE failed\r\n");

			co_return true;
		}

		co_await write_text(socket, "ERROR\r\n");

		co_return true;
	}

private:
	std::size_t shard_id_;
	Config config_;

	asio::io_context io_;
	std::thread thread_;

	std::atomic<bool> stop_{false};

	/*
	 * 只有本 shard 的 io_context thread 访问 data_。
	 *
	 * remote shard 永远只能通过 mailbox 请求。
	 */
	std::unordered_map<std::string, Item> data_;

	/*
	 * 只有 origin shard 自己访问 pending_。
	 *
	 * response 也会先进入 origin shard mailbox，
	 * 最终仍然在同一个 io_context thread 中完成。
	 *
	 * 因此这里不需要 mutex。
	 */
	std::unordered_map<uint64_t, std::shared_ptr<PendingRequest> > pending_;

	uint64_t next_req_id_{1};
};
} // namespace fast_server