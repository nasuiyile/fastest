#pragma once
#include "config.h"
#include <thread>
#include <spdlog/spdlog.h>
#include <boost/asio.hpp>
#include "../protocol/memcahed_request.h"
#include <vector>
#include <string>

namespace fast_server {
namespace asio = boost::asio;
using asio::awaitable;
using asio::ip::tcp;
using asio::use_awaitable;
using std::vector;
using std::string;

class FastestServer {
public:
	explicit FastestServer(const Config cfg) : config(cfg) {
	}

	void start() {
		unsigned int thread_count = std::thread::hardware_concurrency();
		if (thread_count == 0)
			thread_count = 1;
		//创建事件循环
		asio::io_context io(static_cast<int>(thread_count));
		//把协程提交到一个指定的executor上去运行
		co_spawn(io, server(), asio::detached);
		std::vector<std::thread> workers;
		workers.reserve(thread_count - 1);
		for (unsigned int i = 1; i < thread_count; ++i) {
			workers.emplace_back([&io]() {
				io.run();
			});
		}
		io.run();
		for (auto &t : workers) {
			t.join();
		}
	}

	static awaitable<void> session(tcp::socket socket) {
		try {
			asio::streambuf buf;

			while (true) {
				co_await asio::async_read_until(
					socket,
					buf,
					"\r\n",
					use_awaitable
					);

				std::string line = protocol::extract_line(buf);

				if (line.empty())
					continue;

				auto tokens = protocol::split_ws(line);

				if (tokens.empty())
					continue;

				// 注意这里
				co_await parse(std::move(socket), tokens);
			}
		} catch (const boost::system::system_error &e) {
			if (e.code() != asio::error::eof &&
			    e.code() != asio::error::connection_reset) {
				spdlog::error("session error {}", e.what());
			}
		}
	}

	static asio::awaitable<void> parse(tcp::socket socket, vector<std::string_view> tokens) {
		protocol::MemcachedRequest req;
		req.command = std::string(tokens[0]);

		// 2) 存储类命令：解析 <key> <flags> <exptime> <bytes> [noreply]
		if (protocol::is_storage_command(req.command)) {
			if (tokens.size() < 5) {
				// 协议错误，可回写 ERROR
				const char *err = "ERROR\r\n";
				co_await asio::async_write(socket, asio::buffer(err, 7), use_awaitable);
				co_return;
			}
			req.key = std::string(tokens[1]);
			req.flags = std::string(tokens[2]);
			req.exptime = std::string(tokens[3]);
			std::size_t n = 0;
			auto [p, ec] = std::from_chars(tokens[4].data(), tokens[4].data() + tokens[4].size(), n);
			if (ec != std::errc{}) {
				const char *err = "CLIENT_ERROR bad command line format\r\n";
				co_await asio::async_write(socket, asio::buffer(err, std::strlen(err)), use_awaitable);
				co_return;
			}
			req.bytes = n;
			req.noreply = (tokens.size() >= 6 && tokens[5] == "noreply");
			req.has_value = true;

			// 3) 精确读 value + 结尾 \r\n
			req.value.resize(req.bytes);
			co_await asio::async_read(socket, asio::buffer(req.value.data(), req.bytes), use_awaitable);

			// 读掉结尾的 \r\n（2 字节）
			std::array<char, 2> crlf{};
			co_await asio::async_read(socket, asio::buffer(crlf), use_awaitable);

			spdlog::info("SET key={} flags={} exptime={} bytes={} noreply={}",
			             req.key, req.flags, req.exptime, req.bytes, req.noreply);
			spdlog::info("VALUE ({} bytes): [{}]", req.value.size(), req.value);

			// 4) 处理业务逻辑后回写响应
			if (!req.noreply) {
				const char *ok = "STORED\r\n";
				co_await asio::async_write(socket, asio::buffer(ok, std::strlen(ok)), use_awaitable);
			}
			co_return;
		}

		// 5) 读取类命令：get <key> [<key> ...]
		if (protocol::is_read_command(req.command)) {
			// 这里可以逐个 key 查表并组装 VALUE ... END 响应
			for (std::size_t i = 1; i < tokens.size(); ++i) {
				std::string key(tokens[i]);
				spdlog::info("GET key={}", key);
				// TODO: 从你的存储中查 key，组装响应
			}
			const char *end = "END\r\n";
			co_await asio::async_write(socket, asio::buffer(end, std::strlen(end)), use_awaitable);
			co_return;
		}

		// 6) delete <key> [noreply]
		if (protocol::is_delete_command(req.command)) {
			if (tokens.size() >= 2) {
				req.key = std::string(tokens[1]);
				spdlog::info("DELETE key={}", req.key);
			}
			const char *ok = "DELETED\r\n";
			co_await asio::async_write(socket, asio::buffer(ok, std::strlen(ok)), use_awaitable);
			co_return;
		}

		// 7) 其他命令（stats/version/quit...）可按需扩展
		const char *err = "ERROR\r\n";
		co_await asio::async_write(socket, asio::buffer(err, std::strlen(err)), use_awaitable);
	}


	awaitable<void> server() {
		//获取当前协程所绑定的执行器对象
		const auto executor = co_await asio::this_coro::executor;
		auto address = asio::ip::make_address(config.addr);
		tcp::acceptor acceptor(executor, tcp::endpoint(address, this->config.port));
		spdlog::info("Listening on {}:{}", address.to_string(), this->config.port);
		while (true) {
			tcp::socket socket = co_await acceptor.async_accept(use_awaitable);
			spdlog::info("client connected: {}", socket.remote_endpoint().address().to_string());
			asio::co_spawn(executor, session(std::move(socket)), asio::detached);
		}
	}

private:
	Config config;
};
}