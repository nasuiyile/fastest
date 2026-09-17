#include <utility>
#include <array>
#include <iostream>
#include <thread>
#include <vector>

#include <spdlog/spdlog.h>
#include <boost/asio.hpp>

namespace asio = boost::asio;
using asio::ip::tcp;
using asio::awaitable;
using asio::use_awaitable;

static awaitable<void> session(tcp::socket socket) {
    try {
        std::array<char, 4096> buffer{};
        while (true) {
            const std::size_t n = co_await socket.async_read_some(asio::buffer(buffer), use_awaitable);
            co_await asio::async_write(socket, asio::buffer(buffer.data(), n), use_awaitable);
        }
    } catch (const boost::system::system_error &e) {
        if (e.code() != asio::error::eof &&e.code() != asio::error::connection_reset) {
            spdlog::error("session error {}", e.what());
        }
    }
}

static awaitable<void> server(unsigned short port) {
    const auto executor = co_await asio::this_coro::executor;
    tcp::acceptor acceptor(executor, tcp::endpoint(tcp::v4(), port));
    spdlog::info("Listening on 0.0.0.0:{}", port);
    while (true) {
        tcp::socket socket = co_await acceptor.async_accept(use_awaitable);
        spdlog::info("client connected: {}", socket.remote_endpoint().address().to_string());
        asio::co_spawn(executor, session(std::move(socket)), asio::detached);
    }
}


int main() {
    try {
        unsigned int thread_count = std::thread::hardware_concurrency();
        if (thread_count == 0)
            thread_count = 1;
        spdlog::info("Using {} worker threads", thread_count);
        asio::io_context io(static_cast<int>(thread_count));
        asio::co_spawn(io, server(9000), asio::detached);
        // 创建 N-1 个 worker，main 自己也算一个 worker
        std::vector<std::thread> workers;
        workers.reserve(thread_count - 1);
        for (unsigned int i = 1; i < thread_count; ++i) {
            workers.emplace_back([&io]() {
                io.run();
            });
        }
        // main 线程也参与处理
        io.run();
        for (auto &t: workers)
            t.join();
    } catch (const std::exception &e) {
        spdlog::error("{}", e.what());
        return 1;
    }
}
