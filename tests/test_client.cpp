// TCP echo 客户端测试
//
// 用来验证 src/main.cpp 里 server()/session() 的行为：
//   - 原样把收到的数据写回（echo）
//   - 单个连接上可以连续收发多条消息
//   - 大包（跨越 4096 字节 read buffer）也能完整、按序收到
//   - 二进制数据不会被破坏
//   - 多个客户端可以并发连接，互不干扰
//
// 运行前需要先手动启动 fastest（监听 127.0.0.1:9000），
// 这个可执行文件本身只是发起连接的客户端，不负责拉起 server。
//
//   ./fastest &
//   ./fastest_tests            # 跑全部用例
//   ./fastest_tests "[large]"  # 只跑某个 tag

#include <catch2/catch_test_macros.hpp>
#include <boost/asio.hpp>
#include <spdlog/spdlog.h>

#include <string>
#include <vector>
#include <thread>
#include <random>
#include <chrono>

namespace asio = boost::asio;
using asio::ip::tcp;

namespace {

constexpr const char *kHost = "127.0.0.1";
constexpr unsigned short kPort = 9000;
constexpr auto kIoTimeout = std::chrono::seconds(5);

// 建立一条到 server 的连接，失败直接抛异常（Catch2 会报成用例失败）。
tcp::socket connect_to_server(asio::io_context &io) {
    tcp::socket socket(io);
    tcp::resolver resolver(io);
    const auto endpoints = resolver.resolve(kHost, std::to_string(kPort));
    asio::connect(socket, endpoints);
    socket.set_option(tcp::no_delay(true));
    return socket;
}

// 发送 payload，然后阻塞读取，直到收满 payload.size() 字节，
// 返回收到的数据。用超时保护，避免 server 有问题时用例卡死。
std::string echo_roundtrip(tcp::socket &socket, const std::string &payload) {
    asio::write(socket, asio::buffer(payload));

    std::string received;
    received.resize(payload.size());
    std::size_t total_read = 0;

    while (total_read < payload.size()) {
        std::size_t n = 0;
        std::exception_ptr err;
        bool done = false;

        socket.async_read_some(
            asio::buffer(received.data() + total_read, payload.size() - total_read),
            [&](const boost::system::error_code &ec, std::size_t bytes) {
                if (ec) {
                    err = std::make_exception_ptr(boost::system::system_error(ec));
                } else {
                    n = bytes;
                }
                done = true;
            });

        auto &io = static_cast<asio::io_context &>(socket.get_executor().context());
        io.restart();
        const auto deadline = std::chrono::steady_clock::now() + kIoTimeout;
        while (!done && std::chrono::steady_clock::now() < deadline) {
            io.run_one_for(std::chrono::milliseconds(50));
        }
        if (!done) {
            throw std::runtime_error("timed out waiting for echo response");
        }
        if (err) {
            std::rethrow_exception(err);
        }
        total_read += n;
    }
    return received;
}

std::string random_binary_blob(std::size_t size, unsigned seed) {
    std::mt19937 rng(seed);
    std::uniform_int_distribution<int> dist(0, 255);
    std::string blob;
    blob.resize(size);
    for (auto &c : blob) {
        c = static_cast<char>(dist(rng));
    }
    return blob;
}

} // namespace

TEST_CASE("small text message is echoed back unchanged", "[echo][basic]") {
    asio::io_context io;
    tcp::socket socket = connect_to_server(io);

    const std::string sent = "hello fastest tcp server";
    const std::string got = echo_roundtrip(socket, sent);

    REQUIRE(got == sent);
}

TEST_CASE("empty write followed by real message still round-trips", "[echo][basic]") {
    asio::io_context io;
    tcp::socket socket = connect_to_server(io);

    const std::string sent = "ping";
    const std::string got = echo_roundtrip(socket, sent);
    REQUIRE(got == sent);
}

TEST_CASE("multiple sequential messages on one connection", "[echo][session]") {
    asio::io_context io;
    tcp::socket socket = connect_to_server(io);

    for (int i = 0; i < 20; ++i) {
        const std::string sent = "message #" + std::to_string(i);
        const std::string got = echo_roundtrip(socket, sent);
        REQUIRE(got == sent);
    }
}

TEST_CASE("payload larger than the 4096-byte read buffer round-trips intact", "[echo][large]") {
    asio::io_context io;
    tcp::socket socket = connect_to_server(io);

    // session() 用的是 std::array<char, 4096> 做读缓冲，
    // 发一个明显更大的包，确保跨多次 read_some/async_write 依旧数据完整、顺序正确。
    const std::string sent = random_binary_blob(64 * 1024, /*seed=*/42);
    const std::string got = echo_roundtrip(socket, sent);

    REQUIRE(got.size() == sent.size());
    REQUIRE(got == sent);
}

TEST_CASE("binary payload with embedded NUL bytes is not corrupted", "[echo][binary]") {
    asio::io_context io;
    tcp::socket socket = connect_to_server(io);

    std::string sent = random_binary_blob(1024, /*seed=*/7);
    sent[10] = '\0';
    sent[500] = '\0';

    const std::string got = echo_roundtrip(socket, sent);
    REQUIRE(got == sent);
}

TEST_CASE("many concurrent connections are served independently", "[echo][concurrency]") {
    constexpr int kClients = 16;
    std::vector<std::thread> workers;
    std::vector<std::string> results(kClients);
    std::vector<std::exception_ptr> errors(kClients);

    for (int i = 0; i < kClients; ++i) {
        workers.emplace_back([i, &results, &errors] {
            try {
                asio::io_context io;
                tcp::socket socket = connect_to_server(io);
                const std::string sent = "client-" + std::to_string(i) + "-payload";
                results[i] = echo_roundtrip(socket, sent);
            } catch (...) {
                errors[i] = std::current_exception();
            }
        });
    }
    for (auto &t : workers) {
        t.join();
    }

    for (int i = 0; i < kClients; ++i) {
        INFO("client index " << i);
        REQUIRE_FALSE(errors[i]);
        REQUIRE(results[i] == "client-" + std::to_string(i) + "-payload");
    }
}
