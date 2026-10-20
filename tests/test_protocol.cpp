#include <catch2/catch_test_macros.hpp>

#include "protocol/memcached_parser.h"
#include "server/comm/worker.h"

#include <array>
#include <chrono>
#include <functional>
#include <string>

namespace {
using namespace fastest;
using namespace std::chrono_literals;

ParseBatchResult parse(const std::string_view input, ParserLimits limits = {}, std::size_t batch = 128) {
	return MemcachedParser(limits).parse(std::span(input.data(), input.size()), batch);
}

std::string shard_key(std::size_t shard) {
	for (unsigned i = 0; ; ++i) {
		std::string key = "key" + std::to_string(i);
		if (util::get_shard(ItemKey(key).hash, 2) == shard) {
			return key;
		}
	}
}

// Prvalue elements construct directly in the array; Worker need not be movable.
std::array<Worker, 2> make_test_workers(const std::shared_ptr<std::atomic_bool> &stop) {
	auto comms = Worker::Sc::create_shard_comm(2);
	return {Worker(std::move(comms[0]), stop), Worker(std::move(comms[1]), stop)};
}

// Mailboxes start only after commands/errors have been queued. This makes the
// dangerous ordering deterministic: local errors arrive before remote replies.
struct WorkerFixture {
	asio::io_context io;
	std::shared_ptr<std::atomic_bool> stop = std::make_shared<std::atomic_bool>(false);
	std::array<Worker, 2> workers = make_test_workers(stop);
	tcp::socket client {io};
	static constexpr uint64_t id = 42;

	WorkerFixture() {
		tcp::acceptor acceptor(io, {asio::ip::address_v4::loopback(), 0});
		client.connect(acceptor.local_endpoint());
		auto socket = acceptor.accept();
		workers[0].tcp_session_.emplace(id, std::make_unique<TCPSession>(std::move(socket)));
	}

	~WorkerFixture() {
		stop->store(true);
		io.stop();
	}

	TCPSession &session() { return *workers[0].tcp_session_.at(id); }

	void start_mailboxes() {
		for (auto &worker : workers) {
			asio::co_spawn(io, worker.mailbox_request_loop(), asio::detached);
			asio::co_spawn(io, worker.mailbox_response_loop(), asio::detached);
		}
	}

	void until(const std::function<bool()> &done) {
		const auto deadline = std::chrono::steady_clock::now() + 2s;
		while (!done() && std::chrono::steady_clock::now() < deadline) {
			io.restart();
			io.run_one_for(10ms);
		}
		REQUIRE(done());
	}

	std::string read(std::size_t size) {
		std::string result(size, '\0');
		bool done = false;
		boost::system::error_code error;
		asio::async_read(client, asio::buffer(result), [&](auto ec, std::size_t) {
			error = ec;
			done = true;
		});
		// Cancel before throwing so the completion cannot outlive its captures.
		const auto deadline = std::chrono::steady_clock::now() + 2s;
		while (!done && std::chrono::steady_clock::now() < deadline) {
			io.restart();
			io.run_one_for(10ms);
		}
		if (!done) {
			client.cancel();
			io.restart();
			io.poll();
		}
		REQUIRE(done);
		REQUIRE_FALSE(error);
		return result;
	}

	void set(const std::string &key, std::string value) {
		workers[0].send_single_command(SingleKeyCommand(SetCommand {ItemKey(key), 0, 0, std::move(value)}), id);
	}

	void get(const std::string &key) {
		workers[0].send_single_command(SingleKeyCommand(SingleGetCommand {ItemKey(key)}), id);
	}
};
}

TEST_CASE("parse errors preserve the executable prefix and memcached recovery boundary", "[protocol]") {
	const std::string prefix = "set k 0 0 1\r\nx\r\n";
	const std::string malformed = "set k invalid 0 1\r\n";
	const std::string input = prefix + malformed + "x\r\nget k\r\n";
	auto result = parse(input);
	REQUIRE(result.commands.size() == 1);
	REQUIRE(command_type(result.commands[0]) == CommandType::Set);
	REQUIRE(result.stop == StopReason::Error);
	REQUIRE(result.error->code == ParseErrorCode::InvalidNumber);
	REQUIRE(result.error->recovery == ErrorRecovery::Continue);
	REQUIRE(result.error->offset == prefix.size());
	REQUIRE(result.consumed == prefix.size() + malformed.size());
	REQUIRE(result.discard_bytes == 0);
	REQUIRE(to_error_response(*result.error).message == "bad command line format");

	// A bad header does not claim its following value: memcached treats it as
	// the next command, so it generates a second, independent ERROR.
	auto tail = std::string_view(input).substr(result.consumed);
	auto body = parse(tail);
	REQUIRE(body.error->code == ParseErrorCode::UnknownCommand);
	REQUIRE(body.consumed == 3);
	auto last = parse(tail.substr(body.consumed));
	REQUIRE(last.commands.size() == 1);
	REQUIRE(command_type(last.commands[0]) == CommandType::Get);
}

TEST_CASE("fragmented storage and batch limits do not consume incomplete frames", "[protocol]") {
	std::string input = "get missing\nset k 0 0 3\r\nab";
	auto limited = parse(input, {}, 1);
	REQUIRE(limited.stop == StopReason::BatchLimit);
	REQUIRE(limited.consumed == 12);
	auto incomplete = parse(std::string_view(input).substr(limited.consumed));
	REQUIRE(incomplete.stop == StopReason::NeedMoreData);
	REQUIRE(incomplete.consumed == 0);
	REQUIRE(incomplete.minimum_additional_bytes == 3);
	input += "c\r\n";
	auto complete = parse(std::string_view(input).substr(limited.consumed));
	REQUIRE(complete.commands.size() == 1);
	REQUIRE(complete.stop == StopReason::EndOfInput);
	REQUIRE(std::get<SetCommand>(std::get<SingleKeyCommand>(complete.commands[0]).command).value == "abc");
}

TEST_CASE("line errors and noreply match the classic text protocol", "[protocol]") {
	for (const auto input : {"get\r\n", "set k 0 0\r\n", "\n"}) {
		auto result = parse(input);
		REQUIRE(result.stop == StopReason::Error);
		REQUIRE(to_error_response(*result.error).kind == ResponseErrorKind::Error);
		REQUIRE(result.error->recovery == ErrorRecovery::Continue);
	}
	auto invalid = parse("set k invalid 0 1 noreply\r\n");
	REQUIRE(invalid.error->noreply);
	REQUIRE(to_error_response(*invalid.error).noreply);
	const std::string bad_chunk = "set k 0 0 1 noreply\nx!!";
	auto bad = parse(bad_chunk + "get k\n");
	REQUIRE(bad.error->code == ParseErrorCode::BadDataChunk);
	REQUIRE(bad.error->noreply);
	REQUIRE(bad.error->recovery == ErrorRecovery::Continue);
	REQUIRE(bad.consumed == bad_chunk.size());
	REQUIRE(parse(std::string_view(bad_chunk).substr(0, bad_chunk.size() - 1)).stop == StopReason::NeedMoreData);
}

TEST_CASE("oversized values reject from the header and declare a bounded swallow", "[protocol]") {
	ParserLimits limits;
	limits.max_value_bytes = 4;
	const std::string header = "set k 0 0 20 noreply\r\n";
	auto result = parse(header, limits);
	REQUIRE(result.commands.empty());
	REQUIRE(result.error->code == ParseErrorCode::ValueTooLarge);
	REQUIRE(result.error->rejected_set_key->key == "k");
	REQUIRE(result.consumed == header.size());
	REQUIRE(result.discard_bytes == 22);
	REQUIRE(result.error->recovery == ErrorRecovery::Continue);
	REQUIRE(to_error_response(*result.error).kind == ResponseErrorKind::ServerError);
	REQUIRE(result.error->noreply);
	auto add = parse("add k 0 0 20\r\n", limits);
	REQUIRE_FALSE(add.error->rejected_set_key);
	auto invalid = parse("set k 0 0 2147483646\r\n", limits);
	REQUIRE(invalid.error->code == ParseErrorCode::InvalidNumber);
	REQUIRE(invalid.discard_bytes == 0);
}

TEST_CASE("delayed remote payload survives input reuse and errors retain wire order", "[protocol]") {
	WorkerFixture f;
	const auto key = shard_key(1);
	const std::string value(2048, 'v');
	std::string input = "set " + key + " 0 0 2048\r\n" + value + "\r\nbogus\r\n";
	auto batch = parse(input);
	f.workers[0].send_single_command(std::move(std::get<SingleKeyCommand>(batch.commands[0])), f.id);
	f.workers[0].send_error_command(to_error_response(*batch.error), f.id);
	input.assign(input.size(), '!'); // remote mailbox has not consumed the value
	batch.commands.clear();
	f.get(key);
	REQUIRE(f.session().ring_array.next_id() == 0);
	REQUIRE(f.session().ring_array.get(1) != nullptr);
	f.start_mailboxes();
	const auto expected = "STORED\r\nERROR\r\nVALUE " + key + " 0 2048\r\n" + value + "\r\nEND\r\n";
	REQUIRE(f.read(expected.size()) == expected);
	REQUIRE_FALSE(f.session().closing);
}

TEST_CASE("fatal errors drain earlier remote replies before closing and reclaiming the session", "[protocol]") {
	WorkerFixture f;
	f.get(shard_key(1));
	f.workers[0].send_error_command(ErrorResponse {ResponseErrorKind::ClientError, "command line too long", true}, f.id);
	f.session().protocol_error_waiting = true;
	f.session().session_exited = true;
	REQUIRE_FALSE(f.session().closing);
	REQUIRE(f.session().ring_array.next_id() == 0);
	f.start_mailboxes();
	f.until([&] { return !f.workers[0].tcp_session_.contains(f.id); });
	const std::string expected = "END\r\nCLIENT_ERROR command line too long\r\n";
	REQUIRE(f.read(expected.size()) == expected);
	char byte;
	boost::system::error_code ec;
	f.client.read_some(asio::buffer(&byte, 1), ec);
	REQUIRE(ec == asio::error::eof);
}

TEST_CASE("suppressed errors still advance the response sequence", "[protocol]") {
	WorkerFixture f;
	f.get(shard_key(1));
	f.workers[0].send_error_command(ErrorResponse {ResponseErrorKind::ClientError, "bad data chunk", false, true}, f.id);
	f.get(shard_key(0));
	f.start_mailboxes();
	REQUIRE(f.read(10) == "END\r\nEND\r\n");
	REQUIRE(f.session().ring_array.next_id() == 3);
}

TEST_CASE("oversized SET removes old values in the same shard order as successful SET", "[protocol]") {
	for (std::size_t shard = 0; shard != 2; ++shard) {
		CAPTURE(shard);
		WorkerFixture f;
		const auto key = shard_key(shard);
		f.set(key, "old");
		ParserLimits limits;
		limits.max_value_bytes = 4;
		auto rejected = parse("set " + key + " 0 0 20\r\n", limits);
		f.workers[0].send_error_command(to_error_response(*rejected.error), f.id,
		                                std::move(rejected.error->rejected_set_key), true);
		f.get(key);
		f.start_mailboxes();
		f.until([&] { return f.session().ring_array.get(2) != nullptr; });
		REQUIRE(f.session().ring_array.next_id() == 1);
		REQUIRE(f.read(8) == "STORED\r\n");
		REQUIRE(f.client.available() == 0); // error is waiting for the discarded body
		f.workers[0].finish_swallow(f.id);
		const std::string expected = "SERVER_ERROR object too large for cache\r\nEND\r\n";
		REQUIRE(f.read(expected.size()) == expected);
	}
}

TEST_CASE("multi-get waits for all shards before a subsequent parse error", "[protocol]") {
	WorkerFixture f;
	const auto local = shard_key(0);
	const auto remote = shard_key(1);
	f.set(local, "L");
	f.set(remote, "R");
	f.workers[0].send_multi_command(MultiKeyCommand(MultiGetCommand {{ItemKey(local), ItemKey(remote)}}), f.id);
	f.workers[0].send_error_command(ErrorResponse {}, f.id);
	REQUIRE(f.session().ring_array.next_id() == 1);
	REQUIRE_FALSE(f.session().ring_array.is_ready());
	f.start_mailboxes();
	const auto expected = "STORED\r\nSTORED\r\nVALUE " + local + " 0 1\r\nL\r\nVALUE " + remote + " 0 1\r\nR\r\nEND\r\nERROR\r\n";
	REQUIRE(f.read(expected.size()) == expected);
	REQUIRE(f.session().ring_array.next_id() == 4);
}

TEST_CASE("GatherSink copies short fields contiguously and references long values", "[protocol]") {
	const std::string big(4096, 'x');
	const SingleKeyResponse small {SingleGetResponse {.item = ValueItem {"k", 3, "v"}}};
	const SingleKeyResponse large {SingleGetResponse {.item = ValueItem {"big", 0, big}}};
	GatherSink out;
	REQUIRE_FALSE(encode_to(out, small));
	REQUIRE_FALSE(encode_to(out, large));
	const auto buffers = out.buffers();
	// 两条响应的协议文本合并成一段，长 value 单独一段且直接引用原字符串。
	REQUIRE(buffers.size() == 3);
	REQUIRE(buffers[1].data() == std::get<SingleGetResponse>(large.variant()).item->value.data());
	std::string wire;
	for (const auto &buffer : buffers) {
		wire.append(static_cast<const char *>(buffer.data()), buffer.size());
	}
	REQUIRE(wire == "VALUE k 3 1\r\nv\r\nEND\r\nVALUE big 0 4096\r\n" + big + "\r\nEND\r\n");
	REQUIRE(out.size_bytes() == wire.size());
	out.clear();
	REQUIRE(out.empty());
}

TEST_CASE("GatherSink rollback drops a half-encoded response", "[protocol]") {
	const SingleKeyResponse stored {SetResponse {}};
	const MultiKeyResponse bad {MultiGetResponse {.items = {ValueItem {"a", 0, "1"}, ValueItem {"bad key", 0, "2"}}}};
	GatherSink out;
	REQUIRE_FALSE(encode_to(out, stored));
	const auto mark = out.mark();
	REQUIRE_THROWS_AS(encode_to(out, bad), std::invalid_argument); // 第一项已经追加，第二项校验失败
	out.rollback(mark);
	REQUIRE_FALSE(encode_to(out, ErrorResponse {ResponseErrorKind::ServerError, "response encoding failed"}));
	std::string wire;
	for (const auto &buffer : out.buffers()) {
		wire.append(static_cast<const char *>(buffer.data()), buffer.size());
	}
	REQUIRE(wire == "STORED\r\nSERVER_ERROR response encoding failed\r\n");
}

TEST_CASE("responses that become ready during a write leave together as the next batch", "[protocol]") {
	WorkerFixture f;
	const auto key = shard_key(0);
	f.set(key, "v"); // 本分片：立即开始第一次写
	REQUIRE(f.session().writing);
	REQUIRE(f.session().inflight.size() == 1);
	f.get(key);
	f.get(key);
	f.get(key); // 写在途：只入队，不等待
	REQUIRE(f.session().inflight.size() == 1);
	REQUIRE(f.session().backlog() == 3);
	f.until([&] { return f.session().inflight.size() == 3; }); // 第一次写完成后，三条合成一次写
	REQUIRE(f.session().backlog() == 0);
	const std::string reply = "VALUE " + key + " 0 1\r\nv\r\nEND\r\n";
	REQUIRE(f.read(8 + 3 * reply.size()) == "STORED\r\n" + reply + reply + reply);
	f.until([&] { return !f.session().writing; });
}
