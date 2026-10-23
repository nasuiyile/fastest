#include <catch2/catch_test_macros.hpp>

#include "protocol/memcached_parser.h"
#include "server/comm/input_buffer.h"
#include "server/comm/worker.h"

#include <poll.h>

#include <algorithm>
#include <array>
#include <atomic>
#include <chrono>
#include <cstring>
#include <functional>
#include <random>
#include <set>
#include <string>
#include <thread>
#include <vector>

namespace {
using namespace fastest;
using namespace std::chrono_literals;

ParseBatchResult parse(const std::string_view input, ParserLimits limits = {}, std::size_t batch = 128) {
	return MemcachedParser(limits).parse(std::span(input.data(), input.size()), batch);
}

std::vector<std::string> shard_keys(std::size_t shard, std::size_t count) {
	std::vector<std::string> keys;
	for (unsigned i = 0; keys.size() < count; ++i) {
		std::string key = "key" + std::to_string(i);
		if (util::get_shard(ItemKey(key).hash, 2) == shard) {
			keys.push_back(std::move(key));
		}
	}
	return keys;
}

std::string shard_key(std::size_t shard) {
	return shard_keys(shard, 1).front();
}

SingleKeyResponse cache_set(ShardCache &cache, const std::string &key, CacheValuePtr value) {
	return cache.handle_single_command(SingleKeyCommand(SetCommand {ItemKey(key), std::move(value)}));
}

std::optional<ValueItem> cache_get(ShardCache &cache, const std::string &key) {
	auto response = cache.handle_single_command(SingleKeyCommand(SingleGetCommand {ItemKey(key)}));
	return std::move(std::get<SingleGetResponse>(response.variant()).item);
}

// 存储命令里的 value（解析时已写好 flags / expired_at / cas）。
const CacheValue &command_value(const Command &command) {
	return std::visit(
	    [](const auto &c) -> const CacheValue & {
		    if constexpr (requires { c.value.get(); }) {
			    return *c.value;
		    } else {
			    throw std::logic_error("not a storage command");
		    }
	    },
	    std::get<SingleKeyCommand>(command).command);
}

SingleKeyResponse execute(ShardCache &cache, const std::string_view input) {
	auto batch = parse(input);
	REQUIRE(batch.commands.size() == 1);
	return cache.handle_single_command(std::move(std::get<SingleKeyCommand>(batch.commands[0])));
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
		workers[0].sessions().add(id, std::move(socket));
	}

	~WorkerFixture() {
		stop->store(true);
		io.stop();
	}

	TCPSession &session() {
		TCPSession *session = workers[0].sessions().find(id);
		REQUIRE(session != nullptr);
		return *session;
	}

	void start_mailboxes() {
		for (auto &worker : workers) {
			asio::co_spawn(io, worker.mailbox_loop(), asio::detached);
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

	// 和 Handler::session 一样，跨分片发送和响应入环都放在 BatchScope 里，离开时敲门、发起写。
	void set(const std::string &key, std::string_view value) {
		Worker::BatchScope scope(workers[0]);
		workers[0].send_single_command(SingleKeyCommand(SetCommand {ItemKey(key), CacheValue::create(value)}), id);
	}

	void get(const std::string &key) {
		Worker::BatchScope scope(workers[0]);
		queue_get(key);
	}

	// 只入队、不敲门、不发起写：放在测试自己开的 BatchScope 里用。
	void queue_get(const std::string &key) {
		workers[0].send_single_command(SingleKeyCommand(SingleGetCommand {ItemKey(key)}), id);
	}

	void error(const ErrorResponse &response) {
		Worker::BatchScope scope(workers[0]);
		workers[0].send_error_command(response, id);
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
	REQUIRE(std::get<SetCommand>(std::get<SingleKeyCommand>(complete.commands[0]).command).value->view() == "abc");
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
	{
		Worker::BatchScope scope(f.workers[0]);
		f.workers[0].send_single_command(std::move(std::get<SingleKeyCommand>(batch.commands[0])), f.id);
		f.workers[0].send_error_command(to_error_response(*batch.error), f.id);
	}
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
	f.error(ErrorResponse {ResponseErrorKind::ClientError, "command line too long", true});
	f.session().protocol_error_waiting = true;
	f.session().session_exited = true;
	REQUIRE_FALSE(f.session().closing);
	REQUIRE(f.session().ring_array.next_id() == 0);
	f.start_mailboxes();
	f.until([&] { return f.workers[0].sessions().find(f.id) == nullptr; });
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
	f.error(ErrorResponse {ResponseErrorKind::ClientError, "bad data chunk", false, true});
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
		{
			Worker::BatchScope scope(f.workers[0]);
			f.workers[0].send_error_command(to_error_response(*rejected.error), f.id,
			                                std::move(rejected.error->rejected_set_key), true);
		}
		f.get(key);
		f.start_mailboxes();
		f.until([&] { return f.session().ring_array.get(2) != nullptr; });
		REQUIRE(f.session().ring_array.next_id() == 1);
		REQUIRE(f.read(8) == "STORED\r\n");
		REQUIRE(f.client.available() == 0); // error is waiting for the discarded body
		f.workers[0].sessions().finish_swallow(f.id);
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
	{
		Worker::BatchScope scope(f.workers[0]);
		f.workers[0].send_multi_command(MultiKeyCommand(MultiGetCommand {{ItemKey(local), ItemKey(remote)}}), f.id);
		f.workers[0].send_error_command(ErrorResponse {}, f.id);
	}
	REQUIRE(f.session().ring_array.next_id() == 1);
	REQUIRE_FALSE(f.session().ring_array.is_ready());
	f.start_mailboxes();
	const auto expected = "STORED\r\nSTORED\r\nVALUE " + local + " 0 1\r\nL\r\nVALUE " + remote + " 0 1\r\nR\r\nEND\r\nERROR\r\n";
	REQUIRE(f.read(expected.size()) == expected);
	REQUIRE(f.session().ring_array.next_id() == 4);
}

TEST_CASE("GatherSink copies short fields contiguously and references long values", "[protocol]") {
	const std::string big(4096, 'x');
	const SingleKeyResponse small {SingleGetResponse {.item = ValueItem {"k", CacheValue::create("v", 3)}}};
	const SingleKeyResponse large {SingleGetResponse {.item = ValueItem {"big", CacheValue::create(big)}}};
	GatherSink out;
	REQUIRE_FALSE(encode_to(out, small));
	REQUIRE_FALSE(encode_to(out, large));
	const auto buffers = out.buffers();
	// 两条响应的协议文本合并成一段；长 value 连同它自带的 CRLF 单独一段，直接引用 CacheValue。
	REQUIRE(buffers.size() == 3);
	const CacheValue &large_value = *std::get<SingleGetResponse>(large.variant()).item->value;
	REQUIRE(buffers[1].data() == large_value.view_crlf().data());
	REQUIRE(buffers[1].size() == big.size() + 2);
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
	MultiGetResponse items;
	items.items.push_back(ValueItem {"a", CacheValue::create("1")});
	items.items.push_back(ValueItem {"bad key", CacheValue::create("2")});
	const MultiKeyResponse bad {std::move(items)};
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
	REQUIRE(f.session().ring_array.sent_count() == 1);
	f.get(key);
	f.get(key);
	f.get(key); // 写在途：只入队，不等待
	REQUIRE(f.session().ring_array.sent_count() == 1);
	REQUIRE(f.session().backlog() == 3);
	f.until([&] { return f.session().ring_array.sent_count() == 3; }); // 第一次写完成后，三条合成一次写
	REQUIRE(f.session().backlog() == 0);
	const std::string reply = "VALUE " + key + " 0 1\r\nv\r\nEND\r\n";
	REQUIRE(f.read(8 + 3 * reply.size()) == "STORED\r\n" + reply + reply + reply);
	f.until([&] { return !f.session().writing; });
	REQUIRE(f.session().ring_array.sent_count() == 0); // 写完成后释放已发送槽位
}

TEST_CASE("replies produced in one batch leave in a single write", "[protocol]") {
	WorkerFixture f;
	auto &worker = f.workers[0];
	const auto key = shard_key(0);
	constexpr int kGets = 10;
	{
		Worker::BatchScope scope(worker);
		for (int i = 0; i != kGets; ++i) {
			f.queue_get(key); // 本分片：立即执行并入环，但不在这里发起写
		}
		REQUIRE_FALSE(f.session().writing);
		REQUIRE(f.session().ring_array.next_id() == 0);
		REQUIRE(worker.socket_writes() == 0);
	}
	REQUIRE(f.session().writing); // 离开作用域：一次写带上全部响应
	REQUIRE(f.session().ring_array.sent_count() == kGets);
	REQUIRE(worker.socket_writes() == 1);
	std::string expected;
	for (int i = 0; i != kGets; ++i) {
		expected += "END\r\n";
	}
	REQUIRE(f.read(expected.size()) == expected);
	f.until([&] { return !f.session().writing; });
	REQUIRE(worker.socket_writes() == 1);
}

TEST_CASE("remote replies drained in one mailbox round leave in a single write", "[protocol]") {
	WorkerFixture f;
	auto &worker = f.workers[0];
	const auto remote = shard_key(1);
	constexpr int kGets = 10;
	{
		Worker::BatchScope scope(worker);
		for (int i = 0; i != kGets; ++i) {
			f.queue_get(remote);
		}
	}
	REQUIRE(worker.socket_writes() == 0); // 全部在等远端，没有可写的
	f.start_mailboxes();
	std::string expected;
	for (int i = 0; i != kGets; ++i) {
		expected += "END\r\n";
	}
	REQUIRE(f.read(expected.size()) == expected);
	REQUIRE(worker.socket_writes() == 1); // 10 条响应在收件协程的同一轮到达，一次写发出
}

TEST_CASE("a session waiting for its batched write is reclaimed only after the write", "[protocol]") {
	WorkerFixture f;
	auto &worker = f.workers[0];
	{
		Worker::BatchScope scope(worker);
		f.queue_get(shard_key(0)); // 入环，等作用域结束再写
		REQUIRE(f.session().kick_pending);
		f.session().session_exited = true;
		worker.sessions().reclaim_if_idle(f.id, f.session());
		REQUIRE(worker.sessions().find(f.id) != nullptr); // 还在待写列表里：不能回收
	}
	REQUIRE(f.read(5) == "END\r\n");
	f.until([&] { return worker.sessions().find(f.id) == nullptr; }); // 写完成后由写回调回收
}

TEST_CASE("RingArray keeps sent slots until release and drops their late replies", "[ring]") {
	const auto error = [](uint32_t seq, ResponseErrorKind kind) {
		ErrorResponse response;
		response.kind = kind;
		return ResponseMessage {.tcp_id = 1, .seq = seq, .shard_id = 0, .data = response};
	};
	RingArray ring(4);
	ring.append(error(0, ResponseErrorKind::Error));
	ring.append(error(1, ResponseErrorKind::Error));
	REQUIRE(ring.front()->seq == 0);
	ring.advance();
	REQUIRE(ring.front()->seq == 1);
	ring.advance();
	REQUIRE(ring.front() == nullptr);
	REQUIRE(ring.next_id() == 2);
	REQUIRE(ring.sent_count() == 2);

	// 已发送的序号再来一条：丢弃，不改写在途槽位。
	ring.append(error(0, ResponseErrorKind::ServerError));
	REQUIRE(std::get<ErrorResponse>(ring.get(0)->data).kind == ResponseErrorKind::Error);

	// 扩容要算上在途槽位：head 仍是 0。
	for (uint32_t seq = 2; seq != 10; ++seq) {
		ring.append(error(seq, ResponseErrorKind::Error));
	}
	REQUIRE(ring.capacity() == 16);
	REQUIRE(ring.get(0)->seq == 0);
	REQUIRE(ring.get(1)->seq == 1);
	REQUIRE(ring.sent_count() == 2);

	ring.release_sent();
	REQUIRE(ring.get(0) == nullptr);
	REQUIRE(ring.get(1) == nullptr);
	REQUIRE(ring.sent_count() == 0);
	REQUIRE(ring.front()->seq == 2);
}

TEST_CASE("multi-key parts encode in arrival order with one END and no merge", "[protocol]") {
	std::vector<ShardResponse> parts;
	parts.push_back(ShardResponse {
	    .count = 2, .command = MultiKeyResponse {MultiGetResponse {.items = {ValueItem {"a", CacheValue::create("A", 1)}}}}});
	parts.push_back(ShardResponse {.count = 2,
	                               .command = MultiKeyResponse {MultiGetResponse {
	                                   .items = {ValueItem {"b", CacheValue::create("B", 2)},
	                                             ValueItem {"c", CacheValue::create("C", 3)}}}}});
	const auto project = [](const ShardResponse &part) -> const MultiKeyResponse & {
		return part.command;
	};
	std::string wire;
	detail::StringSink sink {wire};
	REQUIRE_FALSE(encode_multi_parts(sink, parts, project));
	REQUIRE(wire == "VALUE a 1 1\r\nA\r\nVALUE b 2 1\r\nB\r\nVALUE c 3 1\r\nC\r\nEND\r\n");

	parts.push_back(ShardResponse {.count = 2, .command = MultiKeyResponse {MultiGetsResponse {}}});
	REQUIRE_THROWS_AS(encode_multi_parts(sink, parts, project), std::logic_error);
	REQUIRE_THROWS_AS(encode_multi_parts(sink, std::vector<ShardResponse> {}, project), std::invalid_argument);
}

TEST_CASE("sent slots stay valid while the ring grows under a blocked write", "[protocol]") {
	WorkerFixture f;
	const auto big = shard_keys(0, 2)[0];
	const auto small = shard_keys(0, 2)[1];
	const std::string value(64 * 1024, 'x'); // > kInlineBytes：GatherSink 只引用，不拷贝
	f.set(big, value);
	f.set(small, "s");
	REQUIRE(f.read(16) == "STORED\r\nSTORED\r\n");
	f.until([&] { return !f.session().writing; });

	// 收发缓冲调到很小，让下面这次写只能发出一部分，保持在途。
	f.session().socket.set_option(asio::socket_base::send_buffer_size(4096));
	f.client.set_option(asio::socket_base::receive_buffer_size(4096));
	f.get(big);
	REQUIRE(f.session().writing);
	REQUIRE(f.session().ring_array.sent_count() == 1);
	const auto initial_capacity = f.session().ring_array.capacity();
	constexpr int kQueued = 300;
	for (int i = 0; i != kQueued; ++i) {
		f.get(small); // 写在途：只入队；超过初始容量时环扩容，搬动在途槽位
	}
	REQUIRE(f.session().ring_array.capacity() > initial_capacity);
	REQUIRE(f.session().ring_array.sent_count() == 1);

	std::string expected = "VALUE " + big + " 0 " + std::to_string(value.size()) + "\r\n" + value + "\r\nEND\r\n";
	const std::string reply = "VALUE " + small + " 0 1\r\ns\r\nEND\r\n";
	for (int i = 0; i != kQueued; ++i) {
		expected += reply;
	}
	REQUIRE(f.read(expected.size()) == expected);
	f.until([&] { return !f.session().writing; });
	REQUIRE(f.session().ring_array.sent_count() == 0);
}

TEST_CASE("the parser copies a value once into its final CacheValue", "[cache_value]") {
	auto result = parse("set k 5 0 3\r\nabc\r\n");
	REQUIRE(result.commands.size() == 1);
	const auto &set = std::get<SetCommand>(std::get<SingleKeyCommand>(result.commands[0]).command);
	REQUIRE(set.value->view() == "abc");
	REQUIRE(set.value->view_crlf() == "abc\r\n");
	REQUIRE(set.value->flags == 5);
	REQUIRE(set.value->use_count() == 1);
}

TEST_CASE("large data blocks are read straight into the final value", "[cache_value]") {
	const std::string prefix = "get a\r\n";
	const std::string header = "set big 7 0 10000 noreply\r\n";
	const std::string arrived(100, 'x');
	const std::string input = prefix + header + arrived;
	auto result = parse(input);
	REQUIRE(result.commands.size() == 1); // 前面的命令照常返回
	REQUIRE(result.stop == StopReason::PendingValue);
	REQUIRE(result.consumed == input.size()); // 已到达的 value 字节已拷进 value，输入可以整体消费
	REQUIRE(result.pending);
	PendingValue &pending = *result.pending;
	REQUIRE(pending.offset == prefix.size());
	REQUIRE(pending.noreply);
	REQUIRE(pending.filled == arrived.size());
	REQUIRE(pending.frame.size() == 10002);

	const auto rest = pending.unfilled();
	REQUIRE(rest.size() == 10002 - arrived.size());
	std::memset(rest.data(), 'y', rest.size() - 2);
	std::memcpy(rest.data() + rest.size() - 2, "\r\n", 2);
	pending.filled = pending.frame.size();
	const CacheValue *raw = std::get<SetCommand>(pending.command.command).value.get();

	auto completed = MemcachedParser::complete(std::move(pending));
	REQUIRE_FALSE(completed.error);
	REQUIRE(completed.consumed == 0);
	REQUIRE(completed.commands.size() == 1);
	const auto &set = std::get<SetCommand>(std::get<SingleKeyCommand>(completed.commands[0]).command);
	REQUIRE(set.value.get() == raw); // 同一块内存，没有再拷贝
	REQUIRE(set.value->flags == 7);
	REQUIRE(set.noreply);
	REQUIRE(set.value->view() == arrived + std::string(10000 - arrived.size(), 'y'));
}

TEST_CASE("a direct-read data block without CRLF is a recoverable bad data chunk", "[cache_value]") {
	auto result = parse("set k 0 0 5000\r\n");
	REQUIRE(result.stop == StopReason::PendingValue);
	REQUIRE(result.commands.empty());
	PendingValue &pending = *result.pending;
	REQUIRE(pending.filled == 0);
	std::memset(pending.unfilled().data(), '!', pending.unfilled().size());
	REQUIRE_THROWS_AS(MemcachedParser::complete(PendingValue(pending)), std::logic_error); // 还没读满
	pending.filled = pending.frame.size();
	auto completed = MemcachedParser::complete(std::move(pending));
	REQUIRE(completed.commands.empty());
	REQUIRE(completed.error->code == ParseErrorCode::BadDataChunk);
	REQUIRE(completed.error->recovery == ErrorRecovery::Continue);
	REQUIRE_FALSE(completed.error->noreply);
}

TEST_CASE("small gaps in a data block still wait in the input buffer", "[cache_value]") {
	const std::string input = "set k 0 0 10000\r\n" + std::string(9000, 'x');
	auto result = parse(input);
	REQUIRE(result.stop == StopReason::NeedMoreData);
	REQUIRE(result.consumed == 0);
	REQUIRE(result.minimum_additional_bytes == 1002);
	ParserLimits never;
	never.direct_read_min_bytes = std::numeric_limits<std::size_t>::max();
	REQUIRE(parse("set k 0 0 100000\r\n", never).stop == StopReason::NeedMoreData);
}

TEST_CASE("ShardCache stores and serves CacheValue by reference count", "[cache_value]") {
	std::optional<ValueItem> hit;
	{
		ShardCache cache;
		CacheValuePtr value = CacheValue::create("hello", 9);
		const CacheValue *raw = value.get();
		REQUIRE(std::get<SetResponse>(cache_set(cache, "k", std::move(value)).variant()).status ==
		        StorageStatus::Stored);
		REQUIRE(cache.peek(ItemKey("k")) == raw); // map 接管了解析时创建的那块内存
		REQUIRE(raw->use_count() == 1);

		hit = cache_get(cache, "k");
		REQUIRE(hit);
		REQUIRE(hit->key == "k");
		REQUIRE(hit->value.get() == raw); // 共享同一块内存，不复制 value
		REQUIRE(raw->use_count() == 2);
		REQUIRE_FALSE(cache_get(cache, "missing"));

		// 覆盖：map 释放旧值的那次计数，响应里的引用不受影响。
		cache_set(cache, "k", CacheValue::create("new"));
		REQUIRE(raw->use_count() == 1);
		REQUIRE(hit->value->view() == "hello");
		REQUIRE(cache_get(cache, "k")->value->view() == "new");

		cache.erase_rejected_set(ItemKey("k"));
		REQUIRE_FALSE(cache_get(cache, "k"));
		cache_set(cache, "left", CacheValue::create("x"));
		REQUIRE(cache.size() == 1);
	}
	// map 析构不影响仍被响应引用的旧值。
	REQUIRE(hit->value->use_count() == 1);
	REQUIRE(hit->value->view() == "hello");
}

TEST_CASE("only an unpublished CacheValue can be stored", "[cache_value]") {
	ShardCache cache;
	CacheValuePtr shared = CacheValue::create("v");
	const CacheValuePtr other = shared; // 已被别处引用：改写 cas 会和读者竞争
	REQUIRE_THROWS_AS(cache_set(cache, "k", std::move(shared)), std::logic_error);
	REQUIRE(cache.size() == 0);
	REQUIRE(other->use_count() == 1);
	REQUIRE(other->cas == 0);
	REQUIRE_THROWS_AS(cache_set(cache, "k", CacheValuePtr {}), std::logic_error);
}

TEST_CASE("ShardCache keeps every value through incremental rehash", "[cache_value]") {
	ShardCache cache;
	constexpr int kCount = 20000;
	for (int i = 0; i < kCount; ++i) {
		cache_set(cache, "key:" + std::to_string(i), CacheValue::create(std::to_string(i * 7), i));
	}
	REQUIRE(cache.size() == kCount);
	for (int i = 0; i < kCount; ++i) {
		auto hit = cache_get(cache, "key:" + std::to_string(i));
		REQUIRE(hit);
		REQUIRE(hit->value->view() == std::to_string(i * 7));
		REQUIRE(hit->value->flags == static_cast<std::uint32_t>(i));
		REQUIRE(hit->value->use_count() == 2);
	}
	REQUIRE(cache.used_bytes() > kCount * sizeof(CacheValue));
}

TEST_CASE("multi-get moves keys into the response and shares values", "[cache_value]") {
	ShardCache cache;
	CacheValuePtr a = CacheValue::create("A", 1);
	const CacheValue *raw_a = a.get();
	cache_set(cache, "a", std::move(a));
	cache_set(cache, "b", CacheValue::create("B", 2));
	auto response = cache.handle_multi_command(
	    MultiKeyCommand(MultiGetCommand {{ItemKey("a"), ItemKey("nope"), ItemKey("b")}}));
	const auto &items = std::get<MultiGetResponse>(response.variant()).items;
	REQUIRE(items.size() == 2);
	REQUIRE(items[0].key == "a");
	REQUIRE(items[0].value.get() == raw_a);
	REQUIRE(items[1].key == "b");
	REQUIRE(items[1].value->flags == 2);
	std::string wire;
	detail::StringSink sink {wire};
	REQUIRE_FALSE(encode_to(sink, response));
	REQUIRE(wire == "VALUE a 1 1\r\nA\r\nVALUE b 2 1\r\nB\r\nEND\r\n");
}

TEST_CASE("gets encodes the cas stored in the CacheValue", "[cache_value]") {
	const SingleKeyResponse response {
	    SingleGetsResponse {.item = CasValueItem {"k", CacheValue::create("xyz", 4, 0, 99)}}};
	std::string wire;
	detail::StringSink sink {wire};
	REQUIRE_FALSE(encode_to(sink, response));
	REQUIRE(wire == "VALUE k 4 3 99\r\nxyz\r\nEND\r\n");
}

TEST_CASE("multi-get whose keys share one remote shard is forwarded whole", "[cache_value]") {
	WorkerFixture f;
	const auto keys = shard_keys(1, 2);
	f.set(keys[0], "first");
	f.set(keys[1], "second");
	{
		Worker::BatchScope scope(f.workers[0]);
		f.workers[0].send_multi_command(MultiKeyCommand(MultiGetCommand {{ItemKey(keys[0]), ItemKey(keys[1])}}), f.id);
	}
	f.start_mailboxes();
	const std::string expected = "STORED\r\nSTORED\r\nVALUE " + keys[0] + " 0 5\r\nfirst\r\nVALUE " + keys[1] +
	                             " 0 6\r\nsecond\r\nEND\r\n";
	REQUIRE(f.read(expected.size()) == expected);
}

TEST_CASE("shard selection leaves the low hash bits to the shard's buckets", "[cache_value]") {
	// 4 个分片：同一分片内 key 的低 2 位必须仍然分散，否则 2 的幂桶只用到 1/4。
	std::set<std::uint64_t> low_bits;
	for (unsigned i = 0; low_bits.size() < 4 && i < 10000; ++i) {
		const ItemKey key("k" + std::to_string(i));
		if (util::get_shard(key.hash, 4) == 0) {
			low_bits.insert(key.hash & 3);
		}
	}
	REQUIRE(low_bits.size() == 4);
	for (std::size_t shards : {1u, 2u, 3u, 7u, 64u}) {
		for (unsigned i = 0; i < 1000; ++i) {
			REQUIRE(util::get_shard(ItemKey(std::to_string(i)).hash, shards) < shards);
		}
	}
}

TEST_CASE("InputBuffer reads in place, consumes by offset and compacts lazily", "[input]") {
	InputBuffer input;
	REQUIRE(input.empty());
	auto tail = input.prepare(16);
	REQUIRE(tail.size() >= 16);
	std::memcpy(tail.data(), "get a\r\nget b", 12);
	input.commit(12);
	REQUIRE(std::string_view(input.readable().data(), input.size()) == "get a\r\nget b");
	const char *first = input.readable().data();
	input.consume(7);
	REQUIRE(std::string_view(input.readable().data(), input.size()) == "get b");
	REQUIRE(input.readable().data() == first + 7); // 消费不搬数据

	// 尾部不够时把未消费的半条命令搬到开头。
	const std::size_t cap = input.capacity();
	tail = input.prepare(cap - 5);
	REQUIRE(input.capacity() == cap);
	REQUIRE(input.readable().data() == first);
	REQUIRE(std::string_view(input.readable().data(), input.size()) == "get b");

	// 再不够就扩容，保留已有数据。
	tail = input.prepare(cap * 3);
	REQUIRE(input.capacity() >= cap * 3 + 5);
	REQUIRE(std::string_view(input.readable().data(), input.size()) == "get b");

	// 读空后释放过大的缓冲区。
	tail = input.prepare(InputBuffer::kRetainBytes * 2);
	input.consume(input.size());
	REQUIRE(input.capacity() == 0);
	REQUIRE(input.empty());
}

TEST_CASE("protocol exptime converts to an absolute expired_at", "[expiry]") {
	using util::ExpiryClock;
	constexpr std::int64_t epoch = 1'700'000'000;
	constexpr std::uint32_t now = 1000;
	constexpr auto to = [](std::uint32_t exptime) { return ExpiryClock::to_expired_at(exptime, now, epoch); };
	static_assert(to(0) == ExpiryClock::kNever);
	static_assert(to(1) == now + 1);
	static_assert(to(ExpiryClock::kMaxRelativeSeconds) == now + ExpiryClock::kMaxRelativeSeconds);
	// 超过 30 天按 unix 时间戳处理；早于纪元的视为已经过期，不会回绕到遥远的未来。
	static_assert(to(ExpiryClock::kMaxRelativeSeconds + 1) == ExpiryClock::kExpired);
	static_assert(to(static_cast<std::uint32_t>(epoch)) == ExpiryClock::kExpired);
	static_assert(to(static_cast<std::uint32_t>(epoch + 1)) == ExpiryClock::kExpired);
	static_assert(to(static_cast<std::uint32_t>(epoch + 5000)) == 5000);
	static_assert(ExpiryClock::to_expired_at(10, std::numeric_limits<std::uint32_t>::max() - 5, epoch) ==
	              std::numeric_limits<std::uint32_t>::max());

	const std::uint32_t first = ExpiryClock::now();
	REQUIRE(first >= 2);
	REQUIRE(ExpiryClock::now() >= first);
	REQUIRE(ExpiryClock::to_expired_at(0) == ExpiryClock::kNever);
}

TEST_CASE("storage commands carry every header field inside the CacheValue", "[expiry]") {
	using util::ExpiryClock;
	const std::uint32_t before = ExpiryClock::now();
	auto result = parse("set a 7 100 1\r\nx\r\n"
	                    "cas b 8 0 1 12345 noreply\r\ny\r\n"
	                    "set c 0 2000000000 1\r\nz\r\n"
	                    "set d 0 2592001 1\r\nw\r\n");
	const std::uint32_t after = ExpiryClock::now();
	REQUIRE(result.commands.size() == 4);

	const CacheValue &a = command_value(result.commands[0]);
	REQUIRE(a.flags == 7);
	REQUIRE(a.expired_at >= before + 100);
	REQUIRE(a.expired_at <= after + 100);
	REQUIRE(a.cas == 0);

	// cas 命令：客户端的 cas_unique 暂存在尚未发布的 value->cas。
	const auto &cas = std::get<CasCommand>(std::get<SingleKeyCommand>(result.commands[1]).command);
	REQUIRE(cas.noreply);
	REQUIRE(cas.value->flags == 8);
	REQUIRE(cas.value->expired_at == ExpiryClock::kNever);
	REQUIRE(cas.value->cas == 12345);

	REQUIRE(command_value(result.commands[2]).expired_at == 2000000000 - ExpiryClock::epoch_unix());
	REQUIRE(command_value(result.commands[3]).expired_at == ExpiryClock::kExpired); // 30 天 + 1 秒：1970 年的时间戳

	// 直接读进 value 的大数据块同样在收到命令头时写好头部字段。
	auto pending = parse("cas big 5 100 10000 77\r\n");
	REQUIRE(pending.stop == StopReason::PendingValue);
	const auto &big = std::get<CasCommand>(pending.pending->command.command);
	REQUIRE(big.value->flags == 5);
	REQUIRE(big.value->cas == 77);
	REQUIRE(big.value->expired_at >= before + 100);
}

TEST_CASE("gat, gats and touch carry an absolute expired_at", "[expiry]") {
	using util::ExpiryClock;
	const std::uint32_t before = ExpiryClock::now();
	auto result = parse("gat 100 k\r\ntouch k 0\r\ngats 3000000 a b\r\n");
	const std::uint32_t after = ExpiryClock::now();
	REQUIRE(result.commands.size() == 3);
	const auto &gat = std::get<SingleGatCommand>(std::get<SingleKeyCommand>(result.commands[0]).command);
	REQUIRE(gat.expired_at >= before + 100);
	REQUIRE(gat.expired_at <= after + 100);
	REQUIRE(std::get<TouchCommand>(std::get<SingleKeyCommand>(result.commands[1]).command).expired_at ==
	        ExpiryClock::kNever);
	REQUIRE(std::get<MultiGatsCommand>(std::get<MultiKeyCommand>(result.commands[2]).command).expired_at ==
	        ExpiryClock::kExpired);
}

TEST_CASE("values are fully converted before they are stored", "[expiry]") {
	using util::ExpiryClock;
	ShardCache cache(5);
	const std::uint32_t before = ExpiryClock::now();
	execute(cache, "set k 3 100 2\r\nhi\r\n");
	const std::uint32_t after = ExpiryClock::now();
	const CacheValue *stored = cache.peek(ItemKey("k"));
	REQUIRE(stored != nullptr);
	REQUIRE(stored->view() == "hi");
	REQUIRE(stored->flags == 3);
	REQUIRE(stored->expired_at >= before + 100);
	REQUIRE(stored->expired_at <= after + 100);
	const std::uint64_t first = stored->cas;
	REQUIRE(first != 0);
	REQUIRE((first & 0xffff) == 5); // 低 16 位是分片号

	// 每次写入都分配新的版本号，同一分片内递增。
	execute(cache, "set k 0 0 1\r\nx\r\n");
	const std::uint64_t second = cache.peek(ItemKey("k"))->cas;
	REQUIRE(second > first);
	REQUIRE((second & 0xffff) == 5);
	REQUIRE(cache.peek(ItemKey("k"))->expired_at == ExpiryClock::kNever);
}

TEST_CASE("cas compares the client's version and stores a fresh one", "[cas]") {
	ShardCache cache(1);
	const auto cas_status = [&](const std::string &input) {
		return std::get<CasResponse>(execute(cache, input).variant()).status;
	};
	REQUIRE(cas_status("cas k 0 0 1 1\r\na\r\n") == CasStatus::NotFound);
	REQUIRE(cache.size() == 0);

	execute(cache, "set k 0 0 1\r\na\r\n");
	const std::uint64_t version = cache.peek(ItemKey("k"))->cas;
	REQUIRE(cas_status("cas k 0 0 1 " + std::to_string(version + 1) + "\r\nb\r\n") == CasStatus::Exists);
	REQUIRE(cache.peek(ItemKey("k"))->view() == "a");

	REQUIRE(cas_status("cas k 9 0 1 " + std::to_string(version) + "\r\nc\r\n") == CasStatus::Stored);
	const CacheValue *stored = cache.peek(ItemKey("k"));
	REQUIRE(stored->view() == "c");
	REQUIRE(stored->flags == 9);
	const std::uint64_t updated = stored->cas;
	REQUIRE(updated != version); // 存进去的是新版本号，不是客户端给的期望值
	REQUIRE((updated & 0xffff) == 1);

	// 旧版本号失效；noreply 不输出，但照常写入。
	REQUIRE(cas_status("cas k 0 0 1 " + std::to_string(version) + "\r\nx\r\n") == CasStatus::Exists);
	REQUIRE(cas_status("cas k 0 0 1 " + std::to_string(updated) + " noreply\r\nd\r\n") == CasStatus::NoReply);
	REQUIRE(cache.peek(ItemKey("k"))->view() == "d");

	// gets 返回的就是 map 里的版本号。
	auto gets = cache.handle_single_command(SingleKeyCommand(SingleGetsCommand {ItemKey("k")}));
	const auto &item = *std::get<SingleGetsResponse>(gets.variant()).item;
	REQUIRE(item.key == "k");
	REQUIRE(item.value->cas == cache.peek(ItemKey("k"))->cas);
	auto multi = cache.handle_multi_command(MultiKeyCommand(MultiGetsCommand {{ItemKey("k"), ItemKey("none")}}));
	const auto &items = std::get<MultiGetsResponse>(multi.variant()).items;
	REQUIRE(items.size() == 1);
	REQUIRE(items[0].value.get() == item.value.get());
	std::string wire;
	detail::StringSink sink {wire};
	REQUIRE_FALSE(encode_to(sink, multi));
	REQUIRE(wire == "VALUE k 0 1 " + std::to_string(item.value->cas) + "\r\nd\r\nEND\r\n");
}

TEST_CASE("expired values read as misses and are removed lazily", "[expiry]") {
	using util::ExpiryClock;
	ShardCache cache;
	const std::uint32_t future = ExpiryClock::now() + 1000;
	cache_set(cache, "dead", CacheValue::create("x", 0, ExpiryClock::kExpired));
	cache_set(cache, "alive", CacheValue::create("y", 0, future));
	REQUIRE(cache.size() == 2);

	// peek 只读：过期的返回 nullptr，但不删除。
	REQUIRE(cache.peek(ItemKey("dead")) == nullptr);
	REQUIRE(cache.peek(ItemKey("alive")) != nullptr);
	REQUIRE(cache.size() == 2);

	// get 未命中，并顺手删除。
	REQUIRE_FALSE(cache_get(cache, "dead"));
	REQUIRE(cache.size() == 1);
	REQUIRE(cache_get(cache, "alive")->value->view() == "y");

	// 多 key 的 gets 同样跳过过期的。
	cache_set(cache, "dead", CacheValue::create("x", 0, ExpiryClock::kExpired));
	auto multi = cache.handle_multi_command(MultiKeyCommand(MultiGetsCommand {{ItemKey("dead"), ItemKey("alive")}}));
	const auto &items = std::get<MultiGetsResponse>(multi.variant()).items;
	REQUIRE(items.size() == 1);
	REQUIRE(items[0].key == "alive");
	REQUIRE(cache.size() == 1);

	// 覆盖过期的 key 等同于新写入。
	cache_set(cache, "dead", CacheValue::create("again"));
	REQUIRE(cache_get(cache, "dead")->value->view() == "again");
}

TEST_CASE("cas and delete treat expired values as absent", "[expiry]") {
	using util::ExpiryClock;
	ShardCache cache(1);
	CacheValuePtr value = CacheValue::create("a", 0, ExpiryClock::kExpired);
	const CacheValue *raw = value.get();
	cache_set(cache, "k", std::move(value));
	const std::uint64_t version = raw->cas; // map 仍持有它，只是已经过期
	REQUIRE(std::get<CasResponse>(execute(cache, "cas k 0 0 1 " + std::to_string(version) + "\r\nb\r\n").variant())
				.status == CasStatus::NotFound);
	REQUIRE(cache.size() == 0);

	cache_set(cache, "k", CacheValue::create("c", 0, ExpiryClock::kExpired));
	REQUIRE(std::get<DeleteResponse>(execute(cache, "delete k\r\n").variant()).status == DeleteStatus::NotFound);
	REQUIRE(cache.size() == 0);

	cache_set(cache, "k", CacheValue::create("d"));
	REQUIRE(std::get<DeleteResponse>(execute(cache, "delete k\r\n").variant()).status == DeleteStatus::Deleted);
	REQUIRE(cache.size() == 0);
}

TEST_CASE("sample_expired removes expired entries and reports ttl stats", "[expiry]") {
	using util::ExpiryClock;
	ShardCache cache;
	const std::uint32_t future = ExpiryClock::now() + 1000;
	constexpr std::size_t kEach = 40;
	const auto set_dead = [&] {
		for (std::size_t i = 0; i < kEach; ++i) {
			cache_set(cache, "dead:" + std::to_string(i), CacheValue::create("x", 0, ExpiryClock::kExpired));
		}
	};
	set_dead();
	for (std::size_t i = 0; i < kEach; ++i) {
		cache_set(cache, "ttl:" + std::to_string(i), CacheValue::create("y", 0, future));
		cache_set(cache, "forever:" + std::to_string(i), CacheValue::create("z"));
	}
	REQUIRE(cache.size() == 3 * kEach);
	REQUIRE(cache.sample_expired(0).sampled == 0);

	// 一次调用内访问 min(limit, size()) 个不同条目：limit 足够大时就是一次全量扫描。
	const auto full = cache.sample_expired(1000);
	REQUIRE(full.sampled == 3 * kEach);
	REQUIRE(full.with_ttl == 2 * kEach);
	REQUIRE(full.expired == kEach);
	REQUIRE(cache.size() == 2 * kEach);
	for (std::size_t i = 0; i < kEach; ++i) {
		REQUIRE(cache.peek(ItemKey("ttl:" + std::to_string(i))) != nullptr);
		REQUIRE(cache.peek(ItemKey("forever:" + std::to_string(i))) != nullptr);
	}

	// 小批量采样：游标在多次调用之间推进，最终清掉全部过期条目。
	set_dead();
	std::size_t removed = 0;
	for (int round = 0; round < 1000 && cache.size() > 2 * kEach; ++round) {
		const auto stats = cache.sample_expired(10);
		REQUIRE(stats.sampled == 10);
		REQUIRE(stats.expired <= stats.with_ttl);
		removed += stats.expired;
	}
	REQUIRE(removed == kEach);
	REQUIRE(cache.size() == 2 * kEach);
}

TEST_CASE("ExpiredMap expires at expired_at and only drops its own reference", "[expiry]") {
	using util::ExpiryClock;
	ExpiredMap map;
	const std::uint32_t at = ExpiryClock::now() + 1000;
	map.insert_or_assign(ItemKey("k"), CacheValue::create("v", 0, at));
	map.insert_or_assign(ItemKey("forever"), CacheValue::create("f"));
	CacheValue *found = map.find(ItemKey("k"));
	REQUIRE(found != nullptr);
	const CacheValuePtr held = CacheValuePtr::share(found); // 模拟在途响应
	REQUIRE(held->use_count() == 2);

	auto stats = map.sample_expired(10, at - 1);
	REQUIRE(stats.sampled == 2);
	REQUIRE(stats.with_ttl == 1);
	REQUIRE(stats.expired == 0);
	REQUIRE(map.size() == 2);

	stats = map.sample_expired(10, at); // 到点即过期
	REQUIRE(stats.expired == 1);
	REQUIRE(map.size() == 1);
	REQUIRE(held->use_count() == 1); // map 只释放自己那次计数，在途响应仍然有效
	REQUIRE(held->view() == "v");
	REQUIRE(map.find(ItemKey("forever"))->view() == "f");

	// 用同一块内存覆盖：map 不会多持有一次计数。
	map.insert_or_assign(ItemKey("k"), CacheValuePtr(held));
	map.insert_or_assign(ItemKey("k"), CacheValuePtr(held));
	REQUIRE(held->use_count() == 2);
	REQUIRE(map.erase(ItemKey("k")));
	REQUIRE(held->use_count() == 1);
	REQUIRE_FALSE(map.erase(ItemKey("k")));
}

TEST_CASE("ttl counter follows every way a ttl entry enters or leaves the map", "[expiry]") {
	using util::ExpiryClock;
	ExpiredMap map;
	const std::uint32_t at = ExpiryClock::now() + 1000;
	for (int i = 0; i < 20; ++i) {
		map.insert_or_assign(ItemKey("forever:" + std::to_string(i)), CacheValue::create("f"));
	}
	REQUIRE(map.ttl_count() == 0);
	// 没有带过期时间的条目：采样直接返回，一个都不访问。
	REQUIRE(map.sample_expired(10).sampled == 0);
	REQUIRE(map.sample_expired(10, at).sampled == 0);

	for (const char *key : {"a", "c", "d", "e"}) {
		map.insert_or_assign(ItemKey(key), CacheValue::create(key, 0, at));
	}
	map.insert_or_assign(ItemKey("b"), CacheValue::create("b", 0, ExpiryClock::kExpired));
	REQUIRE(map.ttl_count() == 5);

	// 覆盖：带 TTL -> 带 TTL 不变，带 TTL -> 永久减一，永久 -> 带 TTL 加一。
	map.insert_or_assign(ItemKey("a"), CacheValue::create("a2", 0, at));
	REQUIRE(map.ttl_count() == 5);
	map.insert_or_assign(ItemKey("a"), CacheValue::create("a3"));
	REQUIRE(map.ttl_count() == 4);
	map.insert_or_assign(ItemKey("forever:0"), CacheValue::create("t", 0, at));
	REQUIRE(map.ttl_count() == 5);

	// 惰性删除。
	REQUIRE(map.find(ItemKey("b")) == nullptr);
	REQUIRE(map.ttl_count() == 4);

	// delete：未过期的返回 true，已过期的返回 false，两者都扣计数；永久的不扣。
	REQUIRE(map.erase(ItemKey("c")));
	REQUIRE(map.ttl_count() == 3);
	map.insert_or_assign(ItemKey("x"), CacheValue::create("x", 0, ExpiryClock::kExpired));
	REQUIRE(map.ttl_count() == 4);
	REQUIRE_FALSE(map.erase(ItemKey("x")));
	REQUIRE(map.ttl_count() == 3);
	REQUIRE(map.erase(ItemKey("a")));
	REQUIRE(map.ttl_count() == 3);

	// 采样删除：d、e、forever:0 到点过期。
	const auto stats = map.sample_expired(1000, at);
	REQUIRE(stats.with_ttl == 3);
	REQUIRE(stats.expired == 3);
	REQUIRE(map.ttl_count() == 0);
	REQUIRE(map.size() == 19);
	REQUIRE(map.sample_expired(1000, at).sampled == 0);
}

namespace {
// 单 key 命令的响应状态，按具体响应类型取出。
template <typename Response>
auto status_of(ShardCache &cache, const std::string_view input) {
	return std::get<Response>(execute(cache, input).variant()).status;
}

MultiKeyResponse execute_multi(ShardCache &cache, const std::string_view input) {
	auto batch = parse(input);
	REQUIRE(batch.commands.size() == 1);
	return cache.handle_multi_command(std::move(std::get<MultiKeyCommand>(batch.commands[0])));
}

std::string wire_of(const SingleKeyResponse &response) {
	std::string wire;
	detail::StringSink sink {wire};
	encode_to(sink, response);
	return wire;
}
} // namespace

TEST_CASE("memcached number parsing mirrors safe_strtoull and safe_strtol", "[number]") {
	constexpr auto u64 = [](std::string_view text) -> std::optional<std::uint64_t> {
		std::uint64_t value = 0;
		return parse_memcached_u64(text, value) ? std::optional(value) : std::nullopt;
	};
	constexpr auto i32 = [](std::string_view text) -> std::optional<std::int32_t> {
		std::int32_t value = 0;
		return parse_memcached_i32(text, value) ? std::optional(value) : std::nullopt;
	};
	static_assert(u64("0") == 0 && u64("+5") == 5 && u64(" \t5") == 5 && u64("5 abc") == 5);
	static_assert(u64(std::string_view("7\0x", 3)) == 7); // NUL 结束数字
	static_assert(u64("18446744073709551615") == UINT64_MAX && !u64("18446744073709551616"));
	static_assert(u64("-0") == 0 && !u64("-1") && !u64("-5")); // strtoull 取反后为负才拒绝
	static_assert(!u64("") && !u64(" ") && !u64("+") && !u64("+-1") && !u64("5x") && !u64("0x1") && !u64("1.5"));
	static_assert(i32("100") == 100 && i32("-1") == -1 && i32("+5") == 5);
	static_assert(i32("4294967295") == -1 && i32("2147483648") == INT32_MIN); // 截断成 int32
	static_assert(i32("-2147483649") == INT32_MAX && i32("-9223372036854775808") == 0);
	static_assert(i32("9223372036854775807") == -1 && !i32("9223372036854775808")); // long 溢出才失败
	static_assert(!i32("abc") && !i32("") && !i32("1.5"));
	SUCCEED();
}

TEST_CASE("key command argument errors follow memcached", "[protocol]") {
	struct Expect {
		std::string_view input;
		ParseErrorCode code;
		bool noreply;
	};
	for (const auto &[input, code, noreply] : std::initializer_list<Expect> {
	         {"touch\r\n", ParseErrorCode::BadArgumentCount, false},
	         {"touch k\r\n", ParseErrorCode::BadArgumentCount, false},
	         {"touch k 1 noreply x\r\n", ParseErrorCode::BadArgumentCount, false},
	         {"touch k abc\r\n", ParseErrorCode::InvalidExptime, false},
	         {"touch k abc noreply\r\n", ParseErrorCode::InvalidExptime, true},
	         {"touch k noreply\r\n", ParseErrorCode::InvalidExptime, true}, // 最后一个参数是 noreply
	         {"incr k\r\n", ParseErrorCode::BadArgumentCount, false},
	         {"incr k -1\r\n", ParseErrorCode::InvalidDelta, false},
	         {"decr k 18446744073709551616 noreply\r\n", ParseErrorCode::InvalidDelta, true},
	         {"gat\r\n", ParseErrorCode::BadArgumentCount, false},
	         {"gat abc k\r\n", ParseErrorCode::InvalidExptime, false},
	         {"gats noreply k\r\n", ParseErrorCode::InvalidExptime, false},
	         {"delete\r\n", ParseErrorCode::BadArgumentCount, false},
	         {"delete k 0 noreply x\r\n", ParseErrorCode::BadArgumentCount, false},
	         {"delete k 1\r\n", ParseErrorCode::BadDeleteArguments, false},
	         {"delete k 0 0\r\n", ParseErrorCode::BadDeleteArguments, false},
	         {"delete k x noreply\r\n", ParseErrorCode::BadDeleteArguments, true},
	         {"set k 0 0 noreply\r\n", ParseErrorCode::InvalidNumber, true},
	         {"set k 0 9223372036854775808 1\r\n", ParseErrorCode::InvalidNumber, false},
	     }) {
		CAPTURE(input);
		const auto result = parse(input);
		REQUIRE(result.stop == StopReason::Error);
		REQUIRE(result.error->code == code);
		REQUIRE(result.error->noreply == noreply);
		REQUIRE(result.error->recovery == ErrorRecovery::Continue);
	}
	REQUIRE(to_error_response(*parse("touch k x\r\n").error).message == "invalid exptime argument");
	REQUIRE(to_error_response(*parse("incr k x\r\n").error).message == "invalid numeric delta argument");
	REQUIRE(to_error_response(*parse("delete k 1\r\n").error).message ==
	        "bad command line format.  Usage: delete <key> [noreply]");
	REQUIRE(to_error_response(*parse("touch\r\n").error).kind == ResponseErrorKind::Error);

	// 合法的写法：多余的非 noreply 参数被忽略，旧版的 delete hold time 0。
	auto ok = parse("touch k 10 garbage\r\nincr k +2 garbage\r\ndelete k 0\r\ndelete k 0 noreply\r\n"
	                "set k 0 -1 1 garbage\r\nx\r\ngat 100\r\ngats 0\r\n");
	REQUIRE(ok.stop == StopReason::EndOfInput);
	REQUIRE(ok.commands.size() == 7);
	const auto single = [&](std::size_t i) -> auto & {
		return std::get<SingleKeyCommand>(ok.commands[i]).command;
	};
	REQUIRE_FALSE(std::get<TouchCommand>(single(0)).noreply);
	REQUIRE(std::get<IncrCommand>(single(1)).delta == 2);
	REQUIRE_FALSE(std::get<DeleteCommand>(single(2)).noreply);
	REQUIRE(std::get<DeleteCommand>(single(3)).noreply);
	REQUIRE(std::get<SetCommand>(single(4)).value->expired_at == util::ExpiryClock::kExpired); // 负数立即过期
	// gat/gats 只带 exptime 时是空查询：空的 Multi*，只回复 END。
	REQUIRE(std::get<MultiGatCommand>(std::get<MultiKeyCommand>(ok.commands[5]).command).keys.empty());
	REQUIRE(std::get<MultiGatsCommand>(std::get<MultiKeyCommand>(ok.commands[6]).command).keys.empty());
}

TEST_CASE("add, replace, append and prepend follow memcached's store rules", "[store]") {
	using util::ExpiryClock;
	ShardCache cache(3, 8);
	REQUIRE(status_of<AddResponse>(cache, "add k 1 0 1\r\na\r\n") == StorageStatus::Stored);
	const std::uint64_t added = cache.peek(ItemKey("k"))->cas;
	REQUIRE(added == ((std::uint64_t {1} << 16) | 3));
	REQUIRE(status_of<AddResponse>(cache, "add k 2 0 1\r\nb\r\n") == StorageStatus::NotStored);
	REQUIRE(status_of<AddResponse>(cache, "add k 2 0 1 noreply\r\nb\r\n") == StorageStatus::NoReply);
	REQUIRE(cache.peek(ItemKey("k"))->view() == "a");
	REQUIRE(cache.peek(ItemKey("k"))->cas == added); // 没写入的 add 不改旧值，也不消耗版本号

	// 过期的 key 等同于不存在：add 写入，replace/append/prepend 不写。
	cache_set(cache, "dead", CacheValue::create("x", 0, ExpiryClock::kExpired));
	REQUIRE(cache.ttl_count() == 1);
	REQUIRE(status_of<ReplaceResponse>(cache, "replace dead 0 0 1\r\ny\r\n") == StorageStatus::NotStored);
	REQUIRE(cache.ttl_count() == 0); // find() 顺手删除，计数同步
	cache_set(cache, "dead", CacheValue::create("x", 0, ExpiryClock::kExpired));
	REQUIRE(status_of<AppendResponse>(cache, "append dead 0 0 1\r\ny\r\n") == StorageStatus::NotStored);
	cache_set(cache, "dead", CacheValue::create("x", 0, ExpiryClock::kExpired));
	REQUIRE(status_of<PrependResponse>(cache, "prepend dead 0 0 1\r\ny\r\n") == StorageStatus::NotStored);
	cache_set(cache, "dead", CacheValue::create("x", 0, ExpiryClock::kExpired));
	REQUIRE(status_of<AddResponse>(cache, "add dead 4 100 1\r\ny\r\n") == StorageStatus::Stored);
	REQUIRE(cache.peek(ItemKey("dead"))->view() == "y");
	REQUIRE(cache.ttl_count() == 1);
	REQUIRE(status_of<ReplaceResponse>(cache, "replace missing 0 0 1\r\ny\r\n") == StorageStatus::NotStored);
	REQUIRE(cache.peek(ItemKey("missing")) == nullptr);

	// replace 使用新的 flags 和过期时间。
	REQUIRE(status_of<ReplaceResponse>(cache, "replace dead 6 0 1\r\nz\r\n") == StorageStatus::Stored);
	REQUIRE(cache.peek(ItemKey("dead"))->flags == 6);
	REQUIRE(cache.ttl_count() == 0);

	// append/prepend 保留旧值的 flags 和过期时间，换新 cas；旧值仍归在途响应。
	const std::uint32_t at = ExpiryClock::now() + 1000;
	cache_set(cache, "c", CacheValue::create("mm", 5, at));
	const auto held = cache_get(cache, "c");
	const std::uint64_t before = held->value->cas;
	REQUIRE(status_of<AppendResponse>(cache, "append c 9 -1 2\r\n>>\r\n") == StorageStatus::Stored);
	REQUIRE(status_of<PrependResponse>(cache, "prepend c 7 0 2\r\n<<\r\n") == StorageStatus::Stored);
	const CacheValue *merged = cache.peek(ItemKey("c"));
	REQUIRE(merged->view() == "<<mm>>");
	REQUIRE(merged->view_crlf() == "<<mm>>\r\n");
	REQUIRE(merged->flags == 5);
	REQUIRE(merged->expired_at == at);
	REQUIRE(merged->cas > before);
	REQUIRE(held->value->view() == "mm");
	REQUIRE(cache.ttl_count() == 1);
	REQUIRE(status_of<AppendResponse>(cache, "append c 0 0 0 noreply\r\n\r\n") == StorageStatus::NoReply);
	REQUIRE(cache.peek(ItemKey("c"))->view() == "<<mm>>");

	// 合并后超过上限（这里是 8 字节）时 NOT_STORED，旧值不变。
	REQUIRE(status_of<AppendResponse>(cache, "append c 0 0 2\r\n12\r\n") == StorageStatus::Stored);
	REQUIRE(status_of<PrependResponse>(cache, "prepend c 0 0 1\r\n!\r\n") == StorageStatus::NotStored);
	REQUIRE(cache.peek(ItemKey("c"))->view() == "<<mm>>12");
}

TEST_CASE("touch and gat change only expired_at and keep the TTL count", "[expiry]") {
	using util::ExpiryClock;
	ShardCache cache;
	CacheValuePtr value = CacheValue::create("v", 4);
	const CacheValue *raw = value.get();
	cache_set(cache, "k", std::move(value));
	const std::uint64_t version = raw->cas;
	REQUIRE(cache.ttl_count() == 0);

	REQUIRE(status_of<TouchResponse>(cache, "touch k 100\r\n") == TouchStatus::Touched);
	REQUIRE(cache.peek(ItemKey("k")) == raw); // 原地修改，不复制 value
	REQUIRE(raw->expired_at > ExpiryClock::now());
	REQUIRE(cache.ttl_count() == 1);
	REQUIRE(status_of<TouchResponse>(cache, "touch k 200 noreply\r\n") == TouchStatus::NoReply);
	REQUIRE(cache.ttl_count() == 1);

	auto gat = execute(cache, "gats 0 k\r\n");
	const auto &item = *std::get<SingleGatsResponse>(gat.variant()).item;
	REQUIRE(item.value.get() == raw);
	REQUIRE(raw->expired_at == ExpiryClock::kNever);
	REQUIRE(cache.ttl_count() == 0);
	REQUIRE(raw->cas == version); // touch/gat 不换 cas
	REQUIRE(wire_of(gat) == "VALUE k 4 1 " + std::to_string(version) + "\r\nv\r\nEND\r\n");

	// 负的 exptime：这次照常返回，之后未命中；重复的 key 只命中第一次。
	cache_set(cache, "a", CacheValue::create("A"));
	cache_set(cache, "b", CacheValue::create("B"));
	auto multi = execute_multi(cache, "gat -1 a a b missing\r\n");
	const auto &items = std::get<MultiGatResponse>(multi.variant()).items;
	REQUIRE(items.size() == 2);
	REQUIRE(items[0].key == "a");
	REQUIRE(items[1].key == "b");
	REQUIRE(cache.ttl_count() == 1); // 第二个 a 碰到过期并顺手删除，只剩 b
	REQUIRE(cache.peek(ItemKey("a")) == nullptr);
	REQUIRE(cache.size() == 2);
	REQUIRE(status_of<TouchResponse>(cache, "touch b 100\r\n") == TouchStatus::NotFound);
	REQUIRE(cache.ttl_count() == 0);
	REQUIRE(cache.size() == 1);
	REQUIRE(std::get<MultiGatsResponse>(execute_multi(cache, "gats 100\r\n").variant()).items.empty());

	// 主动采样看到的 TTL 数与 touch/gat 的修改一致。
	REQUIRE(status_of<TouchResponse>(cache, "touch k -1\r\n") == TouchStatus::Touched);
	REQUIRE(cache.ttl_count() == 1);
	REQUIRE(cache.sample_expired(10).expired == 1);
	REQUIRE(cache.size() == 0);
	REQUIRE(cache.ttl_count() == 0);
}

TEST_CASE("incr and decr rewrite in place only while the map holds the only reference", "[arithmetic]") {
	using util::ExpiryClock;
	ShardCache cache;
	const auto arithmetic = [&](const std::string_view input) {
		return wire_of(execute(cache, input));
	};
	REQUIRE(arithmetic("incr k 1\r\n") == "NOT_FOUND\r\n");

	const std::uint32_t at = ExpiryClock::now() + 1000;
	cache_set(cache, "k", CacheValue::create("1000", 7, at));
	const CacheValue *raw = cache.peek(ItemKey("k"));
	const std::uint64_t first = raw->cas;
	REQUIRE(arithmetic("decr k 999\r\n") == "1\r\n");
	REQUIRE(cache.peek(ItemKey("k")) == raw); // 不分配：长度不变，补空格
	REQUIRE(raw->view_crlf() == "1   \r\n");
	REQUIRE(raw->cas > first);
	REQUIRE(raw->flags == 7);
	REQUIRE(raw->expired_at == at);

	// 有在途响应引用时不能原地改：新 value 同样补空格，旧值不变。
	const auto held = cache_get(cache, "k");
	REQUIRE(arithmetic("incr k 41\r\n") == "42\r\n");
	const CacheValue *fresh = cache.peek(ItemKey("k"));
	REQUIRE(fresh != raw);
	REQUIRE(fresh->view_crlf() == "42  \r\n");
	REQUIRE(fresh->flags == 7);
	REQUIRE(fresh->expired_at == at);
	REQUIRE(held->value->view() == "1   ");
	REQUIRE(cache.ttl_count() == 1);

	// 数字变长：刚好装下数字。
	REQUIRE(arithmetic("incr k 99958\r\n") == "100000\r\n");
	REQUIRE(cache.peek(ItemKey("k"))->view_crlf() == "100000\r\n");
	REQUIRE(arithmetic("decr k 100001\r\n") == "0\r\n"); // decr 最小到 0
	REQUIRE(arithmetic("incr k 18446744073709551615\r\n") == "18446744073709551615\r\n");
	REQUIRE(arithmetic("incr k 2\r\n") == "1\r\n"); // 64 位回绕
	REQUIRE(arithmetic("incr k 1 noreply\r\n").empty());
	REQUIRE(cache.peek(ItemKey("k"))->view() == "2                   ");

	cache_set(cache, "text", CacheValue::create("12abc"));
	REQUIRE(arithmetic("incr text 1\r\n") == "CLIENT_ERROR cannot increment or decrement non-numeric value\r\n");
	REQUIRE(arithmetic("decr text 1 noreply\r\n").empty());
	cache_set(cache, "dead", CacheValue::create("1", 0, ExpiryClock::kExpired));
	REQUIRE(arithmetic("incr dead 1\r\n") == "NOT_FOUND\r\n");
	REQUIRE(cache.peek(ItemKey("dead")) == nullptr);
}

TEST_CASE("gat across shards touches each key on its own shard", "[protocol]") {
	WorkerFixture f;
	const auto local = shard_key(0);
	const auto remote = shard_key(1);
	f.set(local, "L");
	f.set(remote, "R");
	auto batch = parse("gat -1 " + local + " " + remote + "\r\ngat 100\r\n");
	REQUIRE(batch.commands.size() == 2);
	{
		Worker::BatchScope scope(f.workers[0]);
		for (auto &command : batch.commands) {
			f.workers[0].send_multi_command(std::move(std::get<MultiKeyCommand>(command)), f.id);
		}
	}
	f.get(local);
	f.get(remote);
	f.start_mailboxes();
	const std::string expected = "STORED\r\nSTORED\r\nVALUE " + local + " 0 1\r\nL\r\nVALUE " + remote +
	                             " 0 1\r\nR\r\nEND\r\nEND\r\nEND\r\nEND\r\n";
	REQUIRE(f.read(expected.size()) == expected);
}

namespace {
// eventfd 的计数（不消费）：可读就说明有过敲门还没被消费者清掉。
bool doorbell_pending(const int fd) {
	pollfd pfd {fd, POLLIN, 0};
	return ::poll(&pfd, 1, 0) == 1;
}

void drain_eventfd(const int fd) {
	uint64_t value = 0;
	REQUIRE(::read(fd, &value, sizeof(value)) == sizeof(value));
}

struct TestNode : MpscHook {
	TestNode(const uint32_t p, const uint64_t v) : producer(p), value(v) {
	}

	uint32_t producer;
	uint64_t value;
};
} // namespace

TEST_CASE("MpscQueue pops in push order and hands back each node once", "[mailbox]") {
	MpscQueue<TestNode> queue;
	REQUIRE(queue.pop() == nullptr);
	queue.push(new TestNode(0, 1));
	TestNode *node = queue.pop(); // 只有一个节点：要先把占位节点挂到它后面才能取走
	REQUIRE(node != nullptr);
	REQUIRE(node->value == 1);
	delete node;
	REQUIRE(queue.pop() == nullptr);
	for (uint64_t i = 0; i != 5; ++i) {
		queue.push(new TestNode(0, i));
	}
	for (uint64_t i = 0; i != 3; ++i) {
		node = queue.pop();
		REQUIRE(node != nullptr);
		REQUIRE(node->value == i);
		delete node;
	}
	queue.push(new TestNode(0, 5));
	for (uint64_t i = 3; i != 6; ++i) {
		node = queue.pop();
		REQUIRE(node != nullptr);
		REQUIRE(node->value == i);
		delete node;
	}
	REQUIRE(queue.pop() == nullptr);
	queue.push(new TestNode(0, 6)); // 留在队列里：析构时释放（ASan 检查泄漏）
}

TEST_CASE("an envelope constructs, converts and destroys its entries in place", "[mailbox]") {
	const std::string long_key(40, 'k'); // 超过 SSO：key 在堆上，泄漏或重复释放会被 ASan 发现
	auto *envelope = new Envelope;
	envelope->reset(0, 1);
	for (uint32_t i = 0; i != Envelope::kCapacity; ++i) {
		REQUIRE_FALSE(envelope->full());
		envelope->append(7, i, RequestData {SingleKeyRequest {SingleKeyCommand(SingleGetCommand {ItemKey(long_key)})}});
	}
	REQUIRE(envelope->full());
	REQUIRE(envelope->size() == Envelope::kCapacity);
	REQUIRE((*envelope)[3].seq == 3);
	// 执行端原地换成响应：一半换，一半保持请求，析构时两种都要正确释放。
	for (std::size_t i = 0; i != Envelope::kCapacity / 2; ++i) {
		(*envelope)[i].data.emplace<ResponseData>(ErrorResponse {});
	}
	envelope->clear();
	REQUIRE(envelope->size() == 0);
	envelope->reset(1, 0);
	envelope->append(8, 0, RequestData {SingleKeyRequest {SingleKeyCommand(SingleGetCommand {ItemKey(long_key)})}});
	delete envelope; // 析构释放剩下的条目
}

TEST_CASE("a batch sends one envelope per target shard and splits only when it is full", "[mailbox]") {
	WorkerFixture f;
	Worker &local = f.workers[0];
	Worker &remote = f.workers[1];
	const auto key = shard_key(1);
	{
		Worker::BatchScope scope(local);
		for (int i = 0; i != 10; ++i) {
			f.queue_get(key);
		}
		REQUIRE(local.envelopes_sent() == 0); // 批次结束前不发
	}
	REQUIRE(local.envelopes_sent() == 1); // 10 条装一个信封
	{
		Worker::BatchScope scope(local);
		for (int i = 0; i != 2 * Envelope::kCapacity + 3; ++i) {
			f.queue_get(key);
		}
		REQUIRE(local.envelopes_sent() == 3); // 装满的两个先发出去
	}
	REQUIRE(local.envelopes_sent() == 4);
	f.start_mailboxes();
	std::string expected;
	for (int i = 0; i != 10 + 2 * Envelope::kCapacity + 3; ++i) {
		expected += "END\r\n";
	}
	REQUIRE(f.read(expected.size()) == expected); // 跨信封仍按序
	REQUIRE(remote.envelopes_sent() == 4); // 原信封带着响应推回，不另外打包
	REQUIRE(remote.mailbox_messages() == 10 + 2 * Envelope::kCapacity + 3);
}

TEST_CASE("a doorbell writes its eventfd once until the consumer lowers it", "[doorbell]") {
	Doorbell bell;
	const int fd = bell.duplicate_event_fd();
	REQUIRE(fd >= 0);
	REQUIRE_FALSE(bell.is_rung()); // 初始：消费者还没醒，第一个生产者负责敲门

	REQUIRE(bell.ring());
	REQUIRE(bell.is_rung());
	REQUIRE(doorbell_pending(fd));
	REQUIRE_FALSE(bell.ring()); // 已有人敲过：只读一下标志
	REQUIRE_FALSE(bell.ring());
	drain_eventfd(fd); // 消费者醒来，先清 fd；门铃仍抬起，生产者继续不敲
	REQUIRE_FALSE(bell.ring());
	REQUIRE_FALSE(doorbell_pending(fd));

	bell.lower(); // 准备睡：之后的第一次 ring 要写 fd
	REQUIRE(bell.ring());
	REQUIRE(doorbell_pending(fd));
	drain_eventfd(fd);

	bell.lower();
	bell.raise(); // 复查发现了消息，继续醒着：仍不需要敲
	REQUIRE_FALSE(bell.ring());
	REQUIRE_FALSE(doorbell_pending(fd));
	::close(fd);
}

TEST_CASE("concurrent producers never lose a doorbell wakeup", "[doorbell]") {
	// 消费者按 Worker::mailbox_loop 的协议运行：醒着时不放门铃；一轮为空才 lower()，复查仍为空才在 fd 上睡。
	// 生产者每批 1~8 条、批间随机停顿，让消费者在醒着、准备睡、睡着之间来回切换。
	// 丢一次唤醒，最后剩下的消息就没人敲门，poll 超时。
	constexpr int kProducers = 3;
	constexpr uint64_t kPerProducer = 100000;
	constexpr uint64_t kTotal = kProducers * kPerProducer;
	Doorbell bell;
	MpscQueue<TestNode> queue;
	std::atomic<uint64_t> knocks {0};

	std::vector<std::thread> producers;
	for (int p = 0; p < kProducers; ++p) {
		producers.emplace_back([&, p] {
			std::minstd_rand rng(static_cast<unsigned>(p + 1));
			uint64_t sent = 0;
			uint64_t local_knocks = 0;
			while (sent < kPerProducer) {
				const uint64_t batch = std::min<uint64_t>(1 + rng() % 8, kPerProducer - sent);
				for (uint64_t i = 0; i < batch; ++i, ++sent) {
					queue.push(new TestNode(static_cast<uint32_t>(p), sent));
				}
				local_knocks += bell.ring();
				for (unsigned spin = rng() % 16384; spin != 0; --spin) {
					asm volatile("" ::: "memory");
				}
			}
			knocks += local_knocks;
		});
	}

	const int fd = bell.duplicate_event_fd();
	REQUIRE(fd >= 0);
	uint64_t received = 0;
	uint64_t lowers = 0;
	uint64_t sleeps = 0;
	bool lost_wakeup = false;
	bool out_of_order = false;
	std::array<uint64_t, kProducers> next_value {};
	const auto drain = [&] {
		std::size_t n = 0;
		while (TestNode *node = queue.pop()) {
			// 同一个生产者的节点必须按 push 顺序取出（分片间请求的顺序保证依赖这一点）。
			out_of_order |= node->value != next_value[node->producer]++;
			delete node;
			++n;
		}
		received += n;
		return n;
	};
	while (received < kTotal) {
		if (drain() != 0) {
			continue; // 醒着：门铃保持抬起
		}
		bell.lower();
		++lowers;
		if (drain() != 0) {
			bell.raise();
			continue;
		}
		++sleeps;
		pollfd pfd {fd, POLLIN, 0};
		if (::poll(&pfd, 1, 5000) != 1) {
			lost_wakeup = true;
			break;
		}
		drain_eventfd(fd);
	}
	for (auto &producer : producers) {
		producer.join();
	}
	::close(fd);
	REQUIRE_FALSE(lost_wakeup);
	REQUIRE_FALSE(out_of_order);
	REQUIRE(received == kTotal);
	// 每次放下门铃（加上初始的放下状态）最多换来一次 eventfd 写入。
	REQUIRE(knocks.load() <= lowers + 1);
	REQUIRE(sleeps <= lowers);
	WARN("doorbell stress: " << kTotal << " messages, " << knocks.load() << " eventfd writes, " << sleeps
	                         << " sleeps, " << lowers << " lowers");
}

TEST_CASE("a producer that waits for every reply wakes the sleeping consumer every time", "[doorbell]") {
	// 乒乓：每条消息都要等消费者处理完才发下一条，消费者几乎每条都要经历「放下门铃 → 复查 → 睡 → 被敲醒」，
	// 反复穿过最容易丢唤醒的窗口。丢一次就会卡住，poll 超时。
	constexpr uint64_t kRoundTrips = 20000;
	Doorbell bell;
	MpscQueue<TestNode> queue;
	std::atomic<uint64_t> received {0};
	std::atomic<bool> lost_wakeup {false};
	uint64_t knocks = 0;
	std::thread consumer([&] {
		const int fd = bell.duplicate_event_fd();
		uint64_t local = 0;
		const auto drain = [&] {
			std::size_t n = 0;
			while (TestNode *node = queue.pop()) {
				delete node;
				++n;
			}
			local += n;
			received.store(local, std::memory_order_release);
			return n;
		};
		while (local < kRoundTrips) {
			if (drain() != 0) {
				continue;
			}
			bell.lower();
			if (drain() != 0) {
				bell.raise();
				continue;
			}
			pollfd pfd {fd, POLLIN, 0};
			if (::poll(&pfd, 1, 5000) != 1) {
				lost_wakeup = true;
				break;
			}
			uint64_t value = 0;
			[[maybe_unused]] const auto n = ::read(fd, &value, sizeof(value));
		}
		::close(fd);
	});
	for (uint64_t i = 0; i < kRoundTrips && !lost_wakeup; ++i) {
		queue.push(new TestNode(0, i));
		knocks += bell.ring();
		while (received.load(std::memory_order_acquire) <= i && !lost_wakeup) {
		}
	}
	consumer.join();
	REQUIRE_FALSE(lost_wakeup);
	REQUIRE(received.load() == kRoundTrips);
	REQUIRE(knocks <= kRoundTrips);
	WARN("doorbell ping-pong: " << kRoundTrips << " messages, " << knocks << " eventfd writes");
}

TEST_CASE("a batch rings each remote shard once and flushes every 64 messages", "[doorbell]") {
	WorkerFixture f; // 收件协程没启动：门铃只由测试放下
	Worker &w = f.workers[0];
	Doorbell &remote_bell = w.doorbell(1);
	const auto remote = shard_key(1);
	const int fd = remote_bell.duplicate_event_fd();
	REQUIRE(fd >= 0);

	{
		Worker::BatchScope batch(w);
		for (int i = 0; i != 10; ++i) {
			f.queue_get(remote);
		}
		REQUIRE(w.doorbell_knocks() == 0); // 只记下目标分片
		REQUIRE_FALSE(doorbell_pending(fd));
	}
	REQUIRE(w.doorbell_knocks() == 1); // 离开作用域：10 条只敲一次
	drain_eventfd(fd);

	remote_bell.lower();
	{
		Worker::BatchScope batch(w);
		for (int i = 0; i != 63; ++i) {
			f.queue_get(remote);
		}
		REQUIRE(w.doorbell_knocks() == 1); // 装满的 3 个信封（48 条）已发出，还不到 64 条
		f.queue_get(remote); // 第 64 条装满第 4 个信封：已发出 64 条，先把攒下的敲出去，不等作用域结束
		REQUIRE(w.doorbell_knocks() == 2);
		drain_eventfd(fd);
		remote_bell.lower();
		f.queue_get(remote);
		REQUIRE(w.doorbell_knocks() == 2);
	}
	REQUIRE(w.doorbell_knocks() == 3);
	drain_eventfd(fd);

	// 门铃已抬起（对端还没处理）：再来一批也不写 fd。
	{
		Worker::BatchScope batch(w);
		f.queue_get(remote);
		f.queue_get(remote);
	}
	REQUIRE(w.doorbell_knocks() == 3);
	REQUIRE_FALSE(doorbell_pending(fd));
	::close(fd);

	// 一共 10 + 65 + 2 条请求都在对端收件队列里：交给收件协程，响应全部按序到达。
	f.start_mailboxes();
	std::string expected;
	for (int i = 0; i != 77; ++i) {
		expected += "END\r\n";
	}
	REQUIRE(f.read(expected.size()) == expected);
}

TEST_CASE("an awake mailbox keeps its doorbell up and an idle one sleeps with it lowered", "[doorbell]") {
	WorkerFixture f;
	Worker &local = f.workers[0];
	Worker &remote = f.workers[1];
	Doorbell &remote_bell = local.doorbell(1);
	const auto key = shard_key(1);
	f.set(key, "v");
	f.start_mailboxes();
	REQUIRE(f.read(8) == "STORED\r\n");
	f.until([&] { return remote.mailbox_sleeps() >= 1 && !remote_bell.is_rung(); }); // 空闲：门铃放下，睡在 fd 上

	const uint64_t knocks = local.doorbell_knocks();
	const uint64_t handled = remote.mailbox_messages();
	f.get(key);
	REQUIRE(local.doorbell_knocks() == knocks + 1); // 对端在睡：敲一次
	f.until([&] { return remote.mailbox_messages() == handled + 1; });
	// 对端刚处理完一条、让出了 CPU，还醒着：门铃保持抬起，再发不用敲。
	REQUIRE(remote_bell.is_rung());
	const uint64_t sleeps = remote.mailbox_sleeps();
	f.get(key);
	f.get(key);
	REQUIRE(local.doorbell_knocks() == knocks + 1);

	const std::string reply = "VALUE " + key + " 0 1\r\nv\r\nEND\r\n";
	REQUIRE(f.read(3 * reply.size()) == reply + reply + reply);
	f.until([&] { return remote.mailbox_sleeps() > sleeps && !remote_bell.is_rung(); }); // 流量停了：又去睡
	f.get(key);
	REQUIRE(local.doorbell_knocks() == knocks + 2);
	REQUIRE(f.read(reply.size()) == reply);
}
