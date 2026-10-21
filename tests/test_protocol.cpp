#include <catch2/catch_test_macros.hpp>

#include "protocol/memcached_parser.h"
#include "server/comm/input_buffer.h"
#include "server/comm/worker.h"

#include <array>
#include <chrono>
#include <cstring>
#include <functional>
#include <set>
#include <string>
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

	void set(const std::string &key, std::string_view value) {
		workers[0].send_single_command(SingleKeyCommand(SetCommand {ItemKey(key), CacheValue::create(value)}), id);
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
	f.workers[0].send_multi_command(MultiKeyCommand(MultiGetCommand {{ItemKey(keys[0]), ItemKey(keys[1])}}), f.id);
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
