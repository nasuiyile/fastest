#pragma once

#include <algorithm>
#include <charconv>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <limits>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <system_error>
#include <type_traits>
#include <utility>
#include <variant>
#include <vector>

#include "../cache/type.h"
#include "../util/clock.h"

namespace fastest {

// CommandType 保留协议命令名；单 Key / 多 Key 由具体 C++ 类型和外层 variant 区分。
enum class CommandType : std::uint8_t {
	Set,
	Add,
	Replace,
	Append,
	Prepend,
	Cas,
	Get,
	Gets,
	Gat,
	Gats,
	Delete,
	Touch,
	Incr,
	Decr,
	FlushAll,  // 删除全库 底层实际只是更新了删除的时间
	Stats,     // 统计
	Verbosity, // 调整日志级别
	Version,   // 返回服务器版本号
	Quit       // 关闭当前客户端连接
};

// 使用模板复用相同字段，但每个模板实例都是不同的 C++ 类型。
// value 是解析时一次性创建的最终存储块，之后经命令、mailbox 移动进 map，不再复制字节。命令本身不再重复保存
// value 头里已有的字段，解析时就写好：
//   flags      协议 flags。
//   expired_at 协议 exptime 已按 ExpiryClock 换算成绝对时间（0 永不过期，1 已过期）。
//   cas        cas 命令：客户端给的 cas_unique（期望的旧版本）；其他命令：0。value 尚未发布，只有命令持有，
//              所属分片比较之后在 ShardCache::publish() 里改写成新版本号再插入，存进 map 的永远是新版本号。
template <CommandType Type>
struct StorageCommandT {
	static constexpr CommandType command_type = Type;

	ItemKey key;
	CacheValuePtr value;
	bool noreply = false;
};

using SetCommand = StorageCommandT<CommandType::Set>;
using AddCommand = StorageCommandT<CommandType::Add>;
using ReplaceCommand = StorageCommandT<CommandType::Replace>;
using AppendCommand = StorageCommandT<CommandType::Append>;
using PrependCommand = StorageCommandT<CommandType::Prepend>;
using CasCommand = StorageCommandT<CommandType::Cas>;

// 相同的协议命令根据 Key 数量有不同类型，单 Key 不需要动态分配 vector。
template <CommandType Type>
struct SingleRetrievalCommandT {
	static constexpr CommandType command_type = Type;

	ItemKey key;
};

template <CommandType Type>
struct MultiRetrievalCommandT {
	static constexpr CommandType command_type = Type;

	std::vector<ItemKey> keys;
};

using SingleGetCommand = SingleRetrievalCommandT<CommandType::Get>;
using MultiGetCommand = MultiRetrievalCommandT<CommandType::Get>;
using SingleGetsCommand = SingleRetrievalCommandT<CommandType::Gets>;
using MultiGetsCommand = MultiRetrievalCommandT<CommandType::Gets>;

// gat/gats/touch 不带 value，过期时间留在命令里，但同样在解析时换算成绝对的 expired_at。
template <CommandType Type>
struct SingleGetAndTouchCommandT {
	static constexpr CommandType command_type = Type;

	std::uint32_t expired_at = 0;
	ItemKey key;
};

template <CommandType Type>
struct MultiGetAndTouchCommandT {
	static constexpr CommandType command_type = Type;

	std::uint32_t expired_at = 0;
	std::vector<ItemKey> keys;
};

using SingleGatCommand = SingleGetAndTouchCommandT<CommandType::Gat>;
using MultiGatCommand = MultiGetAndTouchCommandT<CommandType::Gat>;
using SingleGatsCommand = SingleGetAndTouchCommandT<CommandType::Gats>;
using MultiGatsCommand = MultiGetAndTouchCommandT<CommandType::Gats>;

struct DeleteCommand {
	static constexpr CommandType command_type = CommandType::Delete;

	ItemKey key;
	bool noreply = false;
};

struct TouchCommand {
	static constexpr CommandType command_type = CommandType::Touch;

	ItemKey key;
	std::uint32_t expired_at = 0;
	bool noreply = false;
};

template <CommandType Type>
struct ArithmeticCommandT {
	static constexpr CommandType command_type = Type;

	ItemKey key;
	std::uint64_t delta = 0;
	bool noreply = false;
};

using IncrCommand = ArithmeticCommandT<CommandType::Incr>;
using DecrCommand = ArithmeticCommandT<CommandType::Decr>;

struct FlushAllCommand {
	static constexpr CommandType command_type = CommandType::FlushAll;

	std::uint32_t delay = 0;
	bool noreply = false;
};

struct VerbosityCommand {
	static constexpr CommandType command_type = CommandType::Verbosity;

	std::uint32_t level = 0;
	bool noreply = false;
};

struct StatsCommand {
	static constexpr CommandType command_type = CommandType::Stats;

	std::string arguments;
};

struct VersionCommand {
	static constexpr CommandType command_type = CommandType::Version;
};

struct QuitCommand {
	static constexpr CommandType command_type = CommandType::Quit;
};

// 第一层按路由粒度区分；第二层保留每种具体命令的独立 variant 类型。
struct SingleKeyCommand {
	using Variant = std::variant<SetCommand, AddCommand, ReplaceCommand, AppendCommand, PrependCommand, CasCommand,
	                             SingleGetCommand, SingleGetsCommand, SingleGatCommand, SingleGatsCommand,
	                             DeleteCommand, TouchCommand, IncrCommand, DecrCommand>;

	Variant command;

	template <typename T>
	    requires std::is_constructible_v<Variant, T>
	explicit SingleKeyCommand(T &&value) : command(std::forward<T>(value)) {
	}

	[[nodiscard]] const ItemKey &item_key() const {
		return std::visit([](const auto &value) -> const ItemKey & { return value.key; }, command);
	}
};

inline std::uint64_t hash(const SingleKeyCommand &command) {
	return command.item_key().hash;
}

struct MultiKeyCommand {
	using Variant = std::variant<MultiGetCommand, MultiGetsCommand, MultiGatCommand, MultiGatsCommand>;

	Variant command;

	template <typename T>
	    requires std::is_constructible_v<Variant, T>
	explicit MultiKeyCommand(T &&value) : command(std::forward<T>(value)) {
	}

	[[nodiscard]] const std::vector<ItemKey> &item_keys() const {
		return std::visit([](const auto &value) -> const std::vector<ItemKey> & { return value.keys; }, command);
	}

	[[nodiscard]] std::vector<ItemKey> &item_keys() {
		return std::visit([](auto &value) -> std::vector<ItemKey> & { return value.keys; }, command);
	}
	[[nodiscard]] CommandType command_type() const noexcept {
		return std::visit(
		    [](const auto &value) noexcept -> CommandType {
			    return std::remove_cvref_t<decltype(value)>::command_type;
		    },
		    command);
	}
};

// 以下命令不携带用于 hash 分片的 Key。
using NoKeyCommand = std::variant<FlushAllCommand, VerbosityCommand, StatsCommand, VersionCommand, QuitCommand>;

using Command = std::variant<SingleKeyCommand, MultiKeyCommand, NoKeyCommand>;

// 返回协议命令名；例如 SingleGet 和 MultiGet 都是 CommandType::Get。
inline CommandType command_type(const Command &command) {
	return std::visit(
	    [](const auto &group) -> CommandType {
		    using T = std::remove_cvref_t<decltype(group)>;
		    if constexpr (requires { group.command; }) {
			    return std::visit(
			        [](const auto &value) -> CommandType { return std::remove_cvref_t<decltype(value)>::command_type; },
			        group.command);
		    } else {
			    return std::visit(
			        [](const auto &value) -> CommandType { return std::remove_cvref_t<decltype(value)>::command_type; },
			        group);
		    }
	    },
	    command);
}

enum class ParseErrorCode : std::uint8_t {
	UnknownCommand,
	BadArgumentCount,
	BadCommandLine,
	InvalidKey,
	InvalidNumber,
	ValueTooLarge,
	BadDataChunk,
	TooManyKeys,
	CommandLineTooLong,
	InvalidLineEnding
};

enum class ErrorRecovery : std::uint8_t {
	Continue,       // 当前命令已经跳过，剩余字节可以继续解析
	CloseConnection // 消息边界不可信，应该断开连接
};

struct ParseError {
	ParseErrorCode code {};
	ErrorRecovery recovery {};
	std::size_t offset = 0;                  // 错误命令起始位置，相对于本次输入
	bool noreply = false;                    // 仅在命令头可以可靠识别 noreply 时抑制错误响应
	std::optional<ItemKey> rejected_set_key; // 过大的 SET 必须在所属分片删除旧值
};

enum class StopReason : std::uint8_t { EndOfInput, NeedMoreData, Error, BatchLimit, PendingValue };

// 存储命令头已完整、数据块还差很多字节时返回（见 ParserLimits::direct_read_min_bytes）。
// value 已按最终大小分配，输入里已到达的部分已拷入；会话把剩余字节（含结尾 CRLF）直接读进 unfilled()，
// 再调用 MemcachedParser::complete()。value 字节因此只拷贝一次，也不会把大 value 堆在连接的输入缓冲区里。
struct PendingValue {
	SingleKeyCommand command;   // 存储命令，value 尚未填满，不能执行
	std::span<std::byte> frame; // value 的 payload + CRLF 槽位，由 command 持有
	std::size_t filled = 0;     // frame 中已写入的字节数
	std::size_t offset = 0;     // 命令起始位置，相对于产生它的那次输入
	bool noreply = false;

	[[nodiscard]] std::span<std::byte> unfilled() const noexcept {
		return frame.subspan(filled);
	}
};

struct ParseBatchResult {
	std::vector<Command> commands;
	std::size_t consumed = 0;
	StopReason stop = StopReason::EndOfInput;
	std::size_t minimum_additional_bytes = 0;
	// Bytes after consumed that the session must discard before parsing the
	// next command. This lets oversized storage values follow memcached's
	// swallow-and-continue behavior without buffering the payload.
	std::size_t discard_bytes = 0;
	std::optional<ParseError> error;
	// stop == PendingValue 时有值；consumed 已包含拷进 value 的字节。
	std::optional<PendingValue> pending;
};

struct ParserLimits {
	std::size_t max_command_line_bytes = 8 * 1024;
	std::size_t max_value_bytes = 16 * 1024 * 1024;
	std::size_t max_get_keys = 1024;
	// 数据块还差至少这么多字节时返回 PendingValue，由会话直接读进 value；更小的缺口仍走 NeedMoreData，
	// 和后续流水线命令一起读，避免为小 value 多一次系统调用。
	std::size_t direct_read_min_bytes = 4096;
};

// 只解析已经传入的字节流，不进行网络读写。
// 完整命令把 value 一次性拷进 CacheValue（这是 value 唯一的一次拷贝）；解析返回后可以立即消费输入缓冲区。
// 不完整命令不会被消费，网络层追加数据后重新传入未消费的部分；缺口很大的数据块例外，见 PendingValue。
// 协议错误通过 ParseBatchResult::error 返回，内存分配失败等仍可能抛出 C++ 异常。
class MemcachedParser {
public:
	explicit MemcachedParser(ParserLimits limits = {}) noexcept : limits_(limits) {
	}

	[[nodiscard]] ParseBatchResult parse(const std::span<const std::byte> input,
	                                     const std::size_t max_commands = 128) const {
		const auto *data = reinterpret_cast<const char *>(input.data());
		return parse(std::span<const char>(data, input.size()), max_commands);
	}

	[[nodiscard]] ParseBatchResult parse(const std::span<const char> input,
	                                     const std::size_t max_commands = 128) const {
		ParseBatchResult result;
		if (max_commands == 0) {
			result.stop = StopReason::BatchLimit;
			return result;
		}

		// 一批命令共用一次时钟读数；没有带相对过期时间的命令时不读时钟。
		ExpiryCache expiry;
		while (result.consumed < input.size()) {
			if (result.commands.size() == max_commands) {
				result.stop = StopReason::BatchLimit;
				return result;
			}

			OneResult one = parse_one(input.subspan(result.consumed), result.consumed, expiry);
			if (one.status == OneStatus::NeedMoreData) {
				result.stop = StopReason::NeedMoreData;
				result.minimum_additional_bytes = one.minimum_additional_bytes;
				return result;
			}

			result.consumed += one.consumed;
			if (one.status == OneStatus::Pending) {
				result.stop = StopReason::PendingValue;
				result.pending = std::move(one.pending);
				return result;
			}
			if (one.status == OneStatus::Error) {
				result.stop = StopReason::Error;
				result.error = std::move(one.error);
				result.discard_bytes = one.discard_bytes;
				return result;
			}

			if (one.command) {
				if (result.commands.empty()) {
					// Estimate once from the first complete command. No allocation
					// for empty/incomplete input, and fewer vector growth copies
					// for pipelined batches (bounded by max_commands).
					result.commands.reserve(std::min(max_commands, input.size() / one.consumed));
				}
				result.commands.emplace_back(std::move(*one.command));
			}
		}

		result.stop = StopReason::EndOfInput;
		return result;
	}

	// PendingValue 的 unfilled() 读满后调用。结尾是 CRLF 时返回这一条命令，否则返回 BadDataChunk，
	// 与数据块一次性到达时的结果和恢复语义相同（声明长度加两个字节都已消费）。consumed 恒为 0。
	[[nodiscard]] static ParseBatchResult complete(PendingValue &&pending) {
		if (pending.filled != pending.frame.size()) {
			throw std::logic_error("PendingValue completed before its data block was filled");
		}
		ParseBatchResult result;
		const std::size_t n = pending.frame.size();
		if (pending.frame[n - 2] != std::byte {'\r'} || pending.frame[n - 1] != std::byte {'\n'}) {
			result.stop = StopReason::Error;
			result.error = ParseError {ParseErrorCode::BadDataChunk, ErrorRecovery::Continue, pending.offset,
			                           pending.noreply, std::nullopt};
			return result;
		}
		result.commands.emplace_back(std::in_place_type<SingleKeyCommand>, std::move(pending.command));
		return result;
	}

private:
	// 协议 exptime -> 绝对 expired_at。相对时间需要 now，按批惰性读取一次。
	class ExpiryCache {
	public:
		[[nodiscard]] std::uint32_t to_expired_at(const std::uint32_t exptime) noexcept {
			if (exptime == 0) {
				return util::ExpiryClock::kNever;
			}
			if (now_ == 0) {
				now_ = util::ExpiryClock::now();
				epoch_unix_ = util::ExpiryClock::epoch_unix();
			}
			return util::ExpiryClock::to_expired_at(exptime, now_, epoch_unix_);
		}

	private:
		std::uint32_t now_ = 0; // ExpiryClock::now() >= 2，0 表示还没读
		std::int64_t epoch_unix_ = 0;
	};

	enum class OneStatus : std::uint8_t { Ok, NeedMoreData, Error, Pending };

	struct OneResult {
		OneStatus status = OneStatus::Ok;
		std::size_t consumed = 0;
		std::size_t minimum_additional_bytes = 0;
		std::optional<Command> command;
		std::optional<ParseError> error;
		std::size_t discard_bytes = 0;
		std::optional<PendingValue> pending;
	};

	class TokenCursor {
	public:
		explicit TokenCursor(std::string_view line) noexcept : line_(line) {
		}

		std::string_view next() noexcept {
			skip_spaces();
			const std::size_t start = pos_;
			while (pos_ < line_.size() && line_[pos_] != ' ') {
				++pos_;
			}
			return line_.substr(start, pos_ - start);
		}

		bool done() noexcept {
			skip_spaces();
			return pos_ == line_.size();
		}

		std::string_view rest() noexcept {
			skip_spaces();
			return line_.substr(pos_);
		}

	private:
		void skip_spaces() noexcept {
			while (pos_ < line_.size() && line_[pos_] == ' ') {
				++pos_;
			}
		}

		std::string_view line_;
		std::size_t pos_ = 0;
	};

	template <typename UInt>
	static bool parse_uint(std::string_view text, UInt &value) noexcept {
		if (text.empty()) {
			return false;
		}
		const char *first = text.data();
		const char *last = first + text.size();
		const auto [ptr, ec] = std::from_chars(first, last, value);
		return ec == std::errc {} && ptr == last;
	}

	static bool valid_key(std::string_view key) noexcept {
		if (key.empty() || key.size() > 250) {
			return false;
		}
		for (unsigned char ch : key) {
			if (ch <= 0x20 || ch == 0x7f) {
				return false;
			}
		}
		return true;
	}

	static bool parse_noreply(TokenCursor &cursor, bool &noreply) noexcept {
		const std::string_view token = cursor.next();
		if (token.empty()) {
			noreply = false;
			return cursor.done();
		}
		noreply = token == "noreply";
		return noreply && cursor.done();
	}

	template <typename T>
	static OneResult success(T command, std::size_t consumed) {
		OneResult result;
		result.consumed = consumed;
		// 直接构造指定的内层 variant，避免嵌套 variant 的隐式转换歧义。
		if constexpr (std::is_constructible_v<SingleKeyCommand, T>) {
			result.command.emplace(std::in_place_type<SingleKeyCommand>, std::move(command));
		} else if constexpr (std::is_constructible_v<MultiKeyCommand, T>) {
			result.command.emplace(std::in_place_type<MultiKeyCommand>, std::move(command));
		} else {
			static_assert(std::is_constructible_v<NoKeyCommand, T>, "unsupported command type");
			result.command.emplace(std::in_place_type<NoKeyCommand>, std::move(command));
		}
		return result;
	}

	static OneResult need_more(std::size_t additional) noexcept {
		OneResult result;
		result.status = OneStatus::NeedMoreData;
		result.minimum_additional_bytes = additional;
		return result;
	}

	static OneResult failure(const ParseErrorCode code, const ErrorRecovery recovery, const std::size_t consumed,
	                         const std::size_t offset, const std::size_t discard_bytes = 0,
	                         const bool noreply = false) noexcept {
		OneResult result;
		result.status = OneStatus::Error;
		result.consumed = consumed;
		result.error = ParseError {code, recovery, offset, noreply, std::nullopt};
		result.discard_bytes = discard_bytes;
		return result;
	}

	OneResult parse_storage(const std::span<const char> input, const std::size_t offset, const std::size_t header_bytes,
	                        const std::string_view name, TokenCursor &cursor, ExpiryCache &expiry) const {
		const std::string_view key = cursor.next();
		const std::string_view flags_text = cursor.next();
		const std::string_view exptime_text = cursor.next();
		const std::string_view bytes_text = cursor.next();

		// Match memcached: a malformed header consumes only the line. Bytes
		// after it are parsed as new commands, even if the client sent a value.
		const std::string_view cas_text = name == "cas" ? cursor.next() : std::string_view {};
		if (key.empty() || flags_text.empty() || exptime_text.empty() || bytes_text.empty() ||
		    (name == "cas" && cas_text.empty())) {
			return failure(ParseErrorCode::BadArgumentCount, ErrorRecovery::Continue, header_bytes, offset);
		}
		const std::string_view option = cursor.next();
		if (!cursor.done()) {
			return failure(ParseErrorCode::BadArgumentCount, ErrorRecovery::Continue, header_bytes, offset);
		}
		const bool noreply = option == "noreply";
		if (!valid_key(key)) {
			return failure(ParseErrorCode::InvalidKey, ErrorRecovery::Continue, header_bytes, offset, 0, noreply);
		}

		std::uint32_t flags = 0;
		std::uint32_t exptime = 0;
		std::size_t value_bytes = 0;
		std::uint64_t cas_unique = 0;
		if (!parse_uint(flags_text, flags) || !parse_uint(exptime_text, exptime) ||
		    !parse_uint(bytes_text, value_bytes) || (name == "cas" && !parse_uint(cas_text, cas_unique)) ||
		    value_bytes > static_cast<std::size_t>(std::numeric_limits<std::int32_t>::max() - 2)) {
			return failure(ParseErrorCode::InvalidNumber, ErrorRecovery::Continue, header_bytes, offset, 0, noreply);
		}

		// For an object that is too large, memcached emits SERVER_ERROR and
		// swallows the declared data block. Tell the session to discard it from
		// the socket directly instead of buffering an unbounded payload.
		if (value_bytes > limits_.max_value_bytes) {
			if (value_bytes > std::numeric_limits<std::size_t>::max() - 2) {
				return failure(ParseErrorCode::InvalidNumber, ErrorRecovery::Continue, header_bytes, offset);
			}
			auto result = failure(ParseErrorCode::ValueTooLarge, ErrorRecovery::Continue, header_bytes, offset,
			                      value_bytes + 2, noreply);
			if (name == "set") {
				result.error->rejected_set_key.emplace(std::string(key));
			}
			return result;
		}

		// value_bytes <= INT32_MAX - 2，加上 CRLF 不会溢出。
		const std::size_t frame_bytes = value_bytes + 2;
		const std::size_t available = input.size() - header_bytes;
		const char *value_begin = input.data() + header_bytes;
		if (available < frame_bytes) {
			const std::size_t missing = frame_bytes - available;
			if (missing < limits_.direct_read_min_bytes) {
				return need_more(missing);
			}
			// 大块数据：现在就分配最终的 value，把已到达的部分拷进去，剩下的由会话直接读进 value。
			// 头部字段在收到命令头时就写好，complete() 之后不再改动。
			CacheValuePtr value =
			    CacheValue::create_for_overwrite(value_bytes, flags, expiry.to_expired_at(exptime), cas_unique);
			const std::span<std::byte> frame = value->mutable_bytes_crlf();
			if (available != 0) {
				std::memcpy(frame.data(), value_begin, available);
			}
			OneResult result;
			result.status = OneStatus::Pending;
			result.consumed = input.size();
			result.pending.emplace(PendingValue {storage_command(name, key, std::move(value), noreply), frame,
			                                     available, offset, noreply});
			return result;
		}

		const std::size_t total_bytes = header_bytes + frame_bytes;
		if (value_begin[value_bytes] != '\r' || value_begin[value_bytes + 1] != '\n') {
			return failure(ParseErrorCode::BadDataChunk, ErrorRecovery::Continue, total_bytes, offset, 0, noreply);
		}

		// 唯一一次拷贝 value：从输入缓冲区直接拷进最终的存储块，之后只移动 CacheValuePtr。
		// cas_unique 只有 cas 命令会解析，其他命令为 0。
		CacheValuePtr value = CacheValue::create(std::string_view(value_begin, value_bytes), flags,
		                                         expiry.to_expired_at(exptime), cas_unique);
		OneResult result;
		result.consumed = total_bytes;
		result.command.emplace(std::in_place_type<SingleKeyCommand>,
		                       storage_command(name, key, std::move(value), noreply));
		return result;
	}

	static SingleKeyCommand storage_command(const std::string_view name, const std::string_view key,
	                                        CacheValuePtr value, const bool noreply) {
		ItemKey item_key(std::string {key});
		if (name == "set") {
			return SingleKeyCommand(SetCommand {std::move(item_key), std::move(value), noreply});
		}
		if (name == "add") {
			return SingleKeyCommand(AddCommand {std::move(item_key), std::move(value), noreply});
		}
		if (name == "replace") {
			return SingleKeyCommand(ReplaceCommand {std::move(item_key), std::move(value), noreply});
		}
		if (name == "append") {
			return SingleKeyCommand(AppendCommand {std::move(item_key), std::move(value), noreply});
		}
		if (name == "prepend") {
			return SingleKeyCommand(PrependCommand {std::move(item_key), std::move(value), noreply});
		}
		return SingleKeyCommand(CasCommand {std::move(item_key), std::move(value), noreply});
	}

	OneResult parse_retrieval(std::size_t offset, std::size_t header_bytes, std::string_view name, TokenCursor &cursor,
	                          ExpiryCache &expiry) const {
		std::uint32_t exptime = 0;
		if ((name == "gat" || name == "gats") && !parse_uint(cursor.next(), exptime)) {
			return failure(ParseErrorCode::InvalidNumber, ErrorRecovery::Continue, header_bytes, offset);
		}
		const std::uint32_t expired_at = expiry.to_expired_at(exptime);

		const std::string_view first_key = cursor.next();
		if (first_key.empty()) {
			return failure(ParseErrorCode::BadArgumentCount, ErrorRecovery::Continue, header_bytes, offset);
		}
		if (!valid_key(first_key)) {
			return failure(ParseErrorCode::InvalidKey, ErrorRecovery::Continue, header_bytes, offset);
		}
		if (limits_.max_get_keys == 0) {
			return failure(ParseErrorCode::TooManyKeys, ErrorRecovery::Continue, header_bytes, offset);
		}

		// 一个 Key 时返回 Single*，且不需要创建 vector。
		if (cursor.done()) {
			if (name == "get") {
				return success(SingleGetCommand {ItemKey(std::string(first_key))}, header_bytes);
			}
			if (name == "gets") {
				return success(SingleGetsCommand {ItemKey(std::string(first_key))}, header_bytes);
			}
			if (name == "gat") {
				return success(SingleGatCommand {expired_at, ItemKey(std::string(first_key))}, header_bytes);
			}
			return success(SingleGatsCommand {expired_at, ItemKey(std::string(first_key))}, header_bytes);
		}

		// 至少两个 Key 时返回 Multi*，保留原有数量和合法性检查。
		std::vector<ItemKey> keys;
		keys.push_back(ItemKey(std::string(first_key)));
		while (!cursor.done()) {
			const std::string_view key = cursor.next();
			if (!valid_key(key)) {
				return failure(ParseErrorCode::InvalidKey, ErrorRecovery::Continue, header_bytes, offset);
			}
			if (keys.size() >= limits_.max_get_keys) {
				return failure(ParseErrorCode::TooManyKeys, ErrorRecovery::Continue, header_bytes, offset);
			}
			keys.push_back(ItemKey(std::string(key)));
		}

		if (name == "get") {
			return success(MultiGetCommand {std::move(keys)}, header_bytes);
		}
		if (name == "gets") {
			return success(MultiGetsCommand {std::move(keys)}, header_bytes);
		}
		if (name == "gat") {
			return success(MultiGatCommand {expired_at, std::move(keys)}, header_bytes);
		}
		return success(MultiGatsCommand {expired_at, std::move(keys)}, header_bytes);
	}

	[[nodiscard]] OneResult parse_one(const std::span<const char> input, const std::size_t offset,
	                                  ExpiryCache &expiry) const {
		// memcached accepts LF and CRLF command lines; value blocks still
		// require CRLF. memchr uses the libc vectorized scan on the hot path.
		const auto *newline = static_cast<const char *>(std::memchr(input.data(), '\n', input.size()));
		if (!newline) {
			if (input.size() > limits_.max_command_line_bytes) {
				return failure(ParseErrorCode::CommandLineTooLong, ErrorRecovery::CloseConnection, 0, offset);
			}
			return need_more(1);
		}
		std::size_t line_length = static_cast<std::size_t>(newline - input.data());
		const std::size_t header_bytes = line_length + 1;
		if (line_length != 0 && input[line_length - 1] == '\r') {
			--line_length;
		}
		if (line_length > limits_.max_command_line_bytes) {
			return failure(ParseErrorCode::CommandLineTooLong, ErrorRecovery::CloseConnection, 0, offset);
		}
		TokenCursor cursor(std::string_view(input.data(), line_length));
		const std::string_view name = cursor.next();

		if (name.empty()) {
			return failure(ParseErrorCode::UnknownCommand, ErrorRecovery::Continue, header_bytes, offset);
		}
		if (name == "set" || name == "add" || name == "replace" || name == "append" || name == "prepend" ||
		    name == "cas") {
			return parse_storage(input, offset, header_bytes, name, cursor, expiry);
		}
		if (name == "get" || name == "gets" || name == "gat" || name == "gats") {
			return parse_retrieval(offset, header_bytes, name, cursor, expiry);
		}
		if (name == "delete") {
			const std::string_view key = cursor.next();
			if (!valid_key(key)) {
				return failure(ParseErrorCode::InvalidKey, ErrorRecovery::Continue, header_bytes, offset);
			}
			bool noreply = false;
			if (!parse_noreply(cursor, noreply)) {
				return failure(ParseErrorCode::BadCommandLine, ErrorRecovery::Continue, header_bytes, offset);
			}
			return success(DeleteCommand {ItemKey(std::string(key)), noreply}, header_bytes);
		}
		if (name == "touch") {
			const std::string_view key = cursor.next();
			if (!valid_key(key)) {
				return failure(ParseErrorCode::InvalidKey, ErrorRecovery::Continue, header_bytes, offset);
			}
			std::uint32_t exptime = 0;
			if (!parse_uint(cursor.next(), exptime)) {
				return failure(ParseErrorCode::InvalidNumber, ErrorRecovery::Continue, header_bytes, offset);
			}
			bool noreply = false;
			if (!parse_noreply(cursor, noreply)) {
				return failure(ParseErrorCode::BadCommandLine, ErrorRecovery::Continue, header_bytes, offset);
			}
			return success(TouchCommand {ItemKey(std::string(key)), expiry.to_expired_at(exptime), noreply},
			               header_bytes);
		}
		if (name == "incr" || name == "decr") {
			const std::string_view key = cursor.next();
			if (!valid_key(key)) {
				return failure(ParseErrorCode::InvalidKey, ErrorRecovery::Continue, header_bytes, offset);
			}
			std::uint64_t delta = 0;
			if (!parse_uint(cursor.next(), delta)) {
				return failure(ParseErrorCode::InvalidNumber, ErrorRecovery::Continue, header_bytes, offset);
			}
			bool noreply = false;
			if (!parse_noreply(cursor, noreply)) {
				return failure(ParseErrorCode::BadCommandLine, ErrorRecovery::Continue, header_bytes, offset);
			}
			if (name == "incr") {
				return success(IncrCommand {ItemKey(std::string(key)), delta, noreply}, header_bytes);
			}
			return success(DecrCommand {ItemKey(std::string(key)), delta, noreply}, header_bytes);
		}
		if (name == "flush_all") {
			std::uint32_t delay = 0;
			bool noreply = false;
			const std::string_view first = cursor.next();
			if (!first.empty()) {
				if (first == "noreply") {
					noreply = true;
					if (!cursor.done()) {
						return failure(ParseErrorCode::BadCommandLine, ErrorRecovery::Continue, header_bytes, offset);
					}
				} else {
					if (!parse_uint(first, delay)) {
						return failure(ParseErrorCode::InvalidNumber, ErrorRecovery::Continue, header_bytes, offset);
					}
					if (!parse_noreply(cursor, noreply)) {
						return failure(ParseErrorCode::BadCommandLine, ErrorRecovery::Continue, header_bytes, offset);
					}
				}
			}
			return success(FlushAllCommand {delay, noreply}, header_bytes);
		}
		if (name == "verbosity") {
			std::uint32_t level = 0;
			if (!parse_uint(cursor.next(), level)) {
				return failure(ParseErrorCode::InvalidNumber, ErrorRecovery::Continue, header_bytes, offset);
			}
			bool noreply = false;
			if (!parse_noreply(cursor, noreply)) {
				return failure(ParseErrorCode::BadCommandLine, ErrorRecovery::Continue, header_bytes, offset);
			}
			return success(VerbosityCommand {level, noreply}, header_bytes);
		}
		if (name == "stats") {
			return success(StatsCommand {std::string(cursor.rest())}, header_bytes);
		}
		if (name == "version" || name == "quit") {
			if (!cursor.done()) {
				return failure(ParseErrorCode::BadCommandLine, ErrorRecovery::Continue, header_bytes, offset);
			}
			if (name == "version") {
				return success(VersionCommand {}, header_bytes);
			}
			return success(QuitCommand {}, header_bytes);
		}
		return failure(ParseErrorCode::UnknownCommand, ErrorRecovery::Continue, header_bytes, offset);
	}

	ParserLimits limits_;
};
} // namespace fastest
