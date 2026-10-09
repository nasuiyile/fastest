#pragma once

#include <charconv>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <optional>
#include <span>
#include <string_view>
#include <system_error>
#include <type_traits>
#include <utility>
#include <variant>
#include <vector>

namespace protocol {
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
    FlushAll,
    Verbosity,
    Stats,
    Version,
    Quit
};

// 使用模板复用相同字段，但每个模板实例都是不同的 C++ 类型。
// cas_unique 只在 CasCommand 中存在。
template <CommandType Type>
struct StorageCommandT {
    static constexpr CommandType command_type = Type;

    std::string_view key;
    std::uint32_t flags = 0;
    std::uint32_t exptime = 0;
    std::string_view value;
    bool noreply = false;
};

using SetCommand = StorageCommandT<CommandType::Set>;
using AddCommand = StorageCommandT<CommandType::Add>;
using ReplaceCommand = StorageCommandT<CommandType::Replace>;
using AppendCommand = StorageCommandT<CommandType::Append>;
using PrependCommand = StorageCommandT<CommandType::Prepend>;

struct CasCommand {
    static constexpr CommandType command_type = CommandType::Cas;

    std::string_view key;
    std::uint32_t flags = 0;
    std::uint32_t exptime = 0;
    std::string_view value;
    std::uint64_t cas_unique = 0;
    bool noreply = false;
};

// 相同的协议命令根据 Key 数量有不同类型，单 Key 不需要动态分配 vector。
template <CommandType Type>
struct SingleRetrievalCommandT {
    static constexpr CommandType command_type = Type;

    std::string_view key;
};

template <CommandType Type>
struct MultiRetrievalCommandT {
    static constexpr CommandType command_type = Type;

    std::vector<std::string_view> keys;
};

using SingleGetCommand = SingleRetrievalCommandT<CommandType::Get>;
using MultiGetCommand = MultiRetrievalCommandT<CommandType::Get>;
using SingleGetsCommand = SingleRetrievalCommandT<CommandType::Gets>;
using MultiGetsCommand = MultiRetrievalCommandT<CommandType::Gets>;

template <CommandType Type>
struct SingleGetAndTouchCommandT {
    static constexpr CommandType command_type = Type;

    std::uint32_t exptime = 0;
    std::string_view key;
};

template <CommandType Type>
struct MultiGetAndTouchCommandT {
    static constexpr CommandType command_type = Type;

    std::uint32_t exptime = 0;
    std::vector<std::string_view> keys;
};

using SingleGatCommand = SingleGetAndTouchCommandT<CommandType::Gat>;
using MultiGatCommand = MultiGetAndTouchCommandT<CommandType::Gat>;
using SingleGatsCommand = SingleGetAndTouchCommandT<CommandType::Gats>;
using MultiGatsCommand = MultiGetAndTouchCommandT<CommandType::Gats>;

struct DeleteCommand {
    static constexpr CommandType command_type = CommandType::Delete;

    std::string_view key;
    bool noreply = false;
};

struct TouchCommand {
    static constexpr CommandType command_type = CommandType::Touch;

    std::string_view key;
    std::uint32_t exptime = 0;
    bool noreply = false;
};

template <CommandType Type>
struct ArithmeticCommandT {
    static constexpr CommandType command_type = Type;

    std::string_view key;
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

    std::string_view arguments;
};

struct VersionCommand {
    static constexpr CommandType command_type = CommandType::Version;
};

struct QuitCommand {
    static constexpr CommandType command_type = CommandType::Quit;
};

// 第一层按路由粒度区分；第二层保留每种具体命令的独立 variant 类型。
using SingleKeyCommand =
    std::variant<SetCommand, AddCommand, ReplaceCommand, AppendCommand, PrependCommand, CasCommand,
                 SingleGetCommand, SingleGetsCommand, SingleGatCommand, SingleGatsCommand,
                 DeleteCommand, TouchCommand, IncrCommand, DecrCommand>;

using MultiKeyCommand = std::variant<MultiGetCommand, MultiGetsCommand, MultiGatCommand, MultiGatsCommand>;

// 以下命令不携带用于 hash 分片的 Key。
using NoKeyCommand = std::variant<FlushAllCommand, VerbosityCommand, StatsCommand, VersionCommand, QuitCommand>;

using Command = std::variant<SingleKeyCommand, MultiKeyCommand, NoKeyCommand>;

// 返回协议命令名；例如 SingleGet 和 MultiGet 都是 CommandType::Get。
inline CommandType command_type(const Command &command) {
    return std::visit(
        [](const auto &group) -> CommandType {
            return std::visit(
                [](const auto &value) -> CommandType {
                    return std::remove_cvref_t<decltype(value)>::command_type;
                },
                group);
        },
        command);
}

enum class ParseErrorCode : std::uint8_t {
    UnknownCommand,
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
    std::size_t offset = 0; // 错误命令起始位置，相对于本次输入
};

enum class StopReason : std::uint8_t { EndOfInput, NeedMoreData, Error, BatchLimit };

struct ParseBatchResult {
    std::vector<Command> commands;
    std::size_t consumed = 0;
    StopReason stop = StopReason::EndOfInput;
    std::size_t minimum_additional_bytes = 0;
    std::optional<ParseError> error;
};

struct ParserLimits {
    std::size_t max_command_line_bytes = 8 * 1024;
    std::size_t max_value_bytes = 16 * 1024 * 1024;
    std::size_t max_get_keys = 1024;
};

// 只解析已经传入的字节流，不进行网络读写。
// 完整命令通过 string_view 引用 input；处理完成前不得移动、覆盖或释放输入缓冲区。
// 不完整命令不会被消费，网络层追加数据后重新传入未消费的部分。
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

       while (result.consumed < input.size()) {
          if (result.commands.size() == max_commands) {
             result.stop = StopReason::BatchLimit;
             return result;
          }

          OneResult one = parse_one(input.subspan(result.consumed), result.consumed);
          if (one.status == OneStatus::NeedMoreData) {
             result.stop = StopReason::NeedMoreData;
             result.minimum_additional_bytes = one.minimum_additional_bytes;
             return result;
          }

          result.consumed += one.consumed;
          if (one.status == OneStatus::Error) {
             result.stop = StopReason::Error;
             result.error = one.error;
             return result;
          }

          if (one.command) {
             result.commands.emplace_back(std::move(*one.command));
          }
       }

       result.stop = StopReason::EndOfInput;
       return result;
    }

private:
    enum class OneStatus : std::uint8_t { Ok, NeedMoreData, Error };

    struct OneResult {
       OneStatus status = OneStatus::Ok;
       std::size_t consumed = 0;
       std::size_t minimum_additional_bytes = 0;
       std::optional<Command> command;
       std::optional<ParseError> error;
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

    static OneResult skip_empty(std::size_t consumed) noexcept {
       OneResult result;
       result.consumed = consumed;
       return result;
    }

    static OneResult need_more(std::size_t additional) noexcept {
       OneResult result;
       result.status = OneStatus::NeedMoreData;
       result.minimum_additional_bytes = additional;
       return result;
    }

    static OneResult failure(const ParseErrorCode code, const ErrorRecovery recovery, const std::size_t consumed,
                             const std::size_t offset) noexcept {
       OneResult result;
       result.status = OneStatus::Error;
       result.consumed = consumed;
       result.error = ParseError {code, recovery, offset};
       return result;
    }

    OneResult parse_storage(const std::span<const char> input, const std::size_t offset, const std::size_t header_bytes,
                            const std::string_view name, TokenCursor &cursor) const {
       const std::string_view key = cursor.next();
       const std::string_view flags_text = cursor.next();
       const std::string_view exptime_text = cursor.next();
       const std::string_view bytes_text = cursor.next();

       // bytes 无法确认时，不能知道下一条命令的起点，必须关闭连接。
       if (key.empty() || flags_text.empty() || exptime_text.empty() || bytes_text.empty()) {
          return failure(ParseErrorCode::BadCommandLine, ErrorRecovery::CloseConnection, header_bytes, offset);
       }
       if (!valid_key(key)) {
          return failure(ParseErrorCode::InvalidKey, ErrorRecovery::CloseConnection, header_bytes, offset);
       }

       std::uint32_t flags = 0;
       std::uint32_t exptime = 0;
       std::size_t value_bytes = 0;
       if (!parse_uint(flags_text, flags) || !parse_uint(exptime_text, exptime) ||
           !parse_uint(bytes_text, value_bytes)) {
          return failure(ParseErrorCode::InvalidNumber, ErrorRecovery::CloseConnection, header_bytes, offset);
       }
       if (value_bytes > limits_.max_value_bytes) {
          return failure(ParseErrorCode::ValueTooLarge, ErrorRecovery::CloseConnection, header_bytes, offset);
       }

       std::uint64_t cas_unique = 0;
       if (name == "cas" && !parse_uint(cursor.next(), cas_unique)) {
          return failure(ParseErrorCode::InvalidNumber, ErrorRecovery::CloseConnection, header_bytes, offset);
       }
       bool noreply = false;
       if (!parse_noreply(cursor, noreply)) {
          return failure(ParseErrorCode::BadCommandLine, ErrorRecovery::CloseConnection, header_bytes, offset);
       }

       const std::size_t available = input.size() - header_bytes;
       if (value_bytes > available || available - value_bytes < 2) {
          const std::size_t missing_value = value_bytes > available ? value_bytes - available : 0;
          const std::size_t suffix_available = available >= value_bytes ? available - value_bytes : 0;
          const std::size_t missing_suffix = 2 - (suffix_available < 2 ? suffix_available : 2);

          // 在自定义极端 ParserLimits 下，也不允许 size_t 溢出。
          const std::size_t max = std::numeric_limits<std::size_t>::max();
          const std::size_t missing = missing_value > max - missing_suffix ? max : missing_value + missing_suffix;
          return need_more(missing);
       }

       const char *value_begin = input.data() + header_bytes;
       if (value_begin[value_bytes] != '\r' || value_begin[value_bytes + 1] != '\n') {
          return failure(ParseErrorCode::BadDataChunk, ErrorRecovery::CloseConnection, header_bytes + value_bytes + 2,
                         offset);
       }

       const std::string_view value(value_begin, value_bytes);
       const std::size_t total_bytes = header_bytes + value_bytes + 2;

       if (name == "set") {
          return success(SetCommand {key, flags, exptime, value, noreply}, total_bytes);
       }
       if (name == "add") {
          return success(AddCommand {key, flags, exptime, value, noreply}, total_bytes);
       }
       if (name == "replace") {
          return success(ReplaceCommand {key, flags, exptime, value, noreply}, total_bytes);
       }
       if (name == "append") {
          return success(AppendCommand {key, flags, exptime, value, noreply}, total_bytes);
       }
       if (name == "prepend") {
          return success(PrependCommand {key, flags, exptime, value, noreply}, total_bytes);
       }
       return success(CasCommand {key, flags, exptime, value, cas_unique, noreply}, total_bytes);
    }

    OneResult parse_retrieval(std::size_t offset, std::size_t header_bytes, std::string_view name,
                              TokenCursor &cursor) const {
       std::uint32_t exptime = 0;
       if ((name == "gat" || name == "gats") && !parse_uint(cursor.next(), exptime)) {
          return failure(ParseErrorCode::InvalidNumber, ErrorRecovery::Continue, header_bytes, offset);
       }

       const std::string_view first_key = cursor.next();
       if (first_key.empty()) {
          return failure(ParseErrorCode::BadCommandLine, ErrorRecovery::Continue, header_bytes, offset);
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
             return success(SingleGetCommand {first_key}, header_bytes);
          }
          if (name == "gets") {
             return success(SingleGetsCommand {first_key}, header_bytes);
          }
          if (name == "gat") {
             return success(SingleGatCommand {exptime, first_key}, header_bytes);
          }
          return success(SingleGatsCommand {exptime, first_key}, header_bytes);
       }

       // 至少两个 Key 时返回 Multi*，保留原有数量和合法性检查。
       std::vector<std::string_view> keys;
       keys.push_back(first_key);
       while (!cursor.done()) {
          const std::string_view key = cursor.next();
          if (!valid_key(key)) {
             return failure(ParseErrorCode::InvalidKey, ErrorRecovery::Continue, header_bytes, offset);
          }
          if (keys.size() >= limits_.max_get_keys) {
             return failure(ParseErrorCode::TooManyKeys, ErrorRecovery::Continue, header_bytes, offset);
          }
          keys.push_back(key);
       }

       if (name == "get") {
          return success(MultiGetCommand {std::move(keys)}, header_bytes);
       }
       if (name == "gets") {
          return success(MultiGetsCommand {std::move(keys)}, header_bytes);
       }
       if (name == "gat") {
          return success(MultiGatCommand {exptime, std::move(keys)}, header_bytes);
       }
       return success(MultiGatsCommand {exptime, std::move(keys)}, header_bytes);
    }

    [[nodiscard]] OneResult parse_one(const std::span<const char> input, const std::size_t offset) const {
       std::size_t line_length = 0;
       bool has_line = false;

       for (std::size_t i = 0; i < input.size(); ++i) {
          if (i > limits_.max_command_line_bytes) {
             return failure(ParseErrorCode::CommandLineTooLong, ErrorRecovery::CloseConnection, 0, offset);
          }
          if (input[i] == '\n') {
             return failure(ParseErrorCode::InvalidLineEnding, ErrorRecovery::CloseConnection, 0, offset);
          }
          if (input[i] == '\r') {
             if (i + 1 == input.size()) {
                return need_more(1);
             }
             if (input[i + 1] != '\n') {
                return failure(ParseErrorCode::InvalidLineEnding, ErrorRecovery::CloseConnection, 0, offset);
             }
             line_length = i;
             has_line = true;
             break;
          }
          if (i == limits_.max_command_line_bytes) {
             return failure(ParseErrorCode::CommandLineTooLong, ErrorRecovery::CloseConnection, 0, offset);
          }
       }
       if (!has_line) {
          return need_more(1);
       }

       const std::size_t header_bytes = line_length + 2;
       TokenCursor cursor(std::string_view(input.data(), line_length));
       const std::string_view name = cursor.next();

       if (name.empty()) {
          return skip_empty(header_bytes);
       }
       if (name == "set" || name == "add" || name == "replace" || name == "append" || name == "prepend" ||
           name == "cas") {
          return parse_storage(input, offset, header_bytes, name, cursor);
       }
       if (name == "get" || name == "gets" || name == "gat" || name == "gats") {
          return parse_retrieval(offset, header_bytes, name, cursor);
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
          return success(DeleteCommand {key, noreply}, header_bytes);
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
          return success(TouchCommand {key, exptime, noreply}, header_bytes);
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
             return success(IncrCommand {key, delta, noreply}, header_bytes);
          }
          return success(DecrCommand {key, delta, noreply}, header_bytes);
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
          return success(StatsCommand {cursor.rest()}, header_bytes);
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
} // namespace protocol
