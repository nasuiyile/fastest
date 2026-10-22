#pragma once

#include "memcached_parser.h"
#include <array>
#include <charconv>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <optional>
#include <span>
#include <stdexcept>
#include <string>
#include <string_view>
#include <system_error>
#include <type_traits>
#include <utility>
#include <variant>
#include <vector>
#include <boost/asio/buffer.hpp>

namespace fastest {

// C++20 + Boost.Asio。这里只做同步编码，不执行 socket I/O：GatherSink 把一批响应编码成
// const_buffer 列表，由调用方用一次 async_write 发出。写完成前，响应对象和 GatherSink 必须保持有效且不被修改。
enum class ResponseType : std::uint8_t {
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
	Quit,
	Error,
	NoReply
};

enum class StorageStatus : std::uint8_t { Stored, NotStored, NoReply };

template <ResponseType Type>
struct StorageResponseT {
	static constexpr ResponseType response_type = Type;
	StorageStatus status = StorageStatus::Stored;
};

using SetResponse = StorageResponseT<ResponseType::Set>;
using AddResponse = StorageResponseT<ResponseType::Add>;
using ReplaceResponse = StorageResponseT<ResponseType::Replace>;
using AppendResponse = StorageResponseT<ResponseType::Append>;
using PrependResponse = StorageResponseT<ResponseType::Prepend>;

enum class CasStatus : std::uint8_t { Stored, Exists, NotFound, NoReply };

struct CasResponse {
	static constexpr ResponseType response_type = ResponseType::Cas;
	CasStatus status = CasStatus::Stored;
};

// 命中项共享 map 里的 CacheValue（只加一次引用计数，不复制 value 字节）；flags、长度、cas 都取自 value。
// key 从命令里移动过来。GatherSink 对超过 kInlineBytes 的 value 直接引用 CacheValue 的 payload+CRLF，
// CacheValue 地址稳定且发布后只读；响应对象（以及它持有的这次引用）必须活到写完成。
struct ValueItem {
	std::string key;
	CacheValuePtr value;
};

// 与 ValueItem 字段相同；独立类型只用于让 gets/gats 在编码时额外输出 value->cas。
struct CasValueItem {
	std::string key;
	CacheValuePtr value;
};

template <ResponseType Type, typename Item>
struct SingleRetrievalResponseT {
	static constexpr ResponseType response_type = Type;
	std::optional<Item> item;
};

template <ResponseType Type, typename Item>
struct MultiRetrievalResponseT {
	static constexpr ResponseType response_type = Type;
	std::vector<Item> items;
};

using SingleGetResponse = SingleRetrievalResponseT<ResponseType::Get, ValueItem>;
using SingleGetsResponse = SingleRetrievalResponseT<ResponseType::Gets, CasValueItem>;
using SingleGatResponse = SingleRetrievalResponseT<ResponseType::Gat, ValueItem>;
using SingleGatsResponse = SingleRetrievalResponseT<ResponseType::Gats, CasValueItem>;

using MultiGetResponse = MultiRetrievalResponseT<ResponseType::Get, ValueItem>;
using MultiGetsResponse = MultiRetrievalResponseT<ResponseType::Gets, CasValueItem>;
using MultiGatResponse = MultiRetrievalResponseT<ResponseType::Gat, ValueItem>;
using MultiGatsResponse = MultiRetrievalResponseT<ResponseType::Gats, CasValueItem>;

enum class DeleteStatus : std::uint8_t { Deleted, NotFound, NoReply };

struct DeleteResponse {
	static constexpr ResponseType response_type = ResponseType::Delete;
	DeleteStatus status = DeleteStatus::Deleted;
};

enum class TouchStatus : std::uint8_t { Touched, NotFound, NoReply };

struct TouchResponse {
	static constexpr ResponseType response_type = ResponseType::Touch;
	TouchStatus status = TouchStatus::Touched;
};

// Value 输出计算后的数值（只有这时 value 有意义）；NotFound 输出 NOT_FOUND；
// NonNumeric 输出 memcached 的 CLIENT_ERROR cannot increment or decrement non-numeric value；NoReply 不输出。
enum class ArithmeticStatus : std::uint8_t { Value, NotFound, NonNumeric, NoReply };

template <ResponseType Type>
struct ArithmeticResponseT {
	static constexpr ResponseType response_type = Type;
	std::uint64_t value = 0;
	ArithmeticStatus status = ArithmeticStatus::Value;
};

using IncrResponse = ArithmeticResponseT<ResponseType::Incr>;
using DecrResponse = ArithmeticResponseT<ResponseType::Decr>;

enum class OkStatus : std::uint8_t { Ok, NoReply };

struct FlushAllResponse {
	static constexpr ResponseType response_type = ResponseType::FlushAll;
	OkStatus status = OkStatus::Ok;
};

struct VerbosityResponse {
	static constexpr ResponseType response_type = ResponseType::Verbosity;
	OkStatus status = OkStatus::Ok;
};

enum class StatsLineType : std::uint8_t { Stat, Item, Prefix };

struct StatsLine {
	StatsLineType type = StatsLineType::Stat;
	std::string key;
	std::string value;
};

enum class StatsTerminator : std::uint8_t { End, Reset, Ok };

struct StatsResponse {
	static constexpr ResponseType response_type = ResponseType::Stats;
	std::vector<StatsLine> lines;
	StatsTerminator terminator = StatsTerminator::End;
};

struct VersionResponse {
	static constexpr ResponseType response_type = ResponseType::Version;
	std::string version;
};

struct QuitResponse {
	static constexpr ResponseType response_type = ResponseType::Quit;
};

enum class ResponseErrorKind : std::uint8_t { Error, ClientError, ServerError };

// message 只能指向静态存储期的字符串（字面量）：错误响应会跨分片、在环形数组里等待，并被 GatherSink 直接引用。
// 用 string_view 而不是 std::string，不需要堆分配，跨分片的请求条目（HopEntry）也因此小 16 字节。
struct ErrorResponse {
	static constexpr ResponseType response_type = ResponseType::Error;
	ResponseErrorKind kind = ResponseErrorKind::Error;
	std::string_view message;
	bool close_connection = false;
	bool noreply = false;
};

// 将解析错误转换为可直接编码发送或放入 ResponseBatch 的响应。
// 未知命令使用协议规定的 ERROR；对象过大使用可恢复的 SERVER_ERROR，其他错误使用 CLIENT_ERROR。
[[nodiscard]] inline ErrorResponse to_error_response(const ParseError &error) {
	ErrorResponse response;
	response.kind = ResponseErrorKind::ClientError;
	response.close_connection = error.recovery == ErrorRecovery::CloseConnection;
	response.noreply = error.noreply;
	switch (error.code) {
	case ParseErrorCode::BadArgumentCount:
	case ParseErrorCode::UnknownCommand:
		response.kind = ResponseErrorKind::Error;
		response.message = "unknown command";
		break;
	case ParseErrorCode::BadCommandLine:
	case ParseErrorCode::InvalidKey:
	case ParseErrorCode::InvalidNumber:
		response.message = "bad command line format";
		break;
	case ParseErrorCode::ValueTooLarge:
		// The session swallows the declared data block before parsing again.
		response.kind = ResponseErrorKind::ServerError;
		response.message = "object too large for cache";
		break;
	case ParseErrorCode::BadDataChunk:
		response.message = "bad data chunk";
		break;
	case ParseErrorCode::TooManyKeys:
		response.message = "too many keys";
		break;
	case ParseErrorCode::CommandLineTooLong:
		response.message = "command line too long";
		break;
	case ParseErrorCode::InvalidLineEnding:
		response.message = "invalid line ending";
		break;
	case ParseErrorCode::InvalidExptime:
		response.message = "invalid exptime argument";
		break;
	case ParseErrorCode::InvalidDelta:
		response.message = "invalid numeric delta argument";
		break;
	case ParseErrorCode::BadDeleteArguments:
		// 与 memcached 逐字节相同（句号后两个空格）。
		response.message = "bad command line format.  Usage: delete <key> [noreply]";
		break;
	default:
		response.kind = ResponseErrorKind::ServerError;
		response.message = "unknown parse error code";
		response.close_connection = true;
		break;
	}
	return response;
}

struct NoReplyResponse {
	static constexpr ResponseType response_type = ResponseType::NoReply;
};

// std::variant 的别名不能增加成员方法，因此使用公开继承的薄包装；不增加数据成员。
template <typename... Types>
class SocketResponseGroup : public std::variant<Types...> {
public:
	using Variant = std::variant<Types...>;
	using Variant::Variant;
	using Variant::operator=;

	Variant &variant() noexcept {
		return *this;
	}
	const Variant &variant() const noexcept {
		return *this;
	}
};

using SingleKeyResponse =
    SocketResponseGroup<SetResponse, AddResponse, ReplaceResponse, AppendResponse, PrependResponse, CasResponse,
                        SingleGetResponse, SingleGetsResponse, SingleGatResponse, SingleGatsResponse, DeleteResponse,
                        TouchResponse, IncrResponse, DecrResponse>;

using MultiKeyResponse = SocketResponseGroup<MultiGetResponse, MultiGetsResponse, MultiGatResponse, MultiGatsResponse>;

using NoKeyResponse = SocketResponseGroup<FlushAllResponse, VerbosityResponse, StatsResponse, VersionResponse,
                                          QuitResponse, NoReplyResponse>;

// 错误可能来自任何命令，独立于正常响应的 key 分类。
using Response = std::variant<SingleKeyResponse, MultiKeyResponse, NoKeyResponse, ErrorResponse>;
using ResponseBatch = std::vector<Response>;

template <typename T>
Response make_response(T value) {
	if constexpr (std::is_same_v<T, ErrorResponse>) {
		return Response(std::in_place_type<ErrorResponse>, std::move(value));
	} else if constexpr (std::is_constructible_v<SingleKeyResponse, T>) {
		return Response(std::in_place_type<SingleKeyResponse>, std::move(value));
	} else if constexpr (std::is_constructible_v<MultiKeyResponse, T>) {
		return Response(std::in_place_type<MultiKeyResponse>, std::move(value));
	} else {
		static_assert(std::is_constructible_v<NoKeyResponse, T>, "unsupported response type");
		return Response(std::in_place_type<NoKeyResponse>, std::move(value));
	}
}

inline ResponseType response_type(const Response &response) {
	return std::visit(
	    [](const auto &value) -> ResponseType {
		    using T = std::remove_cvref_t<decltype(value)>;
		    if constexpr (std::is_same_v<T, ErrorResponse>) {
			    return T::response_type;
		    } else {
			    return std::visit(
			        [](const auto &concrete) { return std::remove_cvref_t<decltype(concrete)>::response_type; },
			        value.variant());
		    }
	    },
	    response);
}

namespace detail {

template <typename T>
inline constexpr bool always_false = false;

template <typename T>
struct IsStorageResponse : std::false_type {};
template <ResponseType Type>
struct IsStorageResponse<StorageResponseT<Type>> : std::true_type {};

template <typename T>
struct IsSingleRetrievalResponse : std::false_type {};
template <ResponseType Type, typename Item>
struct IsSingleRetrievalResponse<SingleRetrievalResponseT<Type, Item>> : std::true_type {};

template <typename T>
struct IsMultiRetrievalResponse : std::false_type {};
template <ResponseType Type, typename Item>
struct IsMultiRetrievalResponse<MultiRetrievalResponseT<Type, Item>> : std::true_type {};

template <typename T>
struct IsArithmeticResponse : std::false_type {};
template <ResponseType Type>
struct IsArithmeticResponse<ArithmeticResponseT<Type>> : std::true_type {};

inline void validate_key(std::string_view key) {
	if (key.empty() || key.size() > 250) {
		throw std::invalid_argument("invalid response key length");
	}
	for (unsigned char ch : key) {
		if (ch <= 0x20 || ch == 0x7f) {
			throw std::invalid_argument("invalid response key characters");
		}
	}
}

inline void validate_line(std::string_view text) {
	for (unsigned char ch : text) {
		if (ch == '\r' || ch == '\n' || ch == 0) {
			throw std::invalid_argument("response line contains CR/LF/NUL");
		}
	}
}

inline void validate_token(std::string_view token) {
	if (token.empty()) {
		throw std::invalid_argument("empty response token");
	}
	for (unsigned char ch : token) {
		if (ch <= 0x20 || ch == 0x7f) {
			throw std::invalid_argument("invalid response token");
		}
	}
}

// 兼容原有 encode_batch；只有显式调用编码接口时才生成完整字节串。
struct StringSink {
	static constexpr bool validation_only = false;
	std::string &bytes;

	void append(std::string_view text) {
		if (!text.empty()) {
			bytes.append(text.data(), text.size());
		}
	}

	template <typename UInt>
	void append_uint(UInt number) {
		std::array<char, std::numeric_limits<UInt>::digits10 + 1> buffer;
		const auto [last, error] = std::to_chars(buffer.data(), buffer.data() + buffer.size(), number);
		if (error != std::errc {}) {
			throw std::logic_error("failed to encode unsigned integer");
		}
		append(std::string_view(buffer.data(), static_cast<std::size_t>(last - buffer.data())));
	}
};

inline std::string_view storage_text(StorageStatus status) {
	switch (status) {
	case StorageStatus::Stored:
		return "STORED\r\n";
	case StorageStatus::NotStored:
		return "NOT_STORED\r\n";
	case StorageStatus::NoReply:
		return {};
	default:
		throw std::invalid_argument("invalid storage response status");
	}
}

inline std::string_view cas_text(CasStatus status) {
	switch (status) {
	case CasStatus::Stored:
		return "STORED\r\n";
	case CasStatus::Exists:
		return "EXISTS\r\n";
	case CasStatus::NotFound:
		return "NOT_FOUND\r\n";
	case CasStatus::NoReply:
		return {};
	default:
		throw std::invalid_argument("invalid cas response status");
	}
}

inline std::string_view delete_text(DeleteStatus status) {
	switch (status) {
	case DeleteStatus::Deleted:
		return "DELETED\r\n";
	case DeleteStatus::NotFound:
		return "NOT_FOUND\r\n";
	case DeleteStatus::NoReply:
		return {};
	default:
		throw std::invalid_argument("invalid delete response status");
	}
}

inline std::string_view touch_text(TouchStatus status) {
	switch (status) {
	case TouchStatus::Touched:
		return "TOUCHED\r\n";
	case TouchStatus::NotFound:
		return "NOT_FOUND\r\n";
	case TouchStatus::NoReply:
		return {};
	default:
		throw std::invalid_argument("invalid touch response status");
	}
}

inline std::string_view ok_text(OkStatus status) {
	switch (status) {
	case OkStatus::Ok:
		return "OK\r\n";
	case OkStatus::NoReply:
		return {};
	default:
		throw std::invalid_argument("invalid OK response status");
	}
}

inline std::string_view stats_prefix(StatsLineType type) {
	switch (type) {
	case StatsLineType::Stat:
		return "STAT ";
	case StatsLineType::Item:
		return "ITEM ";
	case StatsLineType::Prefix:
		return "PREFIX ";
	default:
		throw std::invalid_argument("invalid stats line type");
	}
}

inline std::string_view stats_end(StatsTerminator terminator) {
	switch (terminator) {
	case StatsTerminator::End:
		return "END\r\n";
	case StatsTerminator::Reset:
		return "RESET\r\n";
	case StatsTerminator::Ok:
		return "OK\r\n";
	default:
		throw std::invalid_argument("invalid stats terminator");
	}
}

// 同步编码函数：GatherSink 用于发送，StringSink 用于 encode_batch；都不执行 socket I/O。
template <typename Sink, typename Item>
void emit_retrieval_item(Sink &out, const Item &item) {
	validate_key(item.key);
	if (!item.value) {
		throw std::invalid_argument("response item without value");
	}
	const CacheValue &value = *item.value;
	out.append("VALUE ");
	out.append(item.key);
	out.append(" ");
	out.append_uint(value.flags);
	out.append(" ");
	out.append_uint(value.size());
	if constexpr (std::is_same_v<Item, CasValueItem>) {
		out.append(" ");
		out.append_uint(value.cas);
	}
	out.append("\r\n");
	// CacheValue 自带结尾 CRLF：payload 和 CRLF 作为一段输出，大 value 只占一个 iovec。
	out.append(value.view_crlf());
}

template <typename Sink>
void emit_stats(Sink &out, const StatsResponse &response) {
	const std::string_view terminator = stats_end(response.terminator);
	if (response.terminator != StatsTerminator::End && !response.lines.empty()) {
		throw std::invalid_argument("stats RESET/OK must not have data lines");
	}
	for (const StatsLine &line : response.lines) {
		validate_token(line.key);
		validate_line(line.value);
		out.append(stats_prefix(line.type));
		out.append(line.key);
		if (!line.value.empty()) {
			out.append(" ");
			out.append(line.value);
		}
		out.append("\r\n");
	}
	out.append(terminator);
}

// 普通文本直接引用原字符串；CR/LF/NUL 替换为静态空格，不创建清洗后的字符串。
template <typename Sink>
void emit_error_message(Sink &out, std::string_view message) {
	if constexpr (Sink::validation_only) {
		return;
	}
	if (message.empty()) {
		out.append("unspecified error");
		return;
	}

	constexpr std::string_view spaces = "                                                                ";
	const auto is_control = [](unsigned char ch) {
		return ch == '\r' || ch == '\n' || ch == 0;
	};
	std::size_t start = 0;
	while (start < message.size()) {
		std::size_t last = start;
		while (last < message.size() && !is_control(static_cast<unsigned char>(message[last]))) {
			++last;
		}
		out.append(message.substr(start, last - start));
		start = last;
		while (last < message.size() && is_control(static_cast<unsigned char>(message[last]))) {
			++last;
		}
		while (start < last) {
			const std::size_t count = last - start < spaces.size() ? last - start : spaces.size();
			out.append(spaces.substr(0, count));
			start += count;
		}
	}
}

template <typename Sink>
void emit_error(Sink &out, const ErrorResponse &error) {
	switch (error.kind) {
	case ResponseErrorKind::Error:
		out.append("ERROR\r\n");
		return;
	case ResponseErrorKind::ClientError:
		out.append("CLIENT_ERROR ");
		break;
	case ResponseErrorKind::ServerError:
		out.append("SERVER_ERROR ");
		break;
	default:
		throw std::invalid_argument("invalid response error kind");
	}
	emit_error_message(out, error.message);
	out.append("\r\n");
}

template <typename Sink, typename T>
bool emit_concrete(Sink &out, const T &value) {
	if constexpr (IsStorageResponse<T>::value) {
		out.append(storage_text(value.status));
	} else if constexpr (std::is_same_v<T, CasResponse>) {
		out.append(cas_text(value.status));
	} else if constexpr (IsSingleRetrievalResponse<T>::value) {
		if (value.item) {
			emit_retrieval_item(out, *value.item);
		}
		out.append("END\r\n");
	} else if constexpr (IsMultiRetrievalResponse<T>::value) {
		for (const auto &item : value.items) {
			emit_retrieval_item(out, item);
		}
		out.append("END\r\n");
	} else if constexpr (std::is_same_v<T, DeleteResponse>) {
		out.append(delete_text(value.status));
	} else if constexpr (std::is_same_v<T, TouchResponse>) {
		out.append(touch_text(value.status));
	} else if constexpr (IsArithmeticResponse<T>::value) {
		switch (value.status) {
		case ArithmeticStatus::Value:
			out.append_uint(value.value);
			out.append("\r\n");
			break;
		case ArithmeticStatus::NotFound:
			out.append("NOT_FOUND\r\n");
			break;
		case ArithmeticStatus::NonNumeric:
			out.append("CLIENT_ERROR cannot increment or decrement non-numeric value\r\n");
			break;
		case ArithmeticStatus::NoReply:
			break;
		default:
			throw std::invalid_argument("invalid arithmetic response status");
		}
	} else if constexpr (std::is_same_v<T, FlushAllResponse> || std::is_same_v<T, VerbosityResponse>) {
		out.append(ok_text(value.status));
	} else if constexpr (std::is_same_v<T, StatsResponse>) {
		emit_stats(out, value);
	} else if constexpr (std::is_same_v<T, VersionResponse>) {
		validate_token(value.version);
		out.append("VERSION ");
		out.append(value.version);
		out.append("\r\n");
	} else if constexpr (std::is_same_v<T, QuitResponse>) {
		return true;
	} else if constexpr (std::is_same_v<T, ErrorResponse>) {
		if (!value.noreply) {
			emit_error(out, value);
		}
		return value.close_connection;
	} else if constexpr (std::is_same_v<T, NoReplyResponse>) {
		// noreply：执行完成，但不向客户端输出任何字节。
	} else {
		static_assert(always_false<T>, "missing response encoder case");
	}
	return false;
}

template <typename Sink, typename Variant>
bool emit_group(Sink &out, const Variant &group) {
	return std::visit([&out](const auto &value) { return emit_concrete(out, value); }, group);
}

template <typename Sink>
bool emit_one(Sink &out, const Response &response) {
	return std::visit(
	    [&out](const auto &value) -> bool {
		    using T = std::remove_cvref_t<decltype(value)>;
		    if constexpr (std::is_same_v<T, ErrorResponse>) {
			    return emit_concrete(out, value);
		    } else {
			    return emit_group(out, value.variant());
		    }
	    },
	    response);
}

} // namespace detail

// 同步分散聚集编码器：把一批响应编码成 const_buffer 列表，交给一次 async_write（writev）发出。
// 协议关键字、数字和不超过 kInlineBytes 的字段拷进连续的 arena，相邻片段合并成一个 buffer；
// 更长的字段只引用原字符串，不复制。arena 片段按偏移记录，buffers() 时才换算成指针，
// 因此编码过程中 arena 扩容不会让已记录的片段失效。buffers() 之后到写完成之前不能再追加或 clear()。
class GatherSink {
public:
	static constexpr bool validation_only = false;
	static constexpr std::size_t kInlineBytes = 256;

	struct Mark {
		std::size_t segments = 0;
		std::size_t last_size = 0;
		std::size_t arena = 0;
		std::size_t bytes = 0;
	};

	void append(std::string_view text) {
		if (text.empty()) {
			return;
		}
		if (text.size() > kInlineBytes) {
			segments_.push_back(Segment {text.data(), 0, text.size()}); // 大段：只记指针，不拷贝
			bytes_ += text.size();
			return;
		}
		append_arena(text.data(), text.size()); // 小段：拷进 arena_
	}

	template <typename UInt>
	void append_uint(UInt number) {
		static_assert(std::is_unsigned_v<UInt> && !std::is_same_v<UInt, bool>);
		std::array<char, std::numeric_limits<UInt>::digits10 + 1> digits;
		const auto [last, error] = std::to_chars(digits.data(), digits.data() + digits.size(), number);
		if (error != std::errc {}) {
			throw std::logic_error("failed to encode unsigned integer");
		}
		append_arena(digits.data(), static_cast<std::size_t>(last - digits.data()));
	}

	// 单条响应编码失败时，用编码前的 mark 撤销它已追加的部分，保证不会发出半个协议响应。
	[[nodiscard]] Mark mark() const noexcept {
		return {segments_.size(), segments_.empty() ? 0 : segments_.back().size, arena_.size(), bytes_};
	}

	void rollback(const Mark &mark) {
		segments_.erase(segments_.begin() + static_cast<std::ptrdiff_t>(mark.segments), segments_.end());
		if (!segments_.empty()) {
			segments_.back().size = mark.last_size; // 最后一段可能在 mark 之后被合并加长过
		}
		arena_.resize(mark.arena);
		bytes_ = mark.bytes;
	}

	[[nodiscard]] bool empty() const noexcept {
		return bytes_ == 0;
	}

	[[nodiscard]] std::size_t size_bytes() const noexcept {
		return bytes_;
	}

	// 编码结束后调用；返回的 buffer 在下一次 append()/clear() 之前有效。
	[[nodiscard]] std::span<const boost::asio::const_buffer> buffers() {
		buffers_.clear();
		buffers_.reserve(segments_.size());
		for (const Segment &segment : segments_) {
			const char *data = segment.external != nullptr ? segment.external : arena_.data() + segment.offset;
			buffers_.emplace_back(data, segment.size);
		}
		return buffers_;
	}

	// 写完成后复用；容量过大时释放，避免空闲连接长期占着一次大批量的内存。
	void clear() noexcept {
		segments_.clear();
		buffers_.clear();
		arena_.clear();
		bytes_ = 0;
		if (arena_.capacity() > kRetainArenaBytes) {
			std::string().swap(arena_);
		}
		if (segments_.capacity() > kRetainSegments) {
			std::vector<Segment>().swap(segments_);
			std::vector<boost::asio::const_buffer>().swap(buffers_);
		}
	}

private:
	static constexpr std::size_t kRetainArenaBytes = 16 * 1024;
	static constexpr std::size_t kRetainSegments = 256;

	struct Segment {
		const char *external = nullptr; // 非空：引用外部字符串；为空：arena_[offset, offset + size)
		std::size_t offset = 0;
		std::size_t size = 0;
	};

	void append_arena(const char *data, std::size_t size) {
		const std::size_t offset = arena_.size();
		arena_.append(data, size);
		bytes_ += size;
		// arena 只追加，最后一段若在 arena 里，它一定紧挨着本次写入的位置，直接加长。
		if (!segments_.empty() && segments_.back().external == nullptr) {
			segments_.back().size += size;
		} else {
			segments_.push_back(Segment {nullptr, offset, size});
		}
	}

	std::vector<Segment> segments_;
	std::vector<boost::asio::const_buffer> buffers_;
	std::string arena_;
	std::size_t bytes_ = 0;
};

// 同步编码一条响应，不执行 socket I/O；返回 true 表示这条之后应关闭连接（Quit / 致命错误）。
// 字段非法时抛出 std::logic_error（invalid_argument 是其子类），sink 中已追加的部分由调用方撤销。
template <typename Sink, typename... Types>
bool encode_to(Sink &out, const SocketResponseGroup<Types...> &group) {
	return detail::emit_group(out, group.variant());
}

template <typename Sink>
bool encode_to(Sink &out, const ErrorResponse &error) {
	return detail::emit_concrete(out, error);
}

template <typename Sink>
bool encode_to(Sink &out, const Response &response) {
	return detail::emit_one(out, response);
}

// 一条 multi-key 请求拆到多个分片后的各部分：按 parts 的顺序输出每部分的 items，最后只写一个 END。
// 输出等同于先把 items 合并成一个 MultiKeyResponse 再 encode_to，但不移动条目、不分配内存。
// project(part) 返回 const MultiKeyResponse&；各部分必须是同一种响应类型。
template <typename Sink, typename Parts, typename Project>
bool encode_multi_parts(Sink &out, const Parts &parts, Project project) {
	if (parts.empty()) {
		throw std::invalid_argument("multi-key response without parts");
	}
	return std::visit(
	    [&](const auto &first) -> bool {
		    using T = std::remove_cvref_t<decltype(first)>;
		    static_assert(detail::IsMultiRetrievalResponse<T>::value, "multi-key parts must be retrieval responses");
		    for (const auto &part : parts) {
			    const auto *value = std::get_if<T>(&project(part).variant());
			    if (!value) {
				    throw std::logic_error("Inconsistent multi-key response type");
			    }
			    for (const auto &item : value->items) {
				    detail::emit_retrieval_item(out, item);
			    }
		    }
		    out.append("END\r\n");
		    return false;
	    },
	    project(*parts.begin()).variant());
}

struct EncodedResponseBatch {
	std::string bytes;
	// 已处理的响应对象数，包含 NoReply 和 Quit。
	std::size_t responses_encoded = 0;
	bool close_connection = false;
};

class MemcachedResponseEncoder {
public:
	// 保留旧的同步内存编码接口，不执行 socket I/O。
	static EncodedResponseBatch encode_batch(std::span<const Response> responses) {
		EncodedResponseBatch result;
		detail::StringSink out {result.bytes};
		for (const Response &response : responses) {
			result.close_connection = detail::emit_one(out, response);
			++result.responses_encoded;
			if (result.close_connection) {
				break;
			}
		}
		return result;
	}
};

} // namespace fastest
