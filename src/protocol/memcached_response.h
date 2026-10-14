#pragma once

#include <array>
#include <charconv>
#include <cstddef>
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
#include <boost/asio/ip/tcp.hpp>
#include <boost/asio/write.hpp>

namespace fastest {

// C++20 + Boost.Asio。send_to() 是同步发送接口，socket 必须由调用方独占写入。
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

enum class StorageStatus : std::uint8_t { Stored, NotStored };

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

enum class CasStatus : std::uint8_t { Stored, Exists, NotFound };

struct CasResponse {
	static constexpr ResponseType response_type = ResponseType::Cas;
	CasStatus status = CasStatus::Stored;
};

// Response 持有字符串；发送过程中不复制 key/value，也不能修改或销毁它们。
struct ValueItem {
	std::string key;
	std::uint32_t flags = 0;
	std::string value;
};

struct CasValueItem {
	std::string key;
	std::uint32_t flags = 0;
	std::string value;
	std::uint64_t cas_unique = 0;
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

enum class DeleteStatus : std::uint8_t { Deleted, NotFound };

struct DeleteResponse {
	static constexpr ResponseType response_type = ResponseType::Delete;
	DeleteStatus status = DeleteStatus::Deleted;
};

enum class TouchStatus : std::uint8_t { Touched, NotFound };

struct TouchResponse {
	static constexpr ResponseType response_type = ResponseType::Touch;
	TouchStatus status = TouchStatus::Touched;
};

template <ResponseType Type>
struct ArithmeticResponseT {
	static constexpr ResponseType response_type = Type;
	std::optional<std::uint64_t> value;
};

using IncrResponse = ArithmeticResponseT<ResponseType::Incr>;
using DecrResponse = ArithmeticResponseT<ResponseType::Decr>;

struct FlushAllResponse {
	static constexpr ResponseType response_type = ResponseType::FlushAll;
};

struct VerbosityResponse {
	static constexpr ResponseType response_type = ResponseType::Verbosity;
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

struct SendResult;

struct ErrorResponse {
	static constexpr ResponseType response_type = ResponseType::Error;
	ResponseErrorKind kind = ResponseErrorKind::Error;
	std::string message;
	bool close_connection = false;

	// 错误响应是 Response 的独立分支，也支持直接发送；复用统一的协议编码及固定容量发送器。
	[[nodiscard]] SendResult send_to(boost::asio::ip::tcp::socket &socket) const;
};

struct NoReplyResponse {
	static constexpr ResponseType response_type = ResponseType::NoReply;
};

struct SendResult {
	std::size_t bytes_sent = 0;
	std::size_t responses_sent = 0;
	bool close_connection = false;
};

namespace detail {

template <typename Variant>
SendResult send_group(boost::asio::ip::tcp::socket &socket, const Variant &group);

} // namespace detail

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

	// 正常返回表示整个响应已写完；网络错误抛出 boost::system::system_error。
	// close_connection 仅通知调用方关闭连接，方法自身不会关闭 socket。
	// [[nodiscard]]
	SendResult send_to(boost::asio::ip::tcp::socket &socket) const {
		return detail::send_group(socket, variant());
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

// 仅用于发送前检查，确保较后条目的非法字段不会导致先发出半个协议响应。
struct ValidationSink {
	static constexpr bool validation_only = true;
	void append(std::string_view) const noexcept {
	}

	template <typename UInt>
	void append_uint(UInt) const noexcept {
	}
};

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

// 固定容量的分散聚集发送器。只保存指针和长度，绝不拼接完整响应或复制 value。
class SocketSink {
public:
	static constexpr bool validation_only = false;

	explicit SocketSink(boost::asio::ip::tcp::socket &socket) : socket_(socket) {
	}

	SocketSink(const SocketSink &) = delete;
	SocketSink &operator=(const SocketSink &) = delete;

	void append(std::string_view text) {
		if (text.empty()) {
			return;
		}
		if (buffer_count_ == buffers_.size()) {
			flush();
		}
		buffers_[buffer_count_++] = boost::asio::const_buffer(text.data(), text.size());
	}

	template <typename UInt>
	void append_uint(UInt number) {
		static_assert(std::is_unsigned_v<UInt> && !std::is_same_v<UInt, bool>);
		constexpr std::size_t capacity = std::numeric_limits<UInt>::digits10 + 1;
		static_assert(capacity <= kNumberBytes);

		// 在编码之前保证描述符和数字区都有空间，防止编码后 flush 使数字指针失效。
		if (buffer_count_ == buffers_.size() || numbers_.size() - number_count_ < capacity) {
			flush();
		}

		char *first = numbers_.data() + number_count_;
		const auto [last, error] = std::to_chars(first, first + capacity, number);
		if (error != std::errc {}) {
			throw std::logic_error("failed to encode unsigned integer");
		}

		const std::size_t size = static_cast<std::size_t>(last - first);
		number_count_ += size;
		buffers_[buffer_count_++] = boost::asio::const_buffer(first, size);
	}

	void flush() {
		if (buffer_count_ == 0) {
			return;
		}

		// boost::asio::write 负责处理短写；只有整组缓冲区写完后，才能复用数字区。
		bytes_sent_ +=
		    boost::asio::write(socket_, std::span<const boost::asio::const_buffer>(buffers_.data(), buffer_count_));
		buffer_count_ = 0;
		number_count_ = 0;
	}

	std::size_t bytes_sent() const noexcept {
		return bytes_sent_;
	}

private:
	static constexpr std::size_t kBufferCount = 64;
	static constexpr std::size_t kNumberBytes = 1024;

	boost::asio::ip::tcp::socket &socket_;
	std::array<boost::asio::const_buffer, kBufferCount> buffers_;
	std::array<char, kNumberBytes> numbers_;
	std::size_t buffer_count_ = 0;
	std::size_t number_count_ = 0;
	std::size_t bytes_sent_ = 0;
};

inline std::string_view storage_text(StorageStatus status) {
	switch (status) {
	case StorageStatus::Stored:
		return "STORED\r\n";
	case StorageStatus::NotStored:
		return "NOT_STORED\r\n";
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
	default:
		throw std::invalid_argument("invalid touch response status");
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

template <typename Sink, typename Item>
void emit_retrieval_item(Sink &out, const Item &item) {
	validate_key(item.key);
	out.append("VALUE ");
	out.append(item.key);
	out.append(" ");
	out.append_uint(item.flags);
	out.append(" ");
	out.append_uint(item.value.size());
	if constexpr (std::is_same_v<Item, CasValueItem>) {
		out.append(" ");
		out.append_uint(item.cas_unique);
	}
	out.append("\r\n");
	out.append(item.value);
	out.append("\r\n");
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

// 普通文本直接引用原字符串；仅将 CR/LF/NUL 对应位置改为静态空格，不创建清洗后的字符串。
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
		if (value.value) {
			out.append_uint(*value.value);
			out.append("\r\n");
		} else {
			out.append("NOT_FOUND\r\n");
		}
	} else if constexpr (std::is_same_v<T, FlushAllResponse> || std::is_same_v<T, VerbosityResponse>) {
		out.append("OK\r\n");
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
		emit_error(out, value);
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

template <typename Variant>
SendResult send_group(boost::asio::ip::tcp::socket &socket, const Variant &group) {
	ValidationSink validator;
	const bool close = emit_group(validator, group);

	SocketSink out(socket);
	emit_group(out, group);
	out.flush();
	return SendResult {out.bytes_sent(), 1, close};
}

template <typename T>
SendResult send_concrete(boost::asio::ip::tcp::socket &socket, const T &value) {
	ValidationSink validator;
	const bool close = emit_concrete(validator, value);

	SocketSink out(socket);
	emit_concrete(out, value);
	out.flush();
	return SendResult {out.bytes_sent(), 1, close};
}

} // namespace detail

[[nodiscard]] inline SendResult ErrorResponse::send_to(boost::asio::ip::tcp::socket &socket) const {
	return detail::send_concrete(socket, *this);
}

// 外层 Response 的统一发送入口；三种正常响应分类和 ErrorResponse 都有 send_to(socket) 成员方法。
[[nodiscard]] inline SendResult send_to(const Response &response, boost::asio::ip::tcp::socket &socket) {
	return std::visit([&socket](const auto &group) { return group.send_to(socket); }, response);
}

struct EncodedResponseBatch {
	std::string bytes;
	std::size_t responses_encoded = 0;
	bool close_connection = false;
};

class MemcachedResponseEncoder {
public:
	// 保留旧接口：只有调用此函数时才会分配完整响应字节串。
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

	// 可选批量入口：跨响应共用固定发送区，按顺序发送，遇到 Quit/close_connection 后停止。
	[[nodiscard]] static SendResult send_batch(boost::asio::ip::tcp::socket &socket,
	                                           std::span<const Response> responses) {
		SendResult result;
		detail::ValidationSink validator;

		// 先检查本次实际会发送的全部响应；close 之后的响应不参与检查和发送。
		for (const Response &response : responses) {
			result.close_connection = detail::emit_one(validator, response);
			++result.responses_sent;
			if (result.close_connection) {
				break;
			}
		}

		detail::SocketSink out(socket);
		for (std::size_t index = 0; index < result.responses_sent; ++index) {
			detail::emit_one(out, responses[index]);
		}
		out.flush();
		result.bytes_sent = out.bytes_sent();
		return result;
	}
};

} // namespace comm