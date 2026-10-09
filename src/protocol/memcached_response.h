#pragma once

#include <charconv>
#include <cstddef>
#include <cstdint>
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

namespace protocol {

// 每种请求对应独立的 ResponseType；Error 和 NoReply 是执行/协议层的特殊结果。
// 这里仅定义 Memcached Basic Text Protocol，不包含 Meta Text 或 Binary Protocol。
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

// set/add/replace/append/prepend: STORED 或 NOT_STORED。
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

// cas 的响应状态与普通 storage 不完全相同。
enum class CasStatus : std::uint8_t { Stored, Exists, NotFound };

struct CasResponse {
	static constexpr ResponseType response_type = ResponseType::Cas;

	CasStatus status = CasStatus::Stored;
};

// 响应持有字符串所有权，跨 shard 或 co_await 后仍有效。
// 普通 get/gat 不需要 cas_unique；gets/gats 的命中条目才有该字段。
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

// 单 Key 命中时保存一个 Item；未命中时为 nullopt（只输出 END）。
// 多 Key 只保存命中项，且按需要返回的顺序排列；整个命令只输出一个 END。
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

// incr/decr：value 有值 -> 输出十进制数；nullopt -> NOT_FOUND。
// 数字内容非法等执行错误应返回 ErrorResponse，而不是伪装成 NOT_FOUND。
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

// 常见 stats 输出为 STAT/ITEM/PREFIX 多行 + END。
// stats reset 返回 RESET；部分管理类 stats 命令可能返回 OK。
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

// quit 不发送任何字节；编码器通知网络层在写完前面的响应后关闭连接。
struct QuitResponse {
	static constexpr ResponseType response_type = ResponseType::Quit;
};

enum class ResponseErrorKind : std::uint8_t { Error, ClientError, ServerError };

struct ErrorResponse {
	static constexpr ResponseType response_type = ResponseType::Error;

	ResponseErrorKind kind = ResponseErrorKind::Error;
	std::string message;
	bool close_connection = false;
};

// noreply 命令执行成功后返回这个类型，确保编码器不输出数据。
// 解析失败、服务端失败等需要通知客户端的情况仍应返回 ErrorResponse。
struct NoReplyResponse {
	static constexpr ResponseType response_type = ResponseType::NoReply;
};

// 与 Command 的分类完全一致：外层表示路由类型，内层表示具体响应。
using SingleKeyResponse = std::variant<SetResponse, AddResponse, ReplaceResponse, AppendResponse, PrependResponse,
                                       CasResponse, SingleGetResponse, SingleGetsResponse, SingleGatResponse,
                                       SingleGatsResponse, DeleteResponse, TouchResponse, IncrResponse, DecrResponse>;

using MultiKeyResponse = std::variant<MultiGetResponse, MultiGetsResponse, MultiGatResponse, MultiGatsResponse>;

// 错误和 noreply 不携带 Key 信息，属于编码层的无 Key 响应。
// 这不表示 Error/NoReply 只能对应无 Key 命令；任意命令均可返回这两种响应。
using NoKeyResponse = std::variant<FlushAllResponse, VerbosityResponse, StatsResponse, VersionResponse, QuitResponse,
                                   ErrorResponse, NoReplyResponse>;

// 一个请求通常对应一个 Response，包括 NoReplyResponse 和 QuitResponse。
// Response 持有响应值所有权，可跨 shard 或 co_await 安全移动。
using Response = std::variant<SingleKeyResponse, MultiKeyResponse, NoKeyResponse>;
using ResponseBatch = std::vector<Response>;

// 嵌套 variant 不能从具体响应直接隐式构造；通过工厂函数包装到正确的外层分类。
template <typename T>
Response make_response(T value) {
	if constexpr (std::is_constructible_v<SingleKeyResponse, T>) {
		return Response(std::in_place_type<SingleKeyResponse>, std::move(value));
	} else if constexpr (std::is_constructible_v<MultiKeyResponse, T>) {
		return Response(std::in_place_type<MultiKeyResponse>, std::move(value));
	} else {
		static_assert(std::is_constructible_v<NoKeyResponse, T>, "unsupported response type");
		return Response(std::in_place_type<NoKeyResponse>, std::move(value));
	}
}

// 与 CommandType 一致：SingleGet/MultiGet 均返回 ResponseType::Get。
inline ResponseType response_type(const Response &response) {
	return std::visit(
	    [](const auto &group) -> ResponseType {
		    return std::visit(
		        [](const auto &value) -> ResponseType { return std::remove_cvref_t<decltype(value)>::response_type; },
		        group);
	    },
	    response);
}

struct EncodedResponseBatch {
	std::string bytes;
	std::size_t responses_encoded = 0;
	bool close_connection = false;
};

// 无 socket/Boost 依赖：ResponseBatch -> 可直接用于 async_write 的连续字节串。
// 一个 batch 只编码一次，网络层应对 bytes 执行一次完整的 async_write。
// 返回的 bytes 必须保持存活，直到 async_write 完成。
class MemcachedResponseEncoder {
public:
	static EncodedResponseBatch encode_batch(std::span<const Response> responses) {
		EncodedResponseBatch result;

		for (const Response &response : responses) {
			const bool close = append_one(result.bytes, response);
			++result.responses_encoded;
			if (close) {
				result.close_connection = true;
				break;
			}
		}

		return result;
	}

private:
	template <typename T>
	static constexpr bool always_false = false;

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

	template <typename UInt>
	static void append_uint(std::string &out, UInt number) {
		char buffer[32];
		const auto [last, error] = std::to_chars(buffer, buffer + sizeof(buffer), number);
		if (error != std::errc {}) {
			throw std::logic_error("failed to encode unsigned integer");
		}
		out.append(buffer, last);
	}

	static void validate_key(std::string_view key) {
		if (key.empty() || key.size() > 250) {
			throw std::invalid_argument("invalid response key length");
		}
		for (unsigned char ch : key) {
			if (ch <= 0x20 || ch == 0x7f) {
				throw std::invalid_argument("invalid response key characters");
			}
		}
	}

	static void validate_line(std::string_view text) {
		for (unsigned char ch : text) {
			if (ch == '\r' || ch == '\n' || ch == 0) {
				throw std::invalid_argument("response line contains CR/LF/NUL");
			}
		}
	}

	static void validate_token(std::string_view token) {
		if (token.empty()) {
			throw std::invalid_argument("empty response token");
		}
		for (unsigned char ch : token) {
			if (ch <= 0x20 || ch == 0x7f) {
				throw std::invalid_argument("invalid response token");
			}
		}
	}

	// 单 Key 和多 Key 的 VALUE 行编码规则一致，二者只在 Item 数量上有区别。
	template <typename Item>
	static void append_retrieval_item(std::string &out, const Item &item) {
		validate_key(item.key);
		out.append("VALUE ");
		out.append(item.key);
		out.push_back(' ');
		append_uint(out, item.flags);
		out.push_back(' ');
		append_uint(out, item.value.size());
		if constexpr (std::is_same_v<Item, CasValueItem>) {
			out.push_back(' ');
			append_uint(out, item.cas_unique);
		}
		out.append("\r\n");
		out.append(item.value);
		out.append("\r\n");
	}

	template <typename Item>
	static void append_retrieval(std::string &out, const std::optional<Item> &item) {
		if (item) {
			append_retrieval_item(out, *item);
		}
		out.append("END\r\n");
	}

	template <typename Item>
	static void append_retrieval(std::string &out, const std::vector<Item> &items) {
		for (const Item &item : items) {
			append_retrieval_item(out, item);
		}
		out.append("END\r\n");
	}

	static void append_storage(std::string &out, StorageStatus status) {
		switch (status) {
		case StorageStatus::Stored:
			out.append("STORED\r\n");
			break;
		case StorageStatus::NotStored:
			out.append("NOT_STORED\r\n");
			break;
		default:
			throw std::invalid_argument("invalid storage response status");
		}
	}

	static void append_cas(std::string &out, CasStatus status) {
		switch (status) {
		case CasStatus::Stored:
			out.append("STORED\r\n");
			break;
		case CasStatus::Exists:
			out.append("EXISTS\r\n");
			break;
		case CasStatus::NotFound:
			out.append("NOT_FOUND\r\n");
			break;
		default:
			throw std::invalid_argument("invalid cas response status");
		}
	}

	static void append_stats(std::string &out, const StatsResponse &response) {
		if (response.terminator != StatsTerminator::End && !response.lines.empty()) {
			throw std::invalid_argument("stats RESET/OK must not have data lines");
		}

		for (const StatsLine &line : response.lines) {
			validate_token(line.key);
			validate_line(line.value);
			switch (line.type) {
			case StatsLineType::Stat:
				out.append("STAT ");
				break;
			case StatsLineType::Item:
				out.append("ITEM ");
				break;
			case StatsLineType::Prefix:
				out.append("PREFIX ");
				break;
			default:
				throw std::invalid_argument("invalid stats line type");
			}
			out.append(line.key);
			if (!line.value.empty()) {
				out.push_back(' ');
				out.append(line.value);
			}
			out.append("\r\n");
		}

		switch (response.terminator) {
		case StatsTerminator::End:
			out.append("END\r\n");
			break;
		case StatsTerminator::Reset:
			out.append("RESET\r\n");
			break;
		case StatsTerminator::Ok:
			out.append("OK\r\n");
			break;
		default:
			throw std::invalid_argument("invalid stats terminator");
		}
	}

	static void append_error(std::string &out, const ErrorResponse &error) {
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

		// 自定义错误信息可能来自客户端，不允许通过 CR/LF 注入额外协议行。
		// 这里替换控制字符，不改变实际协议帧边界。
		if (error.message.empty()) {
			out.append("unspecified error");
		} else {
			for (unsigned char ch : error.message) {
				out.push_back(ch == '\r' || ch == '\n' || ch == 0 ? ' ' : static_cast<char>(ch));
			}
		}
		out.append("\r\n");
	}

	// 根据内层具体响应类型选择编码格式；get/gets/gat/gats 共用 VALUE 行编码逻辑。
	template <typename T>
	static bool append_concrete(std::string &out, const T &value) {
		if constexpr (IsStorageResponse<T>::value) {
			append_storage(out, value.status);
		} else if constexpr (std::is_same_v<T, CasResponse>) {
			append_cas(out, value.status);
		} else if constexpr (IsSingleRetrievalResponse<T>::value) {
			append_retrieval(out, value.item);
		} else if constexpr (IsMultiRetrievalResponse<T>::value) {
			append_retrieval(out, value.items);
		} else if constexpr (std::is_same_v<T, DeleteResponse>) {
			switch (value.status) {
			case DeleteStatus::Deleted:
				out.append("DELETED\r\n");
				break;
			case DeleteStatus::NotFound:
				out.append("NOT_FOUND\r\n");
				break;
			default:
				throw std::invalid_argument("invalid delete response status");
			}
		} else if constexpr (std::is_same_v<T, TouchResponse>) {
			switch (value.status) {
			case TouchStatus::Touched:
				out.append("TOUCHED\r\n");
				break;
			case TouchStatus::NotFound:
				out.append("NOT_FOUND\r\n");
				break;
			default:
				throw std::invalid_argument("invalid touch response status");
			}
		} else if constexpr (IsArithmeticResponse<T>::value) {
			if (value.value.has_value()) {
				append_uint(out, *value.value);
				out.append("\r\n");
			} else {
				out.append("NOT_FOUND\r\n");
			}
		} else if constexpr (std::is_same_v<T, FlushAllResponse> || std::is_same_v<T, VerbosityResponse>) {
			out.append("OK\r\n");
		} else if constexpr (std::is_same_v<T, StatsResponse>) {
			append_stats(out, value);
		} else if constexpr (std::is_same_v<T, VersionResponse>) {
			validate_token(value.version);
			out.append("VERSION ");
			out.append(value.version);
			out.append("\r\n");
		} else if constexpr (std::is_same_v<T, QuitResponse>) {
			return true;
		} else if constexpr (std::is_same_v<T, ErrorResponse>) {
			append_error(out, value);
			return value.close_connection;
		} else if constexpr (std::is_same_v<T, NoReplyResponse>) {
			// 有执行结果，但是协议要求不输出响应。
		} else {
			static_assert(always_false<T>, "missing response encoder case");
		}
		return false;
	}

	static bool append_one(std::string &out, const Response &response) {
		return std::visit(
		    [&out](const auto &group) -> bool {
			    return std::visit([&out](const auto &value) -> bool { return append_concrete(out, value); }, group);
		    },
		    response);
	}
};

} // namespace protocol