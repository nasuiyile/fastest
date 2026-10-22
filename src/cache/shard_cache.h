#pragma once

#include "expired_map.h"
#include "../protocol/memcached_number.h"
#include "../server/comm/type.h"

#include <array>
#include <charconv>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <stdexcept>
#include <string_view>
#include <type_traits>
#include <utility>
#include <variant>
#include <vector>

namespace fastest {

// 单个分片的数据，只能由所属线程访问。语义按 memcached 1.6（do_store_item / do_add_delta / do_item_touch）。
// map_（ExpiredMap）里每个 CacheValue* 持有一次引用计数：插入时从命令接管，覆盖/删除/过期/析构时 release()。
// 所有命令都经过惰性过期：过期的 value 按不存在处理并顺手删除；主动过期由外部调用 sample_expired()。
// 读取只 share() 加一次计数交给响应，value 字节从解析到写出 socket 都不再复制。
// 写入新 value 都经过 publish() / ExpiredMap::insert()：value 的 flags、expired_at 在解析时已经是最终值，
// 这里只分配新的 cas 版本号。append/prepend、incr/decr 生成的新 value 沿用旧值的 flags 和过期时间。
// 原地修改只有两处：touch/gat 改 expired_at（经过 ExpiredMap::touch() 维护 TTL 计数），
// incr/decr 在 value 只被 map 持有时原地改写数字（见 store_number()）。
class ShardCache {
public:
	// shard_id 进入 cas 的低 16 位，不同分片生成的 cas 互不相同。
	// max_value_bytes：append/prepend 合并后的上限，超过时与 memcached 分配失败一样回复 NOT_STORED。
	explicit ShardCache(const std::uint16_t shard_id = 0,
	                    const std::size_t max_value_bytes = ParserLimits {}.max_value_bytes) noexcept
	    : max_value_bytes_(max_value_bytes), shard_id_(shard_id) {
	}

	ShardCache(const ShardCache &) = delete;

	ShardCache &operator=(const ShardCache &) = delete;

	void erase_rejected_set(const ItemKey &key) {
		(void)map_.erase(key);
	}

	MultiKeyResponse handle_multi_command(MultiKeyCommand &&command) {
		return std::visit(
		    [this](auto &value) -> MultiKeyResponse {
			    using T = std::remove_cvref_t<decltype(value)>;
			    if constexpr (std::is_same_v<T, MultiGetCommand>) {
				    return handle_multi_retrieval<MultiGetResponse>(std::move(value));
			    } else if constexpr (std::is_same_v<T, MultiGetsCommand>) {
				    return handle_multi_retrieval<MultiGetsResponse>(std::move(value));
			    } else if constexpr (std::is_same_v<T, MultiGatCommand>) {
				    return handle_multi_retrieval<MultiGatResponse>(std::move(value));
			    } else if constexpr (std::is_same_v<T, MultiGatsCommand>) {
				    return handle_multi_retrieval<MultiGatsResponse>(std::move(value));
			    } else {
				    static_assert(detail::always_false<T>, "missing multi-key command handler");
			    }
		    },
		    command.command);
	}

	SingleKeyResponse handle_single_command(SingleKeyCommand &&command) {
		return std::visit(
		    [this](auto &value) -> SingleKeyResponse {
			    using T = std::remove_cvref_t<decltype(value)>;
			    if constexpr (std::is_same_v<T, SetCommand>) {
				    return handle_set(std::move(value));
			    } else if constexpr (std::is_same_v<T, AddCommand>) {
				    return handle_add(std::move(value));
			    } else if constexpr (std::is_same_v<T, ReplaceCommand>) {
				    return handle_replace(std::move(value));
			    } else if constexpr (std::is_same_v<T, AppendCommand>) {
				    return handle_concat<AppendResponse, false>(std::move(value));
			    } else if constexpr (std::is_same_v<T, PrependCommand>) {
				    return handle_concat<PrependResponse, true>(std::move(value));
			    } else if constexpr (std::is_same_v<T, CasCommand>) {
				    return handle_cas(std::move(value));
			    } else if constexpr (std::is_same_v<T, SingleGetCommand>) {
				    return handle_single_retrieval<SingleGetResponse>(std::move(value));
			    } else if constexpr (std::is_same_v<T, SingleGetsCommand>) {
				    return handle_single_retrieval<SingleGetsResponse>(std::move(value));
			    } else if constexpr (std::is_same_v<T, SingleGatCommand>) {
				    return handle_single_retrieval<SingleGatResponse>(std::move(value));
			    } else if constexpr (std::is_same_v<T, SingleGatsCommand>) {
				    return handle_single_retrieval<SingleGatsResponse>(std::move(value));
			    } else if constexpr (std::is_same_v<T, DeleteCommand>) {
				    return handle_delete(std::move(value));
			    } else if constexpr (std::is_same_v<T, TouchCommand>) {
				    return handle_touch(std::move(value));
			    } else if constexpr (std::is_same_v<T, IncrCommand>) {
				    return handle_arithmetic<IncrResponse, true>(std::move(value));
			    } else if constexpr (std::is_same_v<T, DecrCommand>) {
				    return handle_arithmetic<DecrResponse, false>(std::move(value));
			    } else {
				    static_assert(detail::always_false<T>, "missing single-key command handler");
			    }
		    },
		    command.command);
	}

	[[nodiscard]] std::size_t size() const noexcept {
		return map_.size();
	}

	[[nodiscard]] std::size_t used_bytes() const noexcept {
		return map_.used_bytes();
	}

	// 带过期时间的条目数（含已过期但还没删除的），sample_expired() 据此跳过扫描。
	[[nodiscard]] std::size_t ttl_count() const noexcept {
		return map_.ttl_count();
	}

	// 只读查看，不推进增量 rehash，不增加引用计数；已过期的返回 nullptr，但不删除。
	// 返回的指针只在下一次修改 map 之前有效。
	[[nodiscard]] const CacheValue *peek(const ItemKey &key) const {
		return map_.peek(key);
	}

	// 主动过期采样，由所属线程的后台调度调用（见 doc/scheduled_deletion.md）：
	// 从游标处访问最多 limit 个条目，删除其中已过期的，返回本次的统计。
	ExpiredMap::SampleStats sample_expired(const std::size_t limit) {
		return map_.sample_expired(limit);
	}

private:
	// cas 版本号：每分片自增，低 16 位是分片号；从 1 开始，永远不为 0。
	[[nodiscard]] std::uint64_t next_cas() noexcept {
		return (++cas_sequence_ << 16) | shard_id_;
	}

	// 只有尚未发布的 value（只有命令持有）才能写入，否则改写 cas 会和读它的线程竞争。
	static CacheValue *unpublished(const CacheValuePtr &value) {
		CacheValue *const raw = value.get();
		if (raw == nullptr || raw->use_count() != 1) [[unlikely]] {
			throw std::logic_error("only an unpublished CacheValue can be stored");
		}
		return raw;
	}

	// 覆盖写入（set、replace、cas、append/prepend、incr/decr 的新 value）。
	// flags、expired_at 已经是最终值，这里只把 cas 改写成新版本号（cas 命令的期望值在调用前已经用完），然后插入。
	// insert_or_assign 抛异常时所有权不变，value 随命令释放；成功后 map 接管这次计数，旧值释放 map 那一次计数。
	void publish(const ItemKey &key, CacheValuePtr &&value) {
		unpublished(value)->cas = next_cas();
		map_.insert_or_assign(key, std::move(value));
	}

	template <typename Response>
	[[nodiscard]] static Response storage_response(const bool noreply, const bool stored) noexcept {
		if (noreply) {
			return Response {StorageStatus::NoReply};
		}
		return Response {stored ? StorageStatus::Stored : StorageStatus::NotStored};
	}

	SetResponse handle_set(SetCommand &&command) {
		publish(command.key, std::move(command.value));
		return storage_response<SetResponse>(command.noreply, true);
	}

	// add：key 不存在或已过期时写入，否则 NOT_STORED（memcached 只把旧值提到 LRU 头部，不修改它）。
	AddResponse handle_add(AddCommand &&command) {
		CacheValue *const raw = unpublished(command.value);
		std::uint32_t now = 0;
		const bool stored = map_.insert(command.key, std::move(command.value), now);
		if (stored) {
			// 插入之后才分配版本号，没写入的 add 不消耗 cas。value 只有 map 持有，别的线程还看不到它。
			raw->cas = next_cas();
		}
		return storage_response<AddResponse>(command.noreply, stored);
	}

	// replace：只替换存在且未过期的 key；flags、exptime 取命令里的新值。
	ReplaceResponse handle_replace(ReplaceCommand &&command) {
		const bool exists = map_.find(command.key) != nullptr;
		if (exists) {
			publish(command.key, std::move(command.value));
		}
		return storage_response<ReplaceResponse>(command.noreply, exists);
	}

	// append / prepend：key 不存在或已过期时 NOT_STORED。新 value = 旧数据 + 新数据（prepend 相反），
	// flags 和过期时间沿用旧值，命令里的 flags、exptime 被忽略；cas 换成新版本号。
	// 合并后超过上限时 NOT_STORED（memcached 分配合并后的 item 失败时同样回复 NOT_STORED）。
	template <typename Response, bool Prepend, typename Command>
	Response handle_concat(Command &&command) {
		if (!command.value) [[unlikely]] {
			throw std::logic_error("append/prepend command without value");
		}
		const CacheValue *const current = map_.find(command.key);
		bool stored = false;
		if (current != nullptr && current->size() + command.value->size() <= max_value_bytes_) {
			const std::span<const std::byte> head = Prepend ? command.value->bytes() : current->bytes();
			const std::span<const std::byte> tail = Prepend ? current->bytes() : command.value->bytes();
			CacheValuePtr merged =
			    CacheValue::create_for_overwrite(head.size() + tail.size(), current->flags, current->expired_at);
			std::byte *const out = merged->mutable_bytes_crlf().data();
			if (!head.empty()) {
				std::memcpy(out, head.data(), head.size());
			}
			if (!tail.empty()) {
				std::memcpy(out + head.size(), tail.data(), tail.size());
			}
			// publish() 释放旧值，current 之后不能再访问。
			publish(command.key, std::move(merged));
			stored = true;
		}
		return storage_response<Response>(command.noreply, stored);
	}

	CasResponse handle_cas(CasCommand &&command) {
		if (!command.value) [[unlikely]] {
			throw std::logic_error("CAS command without value");
		}
		// 解析时把客户端的 cas_unique 暂存在尚未发布的 value->cas 里；publish() 会改写成新版本号。
		const std::uint64_t expected = command.value->cas;
		// 已过期的按不存在处理（find() 顺手删除），返回 NOT_FOUND。
		const CacheValue *const current = map_.find(command.key);
		CasStatus status = CasStatus::Stored;
		if (current == nullptr) {
			status = CasStatus::NotFound;
		} else if (current->cas != expected) {
			status = CasStatus::Exists;
		} else {
			publish(command.key, std::move(command.value));
		}
		return CasResponse {command.noreply ? CasStatus::NoReply : status};
	}

	DeleteResponse handle_delete(DeleteCommand &&command) {
		// 一次查找：摘下节点并释放 map 持有的那次计数；在途响应仍持有自己的引用，不受影响。
		// 已过期的同样删除，但返回 NOT_FOUND。
		const bool found = map_.erase(command.key);
		if (command.noreply) {
			return DeleteResponse {DeleteStatus::NoReply};
		}
		return DeleteResponse {found ? DeleteStatus::Deleted : DeleteStatus::NotFound};
	}

	// touch：只改过期时间，不改 cas、flags 和数据；已过期的按不存在处理。
	TouchResponse handle_touch(TouchCommand &&command) {
		CacheValue *const value = map_.find(command.key);
		if (value != nullptr) {
			map_.touch(value, command.expired_at);
		}
		if (command.noreply) {
			return TouchResponse {TouchStatus::NoReply};
		}
		return TouchResponse {value != nullptr ? TouchStatus::Touched : TouchStatus::NotFound};
	}

	// gat / gats 先改过期时间再返回命中的 value（与 memcached 的 item_touch 相同，不改 cas）；
	// get / gets 只读。gets/gats 只是响应条目的类型不同（编码时额外输出 value->cas）。
	// 负的 exptime（expired_at == kExpired）时这次照常返回，之后的读取才是未命中。
	template <typename Command>
	void touch_on_hit(CacheValue *value, const Command &command) noexcept {
		if constexpr (requires { command.expired_at; }) {
			map_.touch(value, command.expired_at);
		}
	}

	template <typename Response, typename Command>
	Response handle_single_retrieval(Command &&command) {
		using Item = typename decltype(Response::item)::value_type;
		Response response;
		// find() 顺带推进一步增量 rehash；已过期的当作未命中并删除。
		if (CacheValue *value = map_.find(command.key)) {
			touch_on_hit(value, command);
			response.item.emplace(Item {std::move(command.key.key), CacheValuePtr::share(value)});
		}
		// 未命中时 item 为空，编码器只输出 END。
		return response;
	}

	template <typename Response, typename Command>
	Response handle_multi_retrieval(Command &&command) {
		using Item = typename decltype(Response::items)::value_type;
		Response response;
		response.items.reserve(command.keys.size());
		// 整条命令共用一次时钟读取，只在碰到带过期时间的 value 时才读。
		// 重复的 key 逐个处理：gat -1 k k 第一次命中并立即过期，第二次未命中，与 memcached 相同。
		std::uint32_t now = 0;
		for (ItemKey &key : command.keys) {
			if (CacheValue *value = map_.find(key, now)) {
				touch_on_hit(value, command);
				response.items.push_back(Item {std::move(key.key), CacheValuePtr::share(value)});
			}
		}
		return response;
	}

	// incr / decr：value 按 memcached 的 safe_strtoull 解析（允许前导空白和尾部空白，例如原地写回时补的空格），
	// 不是数字时回复 CLIENT_ERROR；incr 按 64 位无符号回绕，decr 最小到 0。flags、过期时间不变，cas 换新。
	template <typename Response, bool Incr, typename Command>
	Response handle_arithmetic(Command &&command) {
		Response response;
		CacheValue *const value = map_.find(command.key);
		std::uint64_t number = 0;
		if (value == nullptr) {
			response.status = ArithmeticStatus::NotFound;
		} else if (!parse_memcached_u64(value->view(), number)) {
			response.status = ArithmeticStatus::NonNumeric;
		} else {
			if constexpr (Incr) {
				number += command.delta;
			} else {
				number = command.delta > number ? 0 : number - command.delta;
			}
			store_number(command.key, *value, number);
			response.value = number;
		}
		if (command.noreply) {
			response.status = ArithmeticStatus::NoReply;
		}
		return response;
	}

	// 写回结果与 memcached do_add_delta 的原地替换逐字节相同：新数字不比旧数据长时，长度不变、剩余部分补空格
	// （之后 get 会看到尾部空格）；数字变长时长度刚好装下数字。flags、过期时间不变，cas 换新。
	// - value 只被 map 持有（memcached 的 refcount == 2）时原地改写：不分配，也不改 map 的内存计数。
	// - 有在途响应引用旧值时分配新 value，旧值留给在途响应。memcached 每条 get 的回复写完才执行同一连接的下一条命令，
	//   所以同一连接看到的总是原地替换的结果；这里的在途引用可能来自同一连接的流水线，因此新 value 也补空格，
	//   保证结果与执行时机无关（memcached 只在其他连接持有引用时才会给出不补空格的结果，协议允许两者）。
	void store_number(const ItemKey &key, CacheValue &value, const std::uint64_t number) {
		std::array<char, 20> digits; // UINT64_MAX 是 20 位
		const auto [end, error] = std::to_chars(digits.data(), digits.data() + digits.size(), number);
		(void)error; // 20 字节总能装下
		const auto length = static_cast<std::size_t>(end - digits.data());
		const bool fits = length <= value.size();
		if (fits && value.exclusively_owned()) {
			write_padded(value.mutable_bytes_crlf().data(), value.size(), digits.data(), length);
			value.cas = next_cas();
			return;
		}
		const std::size_t size = fits ? value.size() : length;
		CacheValuePtr fresh = CacheValue::create_for_overwrite(size, value.flags, value.expired_at);
		write_padded(fresh->mutable_bytes_crlf().data(), size, digits.data(), length);
		// publish() 释放旧值，value 之后不能再访问。
		publish(key, std::move(fresh));
	}

	static void write_padded(std::byte *out, const std::size_t size, const char *digits,
	                         const std::size_t length) noexcept {
		std::memcpy(out, digits, length);
		std::memset(out + length, ' ', size - length);
	}

	ExpiredMap map_;
	std::size_t max_value_bytes_;
	std::uint64_t cas_sequence_ = 0;
	std::uint16_t shard_id_ = 0;
};

} // namespace fastest
