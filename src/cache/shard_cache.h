#pragma once

#include "core_cache.h"
#include "../server/comm/type.h"

#include <cstdint>
#include <stdexcept>
#include <type_traits>
#include <utility>
#include <variant>
#include <vector>

namespace fastest {

// 单个分片的数据，只能由所属线程访问。
// map_ 里每个 CacheValue* 持有一次引用计数：插入时从命令接管，覆盖/删除/析构时 release()。
// 读取只 share() 加一次计数交给响应，value 字节从解析到写出 socket 都不再复制。
// 所有写入都经过 publish()：value 的 flags、expired_at 在解析时已经是最终值，publish() 只分配新的 cas 版本号。
class ShardCache {
public:
	// shard_id 进入 cas 的低 16 位，不同分片生成的 cas 互不相同。
	explicit ShardCache(const std::uint16_t shard_id = 0) noexcept : shard_id_(shard_id) {
	}

	ShardCache(const ShardCache &) = delete;

	ShardCache &operator=(const ShardCache &) = delete;

	~ShardCache() {
		map_.clear_and_dispose(release_value);
	}

	void erase_rejected_set(const ItemKey &key) {
		map_.erase_and_dispose(key, release_value);
	}

	MultiKeyResponse handle_multi_command(MultiKeyCommand &&command) {
		return std::visit(
		    [this](auto &value) -> MultiKeyResponse {
			    using T = std::remove_cvref_t<decltype(value)>;
			    if constexpr (std::is_same_v<T, MultiGetCommand>) {
				    return handle_multi_retrieval<MultiGetResponse>(std::move(value));
			    } else if constexpr (std::is_same_v<T, MultiGetsCommand>) {
				    return handle_multi_retrieval<MultiGetsResponse>(std::move(value));
			    } else {
				    throw std::logic_error("multi command not implemented");
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
			    } else if constexpr (std::is_same_v<T, CasCommand>) {
				    return handle_cas(std::move(value));
			    } else if constexpr (std::is_same_v<T, SingleGetCommand>) {
				    return handle_single_retrieval<SingleGetResponse>(std::move(value));
			    } else if constexpr (std::is_same_v<T, SingleGetsCommand>) {
				    return handle_single_retrieval<SingleGetsResponse>(std::move(value));
			    } else if constexpr (std::is_same_v<T, DeleteCommand>) {
				    return handle_delete(std::move(value));
			    } else {
				    throw std::logic_error("single command not implemented");
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

	// 只读查看，不推进增量 rehash，不增加引用计数；返回的指针只在下一次修改 map 之前有效。
	[[nodiscard]] const CacheValue *peek(const ItemKey &key) const {
		return map_.peek(key);
	}

private:
	static void release_value(CacheValue *value) noexcept {
		if (value != nullptr) {
			value->release();
		}
	}

	// cas 版本号：每分片自增，低 16 位是分片号；从 1 开始，永远不为 0。
	[[nodiscard]] std::uint64_t next_cas() noexcept {
		return (++cas_sequence_ << 16) | shard_id_;
	}

	// 唯一的写入口。value 必须尚未发布（只有命令持有），否则改写 cas 会和读它的线程竞争。
	// flags、expired_at 已在解析时换算好，这里只把 cas 改写成新版本号（cas 命令的期望值在调用前已经用完），然后插入。
	// insert_or_assign 抛异常时所有权不变，value 随命令释放；成功后 map 接管这次计数，旧值释放 map 那一次计数。
	void publish(const ItemKey &key, CacheValuePtr &&value) {
		CacheValue *const raw = value.get();
		if (raw == nullptr || raw->use_count() != 1) [[unlikely]] {
			throw std::logic_error("only an unpublished CacheValue can be stored");
		}
		raw->cas = next_cas();
		const auto [previous, inserted] = map_.insert_or_assign(key, raw);
		(void)value.release_ownership();
		if (!inserted) {
			release_value(previous);
		}
	}

	SetResponse handle_set(SetCommand &&command) {
		publish(command.key, std::move(command.value));
		if (command.noreply) {
			return SetResponse {StorageStatus::NoReply};
		}
		return SetResponse {StorageStatus::Stored};
	}

	CasResponse handle_cas(CasCommand &&command) {
		if (!command.value) [[unlikely]] {
			throw std::logic_error("CAS command without value");
		}
		// 解析时把客户端的 cas_unique 暂存在尚未发布的 value->cas 里；publish() 会改写成新版本号。
		const std::uint64_t expected = command.value->cas;
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
		const bool found = map_.erase_and_dispose(command.key, release_value);
		if (command.noreply) {
			return DeleteResponse {DeleteStatus::NoReply};
		}
		return DeleteResponse {found ? DeleteStatus::Deleted : DeleteStatus::NotFound};
	}

	// get / gets：两者只差响应条目的类型（gets 编码时额外输出 value->cas）。
	template <typename Response, typename Command>
	Response handle_single_retrieval(Command &&command) {
		using Item = typename decltype(Response::item)::value_type;
		Response response;
		// find() 顺带推进一步增量 rehash。
		if (CacheValue *value = map_.find(command.key)) {
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
		for (ItemKey &key : command.keys) {
			if (CacheValue *value = map_.find(key)) {
				response.items.push_back(Item {std::move(key.key), CacheValuePtr::share(value)});
			}
		}
		return response;
	}

	IncrementalHashMap<CacheValue> map_;
	std::uint64_t cas_sequence_ = 0;
	std::uint16_t shard_id_ = 0;
};

} // namespace fastest
