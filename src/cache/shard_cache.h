#pragma once

#include "core_cache.h"
#include "../server/comm/type.h"

#include <stdexcept>
#include <string>
#include <type_traits>
#include <unordered_map>
#include <variant>

namespace fastest {

class ShardCache {
public:
	void erase_rejected_set(const ItemKey &key) {
		data_.erase(key);
	}

	MultiKeyResponse handle_multi_command(const MultiKeyCommand &command) {
		return std::visit(
		    [this](const auto &value) -> MultiKeyResponse {
			    using T = std::remove_cvref_t<decltype(value)>;
			    if constexpr (std::is_same_v<T, MultiGetCommand>) {
				    return handle_multi_get(value);
			    } else {
				    throw std::logic_error("multi command not implemented");
			    }
		    },
		    command.command);
	}

	SingleKeyResponse handle_single_command(const SingleKeyCommand &command) {
		return handle_single_command(SingleKeyCommand(command));
	}

	SingleKeyResponse handle_single_command(SingleKeyCommand &&command) {
		return std::visit(
		    [this](auto &value) -> SingleKeyResponse {
			    using T = std::remove_cvref_t<decltype(value)>;
			    if constexpr (std::is_same_v<T, SetCommand>) {
				    return handle_set(std::move(value));
			    } else if constexpr (std::is_same_v<T, SingleGetCommand>) {
				    return handle_single_get(value);
			    } else {
				    throw std::logic_error("single command not implemented");
			    }
		    },
		    command.command);
	}

private:
	SetResponse handle_set(SetCommand &&command) {
		// CacheValue{
		// 	.cas = 0,
		// 	.flags = command.flags,
		// 	.expires_at = command.exptime
		//
		// };
		// map_.insert_or_assign()

		// The parser copies the payload once. Transfer its allocation all the
		// way into the cache, including when it crossed a shard mailbox.
		data_.insert_or_assign(std::move(command.key),
		                       Item {.flags = command.flags, .value = std::move(command.value)});
		if (command.noreply) {
			return SetResponse {StorageStatus::NoReply};
		}
		return SetResponse {StorageStatus::Stored};
	}

	SingleGetResponse handle_single_get(const SingleGetCommand &command) const {
		SingleGetResponse response;

		const auto it = data_.find(command.key);
		if (it != data_.end()) {
			response.item = ValueItem {.key = it->first.key, .flags = it->second.flags, .value = it->second.value};
		}
		// 未命中时 item 为空，编码器只输出 END。
		return response;
	}

	MultiGetResponse handle_multi_get(const MultiGetCommand &command) const {
		MultiGetResponse response;
		response.items.reserve(command.keys.size());

		for (const auto &key : command.keys) {
			const auto it = data_.find(key);
			if (it == data_.end()) {
				continue;
			}
			response.items.push_back(
			    ValueItem {.key = it->first.key, .flags = it->second.flags, .value = it->second.value});
		}
		return response;
	}

	std::unordered_map<ItemKey, Item, KeyHash, KeyEqual> data_;

	IncrementalHashMap<CacheValue> map_;
};

} // namespace fastest