#pragma once

#include "../server/comm/type.h"

#include <stdexcept>
#include <string>
#include <type_traits>
#include <unordered_map>
#include <variant>

namespace comm {

class ShardCache {
public:
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
		return std::visit(
		    [this](const auto &value) -> SingleKeyResponse {
			    using T = std::remove_cvref_t<decltype(value)>;

			    if constexpr (std::is_same_v<T, SetCommand>) {
				    return handle_set(value);
			    } else if constexpr (std::is_same_v<T, SingleGetCommand>) {
				    return handle_single_get(value);
			    } else {
				    throw std::logic_error("single command not implemented");
			    }
		    },
		    command.command);
	}

private:
	SetResponse handle_set(const SetCommand &command) {
		// value 是 string_view，必须复制为缓存自己持有的 string。
		// key 不存在时插入，存在时覆盖 flags 和 value。
		data_.insert_or_assign(command.key, Item {command.flags, std::string(command.value)});

		return SetResponse {StorageStatus::Stored};
	}

	SingleGetResponse handle_single_get(const SingleGetCommand &command) const {
		SingleGetResponse response;

		const auto it = data_.find(command.key);
		if (it != data_.end()) {
			response.item = ValueItem {it->first.key, it->second.flags, it->second.value};
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

			response.items.push_back(ValueItem {it->first.key, it->second.flags, it->second.value});
		}

		return response;
	}

	std::unordered_map<ItemKey, Item, KeyHash, KeyEqual> data_;
};

} // namespace comm