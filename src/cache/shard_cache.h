#pragma once

#include "../server/comm/type.h"
namespace comm {
class ShardCache {
public:
	MultiKeyResponse handle_multi_command(const MultiKeyCommand &command) {
		return std::visit(
		    [](const auto &value) -> MultiKeyResponse {
			    using T = std::remove_cvref_t<decltype(value)>;
			    if constexpr (std::is_same_v<T, SetCommand>) {
			    } else if constexpr (std::is_same_v<T, MultiGetCommand>) {
			    } else if constexpr (std::is_same_v<T, MultiGetsCommand>) {
			    } else if constexpr (std::is_same_v<T, MultiGatCommand>) {
			    } else if constexpr (std::is_same_v<T, MultiGatsCommand>) {
			    }
		    },
		    command.command);
	}

	SingleKeyResponse handle_single_command(const SingleKeyCommand &command) {
		return std::visit(
		    [](const auto &value) -> SingleKeyResponse {
			    using T = std::remove_cvref_t<decltype(value)>;
			    if constexpr (std::is_same_v<T, SetCommand>) {
			    } else if constexpr (std::is_same_v<T, AddCommand>) {
			    } else if constexpr (std::is_same_v<T, ReplaceCommand>) {
			    } else if constexpr (std::is_same_v<T, AppendCommand>) {
			    } else if constexpr (std::is_same_v<T, PrependCommand>) {
			    } else if constexpr (std::is_same_v<T, CasCommand>) {
			    } else if constexpr (std::is_same_v<T, SingleGetCommand>) {
			    } else if constexpr (std::is_same_v<T, SingleGetsCommand>) {
			    } else if constexpr (std::is_same_v<T, SingleGatCommand>) {
			    } else if constexpr (std::is_same_v<T, SingleGatsCommand>) {
			    } else if constexpr (std::is_same_v<T, DeleteCommand>) {
			    } else if constexpr (std::is_same_v<T, TouchCommand>) {
			    } else if constexpr (std::is_same_v<T, IncrCommand>) {
			    } else if constexpr (std::is_same_v<T, DecrCommand>) {
			    }
		    },
		    command.command);
	}

private:
	std::unordered_map<ItemKey, Item, KeyHash, KeyEqual> data_;
};
} // namespace comm