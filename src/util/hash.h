# pragma once
#include <cstdint>

namespace util {
inline std::size_t get_shard(const uint64_t hash, const std::size_t core_num) noexcept {
	return hash % static_cast<uint64_t>(core_num);
}
}