# pragma once
#include <cstddef>
#include <cstdint>

namespace util {
// 用 hash 的高 32 位选分片（multiply-shift 区间映射，不做除法）。
// 分片内的 IncrementalHashMap 用 2 的幂个桶、按低位寻址；如果这里也取低位（hash % n），
// n 为 2 的幂时同一分片内所有 key 的低位相同，只会用到 1/n 的桶，冲突链被拉长 n 倍。
inline std::size_t get_shard(const uint64_t hash, const std::size_t core_num) noexcept {
	return static_cast<std::size_t>(((hash >> 32) * static_cast<uint64_t>(core_num)) >> 32);
}
}
