#pragma once
#include <algorithm>
#include <charconv>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <limits>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <system_error>
#include <type_traits>
#include <utility>
#include <variant>
#include <vector>
namespace fastest {

class FastByteHash {
public:
	static constexpr std::uint64_t goldenRatio = 0x9e3779b97f4a7c15ULL;

	explicit constexpr FastByteHash(const std::uint64_t seed = goldenRatio) noexcept : seed_(seed) {
	}

	std::uint64_t operator()(const std::span<const std::byte> key) const noexcept {
		const auto *p = key.data();
		std::size_t n = key.size();

		std::uint64_t h = mix(seed_ ^ (static_cast<std::uint64_t>(n) * goldenRatio));

		while (n >= sizeof(std::uint64_t)) {
			std::uint64_t word;
			std::memcpy(&word, p, sizeof(word));
			// wyhash常量
			/*
			 * 0xa0761d6478bd642f 用来和每个 64 位字相加，制造非线性；
			 * 0xe7037ed1a0b428db 是乘法混合因子；
			 * 0x8ebc6af09c88c6e3 是加法扰动，避免乘法结果落在固定子空间。
			 */
			h ^= mix(word + 0xa0761d6478bd642fULL);
			h = std::rotl(h, 27) * 0xe7037ed1a0b428dbULL + 0x8ebc6af09c88c6e3ULL;

			p += sizeof(word);
			n -= sizeof(word);
		}

		if (n != 0) {
			std::uint64_t tail = 0;
			std::memcpy(&tail, p, n);
			// wyhash 的 tail 常量,避免长短不同的输入产生相同的哈希
			h ^= mix(tail ^ (static_cast<std::uint64_t>(n) * 0x589965cc75374cc3ULL));
		}

		return mix(h);
	}

private:
	static constexpr std::uint64_t mix(std::uint64_t x) noexcept {
		// 俩个splitmix64 的混合常量。进行右移位运算的时候效果最好
		x ^= x >> 30;
		x *= 0xbf58476d1ce4e5b9ULL;
		x ^= x >> 27;
		x *= 0x94d049bb133111ebULL;
		x ^= x >> 31;
		return x;
	}

	std::uint64_t seed_;
};

using Value = std::string;
using Key = std::string;
struct ItemKey {
	uint64_t hash {0};
	Key key;

	explicit ItemKey(Key k)
	    : hash(FastByteHash {}(std::as_bytes(std::span<const char> {k.data(), k.size()}))), key(std::move(k)) {
	}
};
struct Item {
	uint32_t flags {0};
	Value value;
};

// Optional parser -> mailbox -> shard payload. copy_of() makes one copy;
// moves transfer ownership without copying payload bytes. No allocation counters
// or shard-owned allocator are attached, so ownership may cross threads.
class ByteBuffer {
public:
	ByteBuffer() noexcept = default;
	ByteBuffer(const ByteBuffer &) = delete;
	ByteBuffer &operator=(const ByteBuffer &) = delete;

	ByteBuffer(ByteBuffer &&other) noexcept : data_(std::move(other.data_)), size_(std::exchange(other.size_, 0)) {
	}

	ByteBuffer &operator=(ByteBuffer &&other) noexcept {
		if (this != &other) {
			data_ = std::move(other.data_);
			size_ = std::exchange(other.size_, 0);
		}
		return *this;
	}

	[[nodiscard]] static ByteBuffer copy_of(std::span<const std::byte> input) {
		ByteBuffer result;
		if (!input.empty()) {
			result.data_ = std::make_unique_for_overwrite<std::byte[]>(input.size());
			result.size_ = input.size();
			std::memcpy(result.data_.get(), input.data(), input.size());
		}
		return result;
	}

	[[nodiscard]] const std::byte *data() const noexcept {
		return data_.get();
	}
	[[nodiscard]] std::size_t size() const noexcept {
		return size_;
	}
	[[nodiscard]] std::span<const std::byte> bytes() const noexcept {
		return {data_.get(), size_};
	}

	// Requested heap bytes only; allocator rounding/metadata are excluded.
	[[nodiscard]] std::size_t heap_bytes() const noexcept {
		return size_;
	}
	// Includes this object's inline storage exactly once.
	[[nodiscard]] std::size_t memory_usage() const noexcept {
		return sizeof(ByteBuffer) + heap_bytes();
	}

private:
	std::unique_ptr<std::byte[]> data_;
	std::size_t size_ = 0;
};

// Preserve the old payload type name for copy_of()/move callers. There is no
// mimalloc dependency and charge_to() is intentionally removed.
using MimallocBuffer = ByteBuffer;

// Single-owner, non-owning Value* map. Values must remain alive while linked.
// Value must provide: std::size_t memory_usage() const noexcept (preferably O(1));
// It returns sizeof(Value) + its owned external storage estimate, excluding
// Entry/key/hash-table storage. Shared buffers are charged once per entry, not
// deduplicated. No user callbacks may reenter the map or suspend its owner.
//
// Example of a memcached value with a movable payload:
struct CacheValue {
	std::uint64_t cas = 0;
	std::uint32_t flags = 0;
	std::uint32_t expires_at = 0;
	ByteBuffer payload;
	[[nodiscard]] std::size_t memory_usage() const noexcept {
		return sizeof(CacheValue) + payload.heap_bytes();
	}
};
} // namespace fastest