#pragma once

#include <bit>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <limits>
#include <memory>
#include <span>
#include <stdexcept>
#include <string>
#include <utility>

namespace fastest {

class FastByteHash {
public:
    static constexpr std::uint64_t goldenRatio = 0x9e3779b97f4a7c15ULL;

    explicit constexpr FastByteHash(
        std::uint64_t seed = goldenRatio) noexcept
        : seed_(seed) {}

    [[nodiscard]] std::uint64_t operator()(
        std::span<const std::byte> key) const noexcept {
        const auto* p = key.data();
        std::size_t n = key.size();

        std::uint64_t h = mix(
            seed_ ^ (static_cast<std::uint64_t>(n) * goldenRatio));

        while (n >= sizeof(std::uint64_t)) {
            std::uint64_t word;
            std::memcpy(&word, p, sizeof(word));

            h ^= mix(word + 0xa0761d6478bd642fULL);
            h = std::rotl(h, 27) * 0xe7037ed1a0b428dbULL
                + 0x8ebc6af09c88c6e3ULL;

            p += sizeof(word);
            n -= sizeof(word);
        }

        if (n != 0) {
            std::uint64_t tail = 0;
            std::memcpy(&tail, p, n);
            h ^= mix(tail ^ (
                static_cast<std::uint64_t>(n) * 0x589965cc75374cc3ULL));
        }

        return mix(h);
    }

private:
    static constexpr std::uint64_t mix(std::uint64_t x) noexcept {
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
    std::uint64_t hash = 0;
    Key key;

    explicit ItemKey(Key k)
        : hash(FastByteHash{}(std::as_bytes(
              std::span<const char>{k.data(), k.size()}))),
          key(std::move(k)) {}
};

struct Item {
    std::uint32_t flags = 0;
    Value value;
};

// 用于 parser -> mailbox -> shard 之间传递 payload 的所有权。
// copy_of() 分配并复制一次；移动操作不复制 payload 字节。
class ByteBuffer {
public:
    using size_type = std::uint32_t;

    ByteBuffer() noexcept = default;
    ByteBuffer(const ByteBuffer&) = delete;
    ByteBuffer& operator=(const ByteBuffer&) = delete;

    ByteBuffer(ByteBuffer&& other) noexcept
        : data_(std::move(other.data_)),
          size_(std::exchange(other.size_, 0)) {}

    ByteBuffer& operator=(ByteBuffer&& other) noexcept {
        if (this != &other) {
            data_ = std::move(other.data_);
            size_ = std::exchange(other.size_, 0);
        }
        return *this;
    }

    [[nodiscard]] static constexpr std::size_t max_size() noexcept {
        return std::numeric_limits<size_type>::max();
    }

    [[nodiscard]] static ByteBuffer copy_of(
        std::span<const std::byte> input) {
        // 在分配和类型转换前检查，避免长度截断。
        if (input.size() > max_size()) {
            throw std::length_error("ByteBuffer payload exceeds uint32_t");
        }

        ByteBuffer result;
        if (!input.empty()) {
            result.data_ =
                std::make_unique_for_overwrite<std::byte[]>(input.size());
            result.size_ = static_cast<size_type>(input.size());
            std::memcpy(result.data_.get(), input.data(), input.size());
        }
        return result;
    }

    [[nodiscard]] const std::byte* data() const noexcept {
        return data_.get();
    }

    [[nodiscard]] std::size_t size() const noexcept {
        return size_;
    }

    [[nodiscard]] std::span<const std::byte> bytes() const noexcept {
        return {data_.get(), size_};
    }

    // 只统计请求的堆字节数，不包含分配器元数据和大小分级取整。
    [[nodiscard]] std::size_t heap_bytes() const noexcept {
        return size_;
    }

    [[nodiscard]] std::size_t memory_usage() const noexcept {
        return sizeof(ByteBuffer) + heap_bytes();
    }

private:
    friend struct CacheValue;

    std::unique_ptr<std::byte[]> data_;
    size_type size_ = 0;
};

// 兼容旧名称；不依赖 mimalloc，也不提供 charge_to()。
using MimallocBuffer = ByteBuffer;

// 常见 64 位 ABI 布局：
// cas    : 8 字节
// flags  : 4 字节
// size_  : 4 字节
// data_  : 8 字节
// 合计 24 字节。payload 堆空间单独计算。
struct CacheValue {
    std::uint64_t cas = 0;
    std::uint32_t flags = 0;

    CacheValue() noexcept = default;
    CacheValue(const CacheValue&) = delete;
    CacheValue& operator=(const CacheValue&) = delete;

    // ByteBuffer 已经保证长度在 uint32_t 范围内。
    explicit CacheValue(ByteBuffer&& buffer) noexcept
        : size_(std::exchange(buffer.size_, 0)),
          data_(std::move(buffer.data_)) {}

    CacheValue(CacheValue&& other) noexcept
        : cas(other.cas),
          flags(other.flags),
          size_(std::exchange(other.size_, 0)),
          data_(std::move(other.data_)) {}

    CacheValue& operator=(CacheValue&& other) noexcept {
        if (this != &other) {
            data_ = std::move(other.data_);
            size_ = std::exchange(other.size_, 0);
            cas = other.cas;
            flags = other.flags;
        }
        return *this;
    }

    [[nodiscard]] static CacheValue copy_of(
        std::span<const std::byte> input) {
        return CacheValue{ByteBuffer::copy_of(input)};
    }

    // 替换 payload，保留 cas 和 flags，不复制 payload 字节。
    void set_payload(ByteBuffer&& buffer) noexcept {
        data_ = std::move(buffer.data_);
        size_ = std::exchange(buffer.size_, 0);
    }

    [[nodiscard]] const std::byte* data() const noexcept {
        return data_.get();
    }

    [[nodiscard]] std::size_t size() const noexcept {
        return size_;
    }

    [[nodiscard]] std::span<const std::byte> bytes() const noexcept {
        return {data_.get(), size_};
    }

    [[nodiscard]] std::size_t heap_bytes() const noexcept {
        return size_;
    }

    // 对象内联存储统计一次，再加 payload 请求的堆字节数。
    [[nodiscard]] std::size_t memory_usage() const noexcept {
        return sizeof(CacheValue) + heap_bytes();
    }

private:
    std::uint32_t size_ = 0;
    std::unique_ptr<std::byte[]> data_;
};

// 若项目要求常见 64 位 ABI 下固定为 24 字节，可启用：
// static_assert(sizeof(CacheValue) == 24);

} // namespace fastest