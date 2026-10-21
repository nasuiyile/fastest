#pragma once

#include <bit>
#include <cstddef>
#include <cstring>
#include <limits>
#include <memory>
#include <span>
#include <stdexcept>
#include <string>
#include <utility>

namespace fastest {
class CacheValue;

class FastByteHash {
public:
	static constexpr std::uint64_t goldenRatio = 0x9e3779b97f4a7c15ULL;

	explicit constexpr FastByteHash(std::uint64_t seed = goldenRatio) noexcept : seed_(seed) {
	}

	[[nodiscard]] std::uint64_t operator()(std::span<const std::byte> key) const noexcept {
		const auto *p = key.data();
		std::size_t n = key.size();

		std::uint64_t h = mix(seed_ ^ (static_cast<std::uint64_t>(n) * goldenRatio));

		while (n >= sizeof(std::uint64_t)) {
			std::uint64_t word;
			std::memcpy(&word, p, sizeof(word));

			h ^= mix(word + 0xa0761d6478bd642fULL);
			h = std::rotl(h, 27) * 0xe7037ed1a0b428dbULL + 0x8ebc6af09c88c6e3ULL;

			p += sizeof(word);
			n -= sizeof(word);
		}

		if (n != 0) {
			std::uint64_t tail = 0;
			std::memcpy(&tail, p, n);
			h ^= mix(tail ^ (static_cast<std::uint64_t>(n) * 0x589965cc75374cc3ULL));
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
		: hash(FastByteHash{}(std::as_bytes(std::span<const char>{k.data(), k.size()}))), key(std::move(k)) {
	}
};

struct Item {
	std::uint32_t flags = 0;
	Value value;
};


// 从裸指针拿到智能指针只有两个入口，名字写明是否 +1：
//   adopt(p)：接管 p 已有的那次计数（create() 内部、从 map 摘下来的指针）
//   share(p)：新增一次计数（map_.find() 返回的是 map 借给你的指针）
class CacheValuePtr {
public:
	CacheValuePtr() noexcept = default;

	[[nodiscard]] static CacheValuePtr adopt(CacheValue *p) noexcept { return CacheValuePtr(p); }

	[[nodiscard]] static CacheValuePtr share(CacheValue *p) noexcept;

	CacheValuePtr(const CacheValuePtr &other) noexcept;

	CacheValuePtr(CacheValuePtr &&other) noexcept : ptr_(std::exchange(other.ptr_, nullptr)) {
	}

	// copy-and-swap：拷贝和移动共用一个；旧值在形参析构时释放，自赋值也安全。
	CacheValuePtr &operator=(CacheValuePtr other) noexcept {
		std::swap(ptr_, other.ptr_);
		return *this;
	}

	~CacheValuePtr();

	void reset() noexcept { *this = CacheValuePtr(); }
	// 交出这次计数（例如交给 map），调用者负责以后 release()。
	[[nodiscard]] CacheValue *release_ownership() noexcept { return std::exchange(ptr_, nullptr); }

	[[nodiscard]] CacheValue *get() const noexcept { return ptr_; }
	CacheValue *operator->() const noexcept { return ptr_; }
	CacheValue &operator*() const noexcept { return *ptr_; }
	explicit operator bool() const noexcept { return ptr_ != nullptr; }

private:
	explicit CacheValuePtr(CacheValue *p) noexcept : ptr_(p) {
	}

	CacheValue *ptr_ = nullptr;
};

// 一次分配：[CacheValue 24B][payload][\r\n]。
// 发布后（插进 map 或 share 给别的线程）：payload、cas、flags 只读；
// expired_at 只有 owner 线程读写（touch/gat 原地改），远端线程不得读。
class CacheValue {
public:
	CacheValue(const CacheValue &) = delete;

	CacheValue &operator=(const CacheValue &) = delete;


	[[nodiscard]] static CacheValuePtr create(std::span<const std::byte> payload, std::uint32_t flags = 0,
	                                          std::uint32_t expired_at = 0, std::uint64_t cas = 0) {
		if (payload.size() > std::numeric_limits<std::uint32_t>::max() - 2) {
			throw std::length_error("CacheValue payload exceeds uint32_t");
		}
		void *mem = ::operator new(sizeof(CacheValue) + payload.size() + 2);
		auto *v = ::new(mem) CacheValue(cas, flags, expired_at, static_cast<std::uint32_t>(payload.size()));
		std::byte *p = v->payload();
		if (!payload.empty()) {
			std::memcpy(p, payload.data(), payload.size());
		}
		p[payload.size()] = std::byte{'\r'};
		p[payload.size() + 1] = std::byte{'\n'};
		return CacheValuePtr::adopt(v);
	}

	// 调用者已经持有一次计数，relaxed 足够；跨线程可见性靠 mailbox 的 push/pop。
	void retain() const noexcept { ref_count_.fetch_add(1, std::memory_order_relaxed); }

	// 归零时销毁；调用后不得再访问 this。
	void release() const noexcept {
		if (ref_count_.fetch_sub(1, std::memory_order_acq_rel) == 1) {
			this->~CacheValue();
			::operator delete(const_cast<CacheValue *>(this));
		}
	}

	[[nodiscard]] std::uint32_t use_count() const noexcept { return ref_count_.load(std::memory_order_relaxed); }
	[[nodiscard]] std::size_t size() const noexcept { return size_; }
	[[nodiscard]] std::span<const std::byte> bytes() const noexcept { return {payload(), size_}; }
	// 带 \r\n，编码时直接作为一个 iovec。
	[[nodiscard]] std::span<const std::byte> bytes_crlf() const noexcept { return {payload(), size_ + std::size_t{2}}; }
	[[nodiscard]] std::size_t memory_usage() const noexcept { return sizeof(CacheValue) + size_ + 2; }

	std::uint64_t cas = 0;
	std::uint32_t flags = 0;
	std::uint32_t expired_at = 0;

private:
	CacheValue(std::uint64_t c, std::uint32_t f, std::uint32_t e, std::uint32_t n) noexcept
		: cas(c), flags(f), expired_at(e), size_(n) {
	}

	// 私有析构：delete、make_unique、map_.destroy_value() 都编译不过，只能走 release()。
	~CacheValue() = default;

	std::byte *payload() const noexcept {
		return reinterpret_cast<std::byte *>(const_cast<CacheValue *>(this) + 1);
	}

	mutable std::atomic<std::uint32_t> ref_count_{1};
	std::uint32_t size_ = 0;
};

static_assert(sizeof(CacheValue) == 24);

inline CacheValuePtr CacheValuePtr::share(CacheValue *p) noexcept {
	if (p)
		p->retain();
	return CacheValuePtr(p);
}

inline CacheValuePtr::CacheValuePtr(const CacheValuePtr &other) noexcept : ptr_(other.ptr_) {
	if (ptr_)
		ptr_->retain();
}

inline CacheValuePtr::~CacheValuePtr() {
	if (ptr_)
		ptr_->release();
}
}