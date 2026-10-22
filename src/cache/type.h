#pragma once

#include <bit>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <limits>
#include <memory>
#include <span>
#include <stdexcept>
#include <string>
#include <string_view>
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

using Key = std::string;

struct ItemKey {
	std::uint64_t hash = 0;
	Key key;

	explicit ItemKey(Key k)
	    : hash(FastByteHash {}(std::as_bytes(std::span<const char> {k.data(), k.size()}))), key(std::move(k)) {
	}
};

// 所有者一侧的智能指针：命令持有尚未发布的 value，map 接管它那次计数；拷贝、share()、析构都直接改计数，
// 所以 value 发布之后只能在所属分片（owner）线程上拷贝或析构。跨分片的响应不用它，用 ValueLoan。
// 从裸指针拿到智能指针只有两个入口，名字写明是否 +1：
//   adopt(p)：接管 p 已有的那次计数（create() 内部、从 map 摘下来的指针）
//   share(p)：新增一次计数（owner 线程上，map_.find() 返回的是 map 借给你的指针）
class CacheValuePtr {
public:
	CacheValuePtr() noexcept = default;

	[[nodiscard]] static CacheValuePtr adopt(CacheValue *p) noexcept {
		return CacheValuePtr(p);
	}

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

	void reset() noexcept {
		*this = CacheValuePtr();
	}
	// 交出这次计数（例如交给 map），调用者负责以后 release()。
	[[nodiscard]] CacheValue *release_ownership() noexcept {
		return std::exchange(ptr_, nullptr);
	}

	[[nodiscard]] CacheValue *get() const noexcept {
		return ptr_;
	}
	CacheValue *operator->() const noexcept {
		return ptr_;
	}
	CacheValue &operator*() const noexcept {
		return *ptr_;
	}
	explicit operator bool() const noexcept {
		return ptr_ != nullptr;
	}

private:
	explicit CacheValuePtr(CacheValue *p) noexcept : ptr_(p) {
	}

	CacheValue *ptr_ = nullptr;
};

// 一次分配：[CacheValue 24B][payload][\r\n]。
// 发布后（插进 map 或借给别的线程）：payload、cas、flags、size 只读；
// expired_at 只有 owner 线程读写（touch/gat 原地改），远端线程不得读。
// 唯一的例外是 incr/decr：owner 线程在 exclusively_owned()（只有 map 持有）时原地改写数字和 cas，
// 对应 memcached 在 refcount == 2 时的原地替换；有在途引用时改为分配新 value（见 ShardCache::store_number）。
//
// 引用计数是普通整数，不是 atomic：发布之后只有 owner 线程读写它。
// - 命令持有尚未发布的 value（计数 1）：在连接线程创建，经信封交给 owner，队列的 push/pop 提供 happens-before；
// - map 持有一次；owner 借出（ValueLoan::lend）时加一；
// - 远端用完后不改计数，而是把指针送回 owner（ValueLoan 析构 → LoanSink），由 owner 减一、归零时释放。
//   远端「读 payload → 写完成 → 归还记录进信封 → push（release）」，owner「pop（acquire）→ 减一 → 可能原地改写或释放」，
//   所以 owner 看到计数回到 1 或 0 时，远端对 payload 和头部字段的读取都已经结束。
class CacheValue {
public:
	CacheValue(const CacheValue &) = delete;

	CacheValue &operator=(const CacheValue &) = delete;

	// 唯一一次拷贝 value 字节的地方：从输入缓冲区直接拷进最终的存储块。
	[[nodiscard]] static CacheValuePtr create(std::span<const std::byte> payload, std::uint32_t flags = 0,
	                                          std::uint32_t expired_at = 0, std::uint64_t cas = 0) {
		CacheValuePtr v = create_for_overwrite(payload.size(), flags, expired_at, cas);
		if (!payload.empty()) {
			std::memcpy(v->payload(), payload.data(), payload.size());
		}
		return v;
	}

	[[nodiscard]] static CacheValuePtr create(std::string_view payload, std::uint32_t flags = 0,
	                                          std::uint32_t expired_at = 0, std::uint64_t cas = 0) {
		return create(std::as_bytes(std::span<const char>(payload.data(), payload.size())), flags, expired_at, cas);
	}

	// 只分配不初始化 payload（结尾已写好 \r\n）。用于把 socket 数据直接读进 value，省掉经过输入缓冲区的拷贝；
	// 发布前由创建者通过 mutable_bytes_crlf() 填满。
	[[nodiscard]] static CacheValuePtr create_for_overwrite(std::size_t size, std::uint32_t flags = 0,
	                                                        std::uint32_t expired_at = 0, std::uint64_t cas = 0) {
		if (size > std::numeric_limits<std::uint32_t>::max() - 2) {
			throw std::length_error("CacheValue payload exceeds uint32_t");
		}
		void *mem = ::operator new(sizeof(CacheValue) + size + 2);
		auto *v = ::new (mem) CacheValue(cas, flags, expired_at, static_cast<std::uint32_t>(size));
		std::byte *p = v->payload();
		p[size] = std::byte {'\r'};
		p[size + 1] = std::byte {'\n'};
		return CacheValuePtr::adopt(v);
	}

	// 只能在 owner 线程上调用（发布前是唯一持有者所在的线程）。
	void retain() const noexcept {
		++ref_count_;
	}

	// 归零时销毁；调用后不得再访问 this。只能在 owner 线程上调用：远端归还走 ValueLoan → LoanSink。
	void release() const noexcept {
		if (--ref_count_ == 0) {
			this->~CacheValue();
			::operator delete(const_cast<CacheValue *>(this));
		}
	}

	[[nodiscard]] std::uint32_t use_count() const noexcept {
		return ref_count_;
	}

	// 已发布的 value 是否只剩调用者这一份引用（只有 map 持有，没有借出、或借出的都已归还）。
	// 只有 owner 线程读写计数，远端的读取在归还之前完成（见类注释），所以结果在 owner 线程上不会失效。
	[[nodiscard]] bool exclusively_owned() const noexcept {
		return ref_count_ == 1;
	}
	[[nodiscard]] std::size_t size() const noexcept {
		return size_;
	}
	[[nodiscard]] std::span<const std::byte> bytes() const noexcept {
		return {payload(), size_};
	}
	// 带 \r\n，编码时直接作为一个 iovec。
	[[nodiscard]] std::span<const std::byte> bytes_crlf() const noexcept {
		return {payload(), size_ + std::size_t {2}};
	}
	[[nodiscard]] std::string_view view() const noexcept {
		return {reinterpret_cast<const char *>(payload()), size_};
	}
	[[nodiscard]] std::string_view view_crlf() const noexcept {
		return {reinterpret_cast<const char *>(payload()), size_ + std::size_t {2}};
	}
	// 仅限唯一持有者写入：发布前（还没插进 map、也没 share 给别的线程），或者 owner 线程确认 exclusively_owned()。
	// 结尾两个字节是 CRLF 槽位。
	[[nodiscard]] std::span<std::byte> mutable_bytes_crlf() noexcept {
		return {payload(), size_ + std::size_t {2}};
	}
	[[nodiscard]] std::size_t memory_usage() const noexcept {
		return sizeof(CacheValue) + size_ + 2;
	}

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

	mutable std::uint32_t ref_count_ {1}; // 只有 owner 线程读写，见类注释
	std::uint32_t size_ = 0;
};

static_assert(sizeof(CacheValue) == 24);

// 归还借出的 value：每个分片线程绑定一个（Worker），ValueLoan 析构时把指针交给它。
// owner 就是当前分片时直接 release()；否则记下来，搭发往 owner 的信封送回去，由 owner release()。
class LoanSink {
public:
	virtual void give_back(CacheValue *value, std::uint16_t owner) noexcept = 0;

protected:
	~LoanSink() = default;
};

// 当前线程的归还出口。分片线程启动时绑定（Worker::bind_thread），常量初始化，访问不经过 TLS 包装函数。
// 为空表示这个线程上没有分片在运行（单线程的单元测试、停机后的析构），这时直接在本线程 release()：
// 只有在 owner 线程不会同时运行时才成立。
inline constinit thread_local LoanSink *tls_loan_sink = nullptr;

// 借出凭证：跨分片（以及同分片）响应持有的 value 引用，只能移动，8 字节。
// owner 线程 lend() 时加一次计数；凭证可以在任何分片线程上析构，析构时经 tls_loan_sink 送回 owner 减一，
// 借用方从不改 value 的计数（没有原子操作，也不写 owner 的缓存行）。
// 借用方只读：payload、flags、size、cas（发布后不变；incr 原地改写只发生在没有借出的时候）。
// owner 分片号放在指针的高 16 位：用户态地址只用低 48 位（x86-64 / AArch64 的 4 级页表；5 级页表下内核也只在
// mmap 显式给出高地址提示时才分配 47 位以上的地址），归还时不用再读 value 头或重算 key 的 hash。
class ValueLoan {
public:
	ValueLoan() noexcept = default;

	// 只能在 owner 线程上调用，value 必须已经由 owner 持有（例如刚从 map 里查到）。
	[[nodiscard]] static ValueLoan lend(CacheValue *value, const std::uint16_t owner) noexcept {
		value->retain();
		return ValueLoan(value, owner);
	}

	ValueLoan(ValueLoan &&other) noexcept : bits_(std::exchange(other.bits_, 0)) {
	}

	ValueLoan &operator=(ValueLoan &&other) noexcept {
		if (this != &other) {
			give_back();
			bits_ = std::exchange(other.bits_, 0);
		}
		return *this;
	}

	ValueLoan(const ValueLoan &) = delete;
	ValueLoan &operator=(const ValueLoan &) = delete;

	~ValueLoan() {
		give_back();
	}

	[[nodiscard]] CacheValue *get() const noexcept {
		return reinterpret_cast<CacheValue *>(bits_ & kAddressMask);
	}
	[[nodiscard]] std::uint16_t owner() const noexcept {
		return static_cast<std::uint16_t>(bits_ >> kOwnerShift);
	}
	const CacheValue *operator->() const noexcept {
		return get();
	}
	const CacheValue &operator*() const noexcept {
		return *get();
	}
	explicit operator bool() const noexcept {
		return bits_ != 0;
	}

private:
	static constexpr unsigned kOwnerShift = 48;
	static constexpr std::uintptr_t kAddressMask = (std::uintptr_t {1} << kOwnerShift) - 1;
	static_assert(sizeof(std::uintptr_t) == 8, "ValueLoan packs the owner shard into a 64-bit pointer");

	ValueLoan(CacheValue *value, const std::uint16_t owner) noexcept
	    : bits_(reinterpret_cast<std::uintptr_t>(value) | (std::uintptr_t {owner} << kOwnerShift)) {
		// 地址超出 48 位时分片号会把指针弄坏；这在支持的平台上不会发生，坏了就立刻停下，而不是悄悄写错内存。
		if ((reinterpret_cast<std::uintptr_t>(value) & ~kAddressMask) != 0) [[unlikely]] {
			std::abort();
		}
	}

	void give_back() noexcept {
		if (bits_ == 0) {
			return;
		}
		CacheValue *const value = get();
		const std::uint16_t owner_shard = owner();
		bits_ = 0;
		if (LoanSink *const sink = tls_loan_sink) {
			sink->give_back(value, owner_shard);
		} else {
			value->release();
		}
	}

	std::uintptr_t bits_ = 0;
};

static_assert(sizeof(ValueLoan) == 8);

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
} // namespace fastest
