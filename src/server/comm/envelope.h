#pragma once

#include <cassert>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <new>
#include <type_traits>
#include <utility>
#include <variant>

#include "mailbox.h"
#include "type.h"

namespace fastest {
// 一条跨分片命令：去程是请求，执行分片在原位把它换成响应，回程带回发起分片。
struct HopEntry {
	HopEntry(const uint64_t id, const uint32_t s, RequestData &&request) noexcept
		: tcp_id(id), seq(s), data(std::in_place_index<0>, std::move(request)) {
	}

	uint64_t tcp_id; // 连接编号
	uint32_t seq;    // 请求序号，允许回绕
	std::variant<RequestData, ResponseData> data;
};

/*
 * 往返信封：一批发往同一个目标分片的请求，以及它们的响应。
 *
 * 发起分片从自己的池里取出信封，一个解析批次里发往该分片的请求依次装进来，批次结束（或装满）时推进目标分片的收件队列；
 * 目标分片逐条执行，把响应原地写进同一个信封，再把它推回发起分片；发起分片把响应放进各连接的环形数组，然后把信封放回池里。
 * 信封始终由发起分片分配和释放：没有跨线程 free，也不再每条消息分配一次；同步开销按信封付，而不是按消息付。
 *
 * 条目内联存储（placement new），只有前 size() 个已构造。容量取 16：按解析批次发送时大多数信封只有几条，
 * 小容量让在途信封占的内存少（约 1.7KB）；装满就先发出去，大批次只是多几次 exchange。
 */
class alignas(64) Envelope : public MpscHook {
public:
	static constexpr uint16_t kCapacity = 16;

	// 用户提供的构造函数：new Envelope() 也不会把 1.7KB 的条目存储清零。
	Envelope() noexcept {
	}

	~Envelope() {
		clear();
	}

	Envelope(const Envelope &) = delete;
	Envelope &operator=(const Envelope &) = delete;

	// 发起分片取出信封时调用；信封必须是空的。
	void reset(const uint16_t origin, const uint16_t target) noexcept {
		assert(count_ == 0);
		origin_ = origin;
		target_ = target;
		returned_ = false;
	}

	void append(const uint64_t tcp_id, const uint32_t seq, RequestData &&request) noexcept {
		assert(count_ < kCapacity && !returned_);
		::new (static_cast<void *>(slot(count_))) HopEntry(tcp_id, seq, std::move(request));
		++count_;
	}

	// 析构所有条目，信封可以再次装载。
	void clear() noexcept {
		for (std::size_t i = 0; i < count_; ++i) {
			std::destroy_at(entry(i));
		}
		count_ = 0;
	}

	HopEntry &operator[](const std::size_t i) noexcept {
		assert(i < count_);
		return *entry(i);
	}

	[[nodiscard]] uint16_t size() const noexcept {
		return count_;
	}

	[[nodiscard]] bool full() const noexcept {
		return count_ == kCapacity;
	}

	[[nodiscard]] uint16_t origin() const noexcept {
		return origin_;
	}

	[[nodiscard]] uint16_t target() const noexcept {
		return target_;
	}

	// false：请求在去程，在目标分片的收件队列里；true：响应在回程，在发起分片的收件队列里。
	[[nodiscard]] bool returned() const noexcept {
		return returned_;
	}

	void mark_returned() noexcept {
		returned_ = true;
	}

private:
	void *slot(const std::size_t i) noexcept {
		return storage_ + i * sizeof(HopEntry);
	}

	HopEntry *entry(const std::size_t i) noexcept {
		return std::launder(reinterpret_cast<HopEntry *>(slot(i)));
	}

	uint16_t origin_ {0};
	uint16_t target_ {0};
	uint16_t count_ {0};
	bool returned_ {false};
	alignas(HopEntry) std::byte storage_[kCapacity * sizeof(HopEntry)];
};

static_assert(std::is_nothrow_move_constructible_v<RequestData>, "RequestData must be nothrow move-constructible");
} // namespace fastest
