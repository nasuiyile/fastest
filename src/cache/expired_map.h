#pragma once

#include "../util/clock.h"
#include "core_cache.h"
#include "type.h"

#include <cassert>
#include <cstddef>
#include <cstdint>

namespace fastest {

// 过期层：包裹 IncrementalHashMap<CacheValue>，负责惰性过期，并对外提供定期过期采样。
// 与 IncrementalHashMap 一样只能由所属分片线程访问（包括 const 方法）。
//
// 所有权：map_ 里每个 CacheValue* 持有一次引用计数。insert_or_assign() 从调用者接管，
// 覆盖、删除、过期、析构时 release()。find() 只借出指针，要跨过下一次修改继续使用时由调用者 share()。
//
// 过期判断：expired_at != kNever && expired_at <= ExpiryClock::now()，与 memcached 一致，到点即过期；
// kExpired(1) 小于任何 now()（>= 2），不需要特判。expired_at 为 kNever 时不读时钟。
// 发现过期就当场删除并释放，绝不把过期的 value 返回给调用者：
// - 惰性过期：find()、erase() 碰到过期的 value 时直接删除；peek() 是 const，删不了，只返回 nullptr。
// - 主动过期：外部调度（分片线程空闲时）调用 sample_expired()，采样回调里判断过期，由 sample_next() 当场摘下节点并释放。
// size()、used_bytes() 包含已经过期、但还没被惰性删除或采样删除的条目。
//
// ttl_count_：map 里带过期时间（expired_at != kNever）的条目数。插入带过期时间的 value 时加一，
// 这样的条目以任何方式离开 map（覆盖、delete、惰性删除、采样删除）时减一；为 0 时 sample_expired() 直接返回。
// 只由所属线程读写，是普通整数而不是 atomic；放在 map 对象里，不给每个 pair 增加任何内存。
// touch/gat 原地修改 expired_at 必须经过 touch()，由它同步调整计数，不能绕过它直接改 value。
class ExpiredMap {
public:
	using Map = IncrementalHashMap<CacheValue>;

	// 一次采样的统计，调度器据此决定是否继续下一轮（见 doc/scheduled_deletion.md）。
	struct SampleStats {
		std::size_t sampled = 0;  // 实际访问的条目数：min(limit, size())，每次调用内互不相同
		std::size_t with_ttl = 0; // 其中带过期时间的（expired_at != kNever）
		std::size_t expired = 0;  // 其中已过期并被删除的
	};

	ExpiredMap() = default;

	ExpiredMap(const ExpiredMap &) = delete;

	ExpiredMap &operator=(const ExpiredMap &) = delete;

	~ExpiredMap() {
		map_.clear_and_dispose(release_value);
	}

	[[nodiscard]] static bool has_ttl(const CacheValue *value) noexcept {
		return value != nullptr && value->expired_at != util::ExpiryClock::kNever;
	}

	[[nodiscard]] static bool is_expired(const CacheValue &value, const std::uint32_t now) noexcept {
		return value.expired_at != util::ExpiryClock::kNever && value.expired_at <= now;
	}

	[[nodiscard]] static bool is_expired(const CacheValue &value) noexcept {
		return value.expired_at != util::ExpiryClock::kNever && value.expired_at <= util::ExpiryClock::now();
	}

	// 未命中或已过期都返回 nullptr；过期的当场删除，不返回。顺带推进一步增量 rehash。
	// 返回借出的指针，只在下一次修改 map 之前有效。
	[[nodiscard]] CacheValue *find(const ItemKey &key) {
		std::uint32_t now = 0;
		return find(key, now);
	}

	// now 是调用者栈上的时钟缓存，0 表示还没读（ExpiryClock::now() >= 2）：只有碰到带过期时间的 value 才读一次时钟，
	// 多 key 查找共用同一个 now。
	[[nodiscard]] CacheValue *find(const ItemKey &key, std::uint32_t &now) {
		CacheValue *const value = map_.find(key);
		if (value == nullptr || value->expired_at == util::ExpiryClock::kNever) [[likely]] {
			return value;
		}
		if (now == 0) {
			now = util::ExpiryClock::now();
		}
		if (value->expired_at > now) {
			return value;
		}
		map_.erase_and_dispose(key, release_value);
		drop_ttl();
		return nullptr;
	}

	// 只读查看：不推进增量 rehash，不增加引用计数；已过期的返回 nullptr，但不删除。
	[[nodiscard]] const CacheValue *peek(const ItemKey &key) const {
		const CacheValue *const value = map_.peek(key);
		return value != nullptr && !is_expired(*value) ? value : nullptr;
	}

	// 接管 value 的那次计数；key 已存在时覆盖，旧值（不论是否过期）释放 map 那一次计数。
	// 抛异常时所有权不变，value 仍归调用者。
	void insert_or_assign(const ItemKey &key, CacheValuePtr &&value) {
		CacheValue *const raw = value.get();
		const auto [previous, inserted] = map_.insert_or_assign(key, raw);
		(void)value.release_ownership();
		ttl_count_ += has_ttl(raw);
		if (!inserted) {
			// 先按旧值扣计数，再释放：release 之后 previous 可能已经销毁。
			if (has_ttl(previous)) {
				drop_ttl();
			}
			// 新旧是同一块内存时计数至少为 2（map 一次 + 刚交出的一次），这里只减掉多出的一次，不会销毁。
			release_value(previous);
		}
	}

	// add：key 不存在（或已过期）时插入并接管 value 那次计数，返回 true；存在且未过期时返回 false，所有权不变。
	// 先直接插入：key 不存在（add 成功的情况）只有一次查找；已存在时再查一次判断是否过期，过期的当场删除后插入。
	// 抛异常时所有权不变，value 仍归调用者。
	bool insert(const ItemKey &key, CacheValuePtr &&value, std::uint32_t &now) {
		CacheValue *const raw = value.get();
		if (!map_.insert(key, raw)) {
			if (find(key, now) != nullptr) {
				return false;
			}
			const bool inserted = map_.insert(key, raw);
			assert(inserted);
			(void)inserted;
		}
		(void)value.release_ownership();
		ttl_count_ += has_ttl(raw);
		return true;
	}

	// touch / gat：原地改写 map 里 value 的过期时间，同步 ttl_count_。value 必须是本 map 刚 find() 到的指针。
	// expired_at 只有 owner 线程读写，远端在途响应不读它，所以不需要复制 value；不改变 memory_usage()。
	void touch(CacheValue *value, const std::uint32_t expired_at) noexcept {
		const bool had_ttl = has_ttl(value);
		value->expired_at = expired_at;
		const bool has = has_ttl(value);
		if (had_ttl != has) {
			if (has) {
				++ttl_count_;
			} else {
				drop_ttl();
			}
		}
	}

	// 一次查找：摘下节点并释放 map 那一次计数。返回删除前是否存在且未过期：
	// 已过期的同样删除，但按不存在处理（memcached 的 delete 对过期 key 返回 NOT_FOUND）。
	bool erase(const ItemKey &key) {
		const auto [value, found] = map_.extract(key);
		if (!found) {
			return false;
		}
		bool live = value != nullptr;
		if (has_ttl(value)) {
			drop_ttl();
			live = !is_expired(*value);
		}
		release_value(value);
		return live;
	}

	// 主动过期采样，由外部调度调用。从游标处访问 min(limit, size()) 个不同条目，已过期的当场删除。
	// limit 限制的是访问条目数，不是耗时（游标前进可能扫过空桶）。
	SampleStats sample_expired(const std::size_t limit) {
		if (ttl_count_ == 0) {
			return {};
		}
		return sample_expired(limit, util::ExpiryClock::now());
	}

	// now 由调用者给出：一轮调度只读一次时钟；测试也可以指定。
	SampleStats sample_expired(const std::size_t limit, const std::uint32_t now) {
		SampleStats stats;
		if (ttl_count_ == 0) {
			// 没有任何带过期时间的条目，不必扫描（也不推进游标和增量 rehash）。
			return stats;
		}
		// 回调返回 true：sample_next() 在回调返回后立刻摘下这个节点，再把 value 交给 release_value。
		stats.sampled = map_.sample_next(
			limit,
			[&](Map::key_view, const CacheValue *value) {
				if (value == nullptr || value->expired_at == util::ExpiryClock::kNever) {
					return false;
				}
				++stats.with_ttl;
				if (value->expired_at > now) {
					return false;
				}
				++stats.expired;
				return true;
			},
			release_value);
		assert(ttl_count_ >= stats.expired);
		ttl_count_ -= stats.expired;
		return stats;
	}

	[[nodiscard]] std::size_t size() const noexcept {
		return map_.size();
	}

	[[nodiscard]] bool empty() const noexcept {
		return map_.empty();
	}

	[[nodiscard]] std::size_t used_bytes() const noexcept {
		return map_.used_bytes();
	}

	// 带过期时间的条目数（含已过期但还没删除的）。
	[[nodiscard]] std::size_t ttl_count() const noexcept {
		return ttl_count_;
	}

private:
	static void release_value(CacheValue *value) noexcept {
		if (value != nullptr) {
			value->release();
		}
	}

	void drop_ttl() noexcept {
		assert(ttl_count_ != 0);
		--ttl_count_;
	}

	Map map_;
	std::size_t ttl_count_ = 0;
};

} // namespace fastest
