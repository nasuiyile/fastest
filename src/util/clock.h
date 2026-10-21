#pragma once

#include <chrono>
#include <cstdint>
#include <ctime>
#include <limits>

namespace util {

// 过期时间统一表示为“相对纪元的秒数”（u32，约 136 年回绕），存在 CacheValue::expired_at 里：
//   0 = 永不过期，1 = 已经过期；now() 从 2 开始，和这两个特殊值不冲突。
// 纪元在第一次使用时确定一次：单调时钟读数，以及对应的 unix 时间减 2 秒。
// 解析线程在收到命令时换算（memcached 也是在收到命令时换算），存进 map 的永远是换算好的绝对值。
class ExpiryClock {
public:
	static constexpr std::uint32_t kNever = 0;
	static constexpr std::uint32_t kExpired = 1;
	// memcached：不超过 30 天按相对秒数处理，更大的值按 unix 时间戳处理。
	static constexpr std::uint32_t kMaxRelativeSeconds = 60 * 60 * 24 * 30;

	// coarse 单调时钟，vDSO 读取不进内核；精度为 tick（约 1–4ms），过期只需要秒。
	[[nodiscard]] static std::uint32_t now() noexcept {
		const Epoch &e = epoch();
		return static_cast<std::uint32_t>(monotonic_seconds() - e.monotonic) + kFirstSecond;
	}

	// unix 时间 t 对应的相对秒数为 t - epoch_unix()。
	[[nodiscard]] static std::int64_t epoch_unix() noexcept {
		return epoch().unix_time;
	}

	// 协议 exptime 换算成 expired_at。now 只在相对时间时使用，调用方可以按批缓存。
	[[nodiscard]] static constexpr std::uint32_t to_expired_at(const std::uint32_t exptime, const std::uint32_t now,
	                                                           const std::int64_t epoch_unix) noexcept {
		if (exptime == 0) {
			return kNever;
		}
		if (exptime <= kMaxRelativeSeconds) {
			return saturate(static_cast<std::int64_t>(now) + exptime);
		}
		// 早于纪元（含进程启动前）的时间戳视为已经过期，不能回绕成遥远的未来。
		const std::int64_t relative = static_cast<std::int64_t>(exptime) - epoch_unix;
		return relative <= kExpired ? kExpired : saturate(relative);
	}

	[[nodiscard]] static std::uint32_t to_expired_at(const std::uint32_t exptime) noexcept {
		return exptime == 0 ? kNever : to_expired_at(exptime, now(), epoch_unix());
	}

private:
	static constexpr std::uint32_t kFirstSecond = 2;

	struct Epoch {
		std::int64_t monotonic;
		std::int64_t unix_time;
	};

	static const Epoch &epoch() noexcept {
		static const Epoch e {monotonic_seconds(), unix_seconds() - kFirstSecond};
		return e;
	}

	static constexpr std::uint32_t saturate(const std::int64_t seconds) noexcept {
		constexpr auto max = static_cast<std::int64_t>(std::numeric_limits<std::uint32_t>::max());
		return static_cast<std::uint32_t>(seconds > max ? max : seconds);
	}

	static std::int64_t monotonic_seconds() noexcept {
#ifdef CLOCK_MONOTONIC_COARSE
		timespec ts {};
		::clock_gettime(CLOCK_MONOTONIC_COARSE, &ts);
		return static_cast<std::int64_t>(ts.tv_sec);
#else
		return std::chrono::duration_cast<std::chrono::seconds>(std::chrono::steady_clock::now().time_since_epoch())
		    .count();
#endif
	}

	static std::int64_t unix_seconds() noexcept {
		return std::chrono::duration_cast<std::chrono::seconds>(std::chrono::system_clock::now().time_since_epoch())
		    .count();
	}
};

} // namespace util
