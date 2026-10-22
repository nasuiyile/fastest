#pragma once

#include <cstdint>
#include <limits>
#include <string_view>

namespace fastest {

// memcached 文本协议的数字解析规则（util.c 的 safe_strtoull / safe_strtol，基于 strtoull / strtol）。
// incr/decr 的 delta、存进 value 里的计数器、cas unique、exptime 都按这套规则解析，边界行为与 memcached 一致：
//   - 跳过前导空白，接受一个 '+' 或 '-'，之后至少一位十进制数字；
//   - 数字之后只能是空白、NUL 或输入结尾（memcached 的 value 后面总跟着 \r\n，等同于空白）；
//   - 超出 64 位范围（ERANGE）失败。
// 输入不要求以 NUL 结尾，只在栈上计算，不分配内存。
namespace detail {

[[nodiscard]] constexpr bool is_c_space(const char ch) noexcept {
	return ch == ' ' || ch == '\t' || ch == '\n' || ch == '\v' || ch == '\f' || ch == '\r';
}

struct ParsedMagnitude {
	std::uint64_t magnitude = 0;
	bool negative = false;
	bool ok = false;
};

// signed_limit：按 strtol 的 int64 范围检查溢出，否则按 strtoull 的 uint64 范围；溢出即 ERANGE。
[[nodiscard]] constexpr ParsedMagnitude parse_magnitude(const std::string_view text, const bool signed_limit) noexcept {
	ParsedMagnitude result;
	std::size_t i = 0;
	while (i < text.size() && is_c_space(text[i])) {
		++i;
	}
	if (i < text.size() && (text[i] == '+' || text[i] == '-')) {
		result.negative = text[i] == '-';
		++i;
	}
	// strtol 的范围是 [INT64_MIN, INT64_MAX]，strtoull 的范围是 [0, UINT64_MAX]（负号在之后取反）。
	const std::uint64_t limit = !signed_limit     ? std::numeric_limits<std::uint64_t>::max()
	                            : result.negative ? std::uint64_t {1} << 63
	                                              : (std::uint64_t {1} << 63) - 1;
	const std::size_t digits = i;
	std::uint64_t value = 0;
	for (; i < text.size() && text[i] >= '0' && text[i] <= '9'; ++i) {
		const auto digit = static_cast<std::uint64_t>(text[i] - '0');
		if (value > (limit - digit) / 10) {
			return result; // ERANGE
		}
		value = value * 10 + digit;
	}
	if (i == digits) {
		return result; // 没有数字：endptr == str
	}
	if (i < text.size() && !is_c_space(text[i]) && text[i] != '\0') {
		return result;
	}
	result.magnitude = value;
	result.ok = true;
	return result;
}

} // namespace detail

// safe_strtoull：负号按 strtoull 取反，结果作为有符号数为负时才拒绝（所以 "-0" 合法，"-1" 非法）。
[[nodiscard]] constexpr bool parse_memcached_u64(const std::string_view text, std::uint64_t &out) noexcept {
	const detail::ParsedMagnitude parsed = detail::parse_magnitude(text, false);
	if (!parsed.ok) {
		return false;
	}
	const std::uint64_t value = parsed.negative ? std::uint64_t {0} - parsed.magnitude : parsed.magnitude;
	if (parsed.negative && value > static_cast<std::uint64_t>(std::numeric_limits<std::int64_t>::max())) {
		return false;
	}
	out = value;
	return true;
}

// safe_strtol 在 64 位 long 上解析，再截断成 int32（memcached 的 exptime 是 int32_t）：
// 例如 4294967295 截断成 -1，立即过期；超出 64 位范围才失败。
[[nodiscard]] constexpr bool parse_memcached_i32(const std::string_view text, std::int32_t &out) noexcept {
	const detail::ParsedMagnitude parsed = detail::parse_magnitude(text, true);
	if (!parsed.ok) {
		return false;
	}
	const std::uint64_t bits = parsed.negative ? std::uint64_t {0} - parsed.magnitude : parsed.magnitude;
	out = static_cast<std::int32_t>(static_cast<std::uint32_t>(bits));
	return true;
}

} // namespace fastest
