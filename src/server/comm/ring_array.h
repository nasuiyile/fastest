#pragma once

#include <bit>
#include <concepts>
#include <cstddef>
#include <cstdint>
#include <optional>
#include <stdexcept>
#include <type_traits>
#include <utility>
#include <vector>

namespace comm {
template <typename T>
concept HasSeq = requires(const T &s)
{
	requires std::same_as<std::remove_cvref_t<decltype(s.seq)>, uint32_t>;
};

template <HasSeq Slot>
class RingArray {
	static_assert(std::is_nothrow_move_constructible_v<Slot>, "Slot must be nothrow move-constructible");

public:
	explicit RingArray(std::size_t initial_capacity = 32) {
		if (initial_capacity == 0 || initial_capacity > kHalfRange) {
			throw std::invalid_argument("RingArray capacity must be in [1, 2^31]");
		}
		// 容量向上取 2 的幂，保证 seq % capacity 在回绕时仍然正确。
		ring_.resize(std::bit_ceil(initial_capacity));
	}

	void append(uint32_t index, Slot slot) {
		const uint32_t distance = static_cast<uint32_t>(index - next_id_);
		// 丢弃窗口外响应，包括旧序号和恰好相差半圈的歧义情况。
		if (distance >= kHalfRange) {
			return;
		}
		if (slot.seq != index) {
			throw std::invalid_argument("RingArray index does not match slot.seq");
		}
		ensure_capacity(index);
		auto &value = ring_[index % ring_.size()];
		if (value) {
			if (value->seq == index) {
				return; // 重复响应：保留首次写入的数据。
			}
			throw std::logic_error("RingArray slot collision");
		}
		value.emplace(std::move(slot));
	}

	[[nodiscard]]
	Slot *get(uint32_t index) noexcept {
		const auto distance = static_cast<uint32_t>(index - next_id_);
		if (distance >= kHalfRange || distance >= ring_.size()) {
			return nullptr;
		}
		auto &value = ring_[index % ring_.size()];
		if (!value || value->seq != index) {
			return nullptr;
		}
		return &*value;
	}

	// 扩容会使返回的指针失效，不要跨可能发生扩容的 co_await 持有。
	[[nodiscard]]
	Slot *front() noexcept {
		return get(next_id_);
	}

	void pop_front() noexcept {
		if (!front()) {
			return;
		}
		ring_[next_id_ % ring_.size()].reset();
		++next_id_;
	}

	[[nodiscard]]
	uint32_t next_id() const noexcept {
		return next_id_;
	}

	[[nodiscard]]
	std::size_t capacity() const noexcept {
		return ring_.size();
	}

private:
	void ensure_capacity(uint32_t index) {
		const uint32_t distance = static_cast<uint32_t>(index - next_id_);
		if (distance < ring_.size()) {
			return;
		}
		std::size_t new_capacity = ring_.size();
		// append() 已保证 distance < 2^31，扩容最多到 2^31。
		while (distance >= new_capacity) {
			new_capacity *= 2;
		}
		std::vector<std::optional<Slot> > new_ring(new_capacity);
		for (auto &value : ring_) {
			if (!value) {
				continue;
			}
			const auto new_pos = value->seq % new_capacity;
			new_ring[new_pos].emplace(std::move(*value));
		}
		ring_ = std::move(new_ring);
	}

private:
	static constexpr uint32_t kHalfRange = uint32_t{1} << 31;
	uint32_t next_id_ = 0;
	std::vector<std::optional<Slot> > ring_;
};
} // namespace comm