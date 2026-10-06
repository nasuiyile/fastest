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

	// 返回值表示：调用结束后，next_id_ 对应的响应是否已经就绪。
	// [[nodiscard]]
	bool append(Slot slot) {
		const uint32_t seq = slot.seq;
		const uint32_t distance = static_cast<uint32_t>(seq - next_id_);
		// 丢弃旧响应或半圈歧义响应，返回当前队首的就绪状态。
		if (distance >= kHalfRange) {
			return front() != nullptr;
		}
		ensure_capacity(seq);
		auto& value = ring_[seq % ring_.size()];
		if (value) {
			if (value->seq == seq) {
				// 重复响应：保留首次写入的数据。
				return front() != nullptr;
			}
			throw std::logic_error("RingArray slot collision");
		}
		value.emplace(std::move(slot));
		return front() != nullptr;
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

	// 当前需要按序发送的响应是否已经就绪。
	// 只检查 next_id_ 对应的元素，不跳过缺失的响应。
	[[nodiscard]]
	bool is_ready() const noexcept {
		const auto &value = ring_[next_id_ % ring_.size()];
		return value.has_value() && value->seq == next_id_;
	}

	// 尝试取出并删除 next_id_ 对应的元素。
	// 成功：返回元素，清空原槽位，并推进 next_id_。
	// 失败：返回 std::nullopt，不改变状态。
	[[nodiscard]]
	std::optional<Slot> try_pop() noexcept {
		auto &value = ring_[next_id_ % ring_.size()];

		if (!value || value->seq != next_id_) {
			return std::nullopt;
		}

		// 先将元素移动到返回值中。
		std::optional<Slot> result(
			std::in_place,
			std::move(*value)
		);

		// 移动后直接清空已定位的槽位，不再检查原元素的 seq。
		value.reset();
		++next_id_;

		return result;
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