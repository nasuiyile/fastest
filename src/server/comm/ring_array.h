#pragma once

#include <concepts>
#include <cstdint>
#include <optional>
#include <utility>
#include <vector>

namespace comm {
template <typename T>
concept HasSeq = requires(const T &s)
{
	{ s.seq } -> std::convertible_to<uint64_t>;
};

template <HasSeq Slot>
class RingArray {
public:
	explicit RingArray(std::size_t initial_capacity = 32)
		: ring_(initial_capacity) {
	}

	void append(uint64_t index, Slot slot) {
		// 正常情况下不应该收到已经发送过的响应
		if (index < next_id_) {
			return;
		}
		ensure_capacity(index);
		const auto pos = index % ring_.size();
		// 理论上这里不应该覆盖另一个有效请求
		// 如果发生，说明窗口/seq管理有bug
		ring_[pos] = std::move(slot);
	}

	//根据id获取
	[[nodiscard]]
	Slot *get(uint64_t index) {
		if (index < next_id_) {
			return nullptr;
		}
		auto &value = ring_[index % ring_.size()];
		if (!value) {
			return nullptr;
		}
		// 防止不同 seq 因 modulo 落到同一槽位
		if (static_cast<uint64_t>(value->seq) != index) {
			return nullptr;
		}
		return &*value;
	}

	/*
	 * 获取当前可以发送的响应。
	 *
	 * 调用者发送完成之后调用 pop_front()。
	 */
	[[nodiscard]]
	Slot *front() {
		auto &value = ring_[next_id_ % ring_.size()];
		if (!value) {
			return nullptr;
		}
		if (static_cast<uint64_t>(value->seq) != next_id_) {
			return nullptr;
		}
		return &*value;
	}

	void pop_front() {
		auto &value = ring_[next_id_ % ring_.size()];
		value.reset();
		++next_id_;
	}

	[[nodiscard]]
	uint64_t next_id() const noexcept {
		return next_id_;
	}

	[[nodiscard]]
	std::size_t capacity() const noexcept {
		return ring_.size();
	}

private:
	void ensure_capacity(uint64_t index) {
		const uint64_t distance = index - next_id_;
		if (distance < ring_.size()) {
			return;
		}
		std::size_t new_capacity = ring_.size();
		// 不能只扩一次。
		// 例如 capacity=32，但突然收到 seq=1000。
		while (distance >= new_capacity) {
			new_capacity *= 2;
		}
		std::vector<std::optional<Slot> > new_ring(new_capacity);
		for (auto &value : ring_) {
			if (!value) {
				continue;
			}
			const uint64_t seq = static_cast<uint64_t>(value->seq);
			// 注意这里必须 % 新容量
			const auto new_pos = seq % new_capacity;
			new_ring[new_pos] = std::move(value);
		}
		ring_ = std::move(new_ring);
	}

private:
	uint64_t next_id_ = 0;

	std::vector<std::optional<Slot> > ring_;
};
} // namespace fast_server