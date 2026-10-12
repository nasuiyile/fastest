#pragma once

#include <bit>
#include <cstddef>
#include <optional>
#include <stdexcept>
#include <type_traits>
#include <utility>
#include <variant>
#include <vector>

#include "type.h"

namespace comm {

class RingArray {
	using MultiResponses = std::vector<ShardResponse>;
	using Slot = std::optional<RingResponseMessage>;

	static_assert(std::is_nothrow_move_constructible_v<ResponseMessage>,
	              "ResponseMessage must be nothrow move-constructible");
	static_assert(std::is_nothrow_move_constructible_v<RingResponseMessage>,
	              "RingResponseMessage must be nothrow move-constructible");

public:
	explicit RingArray(std::size_t initial_capacity = 128, std::uint32_t initial_seq = 0) : next_id_(initial_seq) {
		if (initial_capacity == 0 || initial_capacity > kHalfRange) {
			throw std::invalid_argument("RingArray capacity must be in [1, 2^31]");
		}
		ring_.resize(std::bit_ceil(initial_capacity));
	}

	// 按值接收；调用方使用 append(std::move(message))，避免复制响应内容。
	// count 是该请求预期收到的分片回复总数；每个分片必须且只能回复一次，未命中也要回复。
	void append(ResponseMessage message) {
		const auto distance = static_cast<std::uint32_t>(message.seq - next_id_);
		if (distance >= kHalfRange) {
			return; // 丢弃旧响应和半圈歧义响应。
		}

		auto *incoming = std::get_if<ShardResponse>(&message.data);
		if (incoming && incoming->count == 0) {
			throw std::invalid_argument("ShardResponse count must be positive");
		}

		ensure_capacity(distance);
		auto &slot = ring_[position(message.seq)];
		if (!slot) {
			std::visit(
			    [&](auto &response) {
				    using T = std::remove_cvref_t<decltype(response)>;
				    if constexpr (std::is_same_v<T, ShardResponse>) {
					    MultiResponses responses;
					    responses.reserve(response.count); // 只预留空间，size 仍为 0。
					    responses.emplace_back(std::move(response));
					    slot.emplace(message.tcp_num, message.seq, std::move(responses));
				    } else {
					    slot.emplace(message.tcp_num, message.seq, std::move(response));
				    }
			    },
			    message.data);
			return;
		}

		if (slot->seq != message.seq) {
			throw std::logic_error("RingArray slot collision");
		}
		if (slot->tcp_num != message.tcp_num) {
			throw std::logic_error("RingArray TCP identifier mismatch");
		}

		auto *responses = std::get_if<MultiResponses>(&slot->data);
		if (!responses) {
			return; // 单 key 响应或终态错误已到达，保留首次结果。
		}

		const auto expected = responses->front().count;
		if (responses->size() == expected) {
			return; // 已收齐，不再接收额外回复。
		}

		if (incoming) {
			if (incoming->count != expected) {
				throw std::logic_error("Inconsistent ShardResponse count");
			}
			responses->emplace_back(std::move(*incoming)); // 已 reserve，不会触发 vector 扩容。
			return;
		}

		// 错误作为整个请求的终态；丢弃已收集的部分结果，后续迟到回复会被忽略。
		if (auto *error = std::get_if<ErrorResponse>(&message.data)) {
			slot->data.emplace<ErrorResponse>(std::move(*error));
			return;
		}
		throw std::logic_error("Single-key response received for an incomplete multi-key request");
	}

	// get() 可以查看尚未收齐的槽位；不得通过返回的指针修改 seq、count 或聚合容器。
	[[nodiscard]] RingResponseMessage *get(std::uint32_t index) noexcept {
		return const_cast<RingResponseMessage *>(std::as_const(*this).get(index));
	}

	[[nodiscard]] const RingResponseMessage *get(std::uint32_t index) const noexcept {
		const auto distance = static_cast<std::uint32_t>(index - next_id_);
		if (distance >= kHalfRange || distance >= ring_.size()) {
			return nullptr;
		}
		const auto &slot = ring_[position(index)];
		return slot && slot->seq == index ? &*slot : nullptr;
	}

	// front() 只返回已就绪的队首；未收齐的多 key 响应返回 nullptr。
	// 扩容、弹出会使相关指针失效；不要跨可能修改 RingArray 的 co_await 持有指针。
	[[nodiscard]] RingResponseMessage *front() noexcept {
		return const_cast<RingResponseMessage *>(std::as_const(*this).front());
	}

	[[nodiscard]] const RingResponseMessage *front() const noexcept {
		const auto *message = get(next_id_);
		return message && ready(*message) ? message : nullptr;
	}

	[[nodiscard]] std::uint32_t next_id() const noexcept {
		return next_id_;
	}
	[[nodiscard]] std::size_t capacity() const noexcept {
		return ring_.size();
	}
	[[nodiscard]] bool is_ready() const noexcept {
		return front() != nullptr;
	}

	// 只弹出已就绪的队首；缺失或未收齐时，不清空槽位，也不推进序号。
	[[nodiscard]] std::optional<RingResponseMessage> try_pop() noexcept {
		auto &slot = ring_[position(next_id_)];
		if (!slot || slot->seq != next_id_ || !ready(*slot)) {
			return std::nullopt;
		}
		std::optional<RingResponseMessage> result(std::in_place, std::move(*slot));
		slot.reset();
		++next_id_;
		return result;
	}

private:
	[[nodiscard]] static bool ready(const RingResponseMessage &message) noexcept {
		const auto *responses = std::get_if<MultiResponses>(&message.data);
		return !responses || (!responses->empty() && responses->size() == responses->front().count);
	}

	[[nodiscard]] std::size_t position(std::uint32_t seq) const noexcept {
		return static_cast<std::size_t>(seq) & (ring_.size() - 1);
	}

	void ensure_capacity(std::uint32_t distance) {
		if (distance < ring_.size()) {
			return;
		}
		const auto new_capacity = std::bit_ceil(static_cast<std::size_t>(distance) + 1);
		const auto new_mask = new_capacity - 1;
		std::vector<Slot> new_ring(new_capacity);
		for (auto &slot : ring_) {
			if (slot) {
				new_ring[static_cast<std::size_t>(slot->seq) & new_mask].emplace(std::move(*slot));
			}
		}
		ring_.swap(new_ring);
	}

	static constexpr std::uint32_t kHalfRange = std::uint32_t {1} << 31;
	std::uint32_t next_id_;
	std::vector<Slot> ring_;
};

} // namespace comm