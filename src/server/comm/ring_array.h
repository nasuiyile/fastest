#pragma once

#include <bit>
#include <cstddef>
#include <cstdint>
#include <optional>
#include <stdexcept>
#include <type_traits>
#include <utility>
#include <variant>
#include <vector>

#include "type.h"

namespace fastest {
class RingArray {
	using MultiResponses = std::vector<ShardResponse>;
	using Slot = std::optional<RingResponseMessage>;

	static_assert(std::is_nothrow_move_constructible_v<ResponseMessage>,
	              "ResponseMessage must be nothrow move-constructible");
	static_assert(std::is_nothrow_move_constructible_v<RingResponseMessage>,
	              "RingResponseMessage must be nothrow move-constructible");

public:
	explicit RingArray(const std::size_t initial_capacity = 128, const std::uint32_t initial_seq = 0)
	    : next_id_(initial_seq) {
		if (initial_capacity == 0 || initial_capacity > kHalfRange) {
			throw std::invalid_argument("RingArray capacity must be in [1, 2^31]");
		}
		ring_.resize(std::bit_ceil(initial_capacity));
	}

	// 按值接收；调用方使用 append(std::move(message))，避免复制响应内容。
	// count 是预期的分片回复总数；每个分片必须且只能回复一次，未命中也要回复。
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
					    responses.reserve(response.count);
					    responses.emplace_back(std::move(response));
					    slot.emplace(message.tcp_id, message.seq, std::move(responses));
				    } else {
					    slot.emplace(message.tcp_id, message.seq, std::move(response));
				    }
			    },
			    message.data);
			return;
		}

		if (slot->seq != message.seq) {
			throw std::logic_error("RingArray slot collision");
		}
		if (slot->tcp_id != message.tcp_id) {
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
			if (incoming->command.index() != responses->front().command.index()) {
				throw std::logic_error("Inconsistent multi-key response type");
			}
			responses->emplace_back(std::move(*incoming));
			return;
		}

		// 错误作为整个请求的终态；丢弃部分结果，忽略后续迟到回复。
		if (auto *error = std::get_if<ErrorResponse>(&message.data)) {
			slot->data.emplace<ErrorResponse>(std::move(*error));
			return;
		}
		throw std::logic_error("Single-key response received for an incomplete multi-key request");
	}

	// get() 可以查看尚未收齐的槽位；不得修改 seq、count、响应类型或聚合容器。
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

	// 返回 ResponseMessage；多分片数据合并为一个 ShardResponse，count = 1。
	// 缺失或未收齐时返回 nullopt；聚合失败时保留槽位和 next_id_，可重试。
	// 聚合可能分配内存，因此这里不能使用 noexcept。
	[[nodiscard]] std::optional<ResponseMessage> try_pop() {
		auto &slot = ring_[position(next_id_)];
		if (!slot || slot->seq != next_id_ || !ready(*slot)) {
			return std::nullopt;
		}

		std::optional<ResponseMessage> result;
		std::visit(
		    [&](auto &response) {
			    using T = std::remove_cvref_t<decltype(response)>;
			    if constexpr (std::is_same_v<T, MultiResponses>) {
				    result.emplace(slot->tcp_id, slot->seq, 0,
				                   ResponseData(std::in_place_type<ShardResponse>, merge_shards(response)));
			    } else {
				    result.emplace(slot->tcp_id, slot->seq, 0,
				                   ResponseData(std::in_place_type<T>, std::move(response)));
			    }
		    },
		    slot->data);

		slot.reset();
		++next_id_;
		return result;
	}

private:
	[[nodiscard]] static bool ready(const RingResponseMessage &message) noexcept {
		const auto *responses = std::get_if<MultiResponses>(&message.data);
		return !responses || (!responses->empty() && responses->size() == responses->front().count);
	}

	// 复用首个分片的 vector；先检查类型和总长度，再一次性 reserve，最后移动条目。
	// 合并顺序是分片到达顺序，各分片内部的条目顺序保持不变。
	[[nodiscard]] static ShardResponse merge_shards(MultiResponses &responses) {
		return std::visit(
		    [&](auto &first) -> ShardResponse {
			    using T = std::remove_cvref_t<decltype(first)>;
			    using Item = typename std::remove_cvref_t<decltype(first.items)>::value_type;
			    static_assert(std::is_nothrow_move_constructible_v<Item>, "Response items must be nothrow movable");

			    std::size_t total = first.items.size();
			    for (std::size_t i = 1; i < responses.size(); ++i) {
				    const auto *part = std::get_if<T>(&responses[i].command.variant());
				    if (!part) {
					    throw std::logic_error("Inconsistent multi-key response type");
				    }
				    if (part->items.size() > first.items.max_size() - total) {
					    throw std::length_error("Merged response exceeds vector max_size");
				    }
				    total += part->items.size();
			    }

			    // reserve 失败时尚未移动条目，原始分片数据保持完整。
			    first.items.reserve(total);
			    for (std::size_t i = 1; i < responses.size(); ++i) {
				    auto &part = std::get<T>(responses[i].command.variant());
				    for (auto &item : part.items) {
					    first.items.emplace_back(std::move(item));
				    }
			    }

			    return ShardResponse {1, std::move(responses.front().command)};
		    },
		    responses.front().command.variant());
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
} // namespace fastest