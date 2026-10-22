#pragma once

#include <bit>
#include <cassert>
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
// 按序号排列的响应环，同时充当写路径的在途缓冲。序号允许回绕，区间都按无符号差计算：
//   [head_, next_id_)  已编码进在途写：GatherSink 引用着槽位里的数据，写完成前不能修改或释放；
//   [next_id_, ...)    等待发送：可能还没到达、多分片还没收齐，或者已经就绪。
// 发送方用 front() + advance() 原地编码，不把响应搬出槽位；写完成后 release_sent() 一次性释放。
//
// 扩容会移动在途槽位。这是安全的，因为 GatherSink 只引用超过 kInlineBytes 的段：
// value 的 payload 在 CacheValuePtr 指向的独立堆对象里，地址不随槽位移动；key 不超过 250B，总是拷进 arena；
// ErrorResponse::message 是 string_view，指向静态字符串。以后给响应加字段时要保持这一点。
class RingArray {
	using MultiResponses = std::vector<ShardResponse>;
	using Slot = std::optional<RingResponseMessage>;

	static_assert(std::is_nothrow_move_constructible_v<ResponseMessage>,
	              "ResponseMessage must be nothrow move-constructible");
	static_assert(std::is_nothrow_move_constructible_v<RingResponseMessage>,
	              "RingResponseMessage must be nothrow move-constructible");

public:
	explicit RingArray(const std::size_t initial_capacity = 128, const std::uint32_t initial_seq = 0)
	    : head_(initial_seq), next_id_(initial_seq) {
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
			return; // 丢弃已发送（含写在途）的迟到响应和半圈歧义响应。
		}

		auto *incoming = std::get_if<ShardResponse>(&message.data);
		if (incoming && incoming->count == 0) {
			throw std::invalid_argument("ShardResponse count must be positive");
		}

		// 容量要从 head_ 算起：在途槽位还占着位置。
		ensure_capacity(static_cast<std::uint32_t>(message.seq - head_));
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

	// get() 可以查看在途槽位和尚未收齐的槽位；不得修改 seq、count、响应类型或聚合容器。
	[[nodiscard]] RingResponseMessage *get(std::uint32_t index) noexcept {
		return const_cast<RingResponseMessage *>(std::as_const(*this).get(index));
	}

	[[nodiscard]] const RingResponseMessage *get(std::uint32_t index) const noexcept {
		const auto distance = static_cast<std::uint32_t>(index - head_);
		if (distance >= kHalfRange || distance >= ring_.size()) {
			return nullptr;
		}
		const auto &slot = ring_[position(index)];
		return slot && slot->seq == index ? &*slot : nullptr;
	}

	// front() 返回下一条待发送且已就绪的响应；未到达或多 key 未收齐时返回 nullptr。
	// 多分片响应不合并：data 仍是 vector<ShardResponse>，由编码端按到达顺序依次输出。
	// 扩容、release_sent() 会使指针失效；不要跨可能修改 RingArray 的 co_await 持有指针。
	[[nodiscard]] const RingResponseMessage *front() const noexcept {
		const auto *message = get(next_id_);
		return message && ready(*message) ? message : nullptr;
	}

	// 把 front() 标记为已交给写操作：只移动发送游标，槽位保留到 release_sent()。
	void advance() noexcept {
		assert(front() != nullptr);
		++next_id_;
	}

	// 写完成（或放弃这次写）后释放 [head_, next_id_)。
	void release_sent() noexcept {
		for (; head_ != next_id_; ++head_) {
			ring_[position(head_)].reset();
		}
	}

	// 下一条要发送的序号；它之前的响应都已交给写操作（可能还在途）。
	[[nodiscard]] std::uint32_t next_id() const noexcept {
		return next_id_;
	}

	// 已交给写操作、还没释放的响应数。
	[[nodiscard]] std::uint32_t sent_count() const noexcept {
		return next_id_ - head_;
	}

	[[nodiscard]] std::size_t capacity() const noexcept {
		return ring_.size();
	}

	[[nodiscard]] bool is_ready() const noexcept {
		return front() != nullptr;
	}

private:
	[[nodiscard]] static bool ready(const RingResponseMessage &message) noexcept {
		const auto *responses = std::get_if<MultiResponses>(&message.data);
		return !responses || (!responses->empty() && responses->size() == responses->front().count);
	}

	[[nodiscard]] std::size_t position(std::uint32_t seq) const noexcept {
		return static_cast<std::size_t>(seq) & (ring_.size() - 1);
	}

	// distance 从 head_ 算起。占用的槽位都落在 [head_, head_ + 旧容量) 内，按新掩码重排不会冲突。
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
	std::uint32_t head_;    // 最老的未释放序号
	std::uint32_t next_id_; // 下一条要发送的序号
	std::vector<Slot> ring_;
};
} // namespace fastest