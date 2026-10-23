#pragma once

#include <cstddef>
#include <cstdint>
#include <utility>
#include <vector>

#include "envelope.h"
#include "shard.h"

namespace fastest {
/*
 * 门铃抑制：本线程攒下的待敲门铃。发出信封时只记下目标分片（按分片编号去重），
 * 到时候一次 fence、每个分片敲一次；对端醒着时敲门只是一次只读的标志检查，不写 eventfd。
 * 只由本线程访问，不用 atomic。
 */
class PendingDoorbells {
public:
	// 已发出的消息攒到这么多条就先敲一次，不中断消费，只限制对端在睡时听到门铃前的延迟。
	static constexpr uint32_t kFlushEvery = 64;

	explicit PendingDoorbells(const std::size_t shard_num) : marked_(shard_num, 0) {
		shards_.reserve(shard_num);
	}

	// 记下要敲 shard 的门铃。返回 true 表示上次敲门之后已攒够 kFlushEvery 条消息，调用方应该先敲一次。
	[[nodiscard]] bool add(const uint16_t shard, const uint16_t messages) noexcept {
		if (!marked_[shard]) {
			marked_[shard] = 1;
			shards_.push_back(shard); // 预留了 shard_num 个，不会扩容
		}
		messages_ += messages;
		return messages_ >= kFlushEvery;
	}

	// 一次 fence，然后每个记下的分片敲一次。返回真正写了 eventfd 的次数（只给统计用）。
	uint32_t ring(SharedSystem &system) noexcept {
		messages_ = 0;
		if (shards_.empty()) {
			return 0;
		}
		Doorbell::fence();
		uint32_t knocked = 0;
		for (const uint16_t shard : shards_) {
			knocked += system.inbox(shard).doorbell.knock();
			marked_[shard] = 0;
		}
		shards_.clear();
		return knocked;
	}

private:
	std::vector<uint16_t> shards_; // 待敲门的分片
	// 每个分片一个字节的去重标记，敲完清零。用 uint8_t 而不是 vector<bool>，判断时不用做位运算。
	std::vector<uint8_t> marked_;
	uint32_t messages_{0}; // 上次敲门之后又发出了多少条消息
};

// 统计计数：只在测试构建（定义了 FASTEST_TESTING）里存在。生产构建里是空类型，配合 [[no_unique_address]] 不占空间，调用也不产生指令。
struct OutboxStats {
#if defined(FASTEST_TESTING)
	uint64_t doorbell_knocks{0}; // 本线程写 eventfd 的次数
	uint64_t envelopes_sent{0};  // 推进别的分片收件队列的信封数（去程和回程）

	void knocked(const uint32_t n) noexcept {
		doorbell_knocks += n;
	}

	void sent_envelope() noexcept {
		++envelopes_sent;
	}
#else
	void knocked(uint32_t) noexcept {
	}

	void sent_envelope() noexcept {
	}
#endif
};

/*
 * 跨分片发件：批量合并加门铃抑制。每个分片一个，只由本线程访问，不用 atomic。
 *
 * 去程：post() 把请求装进发往目标分片的信封（每个目标分片一个），装满 Envelope::kCapacity 条就先发出去，其余的等 flush()。
 * 回程：send_back() 把执行完的信封原样推回发起分片；发起分片处理完响应后 release() 放回池里。
 * 信封始终由发起分片分配、复用和释放，没有跨线程 free。
 * 发出信封只记下门铃（PendingDoorbells），flush()、ring_doorbells() 或攒够 kFlushEvery 条时才敲。
 * 调用方（Worker::BatchScope）保证批次结束时 flush()，批次之间没有打开的信封，也没有待敲的门铃。
 */
class Outbox {
public:
	// 最多留这么多个空信封复用（约 110KB），多出来的直接释放。
	static constexpr std::size_t kPoolMax = 64;

	explicit Outbox(const ShardComm &comm)
		: system_(&comm.system()), open_(comm.shard_num(), nullptr), bells_(comm.shard_num()),
		  self_(static_cast<uint16_t>(comm.current_shard_id())) {
		open_targets_.reserve(comm.shard_num());
		pool_.reserve(kPoolMax);
	}

	// 批次之间不会有打开的信封；在途的信封留在各分片的收件队列里，由队列析构释放。
	~Outbox() {
		for (Envelope *envelope : open_) {
			delete envelope;
		}
		for (Envelope *envelope : pool_) {
			delete envelope;
		}
	}

	Outbox(const Outbox &) = delete;
	Outbox &operator=(const Outbox &) = delete;

	// 去程：装进发往 shard 的信封；装满就先发出去，其余的等 flush()。
	void post(const std::size_t shard, const uint64_t tcp_id, const uint32_t seq, RequestData &&request) {
		Envelope *&envelope = open_[shard];
		if (envelope == nullptr) {
			// 先记分片再取信封：取信封抛异常时只留下一个空位，flush 时跳过。同一分片装满后再打开会重复记，同样跳过。
			open_targets_.push_back(static_cast<uint16_t>(shard));
			envelope = acquire(static_cast<uint16_t>(shard));
		}
		envelope->append(tcp_id, seq, std::move(request));
		if (envelope->full()) {
			push(shard, std::exchange(envelope, nullptr));
		}
	}

	// 回程：别的分片发来的信封执行完（响应已原地写回），推回发起分片。
	void send_back(Envelope &envelope) noexcept {
		envelope.mark_returned();
		push(envelope.origin(), &envelope);
	}

	// 自己发出去的信封带着响应回来、响应处理完之后调用：清空并放回池里。
	void release(Envelope *envelope) noexcept {
		envelope->clear();
		if (pool_.size() < kPoolMax) {
			pool_.push_back(envelope); // 预留了 kPoolMax 个，不会扩容
		} else {
			delete envelope;
		}
	}

	// 批次结束：把还开着的信封都发出去，然后每个目标分片敲一次门铃。
	void flush() noexcept {
		for (const uint16_t shard : open_targets_) {
			if (Envelope *envelope = std::exchange(open_[shard], nullptr)) {
				push(shard, envelope);
			}
		}
		open_targets_.clear();
		ring_doorbells();
	}

	// 把攒下的门铃敲出去：一次 fence，每个目标分片一次。
	void ring_doorbells() noexcept {
		stats_.knocked(bells_.ring(*system_));
	}

#if defined(FASTEST_TESTING)
	[[nodiscard]] uint64_t doorbell_knocks() const noexcept {
		return stats_.doorbell_knocks;
	}

	[[nodiscard]] uint64_t envelopes_sent() const noexcept {
		return stats_.envelopes_sent;
	}
#endif

private:
	// 推进目标分片的收件队列，记下门铃；攒够 kFlushEvery 条时先敲一次。
	void push(const std::size_t shard, Envelope *envelope) noexcept {
		const uint16_t messages = envelope->size(); // push 之后信封归对端，不能再读
		system_->inbox(shard).mailbox.push(envelope);
		stats_.sent_envelope();
		if (bells_.add(static_cast<uint16_t>(shard), messages)) {
			ring_doorbells();
		}
	}

	Envelope *acquire(const uint16_t target) {
		Envelope *envelope;
		if (pool_.empty()) {
			envelope = new Envelope;
		} else {
			envelope = pool_.back();
			pool_.pop_back();
		}
		envelope->reset(self_, target);
		return envelope;
	}

	SharedSystem *system_;
	// 发往各分片、还没发出的信封（按分片编号索引，没有时为 nullptr）和这一批打开过信封的分片。批次之间总是空的。
	std::vector<Envelope *> open_;
	std::vector<uint16_t> open_targets_;
	// 用完的空信封，下次直接取出，不再分配；最多留 kPoolMax 个。
	std::vector<Envelope *> pool_;
	PendingDoorbells bells_;
	uint16_t self_; // 本分片编号，信封的发起分片

	[[no_unique_address]] OutboxStats stats_;
};
} // namespace fastest
