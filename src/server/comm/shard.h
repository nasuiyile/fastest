#pragma once
#include "envelope.h"
#include "mailbox.h"
#include <vector>
#include <memory>
#include <boost/asio.hpp>

#include "../../util/hash.h"

namespace fastest {
namespace asio = boost::asio;
using asio::awaitable;

// 一个分片的收件箱：一个 MPSC 收件队列加一个门铃。别人发来的请求信封和自己发出去、带着响应回来的信封都进这一个队列，
// 用 Envelope::returned() 区分。
struct ShardInbox {
	Doorbell doorbell;
	MpscQueue<Envelope> mailbox;
};

class SharedSystem {
public:
	using Inbox = ShardInbox;

	explicit SharedSystem(std::size_t n) {
		inboxes_.reserve(n);
		for (std::size_t i = 0; i < n; ++i) {
			inboxes_.push_back(std::make_unique<Inbox>());
		}
	}

	[[nodiscard]] std::size_t size() const noexcept {
		return inboxes_.size();
	}

	Inbox &inbox(std::size_t id) noexcept {
		return *inboxes_[id];
	}

	SharedSystem(const SharedSystem &) = delete;

	SharedSystem &operator=(const SharedSystem &) = delete;

private:
	std::vector<std::unique_ptr<Inbox>> inboxes_ {};
};

class ShardComm {
public:
	using System = SharedSystem;
	using Inbox = System::Inbox;

	static std::vector<ShardComm> create_shard_comm(const std::size_t num) {
		std::vector<ShardComm> vec;
		auto system = std::make_shared<System>(num);
		for (std::size_t i = 0; i < num; ++i) {
			vec.push_back(ShardComm(i, num, system));
		}
		return vec;
	}

	[[nodiscard]] inline Inbox &current_inbox() const {
		return system_->inbox(shard_id_);
	}

	[[nodiscard]] inline Doorbell &doorbell(const std::size_t shard_id) const {
		return system_->inbox(shard_id).doorbell;
	}

	[[nodiscard]] std::size_t current_shard_id() const {
		return shard_id_;
	}
	[[nodiscard]] std::size_t shard_num() const {
		return shard_num_;
	}

	[[nodiscard]] bool is_current_shard(const std::size_t shard_id) const {
		return shard_id == shard_id_;
	}

	// 只入队，不敲门；调用方（Worker）负责之后敲目标分片的门铃。信封交给目标分片的队列。
	void push(const std::size_t shard_id, Envelope *envelope) const noexcept {
		system_->inbox(shard_id).mailbox.push(envelope);
	}

	// 不急着处理的信封（只带归还记录）：入队，只在别的消息可能卡在它后面时才敲门（见 MpscQueue::push_quiet）。
	// 返回是否写了 eventfd。
	bool push_quiet(const std::size_t shard_id, Envelope *envelope) const noexcept {
		Inbox &inbox = system_->inbox(shard_id);
		if (!inbox.mailbox.push_quiet(envelope)) {
			return false;
		}
		return inbox.doorbell.knock(); // push_quiet 已经做过 fence
	}

	[[nodiscard]] inline std::size_t get_shard(const uint64_t hash) const {
		return util::get_shard(hash, shard_num_);
	}

	ShardComm(const ShardComm &) = delete;

	ShardComm &operator=(const ShardComm &) = delete;

	ShardComm(ShardComm &&) noexcept = default;

	ShardComm &operator=(ShardComm &&) noexcept = default;

private:
	ShardComm(const std::size_t shard_id, const std::size_t shard_num, std::shared_ptr<System> system)
	    : shard_id_(shard_id), shard_num_(shard_num), system_(std::move(system)) {
	}

	std::size_t shard_id_ = 0;
	// 分片总数
	std::size_t shard_num_ = 0;
	std::shared_ptr<System> system_;
};
} // namespace fastest
