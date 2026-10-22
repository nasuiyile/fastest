#pragma once
#include "mailbox.h"
#include <vector>
#include <memory>
#include <boost/asio.hpp>

#include "../../util/hash.h"

namespace fastest {
namespace asio = boost::asio;
using asio::awaitable;

// 一个分片的收件箱：收请求、收响应两个 MPSC mailbox，共用一个门铃。
template <typename Request, typename Response>
struct ShardInbox {
	Doorbell doorbell;
	Mailbox<Request> requests;
	Mailbox<Response> responses;
};

template <typename Request, typename Response>
class SharedSystem {
public:
	using Inbox = ShardInbox<Request, Response>;

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

template <typename Request, typename Response>
class ShardComm {
public:
	using System = SharedSystem<Request, Response>;
	using Inbox = typename System::Inbox;

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

	// 只入队，不敲门；调用方（Worker）负责之后敲目标分片的门铃。
	bool push_request(std::size_t shard_id, std::unique_ptr<Request> message) {
		return system_->inbox(shard_id).requests.try_push(message);
	}

	bool push_response(std::size_t shard_id, std::unique_ptr<Response> message) {
		return system_->inbox(shard_id).responses.try_push(message);
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
