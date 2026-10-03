#pragma once
#include "mailbox.h"
#include <vector>
#include <memory>
#include <string_view>
#include <functional>

#include "../../util/hardware_info.h"
#include "../../util/hash.h"

namespace comm {
template <typename Message>
class SharedSystem {
public:
	explicit SharedSystem(std::size_t n) {
		mailboxes_.reserve(n);
		for (std::size_t i = 0; i < n; ++i) {
			mailboxes_.push_back(std::make_unique<Mailbox<Message> >());
		}
	}

	[[nodiscard]] std::size_t size() const noexcept {
		return mailboxes_.size();
	}

	[[nodiscard]] std::size_t shard_of(uint64_t hash) const noexcept {
		return hash % mailboxes_.size();
	}

	Mailbox<Message> &mailbox(std::size_t id) noexcept {
		return *mailboxes_[id];
	}

private:
	std::vector<std::unique_ptr<Mailbox<Message> > > mailboxes_{};
};

template <typename Request, typename Response>
class ShardComm {
public:
	std::vector<ShardComm> ShardCommList(const std::size_t num) {
		std::vector<ShardComm> vec;
		auto request_mailbox = std::make_shared<SharedSystem<Request> >(num);
		auto response_mailbox = std::make_shared<SharedSystem<Response> >(num);
		for (std::size_t i = 0; i < num; ++i) {
			vec.push_back(ShardComm(i, num, request_mailbox, response_mailbox));
		}
		return vec;
	}

	[[nodiscard]] bool current_shard() const {
		return shard_id_;
	}

	[[nodiscard]] bool is_current_shard(const std::size_t hash) const {
		return get_shard(hash) == shard_id_;
	}

private:
	[[nodiscard]] std::size_t get_shard(const uint64_t hash) const {
		return util::get_shard(hash, shard_num_);
	}

	ShardComm(const std::size_t shard_id, const std::size_t shard_num,
	          std::shared_ptr<SharedSystem<Request> > request_mailbox,
	          std::shared_ptr<SharedSystem<Response> > response_mailbox)
		: shard_id_(shard_id), shard_num_(shard_num),
		  request_mailbox(std::move(request_mailbox)),
		  response_mailbox(std::move(response_mailbox)) {
	}

	std::size_t shard_id_ = 0;
	std::size_t shard_num_ = 0;
	std::shared_ptr<SharedSystem<Request> > request_mailbox;
	std::shared_ptr<SharedSystem<Response> > response_mailbox;
};
}