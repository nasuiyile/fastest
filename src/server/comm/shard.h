#pragma once
#include "mailbox.h"
#include <vector>
#include <memory>
#include <boost/asio.hpp>

#include "../../util/hash.h"

namespace comm {
namespace asio = boost::asio;
using asio::awaitable;

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


	Mailbox<Message> &mailbox(std::size_t id) noexcept {
		return *mailboxes_[id];
	}


	SharedSystem(const SharedSystem &) = delete;

	SharedSystem &operator=(const SharedSystem &) = delete;

private:
	std::vector<std::unique_ptr<Mailbox<Message> > > mailboxes_{};
	int event_fd_{-1};
};

template <typename Request, typename Response>
class ShardComm {
public:
	static std::vector<ShardComm> create_shard_comm(const std::size_t num) {
		std::vector<ShardComm> vec;
		auto request_mailbox = std::make_shared<SharedSystem<Request> >(num);
		auto response_mailbox = std::make_shared<SharedSystem<Response> >(num);
		for (std::size_t i = 0; i < num; ++i) {
			vec.push_back(ShardComm(i, num, request_mailbox, response_mailbox));
		}
		return vec;
	}

	[[nodiscard]] inline Mailbox<Request> &current_request_mailbox() const {
		return request_mailbox->mailbox(shard_id_);
	}

	[[nodiscard]] inline Mailbox<Response> &current_response_mailbox() const {
		return response_mailbox->mailbox(shard_id_);
	}

	[[nodiscard]] std::size_t current_shard_id() const {
		return shard_id_;
	}

	[[nodiscard]] bool is_current_shard(const std::size_t hash) const {
		return get_shard(hash) == shard_id_;
	}

	awaitable<bool> send_request_to_shard(const uint64_t hash, std::unique_ptr<Request> message) {
		std::size_t shard_id = get_shard(hash);
		Mailbox<Request> &mailbox = request_mailbox->mailbox(shard_id);
		if (mailbox.try_push(message)) {
			co_return true;
		}
		co_return false;
	}

	awaitable<bool> send_response_shard(const uint64_t hash, std::unique_ptr<Response> message) {
		std::size_t shard_id = get_shard(hash);
		Mailbox<Response> &mailbox = response_mailbox->mailbox(shard_id);
		if (mailbox.try_push(message)) {
			co_return true;
		}
		co_return false;
	}


	ShardComm(const ShardComm &) = delete;

	ShardComm &operator=(const ShardComm &) = delete;

	ShardComm(ShardComm &&) noexcept = default;

	ShardComm &operator=(ShardComm &&) noexcept = default;

private:
	[[nodiscard]] inline std::size_t get_shard(const uint64_t hash) const {
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
	//分片总数
	std::size_t shard_num_ = 0;
	std::shared_ptr<SharedSystem<Request> > request_mailbox;
	std::shared_ptr<SharedSystem<Response> > response_mailbox;
};
}