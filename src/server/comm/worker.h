#pragma once

#include "../../util/hardware_info.h"
#include <boost/asio.hpp>
#include <cstring>

#include <vector>

#include "ring_array.h"
#include "shard.h"
#include "type.h"

namespace comm {
namespace asio = boost::asio;

using asio::awaitable;
using asio::use_awaitable;
using asio::ip::tcp;

class Worker {
public:
	static std::vector<Worker> create_workers() {
		const std::size_t core_num = util::core_num();
		std::vector<Sc> shard_comm = Sc::create_shard_comm(core_num);
		// 所有 Worker 共享同一个 stop
		const auto stop = std::make_shared<std::atomic_bool>(false);
		std::vector<Worker> workers;
		workers.reserve(core_num);
		for (Sc &comm : shard_comm) {
			workers.push_back(Worker(std::move(comm), stop));
		}
		return workers;
	}

	awaitable<bool> set(Key k, const Item &item, const uint32_t tcp_fd) {
		const ItemKey key(std::move(k));
		if (is_current_shard(key.hash)) {
			data_.emplace(key, item);
		} else {
			SetCommand request_data = {.key = key, .value = item};
			RequestData request {.command = SetCommand {std::move(request_data)}};
			co_await send_request_to_shard(RequestData(std::move(request_data)), tcp_fd);
		}
	}

private:
	[[nodiscard]] bool is_current_shard(const std::size_t hash) const {
		return shard_comm_.is_current_shard(hash);
	}
	[[nodiscard]] std::size_t current_shard_id() const {
		return shard_comm_.current_shard_id();
	}

	using Sc = ShardComm<RequestMessage, ResponseMessage>;

	awaitable<void> handle_set(const SetCommand &c, const std::unique_ptr<RequestMessage> &message) {
		data_.emplace(c.key, c.value);
		ResponseMessage response {
		    .tcp_fd = message->tcp_fd, .seq = message->seq, .data = ResponseData {.command = {SetResponse {}}}};
		co_await shard_comm_.send_response_shard(c.key.hash, std::make_unique<ResponseMessage>(response));
	}

	awaitable<void> handle_get(const GetCommand &c, std::unique_ptr<RequestMessage> &) {
		data_[c.key];
		co_return;
	}

	awaitable<bool> handle_request_message(std::unique_ptr<RequestMessage> message) {
		auto &cmd = message->data.command;
		co_await std::visit(
		    [&, this](const auto &c) -> awaitable<void> {
			    using T = std::decay_t<decltype(c)>;
			    if constexpr (std::is_same_v<T, SetCommand>)
				    co_await handle_set(c, message);
			    else if constexpr (std::is_same_v<T, GetCommand>)
				    co_await handle_get(c, message);
		    },
		    cmd);
		co_return true;
	}
	void handle_response_message(std::unique_ptr<ResponseMessage> message) {
		// 写到环形数组中。如果满足回复client的条件，获得对应的TCP socket，写入TCP socket。
		ring_array_.append(*message);
		while (ring_array_.is_ready()) {
			std::optional<ResponseMessage> response_message = ring_array_.try_pop();
			if (!response_message.has_value()) {
				return;
			}

			ResponseMessage &msg = *response_message;
			// 真正写入TCP中
		}
	}

	// 接收他人请求
	awaitable<void> mailbox_request_loop() {
		std::size_t shard_id = shard_comm_.current_shard_id();
		const auto executor = co_await asio::this_coro::executor;
		Mailbox<RequestMessage> &mailbox = shard_comm_.current_request_mailbox();
		const auto wait_fd = mailbox.duplicate_event_fd();
		if (wait_fd < 0) {
			spdlog::error("[shard {}] failed to dup mailbox eventfd: {}", shard_id, std::strerror(errno));
			co_return;
		}
		asio::posix::stream_descriptor descriptor(executor, wait_fd);
		while (!stop_->load(std::memory_order_relaxed)) {
			boost::system::error_code ec;
			co_await descriptor.async_wait(asio::posix::stream_descriptor::wait_read,
			                               redirect_error(use_awaitable, ec));
			if (ec) {
				if (ec != asio::error::operation_aborted && !stop_->load(std::memory_order_relaxed)) {
					spdlog::error("[shard {}] mailbox wait failed: {}", shard_id, ec.message());
				}
				break;
			}
			mailbox.clear_notify();
			std::unique_ptr<RequestMessage> message;
			// 一次可以消费掉多个元素
			while (mailbox.pop(message)) {
				try {
					co_await handle_request_message(std::move(message));
				} catch (const std::exception &e) {
					spdlog::error("[shard {}] mailbox handler exception: {}", shard_id, e.what());
				} catch (...) {
					spdlog::error("[shard {}] mailbox handler unknown exception", shard_id);
				}
				message.reset();
			}
		}
	}

	// 接收他人响应
	awaitable<void> mailbox_response_loop() {
		std::size_t shard_id = shard_comm_.current_shard_id();
		const auto executor = co_await asio::this_coro::executor;
		Mailbox<ResponseMessage> &mailbox = shard_comm_.current_response_mailbox();
		const auto wait_fd = mailbox.duplicate_event_fd();
		if (wait_fd < 0) {
			spdlog::error("[shard {}] failed to dup mailbox eventfd: {}", shard_id, std::strerror(errno));
			co_return;
		}
		asio::posix::stream_descriptor descriptor(executor, wait_fd);
		while (!stop_->load(std::memory_order_relaxed)) {
			boost::system::error_code ec;
			co_await descriptor.async_wait(asio::posix::stream_descriptor::wait_read,
			                               redirect_error(use_awaitable, ec));
			if (ec) {
				if (ec != asio::error::operation_aborted && !stop_->load(std::memory_order_relaxed)) {
					spdlog::error("[shard {}] mailbox wait failed: {}", shard_id, ec.message());
				}
				break;
			}
			mailbox.clear_notify();
			std::unique_ptr<ResponseMessage> message;
			// 一次可以消费掉多个元素
			while (mailbox.pop(message)) {
				try {
					handle_response_message(std::move(message));
				} catch (const std::exception &e) {
					spdlog::error("[shard {}] mailbox handler exception: {}", shard_id, e.what());
				} catch (...) {
					spdlog::error("[shard {}] mailbox handler unknown exception", shard_id);
				}
				message.reset();
			}
		}
	}
	awaitable<bool> send_request_to_shard(const RequestData &request_data, const uint32_t tcp_fd) {
		const uint64_t hash = request_data.command.hash();
		RequestMessage request_message = {.tcp_fd = tcp_fd,
		                                  .seq = seq_++,
		                                  .origin_id = static_cast<uint16_t>(shard_comm_.current_shard_id()),
		                                  .data = (request_data)};
		return shard_comm_.send_request_to_shard(hash, std::make_unique<RequestMessage>(std::move(request_message)));
	}

	explicit Worker(Sc shard_comm, std::shared_ptr<std::atomic_bool> stop)
	    : stop_(std::move(stop)), shard_comm_(std::move(shard_comm)) {
	}
	std::unordered_map<ItemKey, Item, KeyHash, KeyEqual> data_;
	uint32_t seq_ {0};
	RingArray<ResponseMessage> ring_array_;
	std::shared_ptr<std::atomic_bool> stop_;
	Sc shard_comm_;
};
} // namespace comm