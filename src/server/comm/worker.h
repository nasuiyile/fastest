#pragma once

#include <boost/asio.hpp>
#include <cstring>
#include <vector>

#include "ring_array.h"
#include "shard.h"
#include "type.h"
#include "../../protocol/memcached_parser.h"

namespace comm {
namespace asio = boost::asio;

using asio::awaitable;
using asio::use_awaitable;
using asio::ip::tcp;

struct TCPSession {
	tcp::socket socket;
	RingArray ring_array;
	uint32_t seq{0};

	explicit TCPSession(tcp::socket &&s) : socket(std::move(s)) {
	}
};

inline MultiKeyCommand rebuild(const MultiKeyCommand &command, std::vector<ItemKey> keys) {
	return std::visit(
		[&keys](const auto &value) -> MultiKeyCommand {
			using T = std::remove_cvref_t<decltype(value)>;
			if constexpr (std::is_same_v<T, MultiGetCommand>) {
				return MultiKeyCommand{MultiGetCommand{std::move(keys)}};
			} else if constexpr (std::is_same_v<T, MultiGetsCommand>) {
				return MultiKeyCommand{MultiGetsCommand{std::move(keys)}};
			} else if constexpr (std::is_same_v<T, MultiGatCommand>) {
				return MultiKeyCommand{MultiGatCommand{value.exptime, std::move(keys)}};
			} else if constexpr (std::is_same_v<T, MultiGatsCommand>) {
				return MultiKeyCommand{MultiGatsCommand{value.exptime, std::move(keys)}};
			}
		},
		command.command);
}

class Worker {
public:
	std::unordered_map<uint64_t, std::unique_ptr<TCPSession> > tcp_session_;
	using Sc = ShardComm<RequestMessage, ResponseMessage>;

	explicit Worker(Sc shard_comm, std::shared_ptr<std::atomic_bool> stop)
		: stop_(std::move(stop)), shard_comm_(std::move(shard_comm)) {
	}

	static std::vector<Worker> create_workers(const std::size_t core_num,
	                                          const std::shared_ptr<std::atomic_bool> &stop) {
		std::vector<Sc> shard_comm = Sc::create_shard_comm(core_num);
		// 所有 Worker 共享同一个 stop
		std::vector<Worker> workers;
		workers.reserve(core_num);
		for (Sc &comm : shard_comm) {
			workers.emplace_back(std::move(comm), stop);
		}
		return workers;
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
					handle_request_message(std::move(message));
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

	[[nodiscard]] std::size_t current_shard_id() const {
		return shard_comm_.current_shard_id();
	}


	void send_multi_command(MultiKeyCommand &command, const uint64_t tcp_id) {
		const std::vector<ItemKey> &item_keys = command.item_keys();
		uint16_t counter = 0;
		for (size_t i = 0; i < item_keys.size(); i++) {
			std::size_t shard = shard_comm_.get_shard(item_keys[i].hash);
			if (!multi_key_index_[shard].has_value()) {
				counter++;
				multi_key_index_[shard] = std::vector<ItemKey>{item_keys[i]};
			} else {
				multi_key_index_[shard].value().emplace_back(item_keys[i]);
			}
		}
		const uint32_t seq = tcp_session_[tcp_id]->seq++;

		for (size_t i = 0; i < multi_key_index_.size(); i++) {
			if (!multi_key_index_[i].has_value()) {
				continue;
			}
			const auto &key_views = multi_key_index_[i].value();
			MultiKeyCommand multi_key_command = rebuild(command, key_views);

			MultiKeyRequest request = {.count = counter, .command = multi_key_command};
			RequestMessage request_message = {.tcp_id = tcp_id, .seq = seq, .data = RequestData{request}};
			shard_comm_.send_request_to_shard(i, std::make_unique<RequestMessage>(request_message));
			multi_key_index_[i].reset();
		}
	}

	void send_single_command(SingleKeyCommand &command, const uint64_t tcp_id) {
		SingleKeyRequest request = {.command = command};
		const std::size_t shard = shard_comm_.get_shard(command.item_key().hash);
		RequestMessage request_message = {
			.tcp_id = tcp_id, .seq = tcp_session_[tcp_id]->seq++, .data = RequestData{request}};
		shard_comm_.send_request_to_shard(shard, std::make_unique<RequestMessage>(request_message));
	}

private:
	[[nodiscard]] bool is_current_shard(const std::size_t hash) const {
		return shard_comm_.is_current_shard(hash);
	}

	SingleKeyResponse handle_single_get_command(const SingleGetCommand &command) {
		if (auto it = data_.find(command.key); it != data_.end()) {
			Item val = it->second;
			SingleGetResponse{.item = ValueItem{
				.key = command.key.key,
				.flags = val.flags,
				.value = val.value
			}};
		} else {
			SingleGetResponse{.item = };
		}
	}

	MultiKeyResponse handle_multi_command(const MultiKeyCommand &command) {
		return std::visit(
			[&command](const auto &value) -> MultiKeyResponse {
				using T = std::remove_cvref_t<decltype(value)>;
				if constexpr (std::is_same_v<T, SetCommand>) {
				} else if constexpr (std::is_same_v<T, MultiGetCommand>) {
				} else if constexpr (std::is_same_v<T, MultiGetsCommand>) {
				} else if constexpr (std::is_same_v<T, MultiGatCommand>) {
				} else if constexpr (std::is_same_v<T, MultiGatsCommand>) {
				}
			},
			command.command);
	}

	SingleKeyResponse handle_single_command(const SingleKeyCommand &command) {
		return std::visit(
			[&command](const auto &value) -> SingleKeyResponse {
				using T = std::remove_cvref_t<decltype(value)>;
				if constexpr (std::is_same_v<T, SetCommand>) {
				} else if constexpr (std::is_same_v<T, AddCommand>) {
				} else if constexpr (std::is_same_v<T, ReplaceCommand>) {
				} else if constexpr (std::is_same_v<T, AppendCommand>) {
				} else if constexpr (std::is_same_v<T, PrependCommand>) {
				} else if constexpr (std::is_same_v<T, CasCommand>) {
				} else if constexpr (std::is_same_v<T, SingleGetCommand>) {
				} else if constexpr (std::is_same_v<T, SingleGetsCommand>) {
				} else if constexpr (std::is_same_v<T, SingleGatCommand>) {
				} else if constexpr (std::is_same_v<T, SingleGatsCommand>) {
				} else if constexpr (std::is_same_v<T, DeleteCommand>) {
				} else if constexpr (std::is_same_v<T, TouchCommand>) {
				} else if constexpr (std::is_same_v<T, IncrCommand>) {
				} else if constexpr (std::is_same_v<T, DecrCommand>) {
				}
			},
			command.command);
	}

	void set(const ItemKey &key, const Item &item) {
		data_[key] = item;
	}


	void handle_request_message(std::unique_ptr<RequestMessage> message) {
		auto &cmd = message->data;
		std::visit(
			[&, this](const auto &c) -> awaitable<void> {
				using T = std::decay_t<decltype(c)>;
				if constexpr (std::is_same_v<T, SingleKeyRequest>) {
					handle_single_command(c.command);
				} else if constexpr (std::is_same_v<T, MultiKeyRequest>)
					handle_multi_command(c.command);
			},
			cmd);
	}

	void handle_response_message(const std::unique_ptr<ResponseMessage> &message) {
		// 写到环形数组中。如果满足回复client的条件，获得对应的TCP socket，写入TCP socket。
		auto ring_array = tcp_session_[message->tcp_num]->ring_array;
		ring_array.append(*message);
		while (ring_array.is_ready()) {
			std::optional<RingResponseMessage> response_message = ring_array.try_pop();
			if (!response_message.has_value()) {
				return;
			}

			RingResponseMessage &msg = *response_message;
			// 真正写入TCP中
		}
	}

	std::unordered_map<ItemKey, Item, KeyHash, KeyEqual> data_;
	// 长度和CPU核心数一样
	std::vector<std::optional<std::vector<ItemKey> > > multi_key_index_;
	std::shared_ptr<std::atomic_bool> stop_;
	Sc shard_comm_;
};
} // namespace comm