#pragma once

#include <boost/asio.hpp>
#include <cstring>
#include <vector>

#include "ring_array.h"
#include "shard.h"
#include "type.h"
#include "../../protocol/memcached_parser.h"
#include "../../cache/shard_cache.h"

namespace fastest {
namespace asio = boost::asio;

using asio::awaitable;
using asio::use_awaitable;
using asio::ip::tcp;

struct TCPSession {
	tcp::socket socket;
	RingArray ring_array;
	uint32_t seq{0};
	bool closing{false}; // 已决定关闭：不再发送，也不再执行新命令
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
					handle_request_message(message);
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
					handle_response_message(message);
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
			if (shard_comm_.is_current_shard(i)) {
				// 如果是当前分片，那么就直接处理
				data_.handle_multi_command(multi_key_command);
				multi_key_index_[i].reset();
			}
			MultiKeyRequest request = {.count = counter, .command = multi_key_command};
			RequestMessage request_message = {.tcp_id = tcp_id, .seq = seq,
			                                  .shard_id = static_cast<uint16_t>(shard_comm_.current_shard_id()),
			                                  .data = RequestData{request}};
			shard_comm_.send_request_to_shard(i, std::make_unique<RequestMessage>(request_message));
			multi_key_index_[i].reset();
		}
	}

	void send_single_command(SingleKeyCommand &command, const uint64_t tcp_id) {
		SingleKeyRequest request = {.command = command};
		const std::size_t shard = shard_comm_.get_shard(command.item_key().hash);
		auto seq = tcp_session_[tcp_id]->seq++;
		if (shard_comm_.is_current_shard(shard)) {
			SingleKeyResponse handle_single_command = data_.handle_single_command(command);
			ResponseMessage response = {.tcp_id = tcp_id, .seq = seq,
			                            .shard_id = static_cast<uint16_t>(shard_comm_.current_shard_id()),
			                            .data = ResponseData{handle_single_command}};
			handle_response_message(std::make_unique<ResponseMessage>(response));
			return;
		}
		RequestMessage request_message = {.tcp_id = tcp_id, .seq = seq,
		                                  .shard_id = static_cast<uint16_t>(shard_comm_.current_shard_id()),
		                                  .data = RequestData{request}};
		shard_comm_.send_request_to_shard(shard, std::make_unique<RequestMessage>(request_message));
	}

private:
	void handle_request_message(const std::unique_ptr<RequestMessage> &message) {
		auto &cmd = message->data;
		ResponseData res = std::visit(
			[&, this](const auto &c) -> ResponseData {
				using T = std::decay_t<decltype(c)>;
				if constexpr (std::is_same_v<T, SingleKeyRequest>) {
					return data_.handle_single_command(c.command);
				} else if constexpr (std::is_same_v<T, MultiKeyRequest>) {
					return ShardResponse{.count = c.count,
					                     .command = data_.handle_multi_command(c.command)};
				}
			},
			cmd);
		// 响应发回请求的发送方；响应里的 shard_id 填自己（响应的发送方）
		ResponseMessage response_message = {.tcp_id = message->tcp_id, .seq = message->seq,
		                                    .shard_id = static_cast<uint16_t>(shard_comm_.current_shard_id()),
		                                    .data = std::move(res)};
		shard_comm_.send_response_shard(message->shard_id,
		                                std::make_unique<ResponseMessage>(std::move(response_message)));
	}

	// ---- Worker private ----
	void handle_response_message(const std::unique_ptr<ResponseMessage> &message) {
		auto it = tcp_session_.find(message->tcp_id);
		if (it == tcp_session_.end() || !it->second)
			return;
		auto &session = *it->second;
		if (session.closing)
			return; // 丢弃迟到响应

		session.ring_array.append(std::move(*message)); // 顺便省一次拷贝
		while (!session.closing) {
			auto popped = session.ring_array.try_pop();
			if (!popped)
				break;
			if (!send_response(session, popped->data)) {
				close_session(session);
			}
		}
	}

	// 返回 false 表示应关闭连接
	bool send_response(TCPSession &session, const ResponseData &data) {
		auto &socket = session.socket;
		try {
			const SendResult r = std::visit(
				[&socket](const auto &value) -> SendResult {
					using T = std::remove_cvref_t<decltype(value)>;
					if constexpr (std::is_same_v<T, ShardResponse>) {
						return value.command.send_to(socket);
					} else {
						return value.send_to(socket);
					}
				},
				data);
			return !r.close_connection; // Quit / 致命错误 → 关闭
		} catch (const boost::system::system_error &e) {
			const auto ec = e.code();
			if (ec != asio::error::broken_pipe && ec != asio::error::connection_reset &&
			    ec != asio::error::eof && ec != asio::error::bad_descriptor) {
				spdlog::warn("[shard {}] send failed: {}", current_shard_id(), e.what());
			}
			return false;
		} catch (const std::logic_error &e) {
			// invalid_argument 是它的子类
			// 校验阶段抛出，尚未写出任何字节：补一条 SERVER_ERROR 占住这个 seq
			spdlog::error("[shard {}] encode failed: {}", current_shard_id(), e.what());
			return send_server_error(session);
		} catch (...) {
			return false; // 不确定写了多少，只能断开
		}
	}

	bool send_server_error(TCPSession &session) {
		try {
			const ErrorResponse err{.kind = ResponseErrorKind::ServerError,
			                        .message = "response encoding failed"};
			return !err.send_to(session.socket).close_connection;
		} catch (...) {
			return false;
		}
	}

	static void close_session(TCPSession &session) {
		session.closing = true;
		boost::system::error_code ec;
		session.socket.shutdown(tcp::socket::shutdown_both, ec);
		session.socket.close(ec); // 唤醒 session() 中的 async_read_some，由它负责 erase
	}

	ShardCache data_;
	// 长度和CPU核心数一样
	std::vector<std::optional<std::vector<ItemKey> > > multi_key_index_;
	std::shared_ptr<std::atomic_bool> stop_;
	Sc shard_comm_;
};
} // namespace comm