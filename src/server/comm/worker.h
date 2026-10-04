#pragma once

#include "../../util/hardware_info.h"
#include <boost/asio.hpp>
#include <cstring>

#include <vector>

#include "ring_array.h"
#include "shard.h"

namespace comm {
namespace asio = boost::asio;

using asio::awaitable;
using asio::ip::tcp;
using asio::use_awaitable;


// 负责抽象发送数据 和响应回调，每个线程都持有这样一个worker，并且可以通过worker和其他线程的mailbox来进行通信

struct RequestMessage {
	uint32_t tcp_fd;    //tcp的32位fd编号
	uint32_t seq;       //请求编号，允许回绕
	uint16_t origin_id; //投递线程的编号
};

struct ResponseMessage {
	uint32_t tcp_fd; //tcp的32位fd编号
	uint32_t seq;    //请求编号，允许回绕
};

class Worker {
public:
	using Sc = ShardComm<RequestMessage, ResponseMessage>;

	static std::vector<Worker> create_workers() {
		const std::size_t core_num = util::core_num();
		std::vector<Sc> shard_comm = Sc::create_shard_comm(core_num);
		// 所有 Worker 共享同一个 stop
		auto stop = std::make_shared<std::atomic_bool>(false);
		std::vector<Worker> workers;
		workers.reserve(core_num);
		for (Sc &comm : shard_comm) {
			workers.push_back(Worker(std::move(comm), stop));
		}
		return workers;
	}

	void handle_request_message(std::unique_ptr<RequestMessage> message) {
		//直接处理 然后写入请求者的response_mailbox
	}

	void handle_response_message(std::unique_ptr<ResponseMessage> message) {
		//写到环形数组中。如果满足回复client的条件，获得对应的TCP socket，写入TCP socket。
		bool ready = ring_array_.append(*message);
		if (!read) {
			return;
		}
		std::optional<ResponseMessage> response_message = ring_array_.try_pop_front();

	}

	//接收他人请求
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
			//一次可以消费掉多个元素
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

	//接收他人响应
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
			//一次可以消费掉多个元素
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

private:
	explicit Worker(Sc shard_comm, std::shared_ptr<std::atomic_bool> stop): stop_(std::move(stop)),
	                                                                        shard_comm_(std::move(shard_comm)) {
	}

	RingArray<ResponseMessage> ring_array_;
	std::shared_ptr<std::atomic_bool> stop_;
	Sc shard_comm_;
};
}