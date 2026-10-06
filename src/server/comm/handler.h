#pragma once

#include <utility>
#include <boost/asio.hpp>
#include "worker.h"
#include "type.h"

namespace comm {
using asio::awaitable;
using asio::use_awaitable;
using asio::ip::tcp;

class Handler {
public:


private:
	explicit Handler(Worker worker) : worker_(std::move(worker)) {};

	Worker worker_;
	/*
	 * 只有本 shard 的 io_context thread 访问 data_。remote shard 永远只能通过 mailbox 请求。
	 */

};
} // namespace comm
