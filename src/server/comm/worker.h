#pragma once

#include "mailbox.h"


#include <boost/asio.hpp>
#include <boost/asio/posix/stream_descriptor.hpp>
#include <spdlog/spdlog.h>
#include <sys/socket.h>

#include <algorithm>
#include <array>
#include <atomic>
#include <charconv>
#include <chrono>
#include <cstdint>
#include <cstring>
#include <memory>
#include <string>
#include <string_view>
#include <thread>
#include <unordered_map>
#include <utility>
#include <vector>

#include "shard.h"

namespace comm {
namespace asio = boost::asio;

using asio::awaitable;
using asio::ip::tcp;
using asio::use_awaitable;

// 负责抽象发送数据 和响应回调，每个线程都持有这样一个worker，并且可以通过worker和其他线程的mailbox来进行通信

struct RequestMessage {
};

struct ResponseMessage {
};

class Worker {
public:


private:
	explicit Worker(const ShardComm<RequestMessage, ResponseMessage> &shard_comm)
	: shard_comm_(shard_comm) {
	}
	ShardComm<RequestMessage, ResponseMessage> shard_comm_;
};
}