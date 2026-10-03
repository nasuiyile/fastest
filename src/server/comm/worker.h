#pragma once

#include "../../util/hardware_info.h"
#include <boost/asio.hpp>
#include <cstring>

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
	using Sc = ShardComm<RequestMessage, ResponseMessage>;

	static std::vector<Worker> create_workers() {
		const std::size_t core_num = util::core_num();
		std::vector<Sc> shard_comm = Sc::create_shard_comm(core_num);
		std::vector<Worker> workers;
		workers.reserve(core_num);
		for (Sc &comm : shard_comm) {
			workers.push_back(Worker(std::move(comm)));
		}
		return workers;
	}

private:
	explicit Worker(ShardComm<RequestMessage, ResponseMessage> shard_comm): shard_comm_(std::move(shard_comm)) {
	}

	Sc shard_comm_;
};
}