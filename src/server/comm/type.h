#pragma once
#include "../../protocol/memcached_parser.h"
#include "../../protocol/memcached_response.h"

namespace fastest {
struct MultiKeyRequest {
	// 需要返回多少个这样的命令才可以
	uint16_t count;
	MultiKeyCommand command;
};

struct SingleKeyRequest {
	SingleKeyCommand command;
};

// Failed oversized SET has a mutation even though it returns an error.
// Route it through the key's mailbox so an earlier SET cannot resurrect stale data.
struct RejectedSetRequest {
	ItemKey key;
	ErrorResponse response;
};

struct ShardResponse {
	// 一共需要多少个
	uint16_t count;
	MultiKeyResponse command;
};

// 按 key 路由的请求；拒绝过大 SET 的删除操作也必须保持分片内顺序。
using RequestData = std::variant<SingleKeyRequest, MultiKeyRequest, RejectedSetRequest>;
using ResponseData = std::variant<SingleKeyResponse, ErrorResponse, ShardResponse>;

using RingResponseData = std::variant<SingleKeyResponse, ErrorResponse, std::vector<ShardResponse> >;

struct ResponseMessage {
	uint64_t tcp_id; // tcp编号
	uint32_t seq;    // 请求编号，允许回绕
	uint16_t shard_id; // 发送这条响应的分片
	ResponseData data;
};

struct RingResponseMessage {
	uint64_t tcp_id; // tcp编号
	uint32_t seq;    // 请求编号，允许回绕
	RingResponseData data;
};

// 负责抽象发送数据 和响应回调，每个线程都持有这样一个worker，并且可以通过worker和其他线程的mailbox来进行通信
struct RequestMessage {
	uint64_t tcp_id; // tcp的32位fd编号
	uint32_t seq;    // 请求编号，允许回绕
	uint16_t shard_id; // 发送这条请求的分片，响应会发回这里
	RequestData data;
};
} // namespace comm
