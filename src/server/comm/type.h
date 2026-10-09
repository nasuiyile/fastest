#pragma once
#include "../../protocol/memcached_parser.h"
#include "../../protocol/memcached_response.h"

namespace comm {
// Memcached value
using Value = std::string;
using Key = std::string;

struct Item {
	uint32_t flags {0};
	Value value;
};

struct ItemKey {
	uint64_t hash {0};
	Key key;

	explicit ItemKey(Key k) : hash(std::hash<Key> {}(k)), key(std::move(k)) {
	}
};

struct MultiKeyRequest {
	// 需要返回多少个这样的命令才可以
	uint16_t count;
	// 保存每个key的hash 防止二次计算
	std::vector<uint64_t> hashes;
	protocol::MultiKeyCommand command;
};
struct SingleKeyRequest {
	// 保存每个key的hash 防止二次计算
	uint64_t hash;
	protocol::SingleKeyCommand command;
};

struct MultiKeyResponse {
	// 一共需要多少个
	uint16_t count;
	std::vector<uint64_t> hashes;
	protocol::MultiKeyResponse command;
};

// 包含俩种枚举，分别是多key和单key操作
using RequestData = std::variant<SingleKeyRequest, MultiKeyRequest>;
using ResponseData = std::variant<protocol::SingleKeyResponse, MultiKeyResponse>;

struct ResponseMessage {
	uint32_t tcp_fd; // tcp的32位fd编号
	uint32_t seq;    // 请求编号，允许回绕
	ResponseData data;
};
// 负责抽象发送数据 和响应回调，每个线程都持有这样一个worker，并且可以通过worker和其他线程的mailbox来进行通信
struct RequestMessage {
	uint32_t tcp_fd; // tcp的32位fd编号
	uint32_t seq;    // 请求编号，允许回绕
	uint64_t hash;   // 投递的hash值
	RequestData data;
};

// 直接用预先算好的 hash
struct KeyHash {
	std::size_t operator()(const ItemKey &k) const noexcept {
		return static_cast<std::size_t>(k.hash);
	}
};

// 注意：相等判断不能只用 hash，还要比 key，避免 hash 冲突导致误判
struct KeyEqual {
	bool operator()(const ItemKey &a, const ItemKey &b) const noexcept {
		return a.hash == b.hash && a.key == b.key;
	}
};
} // namespace comm