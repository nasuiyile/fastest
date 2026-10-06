#pragma once

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
struct SetCommand {
	ItemKey key;
	Item value;
};

struct GetCommand {
	ItemKey key;
};
// using Command = std::variant<SetCommand>;
struct SetResponse {};
struct GetResponse {};

struct RequestCommand : std::variant<SetCommand, GetCommand> {
	using Base = std::variant<SetCommand, GetCommand>;
	using Base::Base; // 继承 variant 的所有构造函数
	[[nodiscard]] uint64_t hash() const noexcept {
		return std::visit([](const auto &c) -> size_t { return c.key.hash; }, *this);
	}
};
struct ResponseCommand : std::variant<SetResponse, GetResponse> {
	using Base = std::variant<SetResponse, GetResponse>;
	// using Base::Base; // 继承 variant 的所有构造函数
};

struct RequestData {
	RequestCommand command;
};

struct ResponseData {
	ResponseCommand command;
};

// 负责抽象发送数据 和响应回调，每个线程都持有这样一个worker，并且可以通过worker和其他线程的mailbox来进行通信
struct RequestMessage {
	uint32_t tcp_fd;    // tcp的32位fd编号
	uint32_t seq;       // 请求编号，允许回绕
	uint16_t origin_id; // 投递线程的编号
	RequestData data;
};

struct ResponseMessage {
	uint32_t tcp_fd; // tcp的32位fd编号
	uint32_t seq;    // 请求编号，允许回绕
	ResponseData data;
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
