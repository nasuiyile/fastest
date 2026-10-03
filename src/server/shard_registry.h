// shard_registry.h
#pragma once
#include "mailbox.h"
#include <vector>
#include <memory>
#include <string_view>
#include <functional>

namespace fast_server {
class ShardRegistry {
public:
	static ShardRegistry &inst() {
		static ShardRegistry r;
		return r;
	}

	void init(size_t n) {
		shards_.resize(n);
		for (auto &p : shards_)
			p = std::make_unique<Mailbox>();
	}

	size_t size() const noexcept { return shards_.size(); }
	Mailbox &mailbox(size_t i) const { return *shards_[i]; }

	size_t shard_of(const std::string_view key) const noexcept {
		return std::hash<std::string_view>{}(key) % shards_.size();
	}

	static thread_local size_t local_shard;


private:
	ShardRegistry() = default;

	std::vector<std::unique_ptr<Mailbox> > shards_;
};

inline thread_local size_t ShardRegistry::local_shard = 0;
} // namespace fast_server