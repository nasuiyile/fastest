//
// Created by 12968 on 2026/9/22.
//


#pragma once

#include <boost/intrusive/unordered_set.hpp>
#include <boost/intrusive/unordered_set_hook.hpp>

#include <bit>
#include <cassert>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <limits>
#include <memory>
#include <memory_resource>
#include <new>
#include <span>
#include <stdexcept>
#include <string_view>
#include <utility>

namespace cache {
namespace bi = boost::intrusive;

class FastByteHash {
public:
	explicit constexpr FastByteHash(std::uint64_t seed = 0x9e3779b97f4a7c15ULL) noexcept : seed_(seed) {
	}

	std::uint64_t operator()(std::span<const std::byte> key) const noexcept {
		const auto *p = key.data();
		std::size_t n = key.size();

		std::uint64_t h = mix(seed_ ^ (static_cast<std::uint64_t>(n) * 0x9e3779b97f4a7c15ULL));

		while (n >= sizeof(std::uint64_t)) {
			std::uint64_t word;
			std::memcpy(&word, p, sizeof(word));

			h ^= mix(word + 0xa0761d6478bd642fULL);
			h = std::rotl(h, 27) * 0xe7037ed1a0b428dbULL + 0x8ebc6af09c88c6e3ULL;

			p += sizeof(word);
			n -= sizeof(word);
		}

		if (n != 0) {
			std::uint64_t tail = 0;
			std::memcpy(&tail, p, n);

			h ^= mix(tail ^ (static_cast<std::uint64_t>(n) * 0x589965cc75374cc3ULL));
		}

		return mix(h);
	}

private:
	static constexpr std::uint64_t mix(std::uint64_t x) noexcept {
		x ^= x >> 30;
		x *= 0xbf58476d1ce4e5b9ULL;
		x ^= x >> 27;
		x *= 0x94d049bb133111ebULL;
		x ^= x >> 31;
		return x;
	}

	std::uint64_t seed_;
};

template <typename Value, typename KeyHash = FastByteHash>
class IncrementalHashMap {
public:
	using value_type = Value;
	using key_view = std::span<const std::byte>;

	static constexpr std::size_t kInitialCapacity = 16;
	static constexpr std::size_t kMaxKeySize = 250;

private:
	using HashHook = bi::unordered_set_base_hook<bi::link_mode<bi::normal_link> >;

	struct Entry : HashHook {
		std::uint64_t hash;
		Value *value;
		std::uint8_t key_size;

		Entry(std::uint64_t hash_value, Value *value_ptr, std::uint8_t size) noexcept
			: hash(hash_value), value(value_ptr), key_size(size) {
		}

		key_view key() const noexcept {
			return {reinterpret_cast<const std::byte *>(this + 1), key_size};
		}

		std::byte *mutable_key_data() noexcept {
			return reinterpret_cast<std::byte *>(this + 1);
		}
	};

	struct LookupKey {
		key_view key;
		std::uint64_t hash;
	};

	struct NodeHash {
		std::size_t operator()(const Entry &entry) const noexcept {
			return static_cast<std::size_t>(entry.hash);
		}

		std::size_t operator()(const LookupKey &lookup) const noexcept {
			return static_cast<std::size_t>(lookup.hash);
		}
	};

	struct NodeEqual {
		bool operator()(const Entry &lhs, const Entry &rhs) const noexcept {
			return equal(lhs.hash, lhs.key(), rhs.hash, rhs.key());
		}

		bool operator()(const LookupKey &lhs, const Entry &rhs) const noexcept {
			return equal(lhs.hash, lhs.key, rhs.hash, rhs.key());
		}

		bool operator()(const Entry &lhs, const LookupKey &rhs) const noexcept {
			return equal(lhs.hash, lhs.key(), rhs.hash, rhs.key);
		}

	private:
		static bool equal(std::uint64_t lhs_hash, key_view lhs, std::uint64_t rhs_hash, key_view rhs) noexcept {
			if (lhs_hash != rhs_hash || lhs.size() != rhs.size()) {
				return false;
			}

			return lhs.empty() || std::memcmp(lhs.data(), rhs.data(), lhs.size()) == 0;
		}
	};

	using Set = bi::unordered_set<Entry,
	                              bi::base_hook<HashHook>,
	                              bi::hash<NodeHash>,
	                              bi::equal<NodeEqual>,
	                              bi::constant_time_size<false>,
	                              bi::cache_begin<true>,
	                              bi::power_2_buckets<true> >;

	using bucket_type = typename Set::bucket_type;
	using bucket_traits = typename Set::bucket_traits;

	struct Table {
		std::size_t capacity;
		std::size_t size = 0;

		// buckets 必须比 set 活得更久。
		std::unique_ptr<bucket_type[]> buckets;
		Set set;

		explicit Table(std::size_t cap)
			: capacity(cap),
			  buckets(std::make_unique<bucket_type[]>(cap)),
			  set(bucket_traits(buckets.get(), cap), NodeHash{}, NodeEqual{}) {
		}

		Table(const Table &) = delete;

		Table &operator=(const Table &) = delete;
	};

public:
	explicit IncrementalHashMap(std::pmr::memory_resource *upstream = std::pmr::get_default_resource(),
	                            KeyHash key_hash = KeyHash{})
		: pool_(std::pmr::pool_options{}, upstream),
		  key_hash_(std::move(key_hash)),
		  current_(std::make_unique<Table>(kInitialCapacity)) {
	}

	~IncrementalHashMap() {
		destroy_all_entries();
	}

	IncrementalHashMap(const IncrementalHashMap &) = delete;

	IncrementalHashMap &operator=(const IncrementalHashMap &) = delete;

	IncrementalHashMap(IncrementalHashMap &&) = delete;

	IncrementalHashMap &operator=(IncrementalHashMap &&) = delete;

	bool insert(key_view key, Value *value) {
		validate_key(key);

		const LookupKey lookup{key, key_hash_(key)};

		if (find_entry_raw(lookup) != nullptr) {
			maintenance_one();
			return false;
		}

		Entry *entry = create_entry(lookup, value);

		auto [it, inserted] = current_->set.insert(*entry);
		(void)it;

		assert(inserted);

		++current_->size;
		++size_;

		maintenance_one();

		return true;
	}

	// second == true  : 新插入
	// second == false : 已存在，返回旧 value
	std::pair<Value *, bool> insert_or_assign(key_view key, Value *value) {
		validate_key(key);

		const LookupKey lookup{key, key_hash_(key)};

		if (Entry *existing = find_entry_raw(lookup)) {
			Value *previous = existing->value;
			existing->value = value;

			maintenance_one();

			return {previous, false};
		}

		Entry *entry = create_entry(lookup, value);

		auto [it, inserted] = current_->set.insert(*entry);
		(void)it;

		assert(inserted);

		++current_->size;
		++size_;

		maintenance_one();

		return {nullptr, true};
	}

	Value *find(key_view key) {
		validate_key(key);

		const LookupKey lookup{key, key_hash_(key)};

		Entry *entry = find_entry_raw(lookup);
		Value *result = entry != nullptr ? entry->value : nullptr;

		maintenance_one();

		return result;
	}

	// 不触发迁移，适合只读检查。
	Value *peek(key_view key) const {
		validate_key(key);

		const LookupKey lookup{key, key_hash_(key)};

		const Entry *entry = find_entry_raw(lookup);

		return entry != nullptr ? entry->value : nullptr;
	}

	bool contains(key_view key) {
		validate_key(key);

		const LookupKey lookup{key, key_hash_(key)};

		const bool found = find_entry_raw(lookup) != nullptr;

		maintenance_one();

		return found;
	}

	bool erase(key_view key) {
		validate_key(key);

		const LookupKey lookup{key, key_hash_(key)};

		bool erased = erase_from(*current_, lookup);

		if (!erased && old_) {
			erased = erase_from(*old_, lookup);
		}

		if (erased) {
			--size_;
		}

		maintenance_one();

		return erased;
	}

	void clear() noexcept {
		destroy_entries_in(*current_);

		if (old_) {
			destroy_entries_in(*old_);
			old_.reset();
		}

		size_ = 0;
	}

	std::size_t size() const noexcept {
		return size_;
	}

	bool empty() const noexcept {
		return size_ == 0;
	}

	std::size_t capacity() const noexcept {
		return current_->capacity;
	}

	bool rehashing() const noexcept {
		return old_ != nullptr;
	}

	std::size_t remaining_to_migrate() const noexcept {
		return old_ != nullptr ? old_->size : 0;
	}

	double load_factor() const noexcept {
		return static_cast<double>(size_) / static_cast<double>(current_->capacity);
	}

	static key_view bytes(std::string_view key) noexcept {
		return {reinterpret_cast<const std::byte *>(key.data()), key.size()};
	}

private:
	static void validate_key(key_view key) {
		if (key.size() > kMaxKeySize) [[unlikely]] {
			throw std::length_error("cache key exceeds 250 bytes");
		}
	}

	Entry *create_entry(const LookupKey &lookup, Value *value) {
		const std::size_t allocation_size = sizeof(Entry) + lookup.key.size();

		void *memory = pool_.allocate(allocation_size, alignof(Entry));

		Entry *entry =
			::new(memory) Entry(lookup.hash, value, static_cast<std::uint8_t>(lookup.key.size()));

		if (!lookup.key.empty()) {
			std::memcpy(entry->mutable_key_data(), lookup.key.data(), lookup.key.size());
		}

		return entry;
	}

	void destroy_entry(Entry *entry) noexcept {
		const std::size_t allocation_size = sizeof(Entry) + entry->key_size;

		entry->~Entry();

		pool_.deallocate(entry, allocation_size, alignof(Entry));
	}

	Entry *find_entry_raw(const LookupKey &lookup) noexcept {
		// rehash 时先查新表，因为新插入的数据一定在 current_。
		auto it = current_->set.find(lookup, NodeHash{}, NodeEqual{});

		if (it != current_->set.end()) {
			return &*it;
		}

		if (old_) {
			auto old_it = old_->set.find(lookup, NodeHash{}, NodeEqual{});

			if (old_it != old_->set.end()) {
				return &*old_it;
			}
		}

		return nullptr;
	}

	const Entry *find_entry_raw(const LookupKey &lookup) const noexcept {
		auto it = current_->set.find(lookup, NodeHash{}, NodeEqual{});

		if (it != current_->set.end()) {
			return &*it;
		}

		if (old_) {
			auto old_it = old_->set.find(lookup, NodeHash{}, NodeEqual{});

			if (old_it != old_->set.end()) {
				return &*old_it;
			}
		}

		return nullptr;
	}

	bool erase_from(Table &table, const LookupKey &lookup) noexcept {
		auto it = table.set.find(lookup, NodeHash{}, NodeEqual{});

		if (it == table.set.end()) {
			return false;
		}

		Entry *entry = &*it;

		table.set.erase(it);

		--table.size;

		destroy_entry(entry);

		return true;
	}

	bool need_grow() const noexcept {
		if (old_) {
			return false;
		}

		// capacity 永远是 2 的幂。
		// 16 -> 12
		// 32 -> 24
		// 64 -> 48
		const std::size_t threshold = current_->capacity - current_->capacity / 4;

		return size_ >= threshold;
	}

	void begin_grow() {
		if (current_->capacity > std::numeric_limits<std::size_t>::max() / 2) [[unlikely]] {
			throw std::length_error("hash table capacity overflow");
		}

		const std::size_t new_capacity = current_->capacity * 2;

		old_ = std::move(current_);
		current_ = std::make_unique<Table>(new_capacity);
	}

	bool migrate_one_node() noexcept {
		if (!old_) {
			return false;
		}

		if (old_->size == 0) {
			old_.reset();
			return false;
		}

		// cache_begin<true> 让 begin() 很便宜。
		auto it = old_->set.begin();

		Entry &entry = *it;

		// 仅摘 hook。
		// Entry、key、value 地址均不变化。
		old_->set.erase(it);
		--old_->size;

		// hash 已缓存在 Entry 中。
		// 不重新计算 key hash，不复制 key，不分配节点。
		auto [new_it, inserted] = current_->set.insert(entry);
		(void)new_it;

		assert(inserted);

		++current_->size;

		if (old_->size == 0) {
			old_.reset();
		}

		return true;
	}

	void maintenance_one() {
		// 一个用户操作最多迁移一个 Entry。
		const bool moved = migrate_one_node();

		if (need_grow()) {
			begin_grow();

			// 达到 0.75 的当前操作立即开始搬第一个节点。
			if (!moved) {
				migrate_one_node();
			}
		}
	}

	void destroy_entries_in(Table &table) noexcept {
		while (table.size != 0) {
			auto it = table.set.begin();

			Entry *entry = &*it;

			table.set.erase(it);
			--table.size;

			destroy_entry(entry);
		}
	}

	void destroy_all_entries() noexcept {
		if (current_) {
			destroy_entries_in(*current_);
		}

		if (old_) {
			destroy_entries_in(*old_);
			old_.reset();
		}

		size_ = 0;
	}

private:
	// 单线程 pool，没有同步开销。
	std::pmr::unsynchronized_pool_resource pool_;

	[[no_unique_address]] KeyHash key_hash_;

	std::unique_ptr<Table> current_;
	std::unique_ptr<Table> old_;

	std::size_t size_ = 0;
};
} // namespace cache