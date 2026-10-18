#pragma once

#include <boost/intrusive/unordered_set.hpp>
#include <boost/intrusive/unordered_set_hook.hpp>

#include <algorithm>
#include <cassert>
#include <concepts>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <functional>
#include <limits>
#include <memory>
#include <new>
#include <span>
#include <stdexcept>
#include <string>
#include <string_view>
#include <type_traits>
#include <utility>
#include <vector>

#include "type.h"

namespace fastest {
namespace bi = boost::intrusive;

//
// Values are charged only while linked. make_value() alone does not charge;
// extract() stops charging immediately even if the returned Value lives longer.
// External in-place size changes require refresh_value_memory(key).
// To own values, pair make_value()/release() with erase_and_dispose() or
// clear_and_dispose(). Ordinary erase()/clear()/destruction never delete Value.
// ItemKey overloads trust the supplied hash and never recompute it (zero is
// valid). If a custom KeyHash or seed is used, ItemKey::hash must use that same
// algorithm and seed. ItemKey's constructor uses the original default FastByteHash.
template <typename Value, typename KeyHash = FastByteHash>
class IncrementalHashMap {
public:
	using value_type = Value;
	using key_view = std::span<const std::byte>;
	using value_owner = std::unique_ptr<Value>;

	static_assert(
	    requires(const Value &value) {
		    { value.memory_usage() } noexcept -> std::same_as<std::size_t>;
	    }, "Value must provide size_t memory_usage() const noexcept, including sizeof(Value)");

	static constexpr std::size_t kInitialCapacity = 16;
	static constexpr std::size_t kMaxKeySize = 250;

	// Estimated current footprint, NOT allocator usable bytes, RSS, peak
	// memory or a hard admission limit. Includes retained vector capacity and
	// both tables during rehash; excludes temporary requests and extracted values.
	struct MemoryUsage {
		std::size_t map_bytes = 0;    // sizeof(IncrementalHashMap)
		std::size_t entry_bytes = 0;  // Entry (including hook/padding) + inline key
		std::size_t value_bytes = 0;  // Sum of linked Value::memory_usage()
		std::size_t table_bytes = 0;  // Table objects + bucket vector capacity
		std::size_t sample_bytes = 0; // Sampling vector capacity; vector is in map_bytes

		[[nodiscard]] std::size_t used_bytes() const noexcept {
			return map_bytes + entry_bytes + value_bytes + table_bytes + sample_bytes;
		}
	};

private:
	using HashHook = bi::unordered_set_base_hook<bi::link_mode<bi::normal_link>>;

	struct Entry : HashHook {
		std::uint64_t hash;
		Value *value;
		std::size_t sample_index = 0;
		// Last committed estimate; also permits accounting same-pointer updates.
		std::size_t value_bytes;
		std::uint8_t key_size;

		Entry(std::uint64_t h, Value *v, std::uint8_t n, std::size_t charge) noexcept
		    : hash(h), value(v), value_bytes(charge), key_size(n) {
		}

		[[nodiscard]] key_view key() const noexcept {
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
		std::size_t operator()(const Entry &e) const noexcept {
			return static_cast<std::size_t>(e.hash);
		}
		std::size_t operator()(const LookupKey &k) const noexcept {
			return static_cast<std::size_t>(k.hash);
		}
	};

	struct NodeEqual {
		bool operator()(const Entry &a, const Entry &b) const noexcept {
			return equal(a.hash, a.key(), b.hash, b.key());
		}
		bool operator()(const LookupKey &a, const Entry &b) const noexcept {
			return equal(a.hash, a.key, b.hash, b.key());
		}
		bool operator()(const Entry &a, const LookupKey &b) const noexcept {
			return equal(a.hash, a.key(), b.hash, b.key);
		}

	private:
		static bool equal(std::uint64_t ah, key_view a, std::uint64_t bh, key_view b) noexcept {
			return ah == bh && a.size() == b.size() && (a.empty() || std::memcmp(a.data(), b.data(), a.size()) == 0);
		}
	};

	using Set = bi::unordered_set<Entry, bi::base_hook<HashHook>, bi::hash<NodeHash>, bi::equal<NodeEqual>,
	                              bi::constant_time_size<false>, bi::cache_begin<true>, bi::power_2_buckets<true>>;
	using bucket_type = typename Set::bucket_type;
	using bucket_traits = typename Set::bucket_traits;

	struct Table {
		std::size_t capacity;
		std::size_t size = 0;
		// Fixed-length vector. Never resize it while set is using its data().
		// Destruction is reversed: set must die BEFORE buckets.
		std::vector<bucket_type> buckets;
		Set set;

		explicit Table(std::size_t cap)
		    : capacity(cap), buckets(cap), set(bucket_traits(buckets.data(), cap), NodeHash {}, NodeEqual {}) {
		}

		[[nodiscard]] std::size_t memory_usage() const noexcept {
			return sizeof(Table) + buckets.capacity() * sizeof(bucket_type);
		}

		Table(const Table &) = delete;
		Table &operator=(const Table &) = delete;
	};
	using TablePtr = std::unique_ptr<Table>;

public:
	explicit IncrementalHashMap(KeyHash key_hash = KeyHash {})
	    : key_hash_(std::move(key_hash)), current_(make_table(kInitialCapacity)) {
	}

	~IncrementalHashMap() {
		clear();
	}
	IncrementalHashMap(const IncrementalHashMap &) = delete;
	IncrementalHashMap &operator=(const IncrementalHashMap &) = delete;
	IncrementalHashMap(IncrementalHashMap &&) = delete;
	IncrementalHashMap &operator=(IncrementalHashMap &&) = delete;

	// Owner-thread-only, O(1). No allocator hooks, global atomics or table scan.
	[[nodiscard]] MemoryUsage memory_usage() const noexcept {
		return {sizeof(*this), entry_bytes_, value_bytes_, current_->memory_usage() + (old_ ? old_->memory_usage() : 0),
		        sample_entries_.capacity() * sizeof(Entry *)};
	}
	[[nodiscard]] std::size_t used_bytes() const noexcept {
		return memory_usage().used_bytes();
	}

	template <typename... Args>
	[[nodiscard]] value_owner make_value(Args &&...args) {
		return std::make_unique<Value>(std::forward<Args>(args)...);
	}

	// Only for a detached Value allocated with ordinary new/make_value().
	void destroy_value(Value *value) noexcept {
		static_assert(std::is_nothrow_destructible_v<Value>);
		delete value;
	}

	// Call after externally changing a linked Value's owned storage. Changes
	// through sample_next() are refreshed automatically, including on exception.
	// Returns false if the key is absent; nullptr values have zero charge.
	bool refresh_value_memory(key_view key) {
		return refresh_value_memory_impl(make_lookup(key));
	}
	bool refresh_value_memory(const ItemKey &key) {
		return refresh_value_memory_impl(make_lookup(key));
	}

	bool insert(key_view key, Value *value) {
		return insert_impl(make_lookup(key), value);
	}
	bool insert(const ItemKey &key, Value *value) {
		return insert_impl(make_lookup(key), value);
	}

	// true: inserted; false: replaced, caller still owns the returned old Value.
	// If old and new pointers are identical, that Value remains linked; do not
	// dispose the returned pointer until it is actually detached.
	// A throw means Value ownership has NOT changed.
	std::pair<Value *, bool> insert_or_assign(key_view key, Value *value) {
		return insert_or_assign_impl(make_lookup(key), value);
	}
	std::pair<Value *, bool> insert_or_assign(const ItemKey &key, Value *value) {
		return insert_or_assign_impl(make_lookup(key), value);
	}

	Value *find(key_view key) {
		Value *result = peek(key);
		maintenance_one();
		return result;
	}
	Value *find(const ItemKey &key) {
		Value *result = peek(key);
		maintenance_one();
		return result;
	}

	// Also owner-thread-only: const does NOT mean thread-safe.
	Value *peek(key_view key) const {
		return peek_impl(make_lookup(key));
	}
	Value *peek(const ItemKey &key) const {
		return peek_impl(make_lookup(key));
	}

	bool contains(key_view key) {
		return contains_impl(make_lookup(key));
	}
	bool contains(const ItemKey &key) {
		return contains_impl(make_lookup(key));
	}

	// Removal frees Entry + key, and returns the borrowed Value* separately.
	// The bool distinguishes "not found" from a stored nullptr.
	std::pair<Value *, bool> extract(key_view key) {
		return extract_impl(make_lookup(key));
	}
	std::pair<Value *, bool> extract(const ItemKey &key) {
		return extract_impl(make_lookup(key));
	}

	// Original semantics: does NOT destroy Value.
	bool erase(key_view key) {
		return extract(key).second;
	}
	bool erase(const ItemKey &key) {
		return extract(key).second;
	}

	// disposer must be noexcept, must not reenter this map or retain its keys.
	template <typename Disposer>
	bool erase_and_dispose(key_view key, Disposer &&disposer) {
		static_assert(std::is_nothrow_invocable_v<Disposer &, Value *>);
		auto [value, found] = extract(key);
		if (found) {
			std::invoke(disposer, value);
		}
		return found;
	}
	template <typename Disposer>
	bool erase_and_dispose(const ItemKey &key, Disposer &&disposer) {
		static_assert(std::is_nothrow_invocable_v<Disposer &, Value *>);
		auto [value, found] = extract(key);
		if (found) {
			std::invoke(disposer, value);
		}
		return found;
	}

	// Visits at most limit distinct entries present when sampling begins.
	// The visitor may mutate Value, but must NOT insert/erase/clear/compact
	// or recursively sample. Save copied keys and erase after this returns.
	template <typename Visitor>
	std::size_t sample_next(std::size_t limit, Visitor &&visitor) {
		const std::size_t count = std::min(limit, sample_entries_.size());
		for (std::size_t i = 0; i < count; ++i) {
			Entry *entry = sample_entries_[sample_cursor_];
			if (++sample_cursor_ == sample_entries_.size()) {
				sample_cursor_ = 0;
			}
			// A visitor may mutate then throw; keep the estimate consistent
			// with the surviving Value on both paths.
			try {
				std::invoke(visitor, entry->key(), entry->value);
			} catch (...) {
				refresh_entry_memory(*entry);
				throw;
			}
			refresh_entry_memory(*entry);
		}
		maintenance_one();
		return count;
	}

	template <typename Visitor>
	std::size_t sample_next_10(Visitor &&visitor) {
		return sample_next(10, std::forward<Visitor>(visitor));
	}

	// Frees all nodes, old_ and the sample vector allocation.
	// Retains current_ buckets for reuse; values keep original borrowed semantics.
	void clear() noexcept {
		clear_and_dispose([](Value *) noexcept {});
	}

	template <typename Disposer>
	void clear_and_dispose(Disposer &&disposer) noexcept {
		static_assert(std::is_nothrow_invocable_v<Disposer &, Value *>);
		destroy_entries_in(*current_, disposer);
		if (old_) {
			destroy_entries_in(*old_, disposer);
			old_.reset();
		}
		size_ = 0;
		sample_cursor_ = 0;
		assert(entry_bytes_ == 0 && value_bytes_ == 0);
		std::vector<Entry *> empty;
		sample_entries_.swap(empty); // clear() alone would retain vector capacity.
	}

	// Explicit O(number of entries + bucket count) compaction, for maintenance
	// or an empty map. May temporarily need BOTH old and replacement storage.
	// Do not run in every erase/TTL step or pretend it has a 1ms time bound.
	// Addresses of Entry/key/Value stay stable. All allocations precede mutation.
	void compact() {
		std::size_t cap = kInitialCapacity;
		while (size_ > cap - cap / 4) {
			cap = doubled(cap);
		}
		auto replacement = make_table(cap);
		std::vector<Entry *> samples {sample_entries_.begin(), sample_entries_.end()};
		move_all_entries(*current_, *replacement);
		if (old_) {
			move_all_entries(*old_, *replacement);
			old_.reset();
		}
		current_ = std::move(replacement);
		sample_entries_.swap(samples);
	}

	// For an idle maintenance task: moves at most one entry, never grows.
	bool rehash_step() noexcept {
		return migrate_one_node();
	}

	[[nodiscard]] std::size_t size() const noexcept {
		return size_;
	}
	[[nodiscard]] bool empty() const noexcept {
		return size_ == 0;
	}
	[[nodiscard]] std::size_t capacity() const noexcept {
		return current_->capacity;
	}
	[[nodiscard]] bool rehashing() const noexcept {
		return old_ != nullptr;
	}
	[[nodiscard]] std::size_t remaining_to_migrate() const noexcept {
		return old_ ? old_->size : 0;
	}
	[[nodiscard]] double load_factor() const noexcept {
		return static_cast<double>(size_) / static_cast<double>(current_->capacity);
	}
	static key_view bytes(std::string_view key) noexcept {
		return {reinterpret_cast<const std::byte *>(key.data()), key.size()};
	}

private:
	LookupKey make_lookup(key_view key) {
		validate_key(key);
		return {key, key_hash_(key)};
	}

	LookupKey make_lookup(key_view key) const {
		validate_key(key);
		return {key, key_hash_(key)};
	}

	static LookupKey make_lookup(const ItemKey &key) {
		const key_view view = bytes(key.key);
		validate_key(view);
		return {view, key.hash};
	}

	bool refresh_value_memory_impl(const LookupKey &lookup) {
		Entry *entry = find_entry_raw(lookup);
		if (entry == nullptr) {
			return false;
		}
		refresh_entry_memory(*entry);
		return true;
	}

	bool insert_impl(const LookupKey &lookup, Value *value) {
		if (find_entry_raw(lookup) != nullptr) {
			maintenance_one();
			return false;
		}
		prepare_insert(); // May throw, before logical insertion.
		Entry *entry = create_entry(lookup, value);
		link_new_entry(entry);
		maintenance_one(); // Never allocates or throws after commit.
		return true;
	}

	std::pair<Value *, bool> insert_or_assign_impl(const LookupKey &lookup, Value *value) {
		if (Entry *existing = find_entry_raw(lookup)) {
			Value *previous = std::exchange(existing->value, value);
			refresh_entry_memory(*existing);
			maintenance_one();
			return {previous, false};
		}
		prepare_insert();
		Entry *entry = create_entry(lookup, value);
		link_new_entry(entry);
		maintenance_one();
		return {nullptr, true};
	}

	Value *peek_impl(const LookupKey &lookup) const {
		const Entry *e = find_entry_raw(lookup);
		return e != nullptr ? e->value : nullptr;
	}

	bool contains_impl(const LookupKey &lookup) {
		const bool found = find_entry_raw(lookup) != nullptr;
		maintenance_one();
		return found;
	}

	std::pair<Value *, bool> extract_impl(const LookupKey &lookup) {
		auto result = extract_from(*current_, lookup);
		if (!result.second && old_) {
			result = extract_from(*old_, lookup);
		}
		if (result.second) {
			--size_;
		}
		maintenance_one();
		return result;
	}

	static void validate_key(key_view key) {
		if (key.size() > kMaxKeySize) [[unlikely]] {
			throw std::length_error("cache key exceeds 250 bytes");
		}
	}

	static std::size_t doubled(std::size_t cap) {
		if (cap > std::numeric_limits<std::size_t>::max() / 2) [[unlikely]] {
			throw std::length_error("hash table capacity overflow");
		}
		return cap * 2;
	}

	TablePtr make_table(std::size_t capacity) {
		return std::make_unique<Table>(capacity);
	}

	void prepare_insert() {
		const std::size_t threshold = current_->capacity - current_->capacity / 4;
		if (!old_ && size_ >= threshold - 1) {
			// Allocate first! Moving current_ first would leave it nullptr if
			// allocation failed, corrupting the old implementation's state.
			auto replacement = make_table(doubled(current_->capacity));
			old_ = std::move(current_);
			current_ = std::move(replacement);
		}
	}

	Entry *create_entry(const LookupKey &lookup, Value *value) {
		const std::size_t bytes = sizeof(Entry) + lookup.key.size();
		void *storage = ::operator new(bytes, std::align_val_t {alignof(Entry)});
		auto *entry = ::new (storage)
		    Entry(lookup.hash, value, static_cast<std::uint8_t>(lookup.key.size()), estimate_value_bytes(value));
		if (!lookup.key.empty()) {
			std::memcpy(entry->mutable_key_data(), lookup.key.data(), lookup.key.size());
		}
		try {
			sample_entries_.push_back(entry);
		} catch (...) {
			std::destroy_at(entry);
			::operator delete(storage, std::align_val_t {alignof(Entry)});
			throw;
		}
		entry->sample_index = sample_entries_.size() - 1;
		return entry;
	}

	void link_new_entry(Entry *entry) noexcept {
		auto [it, inserted] = current_->set.insert(*entry);
		(void)it;
		assert(inserted);
		(void)inserted;
		++current_->size;
		++size_;
		entry_bytes_ += sizeof(Entry) + entry->key_size;
		value_bytes_ += entry->value_bytes;
	}

	void destroy_entry(Entry *entry) noexcept {
		remove_sample_entry(entry);
		const std::size_t bytes = sizeof(Entry) + entry->key_size;
		assert(entry_bytes_ >= bytes && value_bytes_ >= entry->value_bytes);
		entry_bytes_ -= bytes;
		value_bytes_ -= entry->value_bytes;
		std::destroy_at(entry);
		::operator delete(entry, std::align_val_t {alignof(Entry)});
	}

	static std::size_t estimate_value_bytes(const Value *value) noexcept {
		return value != nullptr ? value->memory_usage() : 0;
	}

	void refresh_entry_memory(Entry &entry) noexcept {
		const std::size_t updated = estimate_value_bytes(entry.value);
		assert(value_bytes_ >= entry.value_bytes);
		value_bytes_ -= entry.value_bytes;
		value_bytes_ += updated;
		entry.value_bytes = updated;
	}

	void remove_sample_entry(Entry *entry) noexcept {
		const std::size_t index = entry->sample_index;
		const std::size_t last = sample_entries_.size() - 1;
		assert(index <= last && sample_entries_[index] == entry);
		if (index != last) {
			Entry *moved = sample_entries_.back();
			sample_entries_[index] = moved;
			moved->sample_index = index;
			if (sample_cursor_ == last) {
				sample_cursor_ = index;
			}
		}
		sample_entries_.pop_back();
		if (sample_cursor_ >= sample_entries_.size()) {
			sample_cursor_ = 0;
		}
	}

	const Entry *find_entry_raw(const LookupKey &lookup) const noexcept {
		auto it = current_->set.find(lookup, NodeHash {}, NodeEqual {});
		if (it != current_->set.end()) {
			return &*it;
		}
		if (old_) {
			auto it_old = old_->set.find(lookup, NodeHash {}, NodeEqual {});
			if (it_old != old_->set.end()) {
				return &*it_old;
			}
		}
		return nullptr;
	}
	Entry *find_entry_raw(const LookupKey &lookup) noexcept {
		return const_cast<Entry *>(std::as_const(*this).find_entry_raw(lookup));
	}

	std::pair<Value *, bool> extract_from(Table &table, const LookupKey &lookup) noexcept {
		auto it = table.set.find(lookup, NodeHash {}, NodeEqual {});
		if (it == table.set.end()) {
			return {nullptr, false};
		}
		Entry *entry = &*it;
		Value *value = entry->value;
		table.set.erase(it);
		--table.size;
		destroy_entry(entry);
		return {value, true};
	}

	static void move_one_entry(Table &from, Table &to) noexcept {
		auto it = from.set.begin();
		Entry &entry = *it;
		from.set.erase(it);
		--from.size;
		auto [new_it, inserted] = to.set.insert(entry);
		(void)new_it;
		assert(inserted);
		(void)inserted;
		++to.size;
	}
	static void move_all_entries(Table &from, Table &to) noexcept {
		while (from.size != 0) {
			move_one_entry(from, to);
		}
	}

	bool migrate_one_node() noexcept {
		if (!old_) {
			return false;
		}
		if (old_->size == 0) {
			old_.reset();
			return false;
		}
		move_one_entry(*old_, *current_);
		if (old_->size == 0) {
			old_.reset();
		}
		return true;
	}
	void maintenance_one() noexcept {
		(void)migrate_one_node();
	}

	template <typename Disposer>
	void destroy_entries_in(Table &table, Disposer &disposer) noexcept {
		while (table.size != 0) {
			auto it = table.set.begin();
			Entry *entry = &*it;
			Value *value = entry->value;
			table.set.erase(it);
			--table.size;
			destroy_entry(entry);
			std::invoke(disposer, value);
		}
	}

	[[no_unique_address]] KeyHash key_hash_;
	TablePtr current_;
	TablePtr old_;
	std::size_t size_ = 0;
	std::size_t entry_bytes_ = 0;
	std::size_t value_bytes_ = 0;
	std::vector<Entry *> sample_entries_;
	std::size_t sample_cursor_ = 0;
};

} // namespace fastest
