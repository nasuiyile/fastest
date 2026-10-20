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
#include <string_view>
#include <type_traits>
#include <utility>

#include "type.h"

namespace fastest {
namespace bi = boost::intrusive;

// Owner-thread-only, including const methods. A background timer must run on
// that owner thread, or all access must be protected by an external lock.
//
// Values are charged only while linked. make_value() alone does not charge;
// extract() stops charging immediately even if the returned Value lives longer.
// Value::memory_usage() must be O(1), const, noexcept, and include sizeof(Value).
// Linked Values must stay alive until detached. Size-changing modifications
// must use mutate_value(), sample_next(), or the two-argument
// refresh_value_memory(key, previous_bytes). There is no saved per-entry charge.
// A size-changing Value must be linked under exactly one key in this map;
// shared Value pointers are supported only while their memory usage is fixed.
// Ordinary erase()/clear()/destruction never delete Value. For owning use,
// transfer make_value() ownership only after a successful insert, and use
// erase_and_dispose()/clear_and_dispose() when removing owned values.
//
// ItemKey overloads trust the supplied hash, including zero. With a custom
// KeyHash or seed, ItemKey::hash must use that same algorithm and seed.
// ItemKey's constructor uses the original default FastByteHash.
template <typename Value, typename KeyHash = FastByteHash>
class IncrementalHashMap {
public:
    using value_type = Value;
    using key_view = std::span<const std::byte>;
    using value_owner = std::unique_ptr<Value>;

    static_assert(
        requires(const Value& value) {
            { value.memory_usage() } noexcept -> std::same_as<std::size_t>;
        },
        "Value must provide size_t memory_usage() const noexcept, including sizeof(Value)");

    static constexpr std::size_t kInitialCapacity = 16;
    static constexpr std::size_t kMaxKeySize = 250;

    // Estimates requested storage, not allocator usable bytes, RSS, peak
    // memory, or a hard admission limit. Includes both tables during rehash;
    // excludes temporary requests and detached values.
    struct MemoryUsage {
        std::size_t map_bytes = 0;
        std::size_t entry_bytes = 0;
        std::size_t value_bytes = 0;
        std::size_t table_bytes = 0;
        // Kept for API compatibility. There is no sampling allocation.
        std::size_t sample_bytes = 0;

        [[nodiscard]] std::size_t used_bytes() const noexcept {
            return map_bytes + entry_bytes + value_bytes + table_bytes + sample_bytes;
        }
    };

private:
    using HashHook = bi::unordered_set_base_hook<bi::link_mode<bi::normal_link>>;

    // One allocation: [Entry][1-byte key length][key bytes].
    // Keeping the length outside Entry avoids trailing alignment padding.
    // On a usual 64-bit ABI, sizeof(Entry) == 24: 25 + key.size() requested
    // bytes per entry. No per-entry sampling metadata or separate key allocation.
    struct Entry : HashHook {
        std::uint64_t hash;
        Value* value;
        Entry(std::uint64_t h, Value* v, std::uint8_t n) noexcept
            : hash(h), value(v) {
            *reinterpret_cast<std::byte*>(this + 1) = static_cast<std::byte>(n);
        }

        [[nodiscard]] std::uint8_t key_size() const noexcept {
            return std::to_integer<std::uint8_t>(
                *reinterpret_cast<const std::byte*>(this + 1));
        }

        [[nodiscard]] key_view key() const noexcept {
            return {reinterpret_cast<const std::byte*>(this + 1) + 1, key_size()};
        }

        std::byte* mutable_key_data() noexcept {
            return reinterpret_cast<std::byte*>(this + 1) + 1;
        }

        [[nodiscard]] std::size_t allocation_size() const noexcept {
            return sizeof(Entry) + 1 + key_size();
        }
    };

    static_assert(alignof(Entry) <= alignof(std::max_align_t));

    struct LookupKey {
        key_view key;
        std::uint64_t hash;
    };

    struct NodeHash {
        std::size_t operator()(const Entry& entry) const noexcept {
            return static_cast<std::size_t>(entry.hash);
        }
        std::size_t operator()(const LookupKey& key) const noexcept {
            return static_cast<std::size_t>(key.hash);
        }
    };

    struct NodeEqual {
        bool operator()(const Entry& a, const Entry& b) const noexcept {
            return equal(a.hash, a.key(), b.hash, b.key());
        }
        bool operator()(const LookupKey& a, const Entry& b) const noexcept {
            return equal(a.hash, a.key, b.hash, b.key());
        }
        bool operator()(const Entry& a, const LookupKey& b) const noexcept {
            return equal(a.hash, a.key(), b.hash, b.key);
        }

    private:
        static bool equal(std::uint64_t ah, key_view a,
                          std::uint64_t bh, key_view b) noexcept {
            return ah == bh && a.size() == b.size() &&
                   (a.empty() || std::memcmp(a.data(), b.data(), a.size()) == 0);
        }
    };

    using Set = bi::unordered_set<
        Entry, bi::base_hook<HashHook>, bi::hash<NodeHash>, bi::equal<NodeEqual>,
        bi::constant_time_size<false>, bi::cache_begin<true>, bi::power_2_buckets<true>>;
    using bucket_type = typename Set::bucket_type;
    using bucket_traits = typename Set::bucket_traits;
    using set_iterator = typename Set::iterator;

    struct Table {
        std::size_t capacity;
        std::size_t size = 0;
        // Fixed-length allocation. set must be destroyed BEFORE buckets.
        std::unique_ptr<bucket_type[]> buckets;
        Set set;
        // The next node to sample, or end() to restart at begin().
        set_iterator sample_cursor;

        explicit Table(std::size_t cap)
            : capacity(cap),
              buckets(std::make_unique<bucket_type[]>(cap)),
              set(bucket_traits(buckets.get(), cap), NodeHash{}, NodeEqual{}),
              sample_cursor(set.end()) {}

        [[nodiscard]] std::size_t memory_usage() const noexcept {
            return sizeof(Table) + capacity * sizeof(bucket_type);
        }

        Table(const Table&) = delete;
        Table& operator=(const Table&) = delete;
    };
    using TablePtr = std::unique_ptr<Table>;

    struct CallbackScope {
        bool& active;

        explicit CallbackScope(bool& flag) noexcept : active(flag) {
            assert(!active);
            active = true;
        }
        ~CallbackScope() {
            active = false;
        }

        CallbackScope(const CallbackScope&) = delete;
        CallbackScope& operator=(const CallbackScope&) = delete;
    };

public:
    explicit IncrementalHashMap(KeyHash key_hash = KeyHash{})
        : key_hash_(std::move(key_hash)), current_(make_table(kInitialCapacity)) {}

    ~IncrementalHashMap() {
        clear();
    }

    IncrementalHashMap(const IncrementalHashMap&) = delete;
    IncrementalHashMap& operator=(const IncrementalHashMap&) = delete;
    IncrementalHashMap(IncrementalHashMap&&) = delete;
    IncrementalHashMap& operator=(IncrementalHashMap&&) = delete;

    [[nodiscard]] MemoryUsage memory_usage() const noexcept {
        return {sizeof(*this), entry_bytes_, value_bytes_,
                current_->memory_usage() + (old_ ? old_->memory_usage() : 0), 0};
    }

    [[nodiscard]] std::size_t used_bytes() const noexcept {
        return memory_usage().used_bytes();
    }

    template <typename... Args>
    [[nodiscard]] value_owner make_value(Args&&... args) {
        return std::make_unique<Value>(std::forward<Args>(args)...);
    }

    // Only for a detached Value allocated with ordinary new/make_value().
    void destroy_value(Value* value) noexcept {
        static_assert(std::is_nothrow_destructible_v<Value>);
        delete value;
    }

    // Preferred interface for in-place changes, such as append/prepend.
    // The callback receives Value* by value (possibly nullptr). Its pointer
    // must not be deleted or replaced. Size differences are accounted on both
    // normal return and exception. False means the key is absent.
    // Do not structurally modify this map, recursively mutate, or sample from
    // the callback. Read calls are allowed and do not advance migration.
    template <typename Mutator>
    bool mutate_value(key_view key, Mutator&& mutator) {
        return mutate_value_impl(make_lookup(key), std::forward<Mutator>(mutator));
    }
    template <typename Mutator>
    bool mutate_value(const ItemKey& key, Mutator&& mutator) {
        return mutate_value_impl(make_lookup(key), std::forward<Mutator>(mutator));
    }

    // For external in-place changes: save memory_usage() BEFORE the change,
    // and pass that charge here immediately afterwards, including on exception.
    // No intervening map operation may use the modified Value. The key must
    // still refer to the same Value, and it must be linked only once.
    // Prefer mutate_value(), which handles the exception path automatically.
    // The former one-argument refresh cannot recover the old charge and is
    // intentionally not provided.
    bool refresh_value_memory(key_view key, std::size_t previous_bytes) {
        return refresh_value_memory_impl(make_lookup(key), previous_bytes);
    }
    bool refresh_value_memory(const ItemKey& key, std::size_t previous_bytes) {
        return refresh_value_memory_impl(make_lookup(key), previous_bytes);
    }

    bool insert(key_view key, Value* value) {
        return insert_impl(make_lookup(key), value);
    }
    bool insert(const ItemKey& key, Value* value) {
        return insert_impl(make_lookup(key), value);
    }

    // true: inserted; false: replaced, returning the previous borrowed pointer.
    // If pointers are identical, the returned Value is still linked: do not
    // dispose it until actually detached. A throw leaves ownership unchanged.
    // Same-pointer assignment does not account for earlier external changes;
    // use mutate_value() or refresh_value_memory(key, previous_bytes) instead.
    std::pair<Value*, bool> insert_or_assign(key_view key, Value* value) {
        return insert_or_assign_impl(make_lookup(key), value);
    }
    std::pair<Value*, bool> insert_or_assign(const ItemKey& key, Value* value) {
        return insert_or_assign_impl(make_lookup(key), value);
    }

    Value* find(key_view key) {
        Value* result = peek(key);
        maintenance_one();
        return result;
    }
    Value* find(const ItemKey& key) {
        Value* result = peek(key);
        maintenance_one();
        return result;
    }

    Value* peek(key_view key) const {
        return peek_impl(make_lookup(key));
    }
    Value* peek(const ItemKey& key) const {
        return peek_impl(make_lookup(key));
    }

    bool contains(key_view key) {
        return contains_impl(make_lookup(key));
    }
    bool contains(const ItemKey& key) {
        return contains_impl(make_lookup(key));
    }

    // Frees Entry + key and returns the borrowed Value separately.
    // The bool distinguishes an absent key from a stored nullptr.
    std::pair<Value*, bool> extract(key_view key) {
        return extract_impl(make_lookup(key));
    }
    std::pair<Value*, bool> extract(const ItemKey& key) {
        return extract_impl(make_lookup(key));
    }

    bool erase(key_view key) {
        return extract(key).second;
    }
    bool erase(const ItemKey& key) {
        return extract(key).second;
    }

    // disposer must be noexcept, must not reenter this map or retain its keys.
    template <typename Disposer>
    bool erase_and_dispose(key_view key, Disposer&& disposer) {
        static_assert(std::is_nothrow_invocable_v<Disposer&, Value*>);
        auto [value, found] = extract(key);
        if (found) {
            std::invoke(disposer, value);
        }
        return found;
    }
    template <typename Disposer>
    bool erase_and_dispose(const ItemKey& key, Disposer&& disposer) {
        static_assert(std::is_nothrow_invocable_v<Disposer&, Value*>);
        auto [value, found] = extract(key);
        if (found) {
            std::invoke(disposer, value);
        }
        return found;
    }

    // Visits min(limit, size()) DISTINCT entries per successful call.
    // Each table has a persistent cyclic cursor; sampling alternates tables
    // during rehash. Insertions/migration can postpone a node to a later cycle.
    // There is no strict global iteration order or exactly-once global cycle.
    //
    // The visitor receives Value* by value and may mutate the pointed-to Value,
    // but must NOT delete it, insert/assign/extract/erase, clear/compact, or
    // recursively sample/mutate this map. Copy expired keys and
    // erase them AFTER this method returns. Do not retain borrowed key views.
    // Read calls from the visitor are allowed; migration is paused until the
    // sampling call finishes. This does not permit concurrent thread access.
    //
    // limit bounds visitor calls, not elapsed time: iterator advancement may
    // scan empty buckets. This is not a hard real-time maintenance budget.
    template <typename Visitor>
    std::size_t sample_next(std::size_t limit, Visitor&& visitor) {
        assert(!callback_active_);
        const std::size_t count = std::min(limit, size_);
        std::size_t current_left = current_->size;
        std::size_t old_left = old_ ? old_->size : 0;

        {
            CallbackScope scope(callback_active_);
            for (std::size_t i = 0; i < count; ++i) {
                Table* table;
                if (old_left != 0 && (current_left == 0 || sample_old_next_)) {
                    table = old_.get();
                    --old_left;
                    sample_old_next_ = false;
                } else {
                    assert(current_left != 0);
                    table = current_.get();
                    --current_left;
                    sample_old_next_ = true;
                }

                if (table->sample_cursor == table->set.end()) {
                    table->sample_cursor = table->set.begin();
                }
                Entry* entry = &*table->sample_cursor;
                // Advance before invoking the visitor, including if it throws.
                ++table->sample_cursor;
                invoke_accounted(entry->value, visitor, entry->key(),
                                 static_cast<Value*>(entry->value));
            }
        }

        maintenance_one();
        return count;
    }

    template <typename Visitor>
    std::size_t sample_next_10(Visitor&& visitor) {
        return sample_next(10, std::forward<Visitor>(visitor));
    }

    // Retains current buckets for reuse; does not destroy borrowed Values.
    void clear() noexcept {
        clear_and_dispose([](Value*) noexcept {});
    }

    template <typename Disposer>
    void clear_and_dispose(Disposer&& disposer) noexcept {
        assert(!callback_active_);
        static_assert(std::is_nothrow_invocable_v<Disposer&, Value*>);
        destroy_entries_in(*current_, disposer);
        if (old_) {
            destroy_entries_in(*old_, disposer);
            old_.reset();
        }
        size_ = 0;
        value_bytes_ = 0;
        sample_old_next_ = false;
        assert(entry_bytes_ == 0);
    }

    // Explicit O(entries + buckets) maintenance. May temporarily require both
    // existing and replacement storage. Entry/key/Value addresses stay stable.
    // All allocations precede mutation. Sampling restarts on the new table.
    void compact() {
        assert(!callback_active_);
        std::size_t cap = kInitialCapacity;
        while (size_ > cap - cap / 4) {
            cap = doubled(cap);
        }
        auto replacement = make_table(cap);
        move_all_entries(*current_, *replacement);
        if (old_) {
            move_all_entries(*old_, *replacement);
            old_.reset();
        }
        current_ = std::move(replacement);
        sample_old_next_ = false;
    }

    // Moves at most one node, never grows. Returns false during callbacks.
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
        return {reinterpret_cast<const std::byte*>(key.data()), key.size()};
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
    static LookupKey make_lookup(const ItemKey& key) {
        const key_view view = bytes(key.key);
        validate_key(view);
        return {view, key.hash};
    }

    template <typename Mutator>
    bool mutate_value_impl(const LookupKey& lookup, Mutator&& mutator) {
        assert(!callback_active_);
        Entry* entry = find_entry_raw(lookup);
        if (entry == nullptr) {
            return false;
        }
        {
            CallbackScope scope(callback_active_);
            invoke_accounted(entry->value, std::forward<Mutator>(mutator),
                             static_cast<Value*>(entry->value));
        }
        maintenance_one();
        return true;
    }

    bool refresh_value_memory_impl(const LookupKey& lookup, std::size_t previous_bytes) {
        assert(!callback_active_);
        Entry* entry = find_entry_raw(lookup);
        if (entry == nullptr) {
            return false;
        }
        replace_value_charge(previous_bytes, estimate_value_bytes(entry->value));
        return true;
    }

    bool insert_impl(const LookupKey& lookup, Value* value) {
        assert(!callback_active_);
        if (find_entry_raw(lookup) != nullptr) {
            maintenance_one();
            return false;
        }
        prepare_insert();
        Entry* entry = create_entry(lookup, value);
        link_new_entry(entry);
        maintenance_one();
        return true;
    }

    std::pair<Value*, bool> insert_or_assign_impl(const LookupKey& lookup, Value* value) {
        assert(!callback_active_);
        if (Entry* existing = find_entry_raw(lookup)) {
            const std::size_t previous_bytes = estimate_value_bytes(existing->value);
            Value* previous = std::exchange(existing->value, value);
            replace_value_charge(previous_bytes, estimate_value_bytes(value));
            maintenance_one();
            return {previous, false};
        }
        prepare_insert();
        Entry* entry = create_entry(lookup, value);
        link_new_entry(entry);
        maintenance_one();
        return {nullptr, true};
    }

    Value* peek_impl(const LookupKey& lookup) const {
        const Entry* entry = find_entry_raw(lookup);
        return entry != nullptr ? entry->value : nullptr;
    }

    bool contains_impl(const LookupKey& lookup) {
        const bool found = find_entry_raw(lookup) != nullptr;
        maintenance_one();
        return found;
    }

    std::pair<Value*, bool> extract_impl(const LookupKey& lookup) {
        assert(!callback_active_);
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

    static TablePtr make_table(std::size_t capacity) {
        return std::make_unique<Table>(capacity);
    }

    void prepare_insert() {
        const std::size_t threshold = current_->capacity - current_->capacity / 4;
        if (!old_ && size_ >= threshold - 1) {
            // Allocate first: an allocation failure must leave current_ valid.
            auto replacement = make_table(doubled(current_->capacity));
            old_ = std::move(current_);
            current_ = std::move(replacement);
            // The old table keeps its cursor. The new table starts at begin().
        }
    }

    static Entry* create_entry(const LookupKey& lookup, Value* value) {
        const std::size_t allocation = sizeof(Entry) + 1 + lookup.key.size();
        void* storage = ::operator new(allocation);
        auto* entry = ::new (storage) Entry(
            lookup.hash, value, static_cast<std::uint8_t>(lookup.key.size()));
        if (!lookup.key.empty()) {
            std::memcpy(entry->mutable_key_data(), lookup.key.data(), lookup.key.size());
        }
        return entry;
    }

    void link_new_entry(Entry* entry) noexcept {
        auto [it, inserted] = current_->set.insert(*entry);
        (void)it;
        assert(inserted);
        (void)inserted;
        ++current_->size;
        ++size_;
        entry_bytes_ += entry->allocation_size();
        value_bytes_ += estimate_value_bytes(entry->value);
    }

    void destroy_entry(Entry* entry) noexcept {
        const std::size_t allocation = entry->allocation_size();
        assert(entry_bytes_ >= allocation);
        entry_bytes_ -= allocation;
        std::destroy_at(entry);
        ::operator delete(entry);
    }

    static std::size_t estimate_value_bytes(const Value* value) noexcept {
        return value != nullptr ? value->memory_usage() : 0;
    }

    void replace_value_charge(std::size_t previous_bytes, std::size_t updated_bytes) noexcept {
        assert(value_bytes_ >= previous_bytes);
        value_bytes_ -= previous_bytes;
        value_bytes_ += updated_bytes;
    }

    // The previous charge lives only on the stack for the duration of a
    // callback. Nothing is cached in Entry and this helper does not allocate.
    template <typename Callback, typename... Args>
    void invoke_accounted(Value* value, Callback&& callback, Args&&... args) {
        const std::size_t previous_bytes = estimate_value_bytes(value);
        try {
            std::invoke(std::forward<Callback>(callback), std::forward<Args>(args)...);
        } catch (...) {
            replace_value_charge(previous_bytes, estimate_value_bytes(value));
            throw;
        }
        replace_value_charge(previous_bytes, estimate_value_bytes(value));
    }

    const Entry* find_entry_raw(const LookupKey& lookup) const noexcept {
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

    Entry* find_entry_raw(const LookupKey& lookup) noexcept {
        return const_cast<Entry*>(std::as_const(*this).find_entry_raw(lookup));
    }

    // Every node-removal path must go through this helper. If the node is the
    // next sampling target, advance BEFORE erasing or migrating that node.
    static void unlink_entry(Table& table, set_iterator it) noexcept {
        if (table.sample_cursor == it) {
            ++table.sample_cursor;
        }
        table.set.erase(it);
        --table.size;
    }

    std::pair<Value*, bool> extract_from(Table& table, const LookupKey& lookup) noexcept {
        auto it = table.set.find(lookup, NodeHash{}, NodeEqual{});
        if (it == table.set.end()) {
            return {nullptr, false};
        }
        Entry* entry = &*it;
        Value* value = entry->value;
        const std::size_t value_charge = estimate_value_bytes(value);
        unlink_entry(table, it);
        assert(value_bytes_ >= value_charge);
        value_bytes_ -= value_charge;
        destroy_entry(entry);
        return {value, true};
    }

    static void move_one_entry(Table& from, Table& to) noexcept {
        auto it = from.set.begin();
        Entry& entry = *it;
        unlink_entry(from, it);
        auto [new_it, inserted] = to.set.insert(entry);
        (void)new_it;
        assert(inserted);
        (void)inserted;
        ++to.size;
    }

    static void move_all_entries(Table& from, Table& to) noexcept {
        // No need to advance a cursor on a table that is being drained.
        from.sample_cursor = from.set.end();
        while (from.size != 0) {
            move_one_entry(from, to);
        }
    }

    bool migrate_one_node() noexcept {
        if (callback_active_ || !old_) {
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
    void destroy_entries_in(Table& table, Disposer& disposer) noexcept {
        // clear_and_dispose() resets the aggregate charge to zero afterwards;
        // clearing does not need to ask each Value for its memory usage.
        table.sample_cursor = table.set.end();
        while (table.size != 0) {
            auto it = table.set.begin();
            Entry* entry = &*it;
            Value* value = entry->value;
            unlink_entry(table, it);
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
    bool sample_old_next_ = false;
    bool callback_active_ = false;
};

} // namespace fastest
