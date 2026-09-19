#pragma once

#include <cachelib/allocator/CacheAllocator.h>
#include <cachelib/allocator/CacheTraits.h>

#include <cstddef>
#include <cstdint>
#include <string>
#include <string_view>
#include <variant>

namespace cache {
    class CoreCache final {
    public:
        enum class Policy : uint8_t {
            LRU,
            TinyLFU,
        };

        struct Config {
            Policy policy{Policy::LRU};
            size_t cacheSizeBytes{1024ULL * 1024 * 1024};
            std::string cacheName{"dynamic_cache"};
            size_t estimatedEntries{0};
        };

        CoreCache();

        explicit CoreCache(const Config &config);

        ~CoreCache() = default;

        CoreCache(const CoreCache &) = delete;

        CoreCache &operator=(const CoreCache &) = delete;

        CoreCache(CoreCache &&) = delete;

        CoreCache &operator=(CoreCache &&) = delete;

        [[nodiscard]] bool set(std::string_view key, const void *data, size_t size);

        [[nodiscard]] bool get(std::string_view key, void *buffer, size_t &size);

        [[nodiscard]] bool remove(std::string_view key);

    private:
        using LruCache = facebook::cachelib::CacheAllocator<facebook::cachelib::LruCacheTrait>;
        using TinyLfuCache = facebook::cachelib::CacheAllocator<facebook::cachelib::TinyLFUCacheTrait>;

        template<typename CacheT>
        struct State {
            explicit State(const Config &config);

            CacheT cache;
            facebook::cachelib::PoolId poolId;
        };

        using LruState = State<LruCache>;
        using TinyLfuState = State<TinyLfuCache>;
        using StateVariant = std::variant<LruState, TinyLfuState>;

        static StateVariant makeState(const Config &config);

        template<typename F>
        decltype(auto) dispatch(F &&f);

        StateVariant state_;
    };
} // namespace cache
