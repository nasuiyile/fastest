#include "core_cache.h"

#include <folly/Range.h>

#include <cstring>
#include <limits>
#include <stdexcept>
#include <utility>

namespace cache {
    namespace {
        inline folly::StringPiece toCacheKey(std::string_view key) noexcept {
            return {key.data(), key.size()};
        }

        void validateConfig(const CoreCache::Config &config) {
            if (config.cacheSizeBytes == 0) {
                throw std::invalid_argument("CoreCache: cacheSizeBytes must be > 0");
            }
            if (config.cacheName.empty()) {
                throw std::invalid_argument("CoreCache: cacheName must not be empty");
            }
        }

        template<typename CacheT>
        typename CacheT::Config makeCacheConfig(const CoreCache::Config &config) {
            typename CacheT::Config cacheConfig;
            cacheConfig.setCacheSize(config.cacheSizeBytes).setCacheName(config.cacheName);
            if (config.estimatedEntries != 0) {
                cacheConfig.setAccessConfig(config.estimatedEntries);
            }
            cacheConfig.validate();
            return cacheConfig;
        }

        template<typename CacheT>
        bool setImpl(CacheT &cache, facebook::cachelib::PoolId poolId, std::string_view key, const void *data,
                     size_t size) {
            if (key.empty() || size > std::numeric_limits<uint32_t>::max() || (size != 0 && data == nullptr)) {
                return false;
            }
            auto handle = cache.allocate(poolId, toCacheKey(key), static_cast<uint32_t>(size));
            if (!handle) {
                return false;
            }
            if (size != 0) {
                std::memcpy(handle->getMemory(), data, size);
            }
            cache.insertOrReplace(handle);
            return true;
        }

        template<typename CacheT>
        bool getImpl(CacheT &cache, std::string_view key, void *buffer, size_t &size) {
            if (key.empty()) {
                size = 0;
                return false;
            }

            auto handle = cache.find(toCacheKey(key));
            if (!handle) {
                size = 0;
                return false;
            }

            const size_t valueSize = static_cast<size_t>(handle->getSize());
            const size_t capacity = size;

            size = valueSize;

            if (capacity < valueSize || (valueSize != 0 && buffer == nullptr)) {
                return false;
            }

            if (valueSize != 0) {
                std::memcpy(buffer, handle->getMemory(), valueSize);
            }

            return true;
        }

        template<typename CacheT>
        bool removeImpl(CacheT &cache, std::string_view key) {
            if (key.empty()) {
                return false;
            }

            using RemoveRes = typename CacheT::RemoveRes;
            return cache.remove(toCacheKey(key)) == RemoveRes::kSuccess;
        }
    } // namespace

    template<typename CacheT>
    CoreCache::State<CacheT>::State(const Config &config)
        : cache(makeCacheConfig<CacheT>(config)),
          poolId(cache.addPool("default", cache.getCacheMemoryStats().ramCacheSize)) {
    }

    CoreCache::StateVariant CoreCache::makeState(const Config &config) {
        validateConfig(config);
        switch (config.policy) {
            case Policy::LRU:
                return StateVariant{std::in_place_type<LruState>, config};
            case Policy::TinyLFU:
                return StateVariant{std::in_place_type<TinyLfuState>, config};
        }
        throw std::invalid_argument("CoreCache: unsupported cache policy");
    }

    template<typename F>
    decltype(auto) CoreCache::dispatch(F &&f) {
        switch (state_.index()) {
            case 0:
                return std::forward<F>(f)(std::get<0>(state_));
            case 1:
                return std::forward<F>(f)(std::get<1>(state_));
            default:
                std::terminate();
        }
    }

    CoreCache::CoreCache()
        : CoreCache(Config{}) {
    }

    CoreCache::CoreCache(const Config &config)
        : state_(makeState(config)) {
    }

    bool CoreCache::set(std::string_view key, const void *data, size_t size) {
        return dispatch([&](auto &state) {
            return setImpl(state.cache, state.poolId, key, data, size);
        });
    }

    bool CoreCache::get(std::string_view key, void *buffer, size_t &size) {
        return dispatch([&](auto &state) {
            return getImpl(state.cache, key, buffer, size);
        });
    }

    bool CoreCache::remove(std::string_view key) {
        return dispatch([&](auto &state) {
            return removeImpl(state.cache, key);
        });
    }
} // namespace cache
