/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <string>
#include <thread>
#include <unordered_map>

#include "logdevice/common/stats/Histogram.h"
#include "logdevice/common/types_internal.h"
#include "logdevice/common/util.h"

namespace facebook { namespace logdevice {

/**
 * Maintain a mapping of histogram name to an object of type T. T can either be
 * a HistogramInterface or a ShardedHistogram.
 *
 * In order to create a bundle, inherit from this class and implement the
 * getMap() function:
 *
 * struct MyHistograms : public HistogramBundleBase<HistogramInterface> {
 *  MapType getMap() override {
 *    return { { "append_latency", &append_latency },
 *             { "store_latency", &store_latency   } };
 *  }
 *  LatencyHistogram append_latency;
 *  LatencyHistogram store_latency;
 * };
 *
 */
template <typename T>
struct HistogramBundleBase {
  using MapType = std::unordered_map<std::string, T*>;

  HistogramBundleBase() {}
  HistogramBundleBase(const HistogramBundleBase& /*rhs*/) {
    // Keep mapInitState_ == NOT_INITIALIZED, and empty map_.
    // Default copy constructor in subclasses should work.
  }

  virtual ~HistogramBundleBase<T>() {}

  HistogramInterface* get(const std::string& name, shard_index_t shard) {
    auto it = map().find(name);
    if (it == map().end()) {
      return nullptr;
    }
    return it->second->get(shard);
  }

  T* find(const std::string& name) {
    auto it = map().find(name);
    if (it == map().end()) {
      return nullptr;
    }
    return it->second;
  }

  void merge(HistogramBundleBase<T> const& other);
  void subtract(HistogramBundleBase<T> const& other);
  void clear();
  const MapType& map() const;

 protected:
  virtual MapType getMap() = 0;

  // This makes subclasses of HistogramBundleBase copyable by default,
  // while keeping HistogramBundleBase itself non-copyable
  // (can't meaningfully copy it without knowing the exact type).
  HistogramBundleBase& operator=(const HistogramBundleBase&) {
    // Clear map_ to make the next map() call reinitialize it with pointers
    // valid for this instance.
    mapInitState_ = MapInitState::NOT_INITIALIZED;
    map_.clear();
    return *this;
  }

 private:
  enum class MapInitState {
    NOT_INITIALIZED,
    INITIALIZING,
    INITIALIZED,
  };

  mutable const HistogramBundleBase* self_{nullptr};
  mutable std::atomic<MapInitState> mapInitState_{
      MapInitState::NOT_INITIALIZED};
  mutable MapType map_;
};

class ShardedHistogramBase {
 public:
  virtual HistogramInterface* get(shard_index_t shard) = 0;
  virtual const HistogramInterface* get(shard_index_t shard) const = 0;
  virtual shard_size_t getNumShards() const = 0;
  virtual void merge(ShardedHistogramBase const& other) = 0;
  virtual void subtract(ShardedHistogramBase const& other) = 0;
  virtual void clear() = 0;
  virtual ~ShardedHistogramBase() {}
};

template <typename HistogramType>
class ShardedHistogram : public ShardedHistogramBase {
 public:
  explicit ShardedHistogram() {}

  const HistogramInterface* get(shard_index_t shard) const override {
    return const_cast<ShardedHistogram<HistogramType>*>(this)->get(shard);
  }

  // Because Histograms use quite a lot of memory, we avoid allocating memory
  // for MAX_SHARDS histograms and instead lazily allocate them.
  HistogramInterface* get(shard_index_t shard) override {
    if (shard >= MAX_SHARDS) {
      return nullptr;
    }

    ShardEntry& entry = hists_[shard];
    InitState state = entry.initState_.load();
    if (state == InitState::NOT_INITIALIZED) {
      // No one initialized the entry for this shard yet.
      // Let's try to do it ourselves.
      if (entry.initState_.compare_exchange_weak(
              state, InitState::INITIALIZING)) {
        // Acquired the lock. Take care of the initialization.
        entry.hist = std::make_unique<HistogramType>();
        entry.initState_.store(InitState::INITIALIZED);
        state = InitState::INITIALIZED;
        const shard_size_t s = shard + 1;
        atomic_fetch_max(num_shards_, s);
      }
    }
    ld_check(state != InitState::NOT_INITIALIZED);
    while (state == InitState::INITIALIZING) {
      // If we lost the race, busy-wait for another thread to finish
      // initializing.
      std::this_thread::yield();
      state = entry.initState_.load();
    }
    ld_check(state == InitState::INITIALIZED);
    ld_check(entry.hist);
    return entry.hist.get();
  }

  void add(shard_index_t shard, int64_t v) {
    HistogramInterface* h = get(shard);
    if (h) {
      h->add(v);
    }
  }

  shard_size_t getNumShards() const override {
    return num_shards_.load();
  }

  void merge(ShardedHistogramBase const& other) override;
  void subtract(ShardedHistogramBase const& other) override;
  void clear() override;

  ShardedHistogram& operator=(const ShardedHistogram& other) {
    const size_t n = other.num_shards_.load();
    for (size_t i = 0; i < n; ++i) {
      get(i)->assign(*other.get(i));
    }
    for (size_t i = n; i < num_shards_.load(); ++i) {
      hists_[i].hist.reset();
      hists_[i].initState_.store(InitState::NOT_INITIALIZED);
    }
    num_shards_ = n;
    return *this;
  }

 private:
  std::atomic<shard_size_t> num_shards_{0};

  enum class InitState {
    NOT_INITIALIZED,
    INITIALIZING,
    INITIALIZED,
  };

  struct ShardEntry {
    std::unique_ptr<HistogramType> hist;
    std::atomic<InitState> initState_{InitState::NOT_INITIALIZED};
  };

  std::array<ShardEntry, MAX_SHARDS> hists_;
};

using HistogramBundle = HistogramBundleBase<HistogramInterface>;
using PerShardHistogramBundle = HistogramBundleBase<ShardedHistogramBase>;

template <typename T>
void HistogramBundleBase<T>::merge(HistogramBundleBase const& other) {
  for (auto& i : map()) {
    auto it = other.map().find(i.first);
    ld_check(it != other.map().end());
    i.second->merge(*it->second);
  }
}

template <typename T>
void HistogramBundleBase<T>::subtract(HistogramBundleBase const& other) {
  for (auto& i : map()) {
    auto it = other.map().find(i.first);
    ld_check(it != other.map().end());
    i.second->subtract(*it->second);
  }
}

template <typename T>
void HistogramBundleBase<T>::clear() {
  for (auto& i : map()) {
    i.second->clear();
  }
}

template <typename T>
const typename HistogramBundleBase<T>::MapType&
HistogramBundleBase<T>::map() const {
  MapInitState state = mapInitState_.load();

  if (state == MapInitState::NOT_INITIALIZED) {
    // No one initialized map_ yet. Let's try to do it ourselves.
    if (mapInitState_.compare_exchange_weak(
            state, MapInitState::INITIALIZING)) {
      // Acquired the lock. Initialize map_.
      map_ = const_cast<HistogramBundleBase<T>*>(this)->getMap();
      self_ = this;

      state = MapInitState::INITIALIZING;
      bool ok = mapInitState_.compare_exchange_strong(
          state, MapInitState::INITIALIZED);
      ld_check(ok);
      state = MapInitState::INITIALIZED;
    } else {
      // We lost the race. Some other thread is initializing map_ now.
    }
  }

  ld_check(state != MapInitState::NOT_INITIALIZED);

  while (state == MapInitState::INITIALIZING) {
    // Busy-wait for another thread to finish initializing map_.
    std::this_thread::yield();
    state = mapInitState_.load();
  }

  ld_check(state == MapInitState::INITIALIZED);

  // This assert is the only use of self_. Check that `this` is the
  // same object from which we made the map_. This can fail if a subclass
  // implements operator=() that doesn't call
  // HistogramBundleBase::operator=(). The fix is to call it.
  ld_check(self_ == this);

  return map_;
}

template <typename HistogramType>
void ShardedHistogram<HistogramType>::merge(ShardedHistogramBase const& other) {
  ShardedHistogram const& o = static_cast<ShardedHistogram const&>(other);
  for (shard_index_t i = 0; i < o.getNumShards(); ++i) {
    get(i)->merge(*o.get(i));
  }
}

template <typename HistogramType>
void ShardedHistogram<HistogramType>::subtract(
    ShardedHistogramBase const& other) {
  ShardedHistogram const& o = static_cast<ShardedHistogram const&>(other);
  for (shard_index_t i = 0; i < o.getNumShards(); ++i) {
    get(i)->subtract(*o.get(i));
  }
}

template <typename HistogramType>
void ShardedHistogram<HistogramType>::clear() {
  for (shard_index_t i = 0; i < getNumShards(); ++i) {
    hists_[i].hist->clear();
  }
}

}} // namespace facebook::logdevice
