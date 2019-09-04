/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <vector>

#include <folly/Synchronized.h>
#include <folly/ThreadLocal.h>
#include <folly/container/F14Map.h>
#include <folly/stats/BucketedTimeSeries-defs.h>
#include <folly/stats/BucketedTimeSeries.h>

#include "logdevice/common/ClientID.h"

namespace facebook { namespace logdevice {

template <typename K, typename V, typename H = std::hash<K>>
class TimeSeriesMap {
  using TimeSeriesMapType = TimeSeriesMap<K, V, H>;

 public:
  explicit TimeSeriesMap() {}

  explicit TimeSeriesMap(K key, V value) : map_{{key, value}} {}

  const folly::F14FastMap<K, V, H>& data() const {
    return map_;
  }

  void operator+=(const TimeSeriesMapType& other) {
    for (auto& p : other.map_) {
      map_[p.first] += p.second;
    }
  }

  void operator-=(const TimeSeriesMapType& other) {
    for (auto& p : other.map_) {
      auto it = map_.find(p.first);
      if (it == map_.end()) {
        ld_check(!p.second);
        continue;
      }

      if (it->second == p.second) {
        map_.erase(it);
      } else {
        it->second -= p.second;
      }
    }
  }

  friend TimeSeriesMapType operator*(const TimeSeriesMapType& map,
                                     float scale) {
    folly::F14FastMap<K, V, H> new_map{map.map_};
    for (auto& p : new_map) {
      p.second *= scale;
    }

    return TimeSeriesMapType{std::move(new_map)};
  }

 private:
  explicit TimeSeriesMap(folly::F14FastMap<K, V, H>&& map)
      : map_{std::move(map)} {}

  folly::F14FastMap<K, V, H> map_;
};

// the stats received by the node from the clients
struct PerClientNodeTimeSeriesStats {
  struct Key {
    ClientID client_id;
    NodeID node_id;

    struct Hash {
      std::size_t operator()(const Key& key) const {
        static_assert(sizeof(ClientID) == 4 && sizeof(NodeID) == 4,
                      "Please update this hash function.");
        return folly::hash::twang_mix64(
            folly::to<uint64_t>(key.client_id.getIdx()) << 32 |
            folly::to<uint64_t>(key.node_id.index()));
      }
    };

   private:
    friend bool operator==(const Key& a, const Key& b);
    friend bool operator!=(const Key& a, const Key& b);
  };

  struct Value {
    uint32_t successes;
    uint32_t failures;

    explicit Value() noexcept : successes{0}, failures{0} {}
    explicit Value(uint32_t successes, uint32_t failures) noexcept
        : successes{successes}, failures{failures} {}

    void operator+=(const Value& value) {
      successes += value.successes;
      failures += value.failures;
    }

    void operator-=(const Value& value) {
      ld_check(value.successes <= successes);
      ld_check(value.failures <= failures);
      successes -= value.successes;
      failures -= value.failures;
    }

    void operator*=(float scale) {
      successes = folly::to<uint32_t>(std::lround(scale * successes));
      failures = folly::to<uint32_t>(std::lround(scale * failures));
    }

    bool operator!() const {
      return successes == 0 && failures == 0;
    }

    friend bool operator==(const Value& lhs, const Value& rhs);
  };

  struct ClientNodeValue {
    ClientID client_id;
    NodeID node_id;
    Value value;

    explicit ClientNodeValue(ClientID client_id, NodeID node_id, Value value)
        : client_id{client_id}, node_id{node_id}, value{value} {}
  };

  using TimePoint = std::chrono::steady_clock::time_point;
  using TimeSeries =
      folly::BucketedTimeSeries<TimeSeriesMap<Key, Value, Key::Hash>,
                                std::chrono::steady_clock>;
  /**
   * @params retention_time   The duration that the time series will track stats
   */
  explicit PerClientNodeTimeSeriesStats(
      std::chrono::milliseconds retention_time);

  void append(ClientID client,
              NodeID node,
              uint32_t successes,
              uint32_t failures,
              TimePoint time = std::chrono::steady_clock::now());

  /**
   * Sums over all nodes in the map of this worker, in the time span [from, to)
   * Sum over all nodes in a single function to reduce the overhead of locking
   * the map for each individual node in the map.
   *
   * NOTE:
   * updateCurrentTime should be called before calling any of these functions to
   * ensure that stale data is not read
   */
  std::vector<ClientNodeValue> sum(TimePoint from, TimePoint to) const;
  std::vector<ClientNodeValue> sum() const;

  /**
   * Updates the time series to use /current_time/ as the most recent time, and
   * will discard any older values.
   */
  void updateCurrentTime(TimePoint current_time);

  /**
   * will create a new time series with the updated retention time, and then
   * transfer all the old values to the new time series.
   * It's a NOP if the retention time is the same as it was previously
   */
  void updateRetentionTime(std::chrono::milliseconds retention_time);

  // getters used for testing
  std::chrono::milliseconds retentionTime() const;

  TimeSeries* timeseries() const;

  void reset();

 private:
  std::vector<ClientNodeValue>
  processStats(const TimeSeriesMap<Key, Value, Key::Hash>& total) const;

  std::chrono::milliseconds retention_time_;

  std::unique_ptr<TimeSeries> timeseries_;
};

class BoycottingStatsHolder final {
  using Stats = folly::Synchronized<PerClientNodeTimeSeriesStats>;

 public:
  explicit BoycottingStatsHolder(const std::chrono::milliseconds& retention)
      : retention_(retention) {}

  Stats* get() {
    auto* stats = stats_.get();
    if (!stats) {
      stats = new Stats(PerClientNodeTimeSeriesStats(retention_));
      stats_.reset(stats);
    }
    return stats;
  }

  template <typename Func>
  void runForEach(const Func& func) {
    auto accessor = stats_.accessAllThreads();
    for (auto& stats : accessor) {
      func(stats);
    }
  }

  void updateRetentionTime(std::chrono::milliseconds retention) {
    auto accessor = stats_.accessAllThreads();
    if (retention_ == retention) { // using accessor's mutex
      return;
    }
    retention_ = retention;
    for (auto& stats : accessor) {
      stats.wlock()->updateRetentionTime(retention);
    }
  }

 private:
  std::chrono::milliseconds retention_;
  class Tag {};
  folly::ThreadLocalPtr<Stats, Tag> stats_;
};

inline void perClientNodeStatAdd(BoycottingStatsHolder* holder,
                                 ClientID client_id,
                                 NodeID node_id,
                                 uint32_t success,
                                 uint32_t failure) {
  if (holder) {
    holder->get()->wlock()->append(client_id, node_id, success, failure);
  }
}

}} // namespace facebook::logdevice
