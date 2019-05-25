/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <array>
#include <chrono>
#include <cstdint>
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>
#include <vector>

#include <folly/Conv.h>
#include <folly/Optional.h>
#include <folly/Synchronized.h>
#include <folly/ThreadLocal.h>
#include <folly/container/F14Map.h>

#include "logdevice/common/ClientID.h"
#include "logdevice/common/Priority.h"
#include "logdevice/common/RequestType.h"
#include "logdevice/common/StorageTask-enums.h"
#include "logdevice/common/UpdateableSharedPtr.h"
#include "logdevice/common/configuration/NodeLocation.h"
#include "logdevice/common/configuration/TrafficClass.h"
#include "logdevice/common/protocol/MessageType.h"
#include "logdevice/common/stats/StatsCounter.h"
#include "logdevice/common/types_internal.h"
#include "logdevice/common/util.h"
// Think twice before adding new includes here!  This file is included in many
// translation units and increasing its transitive dependency footprint will
// slow down the build.  We use forward declaration and the pimpl idiom to
// offload most header inclusion to the .cpp file; scroll down for details.

namespace folly {
template <typename TT>
class LegacyStatsClock;
template <typename VT, typename CT>
class MultiLevelTimeSeries;
template <typename VT, typename CT>
class BucketedTimeSeries;
} // namespace folly

namespace facebook { namespace logdevice {

/**
 * @file  This file contains classes and macros used to maintain server-wide
 *        stats counters for LogDevice.
 */

class LatencyHistogram;
class HistogramInterface;
struct ClientHistograms;
struct PerShardHistograms;
struct ServerHistograms;

/**
 * How to combine two Stats objects.
 */
enum class StatsAgg {
  // Use the aggregation type from the *stats.inc file. The most common is SUM,
  // which adds the two values together.
  SUM,
  // Subtract the two values, ignoring the aggregation type in *stats.inc file.
  SUBTRACT,
  // Used for operator=
  ASSIGN,
  // Take maximum of the two values.
  MAX,
};

using StatsAggOptional = folly::Optional<StatsAgg>;

/**
 * Keeps track the rate of appends/reads/etc. for a log group.
 */
struct PerLogTimeSeries {
  PerLogTimeSeries(size_t num_buckets,
                   std::vector<std::chrono::milliseconds> time_intervals);
  ~PerLogTimeSeries();

  void addValue(size_t n);

  // Keeps track of values over time intervals such as the last 1
  // or 10 minutes.  (See timeIntervals_ for exact intervals tracked.)
  //
  // NOTE: This is *not* aggregated across threads nor published to ODS.  The
  // 'stats throughput <name>' admin command collects this from all threads
  // and aggregates on demand.
  //
  // NOTE: Using millisecond granularity to allow folly::BucketedTimeSeries to
  // more accurately estimate the rate for partial buckets
  const std::vector<std::chrono::milliseconds> timeIntervals_;

  using TimeSeries = folly::MultiLevelTimeSeries<
      int64_t,
      folly::LegacyStatsClock<std::chrono::milliseconds>>;
  std::unique_ptr<TimeSeries> timeSeries_;
};

struct CustomCountersTimeSeries {
  CustomCountersTimeSeries();
  void addCustomCounters(const std::map<uint8_t, int64_t>& counters);

  // Keeps track of user provided counters in appends over time intervals such
  // as the last 1 or 10 minutes.  (See .cpp for exact intervals tracked.)
  //
  // NOTE: This is *not* aggregated across threads nor published to ODS.  The
  // 'stats custom counters' admin command collects this from all threads
  // and aggregates on demand.

  using TimeSeries = folly::MultiLevelTimeSeries<
      int64_t,
      folly::LegacyStatsClock<std::chrono::milliseconds>>;
  using CustomCounters = std::unordered_map<uint8_t, TimeSeries>;
  std::unique_ptr<CustomCounters> customCountersTimeSeries_;

  static const std::vector<std::chrono::milliseconds>&
  getCustomCounterIntervals();

  static constexpr int NUM_BUCKETS = 10;
};

/**
 * Stats class contains tracked stat counters / time series specific for a log
 * group.
 */
struct PerLogStats {
  /**
   * Add or subtract most values from @param other.
   */
  void aggregate(PerLogStats const& other, StatsAggOptional agg_override);
#define STAT_DEFINE(name, _) StatsCounter name{};
#include "logdevice/common/stats/per_log_stats.inc" // nolint

  // Per-log-group time series
#define TIME_SERIES_DEFINE(name, _, __, ___) \
  std::shared_ptr<PerLogTimeSeries> name;
#include "logdevice/common/stats/per_log_time_series.inc" // nolint

  std::shared_ptr<CustomCountersTimeSeries> custom_counters;
  // Mutex almost exclusively locked by one thread since PerLogStats objects
  // are contained in thread-local stats
  std::mutex mutex;
};

struct PerTrafficClassStats {
  PerTrafficClassStats() {}

  /**
   * Add values from @param other.
   */
  void aggregate(PerTrafficClassStats const& other,
                 StatsAggOptional agg_override);

  /**
   * Reset most counters to their initial values.
   */
  void reset();

#define STAT_DEFINE(name, _) StatsCounter name{};
#include "logdevice/common/stats/per_traffic_class_stats.inc" // nolint
};

struct PerShapingPriorityStats {
  PerShapingPriorityStats();
  ~PerShapingPriorityStats();

  PerShapingPriorityStats(const PerShapingPriorityStats&) = delete;
  PerShapingPriorityStats& operator=(const PerShapingPriorityStats&) = delete;

  PerShapingPriorityStats(PerShapingPriorityStats&&) noexcept;
  PerShapingPriorityStats& operator=(PerShapingPriorityStats&&) noexcept;

  /**
   * Add values from @param other.
   */
  void aggregate(PerShapingPriorityStats const& other,
                 StatsAggOptional agg_override);

  /**
   * Reset counters to their initial values.
   */
  void reset();

  std::unique_ptr<LatencyHistogram> time_in_queue;

#define STAT_DEFINE(name, _) StatsCounter name{};
#include "logdevice/common/stats/per_msg_priority_stats.inc"    // nolint
#include "logdevice/common/stats/per_priority_stats_common.inc" // nolint
};

struct PerFlowGroupStats {
  PerFlowGroupStats() {}

  /**
   * Add values from @param other.
   */
  void aggregate(PerFlowGroupStats const& other, StatsAggOptional agg_override);

  /**
   * Reset most counters to their initial values.
   */
  void reset();

  /**
   * Aggregates PerShapingPriorityStats across all Priorities.
   */
  PerShapingPriorityStats totalPerShapingPriorityStats() const;

  std::array<PerShapingPriorityStats, asInt(Priority::NUM_PRIORITIES)>
      priorities;

#define STAT_DEFINE(name, _) StatsCounter name{};
#include "logdevice/common/stats/per_flow_group_stats.inc" // nolint
};

struct PerStorageTaskTypeStats {
  PerStorageTaskTypeStats() {}

  /**
   * Add values from @param other.
   */
  void aggregate(PerStorageTaskTypeStats const& other,
                 StatsAggOptional agg_override);

  /**
   * Reset most counters to their initial values.
   */
  void reset();

#define STAT_DEFINE(name, _) StatsCounter name{};
#include "logdevice/common/stats/per_storage_task_type_stats.inc" // nolint
};

// Stats that are collected per shard.
struct PerShardStats {
#define STAT_DEFINE(name, _) StatsCounter name{};
#include "logdevice/common/stats/per_shard_stats.inc" // nolint

  void aggregate(const PerShardStats& other, StatsAggOptional agg_override);

  // Same but with DESTROYING_THREAD defined, i.e. exclude stats which
  // should only be accumulated for living threads.
  void aggregateForDestroyedThread(const PerShardStats& other,
                                   StatsAggOptional agg_override);
  void reset();
};

// Stats that are collected per request type
struct PerRequestTypeStats {
#define STAT_DEFINE(name, _) StatsCounter name{};
#include "logdevice/common/stats/per_request_type_stats.inc" // nolint
  void aggregate(const PerRequestTypeStats& other,
                 StatsAggOptional agg_override);
  void reset();
};

// Stats that are collected per message type
struct PerMessageTypeStats {
#define STAT_DEFINE(name, _) StatsCounter name{};
#include "logdevice/common/stats/per_message_type_stats.inc" // nolint

  void aggregate(const PerMessageTypeStats& other,
                 StatsAggOptional agg_override);
  void reset();
};

struct PerWorkerTimeSeriesStats {
  using TimePoint = std::chrono::steady_clock::time_point;
  using TimeSeries =
      folly::BucketedTimeSeries<uint64_t, std::chrono::steady_clock>;

  // Access to the TimeSeries object need to be threadsafe.
  using SyncedTimeSeries = folly::Synchronized<TimeSeries>;

  /**
   * @params retention_time   How long into the past will the time series retain
   *                          information.
   */
  explicit PerWorkerTimeSeriesStats(std::chrono::milliseconds retention_time);
  PerWorkerTimeSeriesStats(const PerWorkerTimeSeriesStats& other);

  PerWorkerTimeSeriesStats& operator=(const PerWorkerTimeSeriesStats&);

  void addLoad(uint64_t load);

  uint64_t avgLoad(TimePoint from, TimePoint to);

  void aggregate(PerWorkerTimeSeriesStats& other,
                 StatsAggOptional agg_override);

  // defined in .cpp file
  static const int NUM_BUCKETS;

  std::unique_ptr<SyncedTimeSeries> load_stats_;
};

struct PerNodeTimeSeriesStats {
  using TimePoint = std::chrono::steady_clock::time_point;
  using TimeSeries =
      folly::BucketedTimeSeries<uint32_t, std::chrono::steady_clock>;

  // synchronization is required when summing up the time series, because
  // values might be added at the same time. The synchronizedCopy copies
  // shared_ptrs, so the internal datastructure have to be threadsafe
  using SyncedTimeSeries = folly::Synchronized<TimeSeries>;

  /**
   * @param retention_time  How long the time series will track information for
   */
  explicit PerNodeTimeSeriesStats(std::chrono::milliseconds retention_time);

  void
  addAppendSuccess(TimePoint time_of_append = std::chrono::steady_clock::now());
  void
  addAppendFail(TimePoint time_of_append = std::chrono::steady_clock::now());

  /**
   * Sum over the time span between from and to
   * NOTE:
   * updateCurrentTime should be called before calling any of these functions to
   * ensure that stale data is not read
   *
   * @param from The start time
   * @param to   The end time
   * @returns An approximate sum over the last for_duration milliseconds
   */
  uint32_t sumAppendSuccess(TimePoint from, TimePoint to);
  uint32_t sumAppendFail(TimePoint from, TimePoint to);

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

  // getters used in testing
  std::chrono::milliseconds retentionTime() const;
  const SyncedTimeSeries* getAppendSuccess() const;
  const SyncedTimeSeries* getAppendFail() const;

 private:
  // defined in .cpp file
  static const int NUM_BUCKETS;

  // helper function for sumAppendSuccess and sumAppendFail, see their docs for
  // parameters and return type
  uint32_t sumAppend(SyncedTimeSeries* appends, TimePoint from, TimePoint to);

  std::chrono::milliseconds retention_time_;

  // ptrs because folly::BucketedTimeSeries is an incomplete type due to forward
  // declaration
  std::unique_ptr<SyncedTimeSeries> append_success_;
  std::unique_ptr<SyncedTimeSeries> append_fail_;
};

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

class ShardedStats {
 public:
  ShardedStats() {}

  PerShardStats* get(shard_index_t shard) {
    if (shard >= MAX_SHARDS) {
      return nullptr;
    }
    const shard_size_t s = shard + 1;
    atomic_fetch_max(num_shards_, s);
    return &stats_[shard];
  }

  shard_size_t getNumShards() const {
    return num_shards_.load(std::memory_order_relaxed);
  }

  void aggregate(ShardedStats const& other,
                 StatsAggOptional agg_override,
                 bool destroyed_threads = false) {
    for (shard_index_t i = 0; i < other.getNumShards(); ++i) {
      if (destroyed_threads) {
        get(i)->aggregateForDestroyedThread(other.stats_[i], agg_override);
      } else {
        get(i)->aggregate(other.stats_[i], agg_override);
      }
    }
  }

  void reset() {
    for (shard_index_t i = 0; i < getNumShards(); ++i) {
      stats_[i].reset();
    }
  }

 private:
  std::atomic<shard_size_t> num_shards_{0};
  PerShardStats stats_[MAX_SHARDS];
};

struct StatsParams {
  explicit StatsParams() = default;

  bool is_server = false;

  // Used to initialize StatsHolder objects for custom stats, hooked into the
  // client by entities that wrap it, such as LDBench workers.
  enum class StatsSet { DEFAULT = 0, LDBENCH_WORKER, CHARACTERIZE_LOAD };
  StatsSet stats_set{StatsSet::DEFAULT};

  std::string getStatsSetName() const {
    if (is_server) {
      return "server";
    } else {
      switch (stats_set) {
        case StatsSet::LDBENCH_WORKER:
          return "ldbench";
        case StatsSet::CHARACTERIZE_LOAD:
          return "characterize_load";
        case StatsSet::DEFAULT:
          return "client";
      } // let compiler check that all enum values are handled.

      // We'll never get here, but some customers have the compiler configured
      // such that it will complain about the above, so here's a return value:
      ld_check(false);
      return "";
    }
  }

  bool isClientStatsSet() {
    return !is_server && stats_set == StatsSet::DEFAULT;
  }

  // TODO(T40896662) stop supporting this, see task
  folly::Optional<std::string> additional_entity_suffix{folly::none};
  StatsParams& addAdditionalEntitySuffix(std::string suffix) {
    additional_entity_suffix = suffix;
    return *this;
  }

  /**
   * Below are parameters which can be defined in settings
   * The reason for not passing the settings object is to not have Stats depend
   * on Settings
   */
  std::chrono::milliseconds node_stats_retention_time_on_clients =
      std::chrono::seconds(30);

  std::chrono::milliseconds node_stats_retention_time_on_nodes =
      std::chrono::seconds(300);

  std::chrono::milliseconds worker_stats_retention_time =
      std::chrono::seconds(60);

#define TIME_SERIES_DEFINE(name, _, t, buckets)                     \
  std::vector<std::chrono::milliseconds> time_intervals_##name = t; \
  StatsParams& setTimeIntervals_##name(                             \
      std::vector<std::chrono::milliseconds> v) {                   \
    time_intervals_##name = std::move(v);                           \
    return *this;                                                   \
  }                                                                 \
  size_t num_buckets_##name = buckets;                              \
  StatsParams& setNumBuckets_##name(size_t v) {                     \
    num_buckets_##name = v;                                         \
    return *this;                                                   \
  }
#include "logdevice/common/stats/per_log_time_series.inc" // nolint

  // Below here are the setters for the above member variables

  StatsParams& setIsServer(bool is_server) {
    this->is_server = is_server;
    return *this;
  }

  StatsParams&
  setNodeStatsRetentionTimeOnClients(std::chrono::milliseconds duration) {
    node_stats_retention_time_on_clients = duration;
    return *this;
  }

  StatsParams&
  setNodeStatsRetentionTimeOnNodes(std::chrono::milliseconds duration) {
    node_stats_retention_time_on_nodes = duration;
    return *this;
  }

  StatsParams& setWorkerStatsRetentionTime(std::chrono::milliseconds duration) {
    worker_stats_retention_time = duration;
    return *this;
  }

  StatsParams& setStatsSet(StatsSet set) {
    stats_set = set;
    return *this;
  }
};

/**
 * Stats class contains all tracked stats:
 * - Counters defined in server_stats.inc, client_stats.inc and common_stats.inc
 * - Per-message-type and per-request-type counters
 * - Per-log-group stats
 * - Histograms
 * - Per-node stats for append success / fails kept for only a certain time
 *
 * TODO(T40895127): refactor this, such that Stats is abstract and different
 * implementations exist for each set of stats, e.g. server/client/ldbench/etc.
 * Hopefully auto-generate boilerplate code related to importing stats from
 * .inc file.
 */
struct Stats final {
  class EnumerationCallbacks;

  /**
   * Creates client or server stats.
   */
  explicit Stats(const FastUpdateableSharedPtr<StatsParams>* params);

  ~Stats();

  /**
   * Copy constructor and copy-assignment. Thread-safe.
   */
  Stats(const Stats& other);
  Stats& operator=(const Stats& other);

  /**
   * Move constructor and move-assignment. Not thread-safe (but only used in
   * unit tests, so okay).
   */
  Stats(Stats&& other) noexcept(true);
  Stats& operator=(Stats&& other) noexcept(false);

  /**
   * Add all values from @param other.
   */
  void aggregate(Stats const& other,
                 StatsAggOptional agg_override = folly::none);

  /**
   * Same but with DESTROYING_THREAD defined, i.e. exclude stats which
   * should only be accumulated for living threads.
   */
  void aggregateForDestroyedThread(Stats const& other);

  /**
   * Same but only for per-something stats.
   * Only used internally by aggregate() and aggregateForDestroyedThread().
   */
  void aggregateCompoundStats(Stats const& other,
                              StatsAggOptional agg_override,
                              bool destroyed_threads = false);

  /**
   * Reset all counters to their initial values.
   */
  void reset();

  /**
   * Aggregates PerTrafficClassStats across all traffic classes.
   */
  PerTrafficClassStats totalPerTrafficClassStats() const;

  /**
   * Aggregates PerFlowGroupStats across all flow groups.
   */
  PerFlowGroupStats totalPerFlowGroupStats() const;

  /**
   * Aggregates PerShardStats across all shards.
   */
  PerShardStats totalPerShardStats() const;

  /**
   * Aggregate all shards of PerShardHistograms into one. The result is in
   * shard 0 of the returned object.
   */
  std::unique_ptr<PerShardHistograms> totalPerShardHistograms() const;

  /**
   * List all stats, histograms and totals (totalPer*Stats()).
   * This is used in StatsPublisher's and Stats admin command.
   *
   * @param list_all  enumerates stats that aren't being published
   */
  void enumerate(EnumerationCallbacks* cb, bool list_all = true) const;

  /**
   * Calculates derived stats based on non-derived stats.
   * Derived stats are declared in *stats.inc files like the other stats, and
   * are handled the same way. The difference is that their values are set
   * *after* aggregating thread-local stats from all threads (see
   * StatsHolder::aggregate()). This function calculates all derived stats.
   */
  void deriveStats();

  /**
   * Indicates whether we're representing server stats.
   */
  bool isServerStats() const {
    return params->get()->is_server;
  }

  /**
   * Indicates whether we should push stats that have never been non-zero, as
   * this varies depending on what set of stats this is representing.
   */
  bool shouldSkipNeverBumpedStats() const {
    // Only clients don't want this, for now. T40895127 will make this cleaner.
    return params->get()->isClientStatsSet();
  }

  /**
   * Returns the chosen name of this set of stats, which are to be used as ODS
   * key prefix.
   */
  std::string getName() const {
    return params->get()->getStatsSetName();
  }

  /**
   * Returns an additional entity suffix, that stats should also be pushed to.
   * TODO(T40896662): get rid of this.
   */
  folly::Optional<std::string> getAdditionalEntitySuffix() const {
    return params->get()->additional_entity_suffix;
  }

  /**
   * Take a read-lock and make a deep copy of a some map wrapped in a
   * folly::Synchronized, such as per_log_stats.
   */
  template <typename Map>
  auto synchronizedCopy(folly::Synchronized<Map> Stats::*map) const {
    return (this->*map).withRLock([](const auto& locked_map) {
      return std::vector<
          std::pair<typename Map::key_type, typename Map::mapped_type>>(
          locked_map.begin(), locked_map.end());
    });
  }

#define STAT_DEFINE(name, _) StatsCounter name{};
#include "logdevice/common/stats/server_stats.inc" // nolint
#define STAT_DEFINE(name, _) StatsCounter name{};
#include "logdevice/common/stats/common_stats.inc" // nolint

  // per-traffic class stats
  std::array<PerTrafficClassStats, static_cast<int>(TrafficClass::MAX)>
      per_traffic_class_stats = {};

  // per-flow group stats
  // For Network Traffic Shaping
  std::array<PerFlowGroupStats, static_cast<int>(NodeLocation::NUM_ALL_SCOPES)>
      per_flow_group_stats = {};
  // For Read Throttling, we only need 1 SCOPE
  std::array<PerFlowGroupStats, 1> per_flow_group_stats_rt = {};

  // Per-request-type stats
  std::array<PerRequestTypeStats, static_cast<int>(RequestType::MAX)>
      per_request_type_stats = {};

  // Per-message-type stats
  std::array<PerMessageTypeStats, static_cast<int>(MessageType::MAX)>
      per_message_type_stats = {};

  // Per-storage-task-type stats
  std::array<PerStorageTaskTypeStats, static_cast<int>(StorageTaskType::MAX)>
      per_storage_task_type_stats = {};

  // Per-log-group stats
  folly::Synchronized<
      std::unordered_map<std::string, std::shared_ptr<PerLogStats>>>
      per_log_stats;

  // Server histograms. Initialized only on servers.
  std::unique_ptr<ServerHistograms> server_histograms;

  // Per-shard histograms. Initialized only on servers.
  std::unique_ptr<PerShardHistograms> per_shard_histograms;

  // Stats that we keep track of per shard. Initialized only on servers.
  std::unique_ptr<ShardedStats> per_shard_stats;

  // Per-worker stats kept over a specific time span.
  // Only for workers of type GENERAL.
  folly::Synchronized<
      std::unordered_map<worker_id_t,
                         std::shared_ptr<PerWorkerTimeSeriesStats>,
                         worker_id_t::Hash>>
      per_worker_stats;

  // Per-node stats kept over a specific time span
  folly::Synchronized<
      std::unordered_map<NodeID,
                         std::shared_ptr<PerNodeTimeSeriesStats>,
                         NodeID::Hash>>
      per_node_stats;

  // node stats sent from the clients. Keep it in a map to be able to identify
  // the client who sent it.
  folly::Synchronized<PerClientNodeTimeSeriesStats> per_client_node_stats;

  // Client stats go into a separate `client' struct to allow counters with
  // same names as server counters (e.g. `append_success')
  struct ClientStats {
#define STAT_DEFINE(name, _) StatsCounter name{};
#include "logdevice/common/stats/client_stats.inc" // nolint

    ClientStats();
    ~ClientStats();

    ClientStats(const ClientStats&) = delete;
    ClientStats& operator=(const ClientStats&) = delete;

    ClientStats(ClientStats&&) noexcept;
    ClientStats& operator=(ClientStats&&) noexcept;

    std::unique_ptr<ClientHistograms> histograms;

  } client;

  // Custom stats from things that wrap a client, such as an LDBench worker or
  // commandline tool.

  struct LDBenchStats {
#define STAT_DEFINE(name, _) StatsCounter name{};
#include "logdevice/common/stats/ldbench_worker_stats.inc" // nolint

    LDBenchStats();
    ~LDBenchStats();

    LDBenchStats(const LDBenchStats&) = delete;
    LDBenchStats& operator=(const LDBenchStats&) = delete;

    LDBenchStats(LDBenchStats&&) noexcept = default;
    LDBenchStats& operator=(LDBenchStats&&) noexcept = default;

    std::unique_ptr<LatencyHistogram> delivery_latency;
  };

  struct CharacterizeLoadStats {
#define STAT_DEFINE(name, _) StatsCounter name{};
#include "logdevice/common/stats/characterize_load_stats.inc" // nolint

    CharacterizeLoadStats() = default;
    ~CharacterizeLoadStats() = default;

    CharacterizeLoadStats(const CharacterizeLoadStats&) = delete;
    CharacterizeLoadStats& operator=(const CharacterizeLoadStats&) = delete;

    CharacterizeLoadStats(CharacterizeLoadStats&&) noexcept = default;
    CharacterizeLoadStats& operator=(CharacterizeLoadStats&&) noexcept =
        default;
  };

  // Lazily initialized.
  std::unique_ptr<LDBenchStats> ldbench{nullptr};
  std::unique_ptr<CharacterizeLoadStats> characterize_load{nullptr};

  const FastUpdateableSharedPtr<StatsParams>* params;

  // if this Stats object is local to a particular worker thread of type
  // GENERAL, this will contain its id, and -1 otherwise
  worker_id_t worker_id;
};

class Stats::EnumerationCallbacks {
 public:
  virtual ~EnumerationCallbacks() {}
  // Simple stats. Also called for per-something stats aggregated for all
  // values of something. E.g. if there's a per-traffic-class stat
  // 'bytes_sent', this method will be called with name = 'bytes_sent' and
  // val = totalPerTrafficClassStats().bytes_sent. (The per-traffic-class
  // method will be called too, for each traffic class.)
  virtual void stat(const std::string& name, int64_t val) = 0;
  // Per-message-type stats.
  virtual void stat(const std::string& name, MessageType, int64_t val) = 0;
  // Per-shard stats.
  virtual void stat(const std::string& name,
                    shard_index_t shard,
                    int64_t val) = 0;
  // Per-traffic-class stats.
  virtual void stat(const std::string& name, TrafficClass, int64_t val) = 0;
  // Per-flow-group stats.
  virtual void stat(const std::string& name,
                    NodeLocationScope flow_group,
                    int64_t val) = 0;
  // Per-flow-group-and-msg-priority stats.
  virtual void stat(const std::string& name,
                    NodeLocationScope flow_group,
                    Priority,
                    int64_t val) = 0;
  // Per-msg-priority stats (totals of the previous one).
  virtual void stat(const std::string& name, Priority, int64_t val) = 0;
  // Per-request-type stats.
  virtual void stat(const std::string& name, RequestType, int64_t val) = 0;
  // Per-storage-task-type stats.
  virtual void stat(const std::string& name,
                    StorageTaskType type,
                    int64_t val) = 0;
  // Per-worker stats (only for workers of type GENERAL).
  virtual void stat(const std::string& name,
                    worker_id_t worker_id,
                    uint64_t load) = 0;
  // Per-log stats.
  virtual void stat(const char* name,
                    const std::string& log_group,
                    int64_t val) = 0;
  // Simple histograms.
  virtual void histogram(const std::string& name,
                         const HistogramInterface& hist) = 0;
  // Per-shard histograms.
  virtual void histogram(const std::string& name,
                         shard_index_t shard,
                         const HistogramInterface& hist) = 0;
};

/**
 * StatsHolder wraps multiple (thread-local) instances of Stats objects. It
 * supports aggregation and resetting. StatsHolder::get() method should be
 * used to obtain a Stats object local to the current thread.
 */
class StatsHolder {
 public:
  explicit StatsHolder(StatsParams params);
  ~StatsHolder();

  /**
   * Collect stats from all threads.
   */
  Stats aggregate() const;

  /**
   * Reset stats on all threads.
   */
  void reset();

  /**
   * Returns the Stats object for the current thread.
   */
  inline Stats& get();

  /**
   * Executes a function on each thread's Stats object.
   */
  template <typename Func>
  void runForEach(const Func& func);

  FastUpdateableSharedPtr<StatsParams> params_;

 private:
  // Destructor adds stats to dead_stats_.
  struct StatsWrapper;
  struct Tag;

  // Stats aggregated for all destroyed threads.
  mutable std::mutex dead_stats_mutex_;
  Stats dead_stats_;

  // Stats for running threads.
  folly::ThreadLocalPtr<StatsWrapper, Tag> thread_stats_;
};

struct StatsHolder::StatsWrapper {
  Stats stats;
  StatsHolder* owner;

  explicit StatsWrapper(StatsHolder* owner)
      : stats(&owner->params_), owner(owner) {}

  ~StatsWrapper() {
    if (owner) {
      std::lock_guard<std::mutex> lock(owner->dead_stats_mutex_);
      owner->dead_stats_.aggregateForDestroyedThread(stats);
    }
  }
};

Stats& StatsHolder::get() {
  StatsWrapper* wrapper = thread_stats_.get();
  if (!wrapper) {
    wrapper = new StatsWrapper(this);
    thread_stats_.reset(wrapper);
  }
  return wrapper->stats;
}

template <typename Func>
void StatsHolder::runForEach(const Func& func) {
  for (auto& x : thread_stats_.accessAllThreads()) {
    func(x.stats);
  }
  func(dead_stats_);
}

/**
 * Incrememnts per-shard stat in constructor, decrements in destructor.
 * Noncopyable, movable.
 */
class PerShardStatToken {
 public:
  PerShardStatToken() {}

  PerShardStatToken(StatsHolder* stats,
                    StatsCounter PerShardStats::*stat,
                    shard_index_t shard_idx,
                    int64_t added_value = 1) {
    assign(stats, stat, shard_idx, added_value);
  }

  void reset() {
    if (!stats_) {
      shard_idx_ = -1;
      return;
    }
    ld_check(shard_idx_ != -1);
    (stats_->get().per_shard_stats->get(shard_idx_)->*stat_) -= added_value_;
    stats_ = nullptr;
    shard_idx_ = -1;
  }

  void assign(StatsHolder* stats,
              StatsCounter PerShardStats::*stat,
              shard_index_t shard_idx,
              int64_t added_value = 1) {
    ld_check(shard_idx != -1);
    reset();
    stats_ = stats;
    stat_ = stat;
    shard_idx_ = shard_idx;
    added_value_ = added_value;
    if (!stats_ || !stats_->get().per_shard_stats ||
        !stats_->get().per_shard_stats->get(shard_idx_)) {
      stats_ = nullptr;
      return;
    }
    (stats_->get().per_shard_stats->get(shard_idx_)->*stat_) += added_value_;
  }

  void setValue(int64_t new_added_value) {
    // Assert that assign() was called. Note that in tests assign() may be
    // called with null stats_, so we check shard_idx_ instead.
    ld_check(shard_idx_ != -1);

    if (stats_ != nullptr && new_added_value != added_value_) {
      (stats_->get().per_shard_stats->get(shard_idx_)->*stat_) +=
          new_added_value - added_value_;
      added_value_ = new_added_value;
    }
  }

  ~PerShardStatToken() {
    reset();
  }

  PerShardStatToken(const PerShardStatToken&) = delete;
  PerShardStatToken& operator=(const PerShardStatToken&) = delete;

  PerShardStatToken(PerShardStatToken&& rhs) noexcept {
    stats_ = rhs.stats_;
    stat_ = rhs.stat_;
    shard_idx_ = rhs.shard_idx_;
    added_value_ = rhs.added_value_;
    rhs.stats_ = nullptr;
    rhs.shard_idx_ = -1;
  }

  PerShardStatToken& operator=(PerShardStatToken&& rhs) /* may throw */ {
    if (&rhs == this) {
      return *this;
    }
    reset();
    stats_ = rhs.stats_;
    stat_ = rhs.stat_;
    shard_idx_ = rhs.shard_idx_;
    added_value_ = rhs.added_value_;
    rhs.stats_ = nullptr;
    rhs.shard_idx_ = -1;
    return *this;
  }

 private:
  StatsHolder* stats_ = nullptr;
  StatsCounter PerShardStats::*stat_;
  int64_t added_value_ = 0;
  // -1 if not assigned (assign() wasn't called since the last reset() call).
  // May be assigned even if stats_ is nullptr.
  shard_index_t shard_idx_ = -1;
};

#define STAT_ADD(stats_struct, name, val)  \
  do {                                     \
    if (stats_struct) {                    \
      (stats_struct)->get().name += (val); \
    }                                      \
  } while (0)

#define STAT_SET(stats_struct, name, val) \
  do {                                    \
    if (stats_struct) {                   \
      (stats_struct)->get().name = (val); \
    }                                     \
  } while (0)

#define LOG_GROUP_STAT_ADD(stats_struct, log_name, name, val)            \
  do {                                                                   \
    if (stats_struct) {                                                  \
      auto stats_ulock = (stats_struct)->get().per_log_stats.ulock();    \
      auto stats_it = stats_ulock->find((log_name));                     \
      if (stats_it != stats_ulock->end()) {                              \
        /* PerLogStats for log_name already exist (common case). */      \
        /* Just atomically increment the value.  */                      \
        stats_it->second->name += (val);                                 \
      } else {                                                           \
        /* PerLogStats for log_name do not exist yet (rare case). */     \
        /* Upgrade ulock to wlock and emplace new PerLogStats. */        \
        /* No risk of deadlock because we are the only writer thread. */ \
        auto stats_ptr = std::make_shared<PerLogStats>();                \
        stats_ptr->name += (val);                                        \
        stats_ulock.moveFromUpgradeToWrite()->emplace_hint(              \
            stats_it, (log_name), std::move(stats_ptr));                 \
      }                                                                  \
    }                                                                    \
  } while (0)

#define LOG_GROUP_TIME_SERIES_ADD(stats_struct, stat_name, log_name, val)      \
  do {                                                                         \
    if (stats_struct) {                                                        \
      auto stats_ulock = (stats_struct)->get().per_log_stats.ulock();          \
      /* Unfortunately, the type of the lock after a downgrade from write to   \
       * upgrade isn't the same as the type of upgrade lock initially acquired \
       */                                                                      \
      folly::LockedPtr<decltype(stats_ulock)::Synchronized,                    \
                       folly::LockPolicyFromExclusiveToUpgrade>                \
          stats_downgraded_ulock;                                              \
      auto stats_it = stats_ulock->find((log_name));                           \
      if (UNLIKELY(stats_it == stats_ulock->end())) {                          \
        /* PerLogStats for log_name do not exist yet (rare case). */           \
        /* Upgrade ulock to wlock and emplace new PerLogStats. */              \
        /* No risk of deadlock because we are the only writer thread. */       \
        auto stats_ptr = std::make_shared<PerLogStats>();                      \
        auto stats_wlock = stats_ulock.moveFromUpgradeToWrite();               \
        stats_it =                                                             \
            stats_wlock->emplace((log_name), std::move(stats_ptr)).first;      \
        stats_downgraded_ulock = stats_wlock.moveFromWriteToUpgrade();         \
      }                                                                        \
      {                                                                        \
        std::lock_guard<std::mutex> guard(stats_it->second->mutex);            \
        if (UNLIKELY(!stats_it->second->stat_name)) {                          \
          stats_it->second->stat_name = std::make_shared<PerLogTimeSeries>(    \
              (stats_struct)->params_.get()->num_buckets_##stat_name,          \
              (stats_struct)->params_.get()->time_intervals_##stat_name);      \
        }                                                                      \
        stats_it->second->stat_name->addValue(val);                            \
      }                                                                        \
    }                                                                          \
  } while (0)

#define LOG_GROUP_CUSTOM_COUNTERS_ADD(stats_struct, log_name, val)             \
  do {                                                                         \
    if (stats_struct) {                                                        \
      auto stats_ulock = (stats_struct)->get().per_log_stats.ulock();          \
      /* Unfortunately, the type of the lock after a downgrade from write to   \
       * upgrade isn't the same as the type of upgrade lock initially acquired \
       */                                                                      \
      folly::LockedPtr<decltype(stats_ulock)::Synchronized,                    \
                       folly::LockPolicyFromExclusiveToUpgrade>                \
          stats_downgraded_ulock;                                              \
      auto stats_it = stats_ulock->find((log_name));                           \
      if (UNLIKELY(stats_it == stats_ulock->end())) {                          \
        /* PerLogStats for log_name do not exist yet (rare case). */           \
        /* Upgrade ulock to wlock and emplace new PerLogStats. */              \
        /* No risk of deadlock because we are the only writer thread. */       \
        auto stats_ptr = std::make_shared<PerLogStats>();                      \
        auto stats_wlock = stats_ulock.moveFromUpgradeToWrite();               \
        stats_it =                                                             \
            stats_wlock->emplace((log_name), std::move(stats_ptr)).first;      \
        stats_downgraded_ulock = stats_wlock.moveFromWriteToUpgrade();         \
      }                                                                        \
      {                                                                        \
        std::lock_guard<std::mutex> guard(stats_it->second->mutex);            \
        if (UNLIKELY(!stats_it->second->custom_counters)) {                    \
          stats_it->second->custom_counters =                                  \
              std::make_shared<CustomCountersTimeSeries>();                    \
        }                                                                      \
        stats_it->second->custom_counters->addCustomCounters(val);             \
      }                                                                        \
    }                                                                          \
  } while (0)

#define TRAFFIC_CLASS_STAT_ADD(stats_struct, traffic_class, name, val) \
  do {                                                                 \
    if (stats_struct) {                                                \
      TrafficClass c_hygienic = (traffic_class);                       \
      ld_check(c_hygienic != TrafficClass::MAX);                       \
      stats_struct->get()                                              \
          .per_traffic_class_stats[static_cast<int>(c_hygienic)]       \
          .name += (val);                                              \
    }                                                                  \
  } while (0)

#define FLOW_GROUP_STAT_ADD(stats_struct, flow_group, name, val) \
  do {                                                           \
    if (stats_struct) {                                          \
      NodeLocationScope scope = (flow_group).scope();            \
      ld_check(scope <= NodeLocationScope::ROOT);                \
      (stats_struct)                                             \
          ->get()                                                \
          .per_flow_group_stats[static_cast<int>(scope)]         \
          .name += (val);                                        \
    }                                                            \
  } while (0)

#define FLOW_GROUP_STAT_SET(stats, fgp_arr, scope, name, val) \
  do {                                                        \
    if (stats) {                                              \
      ld_check(scope <= NodeLocationScope::ROOT);             \
      fgp_arr[static_cast<int>(scope)].name = (val);          \
    }                                                         \
  } while (0)

#define FLOW_GROUP_MSG_STAT_ADD(stats_struct, flow_group, msg, name, val) \
  do {                                                                    \
    if (stats_struct) {                                                   \
      NodeLocationScope scope = (flow_group).scope();                     \
      Priority priority = (msg)->priority();                              \
      ld_check(scope <= NodeLocationScope::ROOT);                         \
      (stats_struct)                                                      \
          ->get()                                                         \
          .per_flow_group_stats[static_cast<int>(scope)]                  \
          .priorities[asInt(priority)]                                    \
          .name += (val);                                                 \
    }                                                                     \
  } while (0)

#define FLOW_GROUP_PRIORITY_STAT_ADD(                                      \
    stats, fgp_arr, scope, priority, name, val)                            \
  do {                                                                     \
    if (stats) {                                                           \
      fgp_arr[static_cast<int>(scope)].priorities[asInt(priority)].name += \
          (val);                                                           \
    }                                                                      \
  } while (0)

#define FLOW_GROUP_PRIORITY_STAT_SET(                                     \
    stats, fgp_arr, scope, priority, name, val)                           \
  do {                                                                    \
    if (stats) {                                                          \
      fgp_arr[static_cast<int>(scope)].priorities[asInt(priority)].name = \
          (val);                                                          \
    }                                                                     \
  } while (0)

#define FLOW_GROUP_MSG_LATENCY_ADD(stats_struct, flow_group, env) \
  do {                                                            \
    if (stats_struct) {                                           \
      NodeLocationScope scope = (flow_group).scope();             \
      Priority priority = (env).priority();                       \
      (stats_struct)                                              \
          ->get()                                                 \
          .per_flow_group_stats[static_cast<int>(scope)]          \
          .priorities[asInt(priority)]                            \
          .time_in_queue->add((env).age());                       \
    }                                                             \
  } while (0)

#define PER_SHARD_HISTOGRAM_ADD(stats_struct, name, shard, value)             \
  do {                                                                        \
    if (stats_struct && (stats_struct)->get().per_shard_histograms) {         \
      (stats_struct)->get().per_shard_histograms->name.add((shard), (value)); \
    }                                                                         \
  } while (0)

#define HISTOGRAM_ADD(stats_struct, name, usecs)                   \
  do {                                                             \
    if (stats_struct && (stats_struct)->get().server_histograms) { \
      (stats_struct)->get().server_histograms->name.add(usecs);    \
    }                                                              \
  } while (0)

#define CLIENT_HISTOGRAM_ADD(stats_struct, name, usecs)         \
  do {                                                          \
    if (stats_struct) {                                         \
      (stats_struct)->get().client.histograms->name.add(usecs); \
    }                                                           \
  } while (0)

#define PER_SHARD_STAT_OP(stats_struct, name, shard, value, op)         \
  do {                                                                  \
    if ((stats_struct) && (stats_struct)->get().per_shard_stats &&      \
        (stats_struct)->get().per_shard_stats->get(shard)) {            \
      (stats_struct)->get().per_shard_stats->get(shard)->name op value; \
    }                                                                   \
  } while (0)
#define PER_SHARD_STAT_SET(stats_struct, name, shard, value) \
  PER_SHARD_STAT_OP(stats_struct, name, shard, value, =)
#define PER_SHARD_STAT_ADD(stats_struct, name, shard, value) \
  PER_SHARD_STAT_OP(stats_struct, name, shard, value, +=)
#define PER_SHARD_STAT_INCR(stats_struct, name, shard) \
  PER_SHARD_STAT_ADD(stats_struct, name, shard, 1)
#define PER_SHARD_STAT_DECR(stats_struct, name, shard) \
  PER_SHARD_STAT_ADD(stats_struct, name, shard, -1)

#define STORAGE_TASK_TYPE_STAT_ADD(stats_struct, type, name, value)        \
  do {                                                                     \
    if (stats_struct) {                                                    \
      ld_assert(static_cast<int>(type) <                                   \
                (stats_struct)->get().per_storage_task_type_stats.size()); \
      (stats_struct)                                                       \
          ->get()                                                          \
          .per_storage_task_type_stats[static_cast<int>(type)]             \
          .name += value;                                                  \
    }                                                                      \
  } while (0)

#define STORAGE_TASK_TYPE_STAT_INCR(stats_struct, type, name) \
  STORAGE_TASK_TYPE_STAT_ADD(stats_struct, type, name, 1)

#define STAT_SUB(stats_struct, name, val) STAT_ADD(stats_struct, name, -(val))

#define STAT_INCR(stats_struct, name) STAT_ADD(stats_struct, name, 1)
#define STAT_DECR(stats_struct, name) STAT_SUB(stats_struct, name, 1)

#define TRAFFIC_CLASS_STAT_SUB(stats_struct, traffic_class, name, val) \
  TRAFFIC_CLASS_STAT_ADD(stats_struct, traffic_class, name, -(val))
#define TRAFFIC_CLASS_STAT_INCR(stats_struct, traffic_class, name) \
  TRAFFIC_CLASS_STAT_ADD(stats_struct, traffic_class, name, 1)
#define TRAFFIC_CLASS_STAT_DECR(stats_struct, traffic_class, name) \
  TRAFFIC_CLASS_STAT_SUB(stats_struct, traffic_class, name, 1)

#define FLOW_GROUP_STAT_INCR(stats_struct, flow_group, name) \
  FLOW_GROUP_STAT_ADD(stats_struct, flow_group, name, 1)

#define FLOW_GROUP_MSG_STAT_INCR(stats_struct, traffic_class, msg, name) \
  FLOW_GROUP_MSG_STAT_ADD(stats_struct, traffic_class, msg, name, 1)

#define FLOW_GROUP_PRIORITY_STAT_INCR(stats_struct, scope, priority, name) \
  FLOW_GROUP_PRIORITY_STAT_ADD(stats_struct, scope, priority, name, 1)

#define REQUEST_TYPE_STAT_ADD(stats_struct, type, name, value) \
  do {                                                         \
    if (stats_struct) {                                        \
      (stats_struct)                                           \
          ->get()                                              \
          .per_request_type_stats[static_cast<int>(type)]      \
          .name += value;                                      \
    }                                                          \
  } while (0)

#define REQUEST_TYPE_STAT_INCR(stats_struct, type, name) \
  REQUEST_TYPE_STAT_ADD(stats_struct, type, name, 1)

#define MESSAGE_TYPE_STAT_ADD(stats_struct, type, name, value) \
  do {                                                         \
    if (stats_struct) {                                        \
      (stats_struct)                                           \
          ->get()                                              \
          .per_message_type_stats[static_cast<int>(type)]      \
          .name += value;                                      \
    }                                                          \
  } while (0)

#define MESSAGE_TYPE_STAT_INCR(stats_struct, type, name) \
  MESSAGE_TYPE_STAT_ADD(stats_struct, type, name, 1)

// Only for workers of type GENERAL.
#define PER_WORKER_STAT_ADD_SAMPLE(stats_struct, worker_id, load) \
  do {                                                            \
    if (stats_struct) {                                           \
      (stats_struct)                                              \
          ->get()                                                 \
          .per_worker_stats                                       \
          .withULockPtr([&](auto&& stats_ulock) {                 \
            auto stats_it = stats_ulock->find(worker_id);         \
            if (stats_it != stats_ulock->end()) {                 \
              return stats_it->second;                            \
            } else {                                              \
              return stats_ulock.moveFromUpgradeToWrite()         \
                  ->emplace(std::make_pair(                       \
                      worker_id,                                  \
                      std::make_shared<PerWorkerTimeSeriesStats>( \
                          stats_struct->params_.get()             \
                              ->worker_stats_retention_time)))    \
                  .first->second;                                 \
            }                                                     \
          })                                                      \
          ->addLoad(load);                                        \
    }                                                             \
  } while (0)

// expects name to be {AppendSuccess, AppendFail}
#define PER_NODE_STAT_ADD(stats_struct, node_id, name)                  \
  do {                                                                  \
    if (stats_struct) {                                                 \
      (stats_struct)                                                    \
          ->get()                                                       \
          .per_node_stats                                               \
          .withULockPtr([&](auto&& stats_ulock) {                       \
            auto stats_it = stats_ulock->find(node_id);                 \
            if (stats_it != stats_ulock->end()) {                       \
              return stats_it->second;                                  \
            } else {                                                    \
              return stats_ulock.moveFromUpgradeToWrite()               \
                  ->emplace(std::make_pair(                             \
                      node_id,                                          \
                      std::make_shared<PerNodeTimeSeriesStats>(         \
                          stats_struct->params_.get()                   \
                              ->node_stats_retention_time_on_clients))) \
                  .first->second;                                       \
            }                                                           \
          })                                                            \
          ->add##name();                                                \
    }                                                                   \
  } while (0)

#define PER_CLIENT_NODE_STAT_ADD(                         \
    stats_struct, client_id, node_id, success, failure)   \
  do {                                                    \
    if (stats_struct) {                                   \
      (stats_struct)                                      \
          ->get()                                         \
          .per_client_node_stats.wlock()                  \
          ->append(client_id, node_id, success, failure); \
    }                                                     \
  } while (0)
}} // namespace facebook::logdevice
