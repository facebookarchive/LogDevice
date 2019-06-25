/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/common/stats/Stats.h"

#include <algorithm>
#include <cmath>
#include <utility>
#include <vector>

#include <boost/algorithm/string/predicate.hpp>
#include <folly/Conv.h>
#include <folly/Synchronized.h>
#include <folly/stats/BucketedTimeSeries-defs.h>
#include <folly/stats/BucketedTimeSeries.h>
#include <folly/stats/MultiLevelTimeSeries-defs.h>
#include <folly/stats/MultiLevelTimeSeries.h>

#include "logdevice/common/stats/ClientHistograms.h"
#include "logdevice/common/stats/Histogram.h"
#include "logdevice/common/stats/PerShardHistograms.h"
#include "logdevice/common/stats/ServerHistograms.h"

// Instantiate folly::MultiLevelTimeSeries
namespace folly {
template class BucketedTimeSeries<int64_t,
                                  LegacyStatsClock<std::chrono::milliseconds>>;
template class MultiLevelTimeSeries<
    int64_t,
    LegacyStatsClock<std::chrono::milliseconds>>;
} // namespace folly

namespace facebook { namespace logdevice {

static void aggregateStat(StatsAgg agg, StatsCounter& out, int64_t in) {
  switch (agg) {
    case StatsAgg::SUM:
      out += in;
      break;
    case StatsAgg::MAX:
      if (in > out) {
        out = in;
      }
      break;
    case StatsAgg::SUBTRACT:
      out -= in;
      break;
    case StatsAgg::ASSIGN:
      out = in;
      break;
  }
}

static void aggregateStat(StatsAgg agg,
                          StatsAggOptional override,
                          StatsCounter& out,
                          int64_t in) {
  aggregateStat(override.hasValue() ? override.value() : agg, out, in);
}

template <typename H>
static void aggregateHistogram(StatsAggOptional agg, H& out, const H& in) {
  if (!agg.hasValue()) {
    out.merge(in);
    return;
  }
  switch (agg.value()) {
    case StatsAgg::SUM:
      out.merge(in);
      break;
    case StatsAgg::MAX:
      // MAX doesn't make much sense for histograms. Let's just merge them.
      out.merge(in);
      break;
    case StatsAgg::SUBTRACT:
      out.subtract(in);
      break;
    case StatsAgg::ASSIGN:
      out = in;
      break;
  }
}

// won't be an exact copy because of the approximate nature of
// BucketedTimeSeries, but it should be close enough not to matter
template <class T, class V>
static folly::BucketedTimeSeries<T, V> newTimeSeriesWithUpdatedRetentionTime(
    folly::BucketedTimeSeries<T, V>& time_series,
    std::chrono::milliseconds retention_time,
    std::chrono::steady_clock::time_point current_time) {
  const auto bucket_count = time_series.numBuckets();
  const auto bucket_duration = retention_time / bucket_count;

  time_series.update(current_time);

  folly::BucketedTimeSeries<T, V> time_series_new(bucket_count, retention_time);
  for (int i = 0; i < bucket_count; ++i) {
    const auto start_time =
        (current_time - retention_time) + i * bucket_duration;
    const auto end_time = start_time + bucket_duration;
    const auto sum = time_series.sum(start_time, end_time);
    const auto count = time_series.count(start_time, end_time);

    time_series_new.addValueAggregated(start_time, sum, count);
  }

  return time_series_new;
}

void PerLogStats::aggregate(PerLogStats const& other,
                            StatsAggOptional agg_override) {
#define STAT_DEFINE(name, agg) \
  aggregateStat(StatsAgg::agg, agg_override, name, other.name);
#include "logdevice/common/stats/per_log_stats.inc" // nolint
}

void PerTrafficClassStats::aggregate(PerTrafficClassStats const& other,
                                     StatsAggOptional agg_override) {
#define STAT_DEFINE(name, agg) \
  aggregateStat(StatsAgg::agg, agg_override, name, other.name);
#include "logdevice/common/stats/per_traffic_class_stats.inc" // nolint
}

void PerShardStats::aggregate(PerShardStats const& other,
                              StatsAggOptional agg_override) {
#define STAT_DEFINE(name, agg) \
  aggregateStat(StatsAgg::agg, agg_override, name, other.name);
#include "logdevice/common/stats/per_shard_stats.inc" // nolint
}

void PerShardStats::aggregateForDestroyedThread(PerShardStats const& other,
                                                StatsAggOptional agg_override) {
#define DESTROYING_THREAD
#define STAT_DEFINE(name, agg) \
  aggregateStat(StatsAgg::agg, agg_override, name, other.name);
#include "logdevice/common/stats/per_shard_stats.inc" // nolint
}

void PerShardStats::reset() {
#define RESETTING_STATS
#define STAT_DEFINE(name, _) name = {};
#include "logdevice/common/stats/per_shard_stats.inc" // nolint
}

void PerTrafficClassStats::reset() {
#define RESETTING_STATS
#define STAT_DEFINE(name, _) name = {};
#include "logdevice/common/stats/per_traffic_class_stats.inc" // nolint
}

PerShapingPriorityStats::PerShapingPriorityStats()
    : time_in_queue(std::make_unique<LatencyHistogram>()) {}

PerShapingPriorityStats::~PerShapingPriorityStats() = default;

PerShapingPriorityStats::PerShapingPriorityStats(
    PerShapingPriorityStats&&) noexcept = default;

PerShapingPriorityStats& PerShapingPriorityStats::
operator=(PerShapingPriorityStats&&) noexcept = default;

void PerShapingPriorityStats::aggregate(PerShapingPriorityStats const& other,
                                        StatsAggOptional agg_override) {
  aggregateHistogram(agg_override, *time_in_queue, *other.time_in_queue);
#define STAT_DEFINE(name, agg) \
  aggregateStat(StatsAgg::agg, agg_override, name, other.name);
#include "logdevice/common/stats/per_msg_priority_stats.inc"    // nolint
#include "logdevice/common/stats/per_priority_stats_common.inc" // nolint
}

void PerShapingPriorityStats::reset() {
  time_in_queue->clear();
#define RESETTING_STATS
#define STAT_DEFINE(name, _) name = {};
#include "logdevice/common/stats/per_msg_priority_stats.inc"    // nolint
#include "logdevice/common/stats/per_priority_stats_common.inc" // nolint
}

void PerFlowGroupStats::aggregate(PerFlowGroupStats const& other,
                                  StatsAggOptional agg_override) {
  for (size_t idx = 0; idx < priorities.size(); idx++) {
    priorities[idx].aggregate(other.priorities[idx], agg_override);
  }
#define STAT_DEFINE(name, agg) \
  aggregateStat(StatsAgg::agg, agg_override, name, other.name);
#include "logdevice/common/stats/per_flow_group_stats.inc" // nolint
}

void PerFlowGroupStats::reset() {
  for (auto& p : priorities) {
    p.reset();
  }
#define RESETTING_STATS
#define STAT_DEFINE(name, _) name = {};
#include "logdevice/common/stats/per_flow_group_stats.inc" // nolint
}

PerShapingPriorityStats
PerFlowGroupStats::totalPerShapingPriorityStats() const {
  PerShapingPriorityStats total;
  for (auto& p : priorities) {
    total.aggregate(p, folly::none);
  }
  return total;
}

void PerRequestTypeStats::aggregate(const PerRequestTypeStats& other,
                                    StatsAggOptional agg_override) {
#define STAT_DEFINE(name, agg) \
  aggregateStat(StatsAgg::agg, agg_override, name, other.name);
#include "logdevice/common/stats/per_request_type_stats.inc" // nolint
}

void PerRequestTypeStats::reset() {
#define RESETTING_STATS
#define STAT_DEFINE(name, _) name = {};
#include "logdevice/common/stats/per_request_type_stats.inc" // nolint
}

void PerMessageTypeStats::aggregate(const PerMessageTypeStats& other,
                                    StatsAggOptional agg_override) {
#define STAT_DEFINE(name, agg) \
  aggregateStat(StatsAgg::agg, agg_override, name, other.name);
#include "logdevice/common/stats/per_message_type_stats.inc" // nolint
}

void PerMessageTypeStats::reset() {
#define RESETTING_STATS
#define STAT_DEFINE(name, _) name = {};
#include "logdevice/common/stats/per_message_type_stats.inc" // nolint
}

void PerStorageTaskTypeStats::aggregate(PerStorageTaskTypeStats const& other,
                                        StatsAggOptional agg_override) {
#define STAT_DEFINE(name, agg) \
  aggregateStat(StatsAgg::agg, agg_override, name, other.name);
#include "logdevice/common/stats/per_storage_task_type_stats.inc" // nolint
}

void PerStorageTaskTypeStats::reset() {
#define RESETTING_STATS
#define STAT_DEFINE(name, _) name = {};
#include "logdevice/common/stats/per_storage_task_type_stats.inc" // nolint
}

Stats::LDBenchStats::LDBenchStats() {
  delivery_latency = std::make_unique<LatencyHistogram>();
}
Stats::LDBenchStats::~LDBenchStats() = default;

Stats::Stats(const FastUpdateableSharedPtr<StatsParams>* params)
    : per_client_node_stats(PerClientNodeTimeSeriesStats{
          params->get()->node_stats_retention_time_on_nodes}),
      params(params),
      worker_id(-1) {
  if (params->get()->is_server) {
    server_histograms = std::make_unique<ServerHistograms>();
    per_shard_histograms = std::make_unique<PerShardHistograms>();
    per_shard_stats = std::make_unique<ShardedStats>();
  }

  // Initialize custom stats struct/s if needed.
  switch (params->get()->stats_set) {
    case StatsParams::StatsSet::LDBENCH_WORKER:
      ldbench = std::make_unique<LDBenchStats>();
      break;
    case StatsParams::StatsSet::CHARACTERIZE_LOAD:
      characterize_load = std::make_unique<CharacterizeLoadStats>();
      break;
    case StatsParams::StatsSet::DEFAULT:
      break;
  }
}

Stats::~Stats() = default;

Stats::Stats(const Stats& other) : Stats(other.params) {
  aggregate(other, StatsAgg::ASSIGN);
}

Stats& Stats::operator=(const Stats& other) {
  ld_check(!params->get()->is_server);
  aggregate(other, StatsAgg::ASSIGN);
  return *this;
}

Stats::Stats(Stats&& other) noexcept(true) = default;

Stats& Stats::operator=(Stats&& other) noexcept(false) = default;

void Stats::aggregate(Stats const& other, StatsAggOptional agg_override) {
  switch (params->get()->stats_set) {
    case StatsParams::StatsSet::DEFAULT:
      if (params->get()->is_server) {
#define STAT_DEFINE(name, agg) \
  aggregateStat(StatsAgg::agg, agg_override, name, other.name);
#include "logdevice/common/stats/server_stats.inc" // nolint
      } else {
#define STAT_DEFINE(name, agg) \
  aggregateStat(StatsAgg::agg, agg_override, client.name, other.client.name);
#include "logdevice/common/stats/client_stats.inc" // nolint
      }
#define STAT_DEFINE(name, agg) \
  aggregateStat(StatsAgg::agg, agg_override, name, other.name);
#include "logdevice/common/stats/common_stats.inc" // nolint
      break;
    case StatsParams::StatsSet::LDBENCH_WORKER:
#define STAT_DEFINE(name, agg) \
  aggregateStat(               \
      StatsAgg::agg, agg_override, ldbench->name, other.ldbench->name);
#include "logdevice/common/stats/ldbench_worker_stats.inc" // nolint
      break;
    case StatsParams::StatsSet::CHARACTERIZE_LOAD:
#define STAT_DEFINE(name, agg)           \
  aggregateStat(StatsAgg::agg,           \
                agg_override,            \
                characterize_load->name, \
                other.characterize_load->name);
#include "logdevice/common/stats/characterize_load_stats.inc" // nolint
      break;
  } // let compiler check that all enum values are handled.

  aggregateCompoundStats(other, agg_override);
}

void Stats::aggregateForDestroyedThread(Stats const& other) {
#define DESTROYING_THREAD
  switch (params->get()->stats_set) {
    case StatsParams::StatsSet::DEFAULT:
      if (params->get()->is_server) {
#define STAT_DEFINE(name, agg) aggregateStat(StatsAgg::agg, name, other.name);
#include "logdevice/common/stats/server_stats.inc" // nolint
      } else {
#define STAT_DEFINE(name, agg) \
  aggregateStat(StatsAgg::agg, client.name, other.client.name);
#include "logdevice/common/stats/client_stats.inc" // nolint
      }
#define STAT_DEFINE(name, agg) aggregateStat(StatsAgg::agg, name, other.name);
#include "logdevice/common/stats/common_stats.inc" // nolint
      break;
    case StatsParams::StatsSet::LDBENCH_WORKER:
#define STAT_DEFINE(name, agg) \
  aggregateStat(StatsAgg::agg, ldbench->name, other.ldbench->name);
#include "logdevice/common/stats/ldbench_worker_stats.inc" // nolint
      break;
    case StatsParams::StatsSet::CHARACTERIZE_LOAD:
#define STAT_DEFINE(name, agg) \
  aggregateStat(               \
      StatsAgg::agg, characterize_load->name, other.characterize_load->name);
#include "logdevice/common/stats/characterize_load_stats.inc" // nolint
      break;
  } // let compiler check that all enum values are handled.

  aggregateCompoundStats(other, folly::none, true);
}

void Stats::aggregateCompoundStats(Stats const& other,
                                   StatsAggOptional agg_override,
                                   bool destroyed_threads) {
  if (params->get()->stats_set != StatsParams::StatsSet::DEFAULT) {
    switch (params->get()->stats_set) {
      case StatsParams::StatsSet::LDBENCH_WORKER:
        ld_check(other.ldbench);
        ld_check(ldbench);
        aggregateHistogram(agg_override,
                           *ldbench->delivery_latency,
                           *other.ldbench->delivery_latency);
        break;
      default:
        break;
    }

    return;
  }

  for (int i = 0; i < per_traffic_class_stats.size(); ++i) {
    per_traffic_class_stats[i].aggregate(
        other.per_traffic_class_stats[i], agg_override);
  }

  for (int i = 0; i < per_flow_group_stats.size(); ++i) {
    per_flow_group_stats[i].aggregate(
        other.per_flow_group_stats[i], agg_override);
  }
  for (int i = 0; i < per_flow_group_stats_rt.size(); ++i) {
    per_flow_group_stats_rt[i].aggregate(
        other.per_flow_group_stats_rt[i], agg_override);
  }

  for (int i = 0; i < per_request_type_stats.size(); ++i) {
    per_request_type_stats[i].aggregate(
        other.per_request_type_stats[i], agg_override);
  }

  for (int i = 0; i < per_message_type_stats.size(); ++i) {
    per_message_type_stats[i].aggregate(
        other.per_message_type_stats[i], agg_override);
  }

  for (int i = 0; i < per_storage_task_type_stats.size(); ++i) {
    per_storage_task_type_stats[i].aggregate(
        other.per_storage_task_type_stats[i], agg_override);
  }

  // Aggregate per log stats. Use synchronizedCopy() to copy other's
  // per_log_stats into temporary vector, to avoid holding a read lock on it
  // while we aggregate.
  this->per_log_stats.withWLock(
      [&agg_override,
       other_per_log_stats_entries = other.synchronizedCopy(
           &Stats::per_log_stats)](auto& this_per_log_stats) {
        for (const auto& kv : other_per_log_stats_entries) {
          ld_check(kv.second != nullptr);
          auto& stats_ptr = this_per_log_stats[kv.first];
          if (stats_ptr == nullptr) {
            stats_ptr = std::make_shared<PerLogStats>();
          }
          stats_ptr->aggregate(*kv.second, agg_override);
        }
      });

  // Aggregate per worker stats. Also use synchronizedCopy()
  this->per_worker_stats.withWLock(
      [&agg_override,
       other_per_worker_stats_entries = other.synchronizedCopy(
           &Stats::per_worker_stats)](auto& this_per_worker_stats) {
        for (const auto& kv : other_per_worker_stats_entries) {
          ld_check(kv.second != nullptr);
          auto& stats_ptr = this_per_worker_stats[kv.first];
          if (stats_ptr == nullptr) {
            stats_ptr = std::make_shared<PerWorkerTimeSeriesStats>(*kv.second);
          } else {
            stats_ptr->aggregate(*kv.second, agg_override);
          }
        }
      });

  aggregateHistogram(
      agg_override, *client.histograms, *other.client.histograms);

  if (other.server_histograms) {
    ld_check(server_histograms);
    aggregateHistogram(
        agg_override, *server_histograms, *other.server_histograms);
  }

  if (other.per_shard_histograms) {
    ld_check(per_shard_histograms);
    aggregateHistogram(
        agg_override, *per_shard_histograms, *other.per_shard_histograms);
  }

  if (other.per_shard_stats) {
    ld_check(per_shard_stats);
    per_shard_stats->aggregate(
        *other.per_shard_stats, agg_override, destroyed_threads);
  }
}

void Stats::reset() {
#define RESETTING_STATS
  switch (params->get()->stats_set) {
    case StatsParams::StatsSet::DEFAULT:
#define STAT_DEFINE(name, _) name = {};
#include "logdevice/common/stats/common_stats.inc" // nolint
      if (params->get()->is_server) {
#define STAT_DEFINE(name, _) name = {};
#include "logdevice/common/stats/server_stats.inc" // nolint
        for (auto& sts : per_storage_task_type_stats) {
          sts.reset();
        }
        if (server_histograms) {
          server_histograms->clear();
        }
        if (per_shard_histograms) {
          per_shard_histograms->clear();
        }
        if (per_shard_stats) {
          per_shard_stats->reset();
        }
      } else {
#define STAT_DEFINE(name, _) client.name = {};
#include "logdevice/common/stats/client_stats.inc" // nolint
        per_client_node_stats.wlock()->reset();
        client.histograms->clear();
      }

      for (auto& tcs : per_traffic_class_stats) {
        tcs.reset();
      }

      for (auto& fgs : per_flow_group_stats) {
        fgs.reset();
      }

      for (auto& fgs : per_flow_group_stats_rt) {
        fgs.reset();
      }

      for (auto& rts : per_request_type_stats) {
        rts.reset();
      }

      for (auto& mts : per_message_type_stats) {
        mts.reset();
      }

      per_worker_stats.wlock()->clear();

      per_log_stats.wlock()->clear();
      break;
    case StatsParams::StatsSet::LDBENCH_WORKER:
#define STAT_DEFINE(name, _) ldbench->name = {};
#include "logdevice/common/stats/ldbench_worker_stats.inc" // nolint

      ldbench->delivery_latency->clear();
      break;
    case StatsParams::StatsSet::CHARACTERIZE_LOAD:
#define STAT_DEFINE(name, _) characterize_load->name = {};
#include "logdevice/common/stats/characterize_load_stats.inc" // nolint
      break;
  } // let compiler check that all enum values are handled.
}

PerTrafficClassStats Stats::totalPerTrafficClassStats() const {
  PerTrafficClassStats s;
  for (const auto& tcs : per_traffic_class_stats) {
    s.aggregate(tcs, folly::none);
  }
  return s;
}

PerFlowGroupStats Stats::totalPerFlowGroupStats() const {
  PerFlowGroupStats total;
  for (const auto& fg : per_flow_group_stats) {
    total.aggregate(fg, folly::none);
  }
  return total;
}

PerShardStats Stats::totalPerShardStats() const {
  PerShardStats s;
  ld_check(per_shard_stats);

  for (shard_index_t i = 0; i < per_shard_stats->getNumShards(); ++i) {
    s.aggregate(*per_shard_stats->get(i), folly::none);
  }
  return s;
}

std::unique_ptr<PerShardHistograms> Stats::totalPerShardHistograms() const {
  ld_check(params->get()->is_server);
  ld_check(per_shard_histograms);

  auto h = std::make_unique<PerShardHistograms>();
  for (auto& hist : per_shard_histograms->map()) {
    auto* merged = h->get(hist.first, 0);
    ld_check(merged);
    for (shard_index_t idx = 0; idx < hist.second->getNumShards(); ++idx) {
      auto* shard = hist.second->get(idx);
      ld_check(shard);
      merged->merge(*shard);
    }
  }

  return h;
}

void Stats::enumerate(EnumerationCallbacks* cb, bool list_all) const {
  ld_check(cb);

  switch (params->get()->stats_set) {
    case StatsParams::StatsSet::LDBENCH_WORKER:
#define STAT_DEFINE(s, _) cb->stat(#s, ldbench->s);
#include "logdevice/common/stats/ldbench_worker_stats.inc" // nolint

      cb->histogram("delivery_latency", *ldbench->delivery_latency);
      return; // nothing more to do here
    case StatsParams::StatsSet::CHARACTERIZE_LOAD:
#define STAT_DEFINE(s, _) cb->stat(#s, characterize_load->s);
#include "logdevice/common/stats/characterize_load_stats.inc" // nolint
      return; // nothing more to do here
    case StatsParams::StatsSet::DEFAULT:
      break;
  } // let compiler check that all enum values are handled.

  if (params->get()->is_server) {
    // Server simple stats.

#define STAT_DEFINE(s, _) cb->stat(#s, s);
#include "logdevice/common/stats/server_stats.inc" // nolint

    // Per shard.

    ld_check(per_shard_stats);
    for (shard_index_t i = 0; i < per_shard_stats->getNumShards(); ++i) {
#define STAT_DEFINE(c, _) cb->stat(#c, i, per_shard_stats->get(i)->c);
#include "logdevice/common/stats/per_shard_stats.inc" // nolint
    }

    // Aggregated across all shards.
    PerShardStats total = totalPerShardStats();
#define STAT_DEFINE(c, _) cb->stat(#c, total.c);
#include "logdevice/common/stats/per_shard_stats.inc" // nolint
  } else {
    // Client simple stats.

#define STAT_DEFINE(s, _) cb->stat(#s, client.s);
#include "logdevice/common/stats/client_stats.inc" // nolint
  }

  // Stats common for server and client

#define STAT_DEFINE(s, _) cb->stat(#s, s);
#include "logdevice/common/stats/common_stats.inc" // nolint

  // Per traffic class.

  for (int i = 0; i < per_traffic_class_stats.size(); ++i) {
#define STAT_DEFINE(c, _) \
  cb->stat(#c, (TrafficClass)i, per_traffic_class_stats[i].c);
#include "logdevice/common/stats/per_traffic_class_stats.inc" // nolint
  }

  // Aggregated across all traffic classes.

  {
    PerTrafficClassStats total = totalPerTrafficClassStats();
#define STAT_DEFINE(c, _) cb->stat(#c, total.c);
#include "logdevice/common/stats/per_traffic_class_stats.inc" // nolint
  }

  /* Network Traffic Shaping specific stats */
  for (int i = 0; i < per_flow_group_stats.size(); ++i) {
    // Per flow group (NodeLocationScope).
    auto& fgs = per_flow_group_stats[i];
#define STAT_DEFINE(c, _) \
  cb->stat("flow_group." #c, (NodeLocationScope)i, fgs.c);
#include "logdevice/common/stats/per_flow_group_stats.inc" // nolint

    auto msg_priority_totals = fgs.totalPerShapingPriorityStats();
#define STAT_DEFINE(c, _) \
  cb->stat("flow_group." #c, (NodeLocationScope)i, msg_priority_totals.c);
#include "logdevice/common/stats/per_msg_priority_stats.inc"    // nolint
#include "logdevice/common/stats/per_priority_stats_common.inc" // nolint

    // Per flow group and message priority.
    for (int p_idx = 0; p_idx < fgs.priorities.size(); ++p_idx) {
      auto& mps = fgs.priorities[p_idx];
#define STAT_DEFINE(c, _) \
  cb->stat("flow_group." #c, (NodeLocationScope)i, (Priority)p_idx, mps.c);
#include "logdevice/common/stats/per_msg_priority_stats.inc"    // nolint
#include "logdevice/common/stats/per_priority_stats_common.inc" // nolint
    }
  }

  /* Read Throttling specific stats */
  for (int i = 0; i < per_flow_group_stats_rt.size(); ++i) {
    auto& fgs = per_flow_group_stats_rt[i];
    auto priority_totals = fgs.totalPerShapingPriorityStats();
#define STAT_DEFINE(c, _) \
  cb->stat("flow_group_rt." #c, (NodeLocationScope)i, priority_totals.c);
#include "logdevice/common/stats/per_priority_stats_common.inc" // nolint

    // Per flow group and priority.
    for (int p_idx = 0; p_idx < fgs.priorities.size(); ++p_idx) {
      auto& mps = fgs.priorities[p_idx];
#define STAT_DEFINE(c, _) \
  cb->stat("flow_group_rt." #c, (NodeLocationScope)i, (Priority)p_idx, mps.c);
#include "logdevice/common/stats/per_priority_stats_common.inc" // nolint
    }
  }

  {
    // Aggregated across all flow groups.
    auto total = totalPerFlowGroupStats();
#define STAT_DEFINE(c, _) cb->stat("flow_group." #c, total.c);
#include "logdevice/common/stats/per_flow_group_stats.inc" // nolint

    // Aggregated across all flow groups, per message priority.
    for (int p_idx = 0; p_idx < total.priorities.size(); ++p_idx) {
      auto& mps = total.priorities[p_idx];
#define STAT_DEFINE(c, _) cb->stat("flow_group." #c, (Priority)p_idx, mps.c);
#include "logdevice/common/stats/per_msg_priority_stats.inc"    // nolint
#include "logdevice/common/stats/per_priority_stats_common.inc" // nolint
    }

    // Aggregated across all flow groups and message priorities.
    auto mps_total = total.totalPerShapingPriorityStats();
#define STAT_DEFINE(c, _) cb->stat("flow_group." #c, mps_total.c);
#include "logdevice/common/stats/per_msg_priority_stats.inc"    // nolint
#include "logdevice/common/stats/per_priority_stats_common.inc" // nolint
  }

  // Per message type.
  std::vector<std::string> message_type_names(
      static_cast<int>(MessageType::MAX));
#define MESSAGE_TYPE(name, _) \
  message_type_names[int(MessageType::name)] = std::string(#name);
#include "logdevice/common/message_types.inc" // nolint

  ld_check(message_type_names.size() == per_message_type_stats.size());

  for (size_t i = 0; i < per_message_type_stats.size(); ++i) {
    if (message_type_names[i].size() > 0) {
#define STAT_DEFINE(s, _) \
  cb->stat(#s, (MessageType)i, per_message_type_stats[i].s);
#include "logdevice/common/stats/per_message_type_stats.inc" // nolint
    }
  }

  // Per request type.
  for (size_t i = int(RequestType::INVALID) + 1;
       i < per_request_type_stats.size();
       ++i) {
#define STAT_DEFINE(s, _) \
  cb->stat(#s, (RequestType)i, per_request_type_stats[i].s);
#include "logdevice/common/stats/per_request_type_stats.inc" // nolint
  }

  cb->stat(
      "request_worker_usec.INVALID",
      per_request_type_stats[int(RequestType::INVALID)].request_worker_usec);

  // Per storage task type
  std::array<bool, static_cast<int>(StorageTaskType::MAX)>
      publish_stats_by_index = {};
  std::unordered_map<std::string, bool> publish_stats_by_name;
#define STORAGE_TASK_TYPE(type, str_name, v)                              \
  {                                                                       \
    bool publish = list_all ? true : v;                                   \
    ld_check(int(StorageTaskType::type) < publish_stats_by_index.size()); \
    publish_stats_by_index[int(StorageTaskType::type)] = publish;         \
    publish_stats_by_name[str_name] = publish;                            \
    publish_stats_by_name["queue_time." str_name] = publish;              \
  }
#include "logdevice/common/storage_task_types.inc"

  auto publishStorageTaskStats = [&cb](StorageTaskType t,
                                       const PerStorageTaskTypeStats& sts) {
#define STAT_DEFINE(c, _) cb->stat(std::string(#c), t, sts.c);
#include "logdevice/common/stats/per_storage_task_type_stats.inc" // nolint
  };

  // Starting loop from 1, not including UNKNOWN, it will be posted separately
  // after the loop
  PerStorageTaskTypeStats unknown_stats;
  for (int i = 0; i < per_storage_task_type_stats.size(); ++i) {
    if (int(StorageTaskType::UNKNOWN) == i || !publish_stats_by_index[i]) {
      // aggregate all the other stats into "UNKNOWN"
      unknown_stats.aggregate(per_storage_task_type_stats[i], folly::none);
      continue;
    }
    publishStorageTaskStats((StorageTaskType)i, per_storage_task_type_stats[i]);
  }
  publishStorageTaskStats(StorageTaskType::UNKNOWN, unknown_stats);

  // Per log. Use synchronizedCopy() to avoid holding a read lock during
  // callbacks.
  for (auto const& kv : synchronizedCopy(&Stats::per_log_stats)) {
    ld_check(kv.second != nullptr);
#define STAT_DEFINE(name, _) cb->stat(#name, kv.first, kv.second->name);
#include "logdevice/common/stats/per_log_stats.inc" // nolint
  }

  if (params->get()->is_server) {
    // Per worker. Also using synchronizedCopy()
    for (auto const& kv : synchronizedCopy(&Stats::per_worker_stats)) {
      ld_check(kv.second != nullptr);
      auto now = std::chrono::steady_clock::now();
      auto from = now - params->get()->worker_stats_retention_time;
      cb->stat("avg_worker_load", kv.first, kv.second->avgLoad(from, now));
    }

    // Server histograms.
    ld_check(server_histograms);

    for (auto& i : server_histograms->map()) {
      cb->histogram(i.first, *i.second);
    }

    auto publishHist = [&cb](const std::string& name,
                             const ShardedHistogramBase& hist) {
      for (shard_index_t idx = 0; idx < hist.getNumShards(); ++idx) {
        auto* h = hist.get(idx);
        ld_check(h);
        cb->histogram(name, idx, *h);
      }
    };

    // Server per shard histograms.
    using hist_type = std::remove_reference<decltype(
        per_shard_histograms->storage_tasks[0])>::type;
    hist_type unknown_histogram;
    hist_type unknown_queue_histogram;
    ld_check(per_shard_histograms);
    for (auto& hist : per_shard_histograms->map()) {
      auto it = publish_stats_by_name.find(hist.first);
      if (it != publish_stats_by_name.end() && !it->second) {
        if (boost::starts_with(hist.first, "queue_time.")) {
          unknown_queue_histogram.merge(*hist.second);
        } else {
          unknown_histogram.merge(*hist.second);
        }
        continue;
      }
      publishHist(hist.first, *hist.second);
    }
    publishHist("OtherStorageTask", unknown_histogram);
    publishHist("queue_time.OtherStorageTask", unknown_queue_histogram);

    // Aggregated across all shards. Ignoring the publish_stats bit.

    auto total = totalPerShardHistograms();
    ld_check(total != nullptr);
    for (auto& i : total->map()) {
      cb->histogram(i.first, *i.second->get(0));
    }
  } else {
    // Client histograms.

    for (auto& i : client.histograms->map()) {
      cb->histogram(i.first, *i.second);
    }
  }
}

void Stats::deriveStats() {
  if (params->get()->is_server) {
    ld_check(per_shard_stats);
    for (shard_index_t i = 0; i < per_shard_stats->getNumShards(); ++i) {
      PerShardStats& s = *per_shard_stats->get(i);

      s.shards_waiting_for_non_started_restore = s.shard_missing_all_data &&
          !s.shard_waiting_for_undrain && !s.full_restore_set_contains_myself;
      s.non_empty_shards_in_restore =
          !s.shard_missing_all_data && s.full_restore_set_contains_myself;
      s.unwritable_non_restore_shards =
          (s.failed_safe_log_stores || s.failing_log_stores) &&
          !s.full_restore_set_contains_myself;
      s.iterator_errors_in_writable_shards =
          s.failed_safe_log_stores ? 0 : s.iterator_errors.load();
      s.unhealthy_shards = s.iterator_errors || s.shard_missing_all_data ||
          s.failed_safe_log_stores || s.failing_log_stores;
    }
  }
}

Stats::ClientStats::ClientStats()
    : histograms(std::make_unique<ClientHistograms>()) {}

Stats::ClientStats::~ClientStats() = default;

Stats::ClientStats::ClientStats(ClientStats&&) noexcept = default;

Stats::ClientStats& Stats::ClientStats::operator=(ClientStats&&) noexcept =
    default;

StatsHolder::StatsHolder(StatsParams params)
    : params_(std::make_shared<StatsParams>(std::move(params))),
      dead_stats_(&params_) {}

Stats StatsHolder::aggregate() const {
  Stats result(&params_);

  {
    std::lock_guard<std::mutex> lock(dead_stats_mutex_);
    result.aggregate(dead_stats_);
  }

  for (const auto& x : thread_stats_.accessAllThreads()) {
    result.aggregate(x.stats);
  }

  result.deriveStats();

  return result;
}

void StatsHolder::reset() {
  for (auto& x : thread_stats_.accessAllThreads()) {
    x.stats.reset();
  }
  dead_stats_.reset();
}

StatsHolder::~StatsHolder() {
  // No need to update dead_stats_ when StatsHolder is being destroyed.
  for (auto& x : thread_stats_.accessAllThreads()) {
    x.owner = nullptr;
  }
}

PerLogTimeSeries::PerLogTimeSeries(
    size_t num_buckets,
    std::vector<std::chrono::milliseconds> time_intervals)
    : timeIntervals_(std::move(time_intervals)),
      timeSeries_(std::make_unique<TimeSeries>(num_buckets,
                                               timeIntervals_.size(),
                                               timeIntervals_.data())) {}

PerLogTimeSeries::~PerLogTimeSeries() = default;

void PerLogTimeSeries::addValue(size_t n) {
  using namespace std::chrono;
  auto now = duration_cast<TimeSeries::Duration>(
      steady_clock::now().time_since_epoch());
  timeSeries_->addValue(now, n);
}

CustomCountersTimeSeries::CustomCountersTimeSeries()
    : customCountersTimeSeries_(new CustomCounters()) {}

const std::vector<std::chrono::milliseconds>&
CustomCountersTimeSeries::getCustomCounterIntervals() {
  static std::vector<std::chrono::milliseconds> intervals{
      std::chrono::seconds(120), std::chrono::seconds(600)};
  return intervals;
};

void CustomCountersTimeSeries::addCustomCounters(
    const std::map<uint8_t, int64_t>& counters) {
  using namespace std::chrono;
  auto now = duration_cast<TimeSeries::Duration>(
      steady_clock::now().time_since_epoch());
  for (auto counter : counters) {
    auto it = customCountersTimeSeries_->find(counter.first);
    if (it == customCountersTimeSeries_->end()) {
      it = customCountersTimeSeries_
               ->emplace(std::make_pair(
                   counter.first,
                   TimeSeries(NUM_BUCKETS,
                              getCustomCounterIntervals().size(),
                              getCustomCounterIntervals().data())))
               .first;
    }
    it->second.addValue(now, counter.second);
  }
}

const int PerWorkerTimeSeriesStats::NUM_BUCKETS{10};

PerWorkerTimeSeriesStats::PerWorkerTimeSeriesStats(
    std::chrono::milliseconds retention_time) {
  auto tmp = TimeSeries(NUM_BUCKETS, retention_time);
  load_stats_ = std::make_unique<SyncedTimeSeries>(std::move(tmp));
}

PerWorkerTimeSeriesStats::PerWorkerTimeSeriesStats(
    const PerWorkerTimeSeriesStats& other) {
  *this = other;
}

PerWorkerTimeSeriesStats& PerWorkerTimeSeriesStats::
operator=(const PerWorkerTimeSeriesStats& other) {
  load_stats_ = std::make_unique<SyncedTimeSeries>(*other.load_stats_);
  return *this;
}

void PerWorkerTimeSeriesStats::addLoad(uint64_t load) {
  auto now = std::chrono::duration_cast<TimeSeries::Duration>(
      std::chrono::steady_clock::now().time_since_epoch());
  load_stats_->wlock()->addValue(now, load);
}

uint64_t PerWorkerTimeSeriesStats::avgLoad(TimePoint from, TimePoint to) {
  // add a single nano second because if _to_ is the last inserted time, the
  // whole bucket will not be counted otherwise and values will be truncated
  to += std::chrono::nanoseconds{1};
  load_stats_->wlock()->update(to);
  return load_stats_->rlock()->avg(from, to);
}

// We must drop the const for `other' because we need to call update on other.
void PerWorkerTimeSeriesStats::aggregate(PerWorkerTimeSeriesStats& other,
                                         StatsAggOptional /* unused */) {
  auto now = load_stats_->rlock()->getLatestTime();
  other.load_stats_->wlock()->update(now);
  // The following aggregation might have some loss of precision if both
  // BucketedTimeSeries have buckets that are a little displaced from each
  // other. However, it should be a non-issue in practice, since we don't
  // expect to aggregate two non-empty BucketedTimeSeries in this use case.
  other.load_stats_->withRLock(
      [&, &load_stats = this->load_stats_](const TimeSeries& otherTS) {
        otherTS.forEachBucket(
            [&load_stats, &otherTS](TimeSeries::Bucket /* unused */,
                                    TimePoint start,
                                    TimePoint end) -> bool {
              auto nsamples = otherTS.count(start, end);
              auto sm = otherTS.sum(start, end);
              load_stats->wlock()->addValueAggregated(start, sm, nsamples);
              return true;
            });
      });
}

const int PerNodeTimeSeriesStats::NUM_BUCKETS{10};

PerNodeTimeSeriesStats::PerNodeTimeSeriesStats(
    std::chrono::milliseconds retention_time)
    : retention_time_(retention_time) {
  auto tmp = TimeSeries(NUM_BUCKETS, retention_time);
  append_success_ = std::make_unique<SyncedTimeSeries>(tmp);
  append_fail_ = std::make_unique<SyncedTimeSeries>(std::move(tmp));
}
void PerNodeTimeSeriesStats::addAppendSuccess(TimePoint time_of_append) {
  append_success_->wlock()->addValue(time_of_append, 1);
}

void PerNodeTimeSeriesStats::addAppendFail(TimePoint time_of_append) {
  append_fail_->wlock()->addValue(time_of_append, 1);
}

uint32_t PerNodeTimeSeriesStats::sumAppendSuccess(TimePoint from,
                                                  TimePoint to) {
  return sumAppend(append_success_.get(), from, to);
}

uint32_t PerNodeTimeSeriesStats::sumAppendFail(TimePoint from, TimePoint to) {
  return sumAppend(append_fail_.get(), from, to);
}

uint32_t PerNodeTimeSeriesStats::sumAppend(SyncedTimeSeries* appends,
                                           TimePoint from,
                                           TimePoint to) {
  // add a single nano second because if _to_ is the last inserted time, the
  // whole bucket will not be counted otherwise and values will be truncated
  return appends->rlock()->sum(from, to + std::chrono::nanoseconds{1});
}

void PerNodeTimeSeriesStats::updateCurrentTime(TimePoint current_time) {
  const auto update = [current_time](auto& locked_map) {
    locked_map.update(current_time);
  };

  append_success_->withWLock(update);
  append_fail_->withWLock(update);
}

void PerNodeTimeSeriesStats::updateRetentionTime(
    std::chrono::milliseconds retention_time) {
  if (this->retention_time_ == retention_time) {
    return;
  }

  this->retention_time_ = retention_time;

  const auto now = std::chrono::steady_clock::now();

  const auto update_retention = [&](auto& time_series) {
    return std::make_unique<SyncedTimeSeries>(
        newTimeSeriesWithUpdatedRetentionTime(
            time_series, retention_time, now));
  };

  append_success_ = append_success_->withWLock(update_retention);
  append_fail_ = append_fail_->withWLock(update_retention);
}

std::chrono::milliseconds PerNodeTimeSeriesStats::retentionTime() const {
  return retention_time_;
}

const PerNodeTimeSeriesStats::SyncedTimeSeries*
PerNodeTimeSeriesStats::getAppendSuccess() const {
  return append_success_.get();
}

const PerNodeTimeSeriesStats::SyncedTimeSeries*
PerNodeTimeSeriesStats::getAppendFail() const {
  return append_fail_.get();
}

using ClientNodeTimeSeriesMap =
    TimeSeriesMap<PerClientNodeTimeSeriesStats::Key,
                  PerClientNodeTimeSeriesStats::Value,
                  PerClientNodeTimeSeriesStats::Key::Hash>;
PerClientNodeTimeSeriesStats::PerClientNodeTimeSeriesStats(
    std::chrono::milliseconds retention_time)
    : retention_time_(retention_time),
      timeseries_(std::make_unique<TimeSeries>(30, retention_time)) {}

PerClientNodeTimeSeriesStats::TimeSeries*
PerClientNodeTimeSeriesStats::timeseries() const {
  return timeseries_.get();
}

void PerClientNodeTimeSeriesStats::append(ClientID client,
                                          NodeID node,
                                          uint32_t successes,
                                          uint32_t failures,
                                          TimePoint time) {
  timeseries_->addValue(
      time,
      ClientNodeTimeSeriesMap(
          PerClientNodeTimeSeriesStats::Key{client, node},
          PerClientNodeTimeSeriesStats::Value{successes, failures}));
}

void PerClientNodeTimeSeriesStats::reset() {
  timeseries_->clear();
}

bool operator==(const PerClientNodeTimeSeriesStats::Value& lhs,
                const PerClientNodeTimeSeriesStats::Value& rhs) {
  return lhs.successes == rhs.successes && lhs.failures == rhs.failures;
}

bool operator==(const PerClientNodeTimeSeriesStats::Key& a,
                const PerClientNodeTimeSeriesStats::Key& b) {
  return a.client_id == b.client_id && a.node_id == b.node_id;
}

bool operator!=(const PerClientNodeTimeSeriesStats::Key& a,
                const PerClientNodeTimeSeriesStats::Key& b) {
  return a.client_id != b.client_id || a.node_id != b.node_id;
}

using ClientNodeValue = PerClientNodeTimeSeriesStats::ClientNodeValue;
std::vector<ClientNodeValue>
PerClientNodeTimeSeriesStats::sum(TimePoint from, TimePoint to) const {
  auto total = timeseries_->sum(from, to);
  return processStats(total);
}

std::vector<ClientNodeValue> PerClientNodeTimeSeriesStats::sum() const {
  auto total = timeseries_->sum();
  return processStats(total);
}

std::vector<ClientNodeValue> PerClientNodeTimeSeriesStats::processStats(
    const ClientNodeTimeSeriesMap& total) const {
  const auto& map = total.data();
  std::vector<ClientNodeValue> result{};
  for (const auto& p : map) {
    result.emplace_back(
        ClientNodeValue{p.first.client_id, p.first.node_id, p.second});
  }

  return result;
}

void PerClientNodeTimeSeriesStats::updateCurrentTime(TimePoint current_time) {
  timeseries_->update(current_time);
}

void PerClientNodeTimeSeriesStats::updateRetentionTime(
    std::chrono::milliseconds retention_time) {
  if (this->retention_time_ == retention_time) {
    return;
  }

  this->retention_time_ = retention_time;
  const auto now = std::chrono::steady_clock::now();
  auto new_timeseries = std::make_unique<TimeSeries>(
      newTimeSeriesWithUpdatedRetentionTime(*timeseries_, retention_time, now));
  timeseries_.swap(new_timeseries);
}

std::chrono::milliseconds PerClientNodeTimeSeriesStats::retentionTime() const {
  return retention_time_;
}

namespace {
// This statically asserts that all names are unique in the union of:
//  - common_stats.inc
//  - server_stats.inc,
//  - per_shard_stats.inc,
//  - per_traffic_class_stats.inc.
// The uniqueness is needed to prevent totals for per-something stats from
// having the same name as a non-per-something stat (see enumerate() for
// totals logic).
enum class StatsUniquenessCheck {
#define STAT_DEFINE(c, _) c,
#include "logdevice/common/stats/common_stats.inc" // nolint
#define STAT_DEFINE(c, _) c,
#include "logdevice/common/stats/server_stats.inc" // nolint
#define STAT_DEFINE(c, _) c,
#include "logdevice/common/stats/per_shard_stats.inc" // nolint
#define STAT_DEFINE(c, _) c,
#include "logdevice/common/stats/per_traffic_class_stats.inc" // nolint
};
} // namespace
}} // namespace facebook::logdevice
