/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "logdevice/server/sequencer_boycotting/PerClientNodeStatsAggregator.h"

#include "logdevice/common/ClientID.h"
#include "logdevice/common/Worker.h"

namespace facebook { namespace logdevice {

namespace {
/**
 * Returns a list of pairs, where the first element is the start of the time
 * interval, and the second element is the end of the interval. Sorted by newest
 * first.
 */
auto getTimeIntervals(int period_count,
                      std::chrono::milliseconds duration,
                      std::chrono::steady_clock::time_point current_time) {
  std::vector<std::pair<std::chrono::steady_clock::time_point,
                        std::chrono::steady_clock::time_point>>
      time_intervals;
  for (uint8_t i = 0; i < period_count; ++i) {
    auto start = current_time - (1 + i) * duration;
    auto end = start + duration + std::chrono::nanoseconds{1};
    time_intervals.emplace_back(std::make_pair(start, end));
  }

  return time_intervals;
}
} // namespace

BucketedNodeStats
PerClientNodeStatsAggregator::aggregate(unsigned int period_count) const {
  auto per_client_node_stats = fromRawStats(period_count);

  if (per_client_node_stats.empty()) {
    return BucketedNodeStats{};
  }

  const auto nodes = getNodes(per_client_node_stats);

  const auto all_counts =
      getAllCounts(per_client_node_stats, nodes, period_count);

  BucketedNodeStats stats;
  stats.node_ids.resize(nodes.size());
  stats.summed_counts->resize(
      boost::extents[all_counts.shape()[0]][period_count]);

  auto worst_clients_to_find = getWorstClientCount();
  if (worst_clients_to_find) {
    stats.client_counts->resize(
        boost::extents[all_counts.shape()[0]][period_count]
                      [worst_clients_to_find]);
  }

  for (const auto& node : nodes) {
    const auto node_idx = node.second;
    ld_check(node_idx < stats.node_ids.size());
    stats.node_ids[node_idx] = node.first;

    for (unsigned int bucket_idx = 0;
         bucket_idx < all_counts[node_idx].shape()[0];
         ++bucket_idx) {
      auto worst_client_idxs = findWorstClients(
          all_counts[node_idx][bucket_idx], worst_clients_to_find);

      unsigned int client_count_idx = 0;
      for (unsigned int client_idx = 0; client_idx < all_counts.shape()[2];
           ++client_idx) {
        if (!worst_client_idxs.count(client_idx)) {
          auto& summed_count = (*stats.summed_counts)[node_idx][bucket_idx];
          const auto& all_count = all_counts[node_idx][bucket_idx][client_idx];

          // only increment if there were values reported
          if (all_count.successes + all_count.fails > 0) {
            ++summed_count.client_count;
            summed_count.successes += all_count.successes;
            summed_count.fails += all_count.fails;
          }
        } else {
          (*stats.client_counts)[node_idx][bucket_idx][client_count_idx] =
              all_counts[node_idx][bucket_idx][client_idx];
          ++client_count_idx;
        }
      }
    }
  }

  return stats;
}

std::chrono::milliseconds
PerClientNodeStatsAggregator::getAggregationPeriod() const {
  return Worker::settings()
      .sequencer_boycotting.node_stats_controller_aggregation_period;
}

StatsHolder* PerClientNodeStatsAggregator::getStats() const {
  return Worker::onThisThread()->stats();
}

unsigned int PerClientNodeStatsAggregator::getWorstClientCount() const {
  return Worker::settings()
      .sequencer_boycotting.node_stats_send_worst_client_count;
}

PerClientNodeStatsAggregator::PerClientCounts
PerClientNodeStatsAggregator::fromRawStats(unsigned int period_count) const {
  const auto now = std::chrono::steady_clock::now();

  const auto time_intervals =
      getTimeIntervals(period_count, getAggregationPeriod(), now);

  PerClientCounts per_client_node_stats;
  getStats()->runForEach([&](auto& thread_stats) {
    for (auto& per_client_stats :
         thread_stats.synchronizedCopy(&Stats::per_client_node_stats)) {
      auto client_map_it =
          per_client_node_stats
              .emplace(std::make_pair(
                  per_client_stats.first,
                  NodeMap<std::vector<BucketedNodeStats::ClientNodeStats>>{}))
              .first;

      per_client_stats.second->updateCurrentTime(now);

      for (int period_index = 0; period_index < period_count; ++period_index) {
        const auto& interval = time_intervals[period_index];

        // sum over success
        this->perNodeSumForPeriod(
            period_index,
            period_count,
            per_client_stats.second->sumAppendSuccess(
                interval.first, interval.second),
            [](BucketedNodeStats::ClientNodeStats& node_stats) -> uint32_t& {
              return node_stats.successes;
            },
            client_map_it->second);

        // sum over fails
        this->perNodeSumForPeriod(
            period_index,
            period_count,
            per_client_stats.second->sumAppendFail(
                interval.first, interval.second),
            [](BucketedNodeStats::ClientNodeStats& node_stats) -> uint32_t& {
              return node_stats.fails;
            },
            client_map_it->second);
      }
    }
  });

  return per_client_node_stats;
}

void PerClientNodeStatsAggregator::perNodeSumForPeriod(
    int period_index,
    int period_count,
    const NodeMap<uint32_t>& append_counts,
    std::function<uint32_t&(BucketedNodeStats::ClientNodeStats&)>
        stats_variable_getter,
    /*mutable*/
    NodeMap<std::vector<BucketedNodeStats::ClientNodeStats>>& node_stats)
    const {
  for (auto& per_node_appends : append_counts) {
    const auto node = per_node_appends.first;

    auto node_stats_entry = node_stats.find(node);
    if (node_stats_entry == node_stats.end()) {
      node_stats_entry =
          node_stats
              .emplace(std::make_pair(
                  per_node_appends.first,
                  std::vector<BucketedNodeStats::ClientNodeStats>(
                      period_count)))
              .first;
    }

    auto& append_count = per_node_appends.second;

    stats_variable_getter(node_stats_entry->second.at(period_index)) +=
        append_count;
  }
}

PerClientNodeStatsAggregator::NodeMap<unsigned int>
PerClientNodeStatsAggregator::getNodes(const PerClientCounts& counts) const {
  NodeMap<unsigned int> nodes;
  unsigned int node_idx = 0;
  for (const auto& client_entry : counts) {
    for (const auto& node_entry : client_entry.second) {
      if (!nodes.count(node_entry.first)) {
        nodes.emplace(node_entry.first, node_idx);
        ++node_idx;
      }
    }
  }
  return nodes;
}

boost::multi_array<BucketedNodeStats::ClientNodeStats, 3>
PerClientNodeStatsAggregator::getAllCounts(
    const PerClientCounts& counts,
    const NodeMap<unsigned int>& node_idxs,
    unsigned int period_count) const {
  boost::multi_array<BucketedNodeStats::ClientNodeStats, 3> all_counts(
      boost::extents[node_idxs.size()][period_count][counts.size()]);

  unsigned int client_idx = 0;
  for (const auto& client_entry : counts) {
    for (const auto& node_entry : client_entry.second) {
      unsigned int period_idx = 0;
      for (const auto& bucket : node_entry.second) {
        all_counts[node_idxs.at(node_entry.first)][period_idx][client_idx] =
            bucket;
        ++period_idx;
      }
    }
    ++client_idx;
  }

  return all_counts;
}

std::unordered_set<unsigned int> PerClientNodeStatsAggregator::findWorstClients(
    const boost::detail::multi_array::
        const_sub_array<BucketedNodeStats::ClientNodeStats, 1>& row,
    unsigned int client_count) const {
  struct RatioWithIdx {
    unsigned int idx;
    double ratio;
  };

  auto successRatio = [](double suc, double fail) -> double {
    return suc + fail != 0 ? suc / (suc + fail) : 1.0;
  };

  std::vector<RatioWithIdx> to_sort;
  to_sort.reserve(row.size());
  for (unsigned int i = 0; i < row.size(); ++i) {
    to_sort.emplace_back(
        RatioWithIdx{i, successRatio(row[i].successes, row[i].fails)});
  }

  std::sort(to_sort.begin(), to_sort.end(), [](auto& lhs, auto& rhs) {
    return lhs.ratio < rhs.ratio;
  });

  // only use the first client_count values
  to_sort.resize(client_count);

  std::unordered_set<unsigned int> client_idxs;

  std::transform(to_sort.cbegin(),
                 to_sort.cend(),
                 std::inserter(client_idxs, client_idxs.begin()),
                 [](auto& val) { return val.idx; });

  return client_idxs;
}
}} // namespace facebook::logdevice
