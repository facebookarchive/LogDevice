/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <chrono>
#include <unordered_map>

#include "logdevice/common/BucketedNodeStats.h"
#include "logdevice/common/stats/Stats.h"

/**
 * @file Used to aggregate PerClientNodeTimeSeriesStats into time buckets
 */

namespace facebook { namespace logdevice {
class PerClientNodeStatsAggregator {
 protected:
  template <class T>
  using ClientMap = std::unordered_map<ClientID, T, ClientID::Hash>;
  template <class T>
  using NodeMap = std::unordered_map<NodeID, T, NodeID::Hash>;
  using PeriodStatsPair =
      std::pair<uint32_t, BucketedNodeStats::ClientNodeStats>;
  using PerClientCounts = ClientMap<NodeMap<std::vector<PeriodStatsPair>>>;

 public:
  virtual ~PerClientNodeStatsAggregator() = default;
  /**
   * Aggregates the per-node stats receieved from clients
   * @params period_count For how many aggregation periods should this node
   *                      return stats for. Useful if a controller just starts
   *                      up to request stats for a longer period
   */
  BucketedNodeStats aggregate(unsigned int period_count) const;

 protected:
  /**
   * @returns The period at which stats are collected from the nodes, as defined
   *          in Settings::per_node_stats_controller_aggregation_period
   */
  virtual std::chrono::milliseconds getAggregationPeriod() const;

  /**
   * @returns The amount of worst clients to include in the result
   */
  virtual unsigned int getWorstClientCount() const;

  virtual StatsHolder* getStats() const;

 private:
  /**
   * Gather stats from the StatsHolder
   * @params period_count     The amount of
   *                          node_stats_controller_aggregation_periods stats
   *                          should be gathered
   * @params stats            The stats holder of this node
   * @returns                 Stats reported about each node from each client,
   *                          for each requested period
   */
  PerClientCounts fromRawStats(unsigned int period_count) const;

  /**
   * @returns The client_count worst client indices for the given period
   */
  std::unordered_set<unsigned int> findWorstClients(
      const boost::detail::multi_array::
          const_sub_array<BucketedNodeStats::ClientNodeStats, 1>& row,
      unsigned int client_count) const;

  /**
   * @returns A map of all tracked nodes, mapped to their index in the matrix
   */
  NodeMap<unsigned int> getNodes(const PerClientCounts& counts) const;
  /**
   * Turn the nested maps into a matrix to make it easier to find the worst node
   * per bucket and node
   *
   * @returns All values in counts in a 3-dimensional matrix where the first
   * dimension is node, second is bucket, third is client
   */
  boost::multi_array<BucketedNodeStats::ClientNodeStats, 3>
  getAllCounts(const PerClientCounts& counts,
               const NodeMap<unsigned int>& node_idxs,
               unsigned int period_count) const;
};
}} // namespace facebook::logdevice
