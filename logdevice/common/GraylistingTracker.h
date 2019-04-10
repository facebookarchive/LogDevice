/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <chrono>

#include "logdevice/common/ExponentialBackoffAdaptiveVariable.h"
#include "logdevice/common/NodeID.h"
#include "logdevice/common/Timer.h"
#include "logdevice/common/WorkerTimeoutStats.h"
#include "logdevice/common/configuration/nodes/NodesConfiguration.h"
#include "logdevice/common/stats/Stats.h"

namespace facebook { namespace logdevice {

/**
 * @file A centeralized place where all the graylisting logic exists.
 * The graylisting is worker-local and is based on STORE latency outlier
 * detection logic.
 * The graylist is refreshed every *graylisting_refresh_interval* seconds.
 */
class GraylistingTracker {
 protected:
  using Timestamp = std::chrono::steady_clock::time_point;
  using Latencies =
      std::vector<std::pair<node_index_t, WorkerTimeoutStats::Latency>>;
  using PerRegionLatencies = std::map<std::string, Latencies>;
  using ChronoExponentialBackoff =
      ChronoExponentialBackoffAdaptiveVariable<std::chrono::seconds>;

 public:
  virtual ~GraylistingTracker() = default;

  // 1. Calculates the new outliers from the worker's histogram
  // 2. Update the potential graylisted nodes
  // 3. Graylist nodes in the potential graylist if they are past the grace
  // period
  // 4. Remove expired graylists
  // 5. Trim the graylist to at most max_graylisted_nodes by ejecting old
  // graylisted nodes
  // 5. Cache the graylisted nodes in graylist_
  void updateGraylist(Timestamp now);

  // Return the previously cached graylisted nodes. This list will get refreshed
  // periodically, but to force refresh it, call updateGraylist()
  const std::unordered_set<node_index_t>& getGraylistedNodes() const;

  // Start refreshing the graylist
  void start();

  // Stop refreshing the graylist. The last calculated graylist will continue
  // being returned from getGraylistedNodes unless reset() is called
  void stop();

  // Check if the refresh timer is running or not
  bool isRunning() const;

  // Notifies the tracker when the settings change. Based on the change in the
  // settings, the tracker can start or tear down itself
  void onSettingsUpdated();

  // Clear the current cached graylist. Unless the timer is stopped, a new
  // graylist will be calculated in the next timer tick.
  void resetGraylist();

 protected:
  // Get the maximum number of nodes to be graylisted. This is calculated from]
  // the threshold flag and the number of nodes in the cluster.
  int64_t getMaxGraylistedNodes() const;

  // Given a list of latencies of all nodes in the cluster, group them per
  // region
  PerRegionLatencies groupLatenciesPerRegion(Latencies latencies);

  static std::vector<node_index_t>
  roundRobinFlattenVector(const std::vector<std::vector<node_index_t>>& vectors,
                          int64_t max_size);

  std::vector<node_index_t> findOutlierNodes(PerRegionLatencies regions);

  const std::unordered_map<node_index_t, Timestamp>&
  getGraylistDeadlines() const;

  virtual WorkerTimeoutStats& getWorkerTimeoutStats();

  // The duration through which a node need to be consistently an outlier to get
  // graylisted
  virtual std::chrono::seconds getGracePeriod() const;

  // Max duration of an active graylist
  virtual std::chrono::seconds getMaxGraylistingDuration() const;

  // The duration through which a recently ungraylisted node will be monitored
  // and graylisted as soon as it becomes an outlier
  virtual std::chrono::seconds getMonitoredPeriod() const;

  // Min duration of an active graylist
  virtual std::chrono::seconds getGraylistingDuration() const;

  // The percentage of the nodes that are allowed to be graylisted
  virtual double getGraylistNodeThreshold() const;

  virtual std::shared_ptr<const configuration::nodes::NodesConfiguration>
  getNodesConfiguration() const;

  virtual StatsHolder* getStats();

 private:
  // Update and get duration of an active graylist for node
  virtual std::chrono::seconds getUpdatedGraylistingDuration(node_index_t node,
                                                             Timestamp now);

  // Removes expired graylisted nodes from the graylist_deadlines_ map
  void removeExpiredGraylistedNodes(Timestamp now);

  // Removes expired monitored nodes from the monitored_outliers_ map
  void removeExpiredMonitoredNodes(Timestamp now);

  // Get the latency estimation for each node in the cluster
  Latencies getLatencyEstimationForNodes(
      const configuration::nodes::NodesConfiguration& nodes_configuration);

  void updateActiveGraylist();

  // Finds outliers in a certain region and returns the node indexes of the
  // outliers sorted descending according to their latency (highest latency
  // first)
  std::vector<node_index_t>
  findSortedOutlierNodesPerRegion(Latencies latencies);

  // Create backoff variable if not exists
  void createGraylistingBackoff(node_index_t node);

  std::chrono::seconds positiveFeedbackAndGet(node_index_t node, Timestamp now);

  void negativeFeedback(node_index_t node);

  // Based on the outliers, update the potential_graylist. Add new outliers
  // to the potential graylist and return the confirmed outliers.
  std::vector<node_index_t>
  updatePotentialGraylist(Timestamp now, std::vector<node_index_t> outliers);

  static std::vector<std::pair<Timestamp, node_index_t>>
  getSortedGraylistDeadlines(
      std::unordered_map<node_index_t, Timestamp> deadlines);

  // A map determines when a node started being a potential graylisted
  std::unordered_map<node_index_t, Timestamp> potential_graylist_;

  // A map determines when a node started being monitored after ungreylisting
  std::unordered_map<node_index_t, Timestamp> monitored_outliers_;

  // A map determines the timestamp until which the node is graylisted
  std::unordered_map<node_index_t, Timestamp> graylist_deadlines_;

  // Determines per-node exponential backoffs
  // to calculate timestamps for graylist_deadlines_
  // Contains only nodes that have been in graylist at least once
  // Backoff variable is only created when node added to actual graylist
  std::unordered_map<node_index_t, std::unique_ptr<ChronoExponentialBackoff>>
      graylist_backoffs_;

  // The active graylisted nodes cached as they will be called frequently
  // from the appenders
  std::unordered_set<node_index_t> graylist_;

  Timer timer_;
};

}} // namespace facebook::logdevice
