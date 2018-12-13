/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <chrono>

#include "logdevice/common/NodeID.h"
#include "logdevice/common/Timer.h"
#include "logdevice/common/WorkerTimeoutStats.h"
#include "logdevice/common/configuration/Node.h"
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

  virtual WorkerTimeoutStats& getWorkerTimeoutStats();

  // The duration through which a node need to be consistently an outlier to get
  // graylisted
  virtual std::chrono::seconds getGracePeriod() const;

  // The duration of an active graylist
  virtual std::chrono::seconds getGraylistingDuration() const;

  // The percentage of the nodes that are allowed to be graylisted
  virtual double getGraylistNodeThreshold() const;

  // Get the nodes of the cluster
  virtual const configuration::Nodes& getNodes() const;

  virtual StatsHolder* getStats();

 private:
  // Removes expired graylisted nodes from the graylist_deadlines_ map
  void removeExpiredGraylistedNodes(Timestamp now);

  // Get the latency estimation for each node in the cluster
  Latencies getLatencyEstimationForNodes(const configuration::Nodes& nodes);

  void updateActiveGraylist();

  // Finds outliers in a certain region and returns the node indexes of the
  // outliers sorted descending according to their latency (highest latency
  // first)
  std::vector<node_index_t>
  findSortedOutlierNodesPerRegion(Latencies latencies);

  // Based on the outliers, update the potential_graylist. Add new outliers
  // to the potential graylist and return the confirmed outliers.
  std::vector<node_index_t>
  updatePotentialGraylist(Timestamp now, std::vector<node_index_t> outliers);

  static std::vector<std::pair<Timestamp, node_index_t>>
  getSortedGraylistDeadlines(
      std::unordered_map<node_index_t, Timestamp> deadlines);

  // A map determines when a node started being a potential graylisted
  std::unordered_map<node_index_t, Timestamp> potential_graylist_;

  // A map determines the timestamp until which the node is graylisted
  std::unordered_map<node_index_t, Timestamp> graylist_deadlines_;

  // The active graylisted nodes cached as they will be called frequently
  // from the appenders
  std::unordered_set<node_index_t> graylist_;

  Timer timer_;
};

}} // namespace facebook::logdevice
