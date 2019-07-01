/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <folly/Optional.h>
#include <folly/dynamic.h>
#include <folly/stats/BucketedTimeSeries-defs.h>
#include <folly/stats/BucketedTimeSeries.h>

#include "logdevice/common/NodeID.h"
#include "logdevice/common/Timer.h"
#include "logdevice/common/configuration/Node.h"
#include "logdevice/common/configuration/nodes/NodesConfiguration.h"
#include "logdevice/server/sequencer_boycotting/MovingAverageAppendOutlierDetector.h"
#include "logdevice/server/sequencer_boycotting/NodeStatsControllerCallback.h"
#include "logdevice/server/sequencer_boycotting/PerClientNodeStatsAggregator.h"

namespace facebook { namespace logdevice {

class ServerProcessor;
class FailureDetector;

/**
 * @file A NodeStatsController is one of few selected nodes in the cluster. It
 * is selected during runtime, and it may change during the lifetime of the
 * cluster as nodes goes down and comes back up. The purpose of the
 * NodeStatsController is to collect stats from all other nodes, aggregate them,
 * and then find outliers in terms of append performance. Performance is
 * currently only measured in append successes vs append fails.
 *
 * If a node is deemed to be an outlier in terms of append performance, it will
 * be boycotted. A boycott will prohibit sequencer placement on the boycotted
 * node.
 */

class NodeStatsController : public NodeStatsControllerCallback {
 public:
  NodeStatsController() = default;
  virtual ~NodeStatsController() = default;

  virtual void onStatsReceived(msg_id_t msg_id,
                               NodeID from,
                               BucketedNodeStats stats) override;

  virtual void getDebugInfo(InfoAppendOutliersTable* table) override;

  /**
   * @params  nthreads The worker count
   * @returns          What worker to run the controller on
   */
  static constexpr int getThreadAffinity(int nthreads) {
    return 11 % nthreads;
  }

  /**
   * @returns true if the controller is started, false otherwise
   */
  bool isStarted();
  /**
   * Start periodically aggregating stats from all nodes and make decisions
   * regarding boycotting.
   */
  void start();
  /**
   * stop() should be executed if this node is no longer selected as a
   * controller. The timers will be stopped, but the data kept.
   */
  void stop();

  /**
   * indirectly called by the boycott tracker whenever this node decides
   * to boycott a sequencer node. Gathers the NodeStatsController state to
   * log it at the time of the boycott
   */
  void traceBoycott(NodeID boycotted_node,
                    std::chrono::system_clock::time_point boycott_start_time,
                    std::chrono::milliseconds boycott_duration) override;

 protected:
  virtual void activateAggregationTimer();
  virtual void cancelAggregationTimer();
  void aggregationTimerCallback();

  virtual void activateResponseTimer();
  virtual void cancelResponseTimer();
  void responseTimerCallback();

  /**
   * Adds the received_stats_ to the outlier detector
   */
  virtual void aggregate();
  /**
   * Given the aggregated_stats_, perform outlier detection to decide if a
   * node should be boycotted.
   *
   * @returns The NodeIDs that should be boycotted, in order of worst performing
   *          node to best performing node
   */
  virtual std::vector<NodeID> detectOutliers() const;
  /**
   * Will boycott on an best-effort basis. For example, if only a single node
   * may be boycotted, but two outliers are given, only the first (the worst)
   * outlier will be boycotted.
   *
   * @params outliers The nodes to boycott, in order of worst performing node to
   *                  best performing node
   */
  virtual void boycott(std::vector<NodeID> outliers);

  /**
   * Sends a message to all nodes (including itself) to respond with their
   * stats.  The message will only be sent once, any errors during sending is
   * simply discarded. This is because if a node for example responds with
   * NOBUFS, no need to hammer it even more.
   */
  virtual void sendCollectStatsMessage();

  /**
   * @returns Returns the max node index in the server config
   */
  virtual size_t getMaxNodeIndex() const;

  /**
   * @returns The time which the stats should be tracked for, as defined in the
   *          settings
   */
  virtual std::chrono::milliseconds getRetentionTime() const;

  /**
   * @returns The period at which stats are collected from the nodes
   */
  virtual std::chrono::milliseconds getAggregationPeriod() const;

  /**
   * @return The NodeID of all nodes
   */
  virtual std::vector<NodeID> getTargetNodes() const;

  /**
   * @return The nodes defined in the nodes configuration
   */
  virtual std::shared_ptr<const configuration::nodes::NodesConfiguration>
  getNodesConfiguration() const;

  /**
   * @return The failure detector if set, nullptr otherwise
   */
  virtual FailureDetector* getFailureDetector() const;

  virtual ServerProcessor* getProcessor() const;

  virtual double removeWorstClientPercentage() const;
  virtual unsigned int getRequiredClientCount() const;

  folly::dynamic getStateInJson();

  /**
   * Node indices starts from 0 and increases linearly over the amount of nodes.
   *
   * Nodes are added and removed from the cluster during runtime. We have to
   * make sure to resize the vectors when such occasions occur. There may be
   * gaps in the node indices, but these will later be filled with new nodes, so
   * even if there might be unused space it will eventually be used as new nodes
   * are added. Resizing of the cluster does not occur often. Once a new node
   * gets the same index as a previous node, those stats will most probably be
   * stale and will be removed.
   */

  /**
   * When a message is sent out to the nodes to collect their stats stats,
   * store their response in this vector.
   * Use NodeID::index from the node that sent the stats as index in the vector
   */
  std::vector<BucketedNodeStats> received_stats_;

  msg_id_t current_msg_id_{0};

 private:
  bool is_started_{false};

  Timer aggregation_timer_;
  Timer response_timer_;

  /**
   * Set before sending a message. Used to not have to not get the current time
   * each time a message is received
   */
  std::chrono::steady_clock::time_point last_send_time_;
  /**
   * Tracks the last time stats was received from a node. Used to calculate the
   * amount of buckets to collect from the node. Update the value with the
   * latest_send_time_ once a message is received. Indexing in the vector is the
   * node_index of the node the message was received from
   */
  std::vector<std::chrono::steady_clock::time_point> last_receive_time_;

  std::unique_ptr<AppendOutlierDetector> outlier_detector_{
      std::make_unique<MovingAverageAppendOutlierDetector>()};

  using AggregateStatsCallback =
      std::function<void(node_index_t node_index,
                         AppendOutlierDetector::NodeStats stats,
                         std::chrono::steady_clock::time_point time)>;

  void aggregateThroughCallback(AggregateStatsCallback cb);
};
}} // namespace facebook::logdevice
