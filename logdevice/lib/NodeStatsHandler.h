/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include "logdevice/common/ExponentialBackoffTimer.h"
#include "logdevice/common/Timer.h"
#include "logdevice/common/protocol/NODE_STATS_Message.h"
#include "logdevice/common/stats/Stats.h"
#include "logdevice/lib/NodeStatsMessageCallback.h"

namespace facebook { namespace logdevice {

struct NodeStats {
  NodeID id_;
  uint32_t append_successes_{0};
  uint32_t append_fails_{0};
};

class NodeStatsHandler : public NodeStatsMessageCallback {
 public:
  NodeStatsHandler() = default;
  virtual ~NodeStatsHandler() override = default;

  static constexpr int getThreadAffinity(int nthreads) {
    // The NodeStatsHandler should only exist on a single worker
    return 5 % nthreads;
  }

  /**
   * Set up the timers. Should only be called once, and has to be called before
   * start
   */
  void init();

  /**
   * will start periodically sending NODE_STATS_Messages with the period set in
   * the config during init
   */
  void start();

  // NodeStatsMessageCallback impl

  virtual void onMessageSent(Status status) override;

  virtual void onReplyFromNode(uint64_t msg_id) override;

 protected: // for tests
  // returns the same as Sender::sendMessage
  virtual int sendStats(std::vector<NodeStats>&& stats);
  virtual std::vector<NodeStats> collectStats();

  virtual void updateDestination();

  virtual void activateAggregationTimer();
  virtual void activateRetryTimer();
  virtual void cancelRetryTimer();
  virtual void resetRetryTimer();
  virtual void activateClientTimeoutTimer();
  virtual void cancelClientTimeoutTimer();

  virtual void onClientTimeout();

  void onAggregation();
  void onRetry();

  /**
   * Some tests initialize an empty config, which will have an empty node set.
   * This will cause the RandomNodeSelector to fail, and throw an exception.
   * Use this function to check if the node set has at least one node, to allow
   * for such tests
   */
  virtual bool hasValidNodeSet() const;

  uint64_t current_msg_id_{0};

 private:
  void prepareAndSendStats();

  // find a random node which is alive, to be the new destination
  NodeID findDestinationNode() const;

  std::unique_ptr<NODE_STATS_Message>
  createMessage(std::vector<NodeStats>&& stats) const;

  std::chrono::milliseconds getAggregationDelay() const;

  // Where to send the stats to
  NodeID destination_;

  Timer aggregation_timer_;
  Timer client_timeout_timer_;
  // between unsuccessful sends, this timer defines the delay before retrying
  ExponentialBackoffTimer retry_timer_;
};

}} // namespace facebook::logdevice
