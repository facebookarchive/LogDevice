/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "logdevice/server/sequencer_boycotting/NodeStatsController.h"

#include <gmock/gmock.h>
#include <gtest/gtest.h>

using namespace facebook::logdevice;
using namespace testing;
using namespace std::literals::chrono_literals;

class MockNodeStatsController : public NodeStatsController {
 public:
  explicit MockNodeStatsController(node_index_t max_node_index = 1)
      : NodeStatsController(), max_node_index_(max_node_index) {
    ON_CALL(*this, activateAggregationTimer())
        .WillByDefault(
            Invoke(this, &MockNodeStatsController::aggregationTimerCallback));
    ON_CALL(*this, getRetentionTime()).WillByDefault(Return(0s));
    ON_CALL(*this, getAggregationPeriod()).WillByDefault(Return(30s));
  }
  ~MockNodeStatsController() override = default;

  MOCK_METHOD0(activateAggregationTimer, void());
  MOCK_METHOD0(sendCollectStatsMessage, void());
  MOCK_METHOD0(activateResponseTimer, void());
  MOCK_METHOD0(cancelAggregationTimer, void());
  MOCK_METHOD0(cancelResponseTimer, void());

  MOCK_CONST_METHOD0(getRetentionTime, std::chrono::milliseconds());
  MOCK_CONST_METHOD0(getAggregationPeriod, std::chrono::milliseconds());
  MOCK_CONST_METHOD0(
      getNodesConfiguration,
      std::shared_ptr<const configuration::nodes::NodesConfiguration>());

  size_t getMaxNodeIndex() const override {
    return max_node_index_;
  }

  // getters of member variables
  const std::vector<BucketedNodeStats>& getReceivedStats() const {
    return received_stats_;
  }

  msg_id_t getCurrentMsgId() const {
    return current_msg_id_;
  }

 private:
  const node_index_t max_node_index_;
};

TEST(NodeStatsControllerTest, StartCollectsStats) {
  MockNodeStatsController stats_controller;

  EXPECT_CALL(stats_controller, activateAggregationTimer()).Times(1);
  EXPECT_CALL(stats_controller, sendCollectStatsMessage()).Times(1);
  EXPECT_CALL(stats_controller, activateResponseTimer()).Times(1);

  stats_controller.start();
}

TEST(NodeStatsControllerTest, StopCancelsTimers) {
  MockNodeStatsController stats_controller;

  EXPECT_CALL(stats_controller, cancelAggregationTimer()).Times(1);
  EXPECT_CALL(stats_controller, cancelResponseTimer()).Times(1);

  stats_controller.stop();
}

TEST(NodeStatsControllerTest, OnStatsReceivedSimple) {
  MockNodeStatsController stats_controller;
  NodeID from{1}, stats_about{0};
  BucketedNodeStats expected;
  expected.node_ids.emplace_back(stats_about);
  // equality check compares size, no need to fill with values
  expected.client_counts->resize(boost::extents[1][2][3]);
  expected.summed_counts->resize(boost::extents[1][2]);

  stats_controller.start();

  EXPECT_EQ(0, stats_controller.getCurrentMsgId());
  stats_controller.onStatsReceived(0, from, std::move(expected));

  // should be stored in the index of the node that sent the stats
  EXPECT_THAT(stats_controller.getReceivedStats().at(from.index()).node_ids,
              ElementsAre(stats_about));
  // we set a non-zero size, they should not be empty
  EXPECT_FALSE(stats_controller.getReceivedStats()
                   .at(from.index())
                   .summed_counts->empty());
  EXPECT_FALSE(stats_controller.getReceivedStats()
                   .at(from.index())
                   .client_counts->empty());
}

// out of bounds node index. Don't crash, just discard
TEST(NodeStatsControllerTest, OnStatsReceivedNodeIndexOOB) {
  node_index_t max_node_index{3};
  NodeID from{static_cast<node_index_t>(max_node_index + 1)}, stats_about{1};
  BucketedNodeStats stats;
  // equality check compares size, no need to fill with values
  stats.client_counts->resize(boost::extents[1][2][3]);
  stats.summed_counts->resize(boost::extents[1][2]);

  MockNodeStatsController stats_controller(max_node_index);

  stats_controller.start();
  std::vector<BucketedNodeStats> default_stats(
      stats_controller.getReceivedStats().size());

  EXPECT_EQ(0, stats_controller.getCurrentMsgId());
  stats_controller.onStatsReceived(0, from, std::move(stats));

  EXPECT_EQ(default_stats, stats_controller.getReceivedStats());
}

TEST(NodeStatsControllerTest, OnStatsReceivedIncorrectMsgID) {
  NodeID from{0}, stats_about{1};
  BucketedNodeStats stats;
  // equality check compares size, no need to fill with values
  stats.client_counts->resize(boost::extents[1][2][3]);
  stats.summed_counts->resize(boost::extents[1][2]);

  // the first msg should have id 0
  NodeStatsController::msg_id_t incorrect_msg_id{123};

  MockNodeStatsController stats_controller;

  stats_controller.start();
  std::vector<BucketedNodeStats> default_stats(
      stats_controller.getReceivedStats().size());

  EXPECT_EQ(0, stats_controller.getCurrentMsgId());
  stats_controller.onStatsReceived(incorrect_msg_id, from, std::move(stats));

  EXPECT_EQ(default_stats, stats_controller.getReceivedStats());
}
