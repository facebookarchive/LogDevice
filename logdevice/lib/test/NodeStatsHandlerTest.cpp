/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "logdevice/lib/NodeStatsHandler.h"

#include <gtest/gtest.h>

using namespace facebook::logdevice;

class MockNodeStatsHandler : public NodeStatsHandler {
 public:
  // make public
  void onClientTimeout() override {
    NodeStatsHandler::onClientTimeout();
  }

  auto msgId() const {
    return current_msg_id_;
  }

  bool is_client_timer_active{false};
  int send_count{0};
  int stats_collect_count{0};
  int destination{0};
  int retry_count{0};

 protected:
  int sendStats(std::vector<NodeStats>&& /*stats*/) override {
    ++send_count;
    return 0;
  }

  std::vector<NodeStats> collectStats() override {
    ++stats_collect_count;
    return {};
  }

  void activateAggregationTimer() override {
    // the timer will activate once aggregation is over, make sure not to loop
    bool should_aggregate = !did_aggregate;
    did_aggregate = true;

    if (should_aggregate) {
      onAggregation();
    }
  }

  void activateRetryTimer() override {
    ++retry_count;
    // fire right away
    onRetry();
  }

  void cancelRetryTimer() override {
    // nop
  }

  void resetRetryTimer() override {
    // nop
  }

  void activateClientTimeoutTimer() override {
    is_client_timer_active = true;
  }

  void cancelClientTimeoutTimer() override {
    is_client_timer_active = false;
  }

  void updateDestination() override {
    ++destination;
  }

  bool hasValidNodeSet() const override {
    return true;
  }

  bool did_aggregate{false};
};

TEST(NodeStatsHandlerTest, sendSimple) {
  MockNodeStatsHandler handler;

  EXPECT_EQ(0, handler.send_count);
  EXPECT_EQ(0, handler.stats_collect_count);

  handler.start();

  EXPECT_EQ(1, handler.send_count);
  EXPECT_EQ(1, handler.stats_collect_count);
  EXPECT_FALSE(handler.is_client_timer_active);

  handler.onMessageSent(Status::OK);

  EXPECT_TRUE(handler.is_client_timer_active);

  handler.onReplyFromNode(handler.msgId());

  EXPECT_EQ(1, handler.send_count);
  EXPECT_EQ(1, handler.stats_collect_count);
  EXPECT_EQ(0, handler.retry_count);
  EXPECT_FALSE(handler.is_client_timer_active);
}

TEST(NodeStatsHandlerTest, sendFail) {
  MockNodeStatsHandler handler;

  EXPECT_EQ(0, handler.send_count);
  EXPECT_EQ(0, handler.stats_collect_count);

  handler.start();

  EXPECT_EQ(1, handler.send_count);
  EXPECT_EQ(1, handler.stats_collect_count);

  auto destination_old = handler.destination;
  auto msg_id_old = handler.msgId();

  handler.onMessageSent(Status::FAILED);

  EXPECT_EQ(1, handler.retry_count);
  // should collect and send stats again
  EXPECT_EQ(2, handler.send_count);
  EXPECT_EQ(2, handler.stats_collect_count);

  // destination should be updated and a new request posted
  EXPECT_NE(destination_old, handler.destination);
  EXPECT_NE(msg_id_old, handler.msgId());

  handler.onMessageSent(Status::OK);

  handler.onReplyFromNode(handler.msgId());

  EXPECT_EQ(2, handler.send_count);
  EXPECT_EQ(2, handler.stats_collect_count);
  EXPECT_EQ(1, handler.retry_count);
}

TEST(NodeStatsHandlerTest, sendWithClientTimeout) {
  MockNodeStatsHandler handler;

  EXPECT_EQ(0, handler.send_count);
  EXPECT_EQ(0, handler.stats_collect_count);

  handler.start();

  EXPECT_EQ(1, handler.send_count);
  EXPECT_EQ(1, handler.stats_collect_count);

  auto destination_old = handler.destination;
  auto msg_id_old = handler.msgId();

  handler.onClientTimeout();

  EXPECT_EQ(0, handler.retry_count);
  // should collect and send stats again
  EXPECT_EQ(2, handler.send_count);
  EXPECT_EQ(2, handler.stats_collect_count);

  // destination should be updated and a new request posted
  EXPECT_NE(destination_old, handler.destination);
  EXPECT_NE(msg_id_old, handler.msgId());

  handler.onMessageSent(Status::OK);

  handler.onReplyFromNode(handler.msgId());

  EXPECT_EQ(2, handler.send_count);
  EXPECT_EQ(2, handler.stats_collect_count);
  EXPECT_EQ(0, handler.retry_count);
}
