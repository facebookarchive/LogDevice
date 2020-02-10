/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once
#include <queue>

#include <gtest/gtest.h>

#include "logdevice/common/FlowGroup.h"
#include "logdevice/common/FlowGroupDependencies.h"

////////////////////////////////////////////////////////////////////////////////
// Macros

/**
 * Check that the content of an Envelope queue on a Socket.
 */
#define CHECK_SENDQ(...)                                               \
  {                                                                    \
    std::vector<size_t> expected_q = {__VA_ARGS__};                    \
    std::vector<size_t> actual_q(sent.size());                         \
    std::transform(sent.begin(),                                       \
                   sent.end(),                                         \
                   actual_q.begin(),                                   \
                   [](const FlowOperation& op) { return op.cost(); }); \
    ASSERT_EQ(expected_q, actual_q);                                   \
  }

#define CHECK_SENT(type, size)                             \
  {                                                        \
    ASSERT_FALSE(sent.empty());                            \
    const auto expected = FlowOperation(sent, type, size); \
    EXPECT_EQ(expected, sent.front());                     \
    sent.pop_front();                                      \
  }

namespace facebook { namespace logdevice {

// FlowGroups can be applied to any metered operation. We use a mock
// operation that has an associated cost in order to avoid tying this
// test to any other subsystem (e.g. networking).
class FlowOperation : public BWAvailableCallback {
 public:
  using OpType = std::string;
  using QueueType = std::deque<FlowOperation>;
  FlowOperation(QueueType& sent_q, OpType type, size_t cost)
      : sent_q_(sent_q), type_(type) {
    cost_ = cost;
  }

  bool operator==(const FlowOperation& rhs) const {
    return &sent_q_ == &rhs.sent_q_ && type_ == rhs.type_ && cost_ == rhs.cost_;
  }

  const OpType& type() const {
    return type_;
  }

  size_t cost() const {
    return cost_;
  }

  void operator()(FlowGroup& flow_group, std::mutex&) override {
    ASSERT_TRUE(flow_group.drain(cost(), priority()));
    // Record that we've been released.
    sent_q_.push_back(*this);
    delete this;
  }

 private:
  QueueType& sent_q_;
  OpType type_;
  size_t cost_;
};

class FlowGroupTest : public ::testing::Test {
 public:
  FlowGroupTest() {
    flow_group = std::make_unique<FlowGroup>(
        std::make_unique<NwShapingFlowGroupDeps>(nullptr, nullptr));
    flow_group->setScope(NodeLocationScope::ROOT);

    // Setup a default update from the TrafficShaper
    FlowGroupPolicy policy;
    policy.setEnabled(true);
    // Set burst capacity small to increase the likelyhood of experiencing
    // a message deferral during a test run.
    policy.set(Priority::MAX, /*burst*/ 10000, /*Bps*/ 1000000);
    policy.set(Priority::CLIENT_HIGH, /*burst*/ 10000, /*Bps*/ 1000000);
    policy.set(Priority::CLIENT_NORMAL, /*burst*/ 10000, /*Bps*/ 1000000);
    policy.set(Priority::CLIENT_LOW, /*burst*/ 10000, /*Bps*/ 1000000);
    policy.set(Priority::BACKGROUND, /*burst*/ 10000, /*Bps*/ 1000000);
    policy.set(Priority::IDLE, /*burst*/ 10000, /*Bps*/ 1000000);
    policy.set(FlowGroup::PRIORITYQ_PRIORITY, /*burst*/ 10000, /*Bps*/ 1000000);
    // Tests in this suite only use a single FlowGroup, so we do not normalize
    // the policy for multiple Senders.
    update.policy =
        policy.normalize(/*Senders*/ 1, std::chrono::microseconds(1000));
  }

  void SetUp() override {}

  bool run() {
    SteadyTimestamp run_deadline(SteadyTimestamp::now() +
                                 flow_groups_run_yield_interval);
    return flow_group->run(flow_meter_mutex, run_deadline);
  }

  void resetMeter(int32_t level) {
    for (auto& e : flow_group->meter_.entries) {
      e.reset(level);
    }
  }

  static constexpr size_t NUM_WORKERS{16};

  std::mutex flow_meter_mutex;
  FlowGroupsUpdate::GroupEntry update;
  std::unique_ptr<FlowGroup> flow_group;
  std::deque<FlowOperation> sent;
  std::chrono::milliseconds flow_groups_run_yield_interval{10};
};

}} // namespace facebook::logdevice
