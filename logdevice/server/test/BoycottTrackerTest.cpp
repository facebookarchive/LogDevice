/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "logdevice/server/sequencer_boycotting/BoycottTracker.h"

#include <gmock/gmock.h>
#include <gtest/gtest.h>

using namespace facebook::logdevice;
using namespace std::literals::chrono_literals;
using namespace ::testing;

namespace {
auto time_point_to_ns(std::chrono::system_clock::time_point tp) {
  return tp.time_since_epoch();
}

// returns the values in the map
template <typename Key, typename Value>
std::vector<Value> values(std::unordered_map<Key, Value> map) {
  std::vector<Value> vec;
  vec.reserve(map.size());

  std::transform(
      map.cbegin(), map.cend(), std::back_inserter(vec), [](auto& entry) {
        return entry.second;
      });

  return vec;
}

class MockBoycottTracker : public BoycottTracker {
 public:
  MOCK_CONST_METHOD0(getMaxBoycottCount, unsigned int());
  MOCK_CONST_METHOD0(getBoycottDuration, std::chrono::milliseconds());

  // don't care about spread time in tests
  std::chrono::milliseconds getBoycottSpreadTime() const override {
    return 0ms;
  }

  // not using the tracer in BoycottTracker tests
  int postRequest(std::unique_ptr<Request>&) override {
    return 0;
  }
};
} // namespace

TEST(BoycottTrackerTest, NoNodes) {
  MockBoycottTracker tracker;
  EXPECT_CALL(tracker, getMaxBoycottCount()).WillRepeatedly(Return(1));

  // make sure not to crash when no boycotts have been set
  tracker.calculateBoycotts(std::chrono::system_clock::now());

  SUCCEED();
}

TEST(BoycottTrackerTest, Future) {
  const auto now = std::chrono::system_clock::now();
  const auto now_ns = time_point_to_ns(now);

  MockBoycottTracker tracker;
  EXPECT_CALL(tracker, getMaxBoycottCount()).WillRepeatedly(Return(1));
  tracker.updateReportedBoycotts(
      {{1, now_ns - 10s, 15s}, {2, now_ns + 10s, 15s}});

  tracker.calculateBoycotts(now);
  EXPECT_TRUE(tracker.isBoycotted(1));
  EXPECT_FALSE(tracker.isBoycotted(2));
}

TEST(BoycottTrackerTest, UseOldest) {
  const auto now = std::chrono::system_clock::now();
  const auto now_ns = time_point_to_ns(now);

  MockBoycottTracker tracker;
  EXPECT_CALL(tracker, getMaxBoycottCount()).WillRepeatedly(Return(1));
  tracker.updateReportedBoycotts(
      {{1, now_ns - 15s, 20s}, {2, now_ns - 5s, 20s}});

  tracker.calculateBoycotts(now);
  EXPECT_TRUE(tracker.isBoycotted(1));
  EXPECT_FALSE(tracker.isBoycotted(2));
}

TEST(BoycottTrackerTest, UpdateReportedBoycotts) {
  const auto now = std::chrono::system_clock::now();
  const auto now_ns = time_point_to_ns(now);

  MockBoycottTracker tracker;
  EXPECT_CALL(tracker, getMaxBoycottCount()).WillRepeatedly(Return(2));
  tracker.updateReportedBoycotts(
      {{1, now_ns - 10s, 20s}, {2, now_ns - 10s, 20s}});

  tracker.calculateBoycotts(now);
  EXPECT_TRUE(tracker.isBoycotted(1));
  EXPECT_TRUE(tracker.isBoycotted(2));

  // don't update with older values
  tracker.updateReportedBoycotts(
      {{1, now_ns - 20s, 15s}, {2, now_ns - 20s, 15s}});

  tracker.calculateBoycotts(now);
  // if the values were updated, then these would be false because the duration
  // is 15s
  EXPECT_TRUE(tracker.isBoycotted(1));
  EXPECT_TRUE(tracker.isBoycotted(2));

  // if new nodes are given, use those as well, even if they're older
  tracker.updateReportedBoycotts(
      {{3, now_ns - 10s, 15s}, {4, now_ns - 10s, 15s}});

  tracker.calculateBoycotts(now);
  EXPECT_FALSE(tracker.isBoycotted(1));
  EXPECT_FALSE(tracker.isBoycotted(2));
  EXPECT_TRUE(tracker.isBoycotted(3));
  EXPECT_TRUE(tracker.isBoycotted(4));

  // N3 & N4 boycotts expire at now_ns + 5s. While N1 & N2 expire
  // at now_ns + 10s. Let's expire N3 & N4 and update N1 & N2.
  tracker.updateReportedBoycotts(
      {{1, now_ns - 5s, 15s}, {2, now_ns - 5s, 15s}});

  tracker.calculateBoycotts(now + 6s);
  EXPECT_TRUE(tracker.isBoycotted(1));
  EXPECT_TRUE(tracker.isBoycotted(2));
}

TEST(BoycottTrackerTest, Reset) {
  const auto now = std::chrono::system_clock::now();
  const auto now_ns = time_point_to_ns(now);

  MockBoycottTracker tracker;
  EXPECT_CALL(tracker, getMaxBoycottCount()).WillRepeatedly(Return(1));

  tracker.updateReportedBoycotts({{1, now_ns - 20s, 30s, false}});

  tracker.calculateBoycotts(now);
  EXPECT_TRUE(tracker.isBoycotted(1));

  tracker.updateReportedBoycotts({{1, now_ns, 30s, true}});
  tracker.calculateBoycotts(now);
  EXPECT_FALSE(tracker.isBoycotted(1));
}

TEST(BoycottTrackerTest, BoycottsByThisNode) {
  const auto now = std::chrono::system_clock::now();
  const auto now_ns = time_point_to_ns(now);

  MockBoycottTracker tracker;
  EXPECT_CALL(tracker, getMaxBoycottCount()).WillRepeatedly(Return(1));
  EXPECT_CALL(tracker, getBoycottDuration()).WillRepeatedly(Return(30s));

  tracker.setLocalOutliers({NodeID{1}, NodeID{2}});

  tracker.calculateBoycotts(now - 20s);
  // only the worst one should now be boycotted, because only one node may be
  // boycotted
  EXPECT_TRUE(tracker.isBoycotted(1));
  EXPECT_FALSE(tracker.isBoycotted(2));
  // only forward as many nodes as this node may boycott
  EXPECT_THAT(values(tracker.getBoycottsForGossip()),
              ElementsAre(Boycott{1, now_ns - 20s, 30s}));
  EXPECT_EQ(1, tracker.getBoycottsForGossip().size());

  tracker.updateReportedBoycotts({{3, now_ns - 5s, 60s}});
  // forward the newly given value in upcoming gossips
  EXPECT_THAT(values(tracker.getBoycottsForGossip()),
              UnorderedElementsAre(
                  Boycott{1, now_ns - 20s, 30s}, Boycott{3, now_ns - 5s, 60s}));

  tracker.calculateBoycotts(now);
  // still use oldest as the boycotted value
  EXPECT_TRUE(tracker.isBoycotted(1));
  EXPECT_FALSE(tracker.isBoycotted(3));

  // After having re-calculated, any non-active boycotts will be removed
  EXPECT_THAT(values(tracker.getBoycottsForGossip()),
              UnorderedElementsAre(Boycott{1, now_ns - 20s, 30s}));
}
