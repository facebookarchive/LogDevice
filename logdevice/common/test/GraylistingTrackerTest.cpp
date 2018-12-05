/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "logdevice/common/GraylistingTracker.h"

#include <chrono>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "logdevice/common/WorkerTimeoutStats.h"

using namespace facebook::logdevice;
using namespace std::literals::chrono_literals;
using namespace ::testing;

namespace {

uint32_t esn{0};

STORE_Header buildStore() {
  STORE_Header header;
  RecordID rid(esn_t{esn++}, epoch_t{1}, logid_t{1});
  header.rid = rid;
  header.wave = 1;
  return header;
}

struct MockWorkerTimeoutStats : public WorkerTimeoutStats {
  uint64_t getMinSamplesPerBucket() const override {
    return 1;
  }
};

MockWorkerTimeoutStats buildWorkerStats(
    std::vector<std::pair<node_index_t, std::chrono::milliseconds>> requests,
    int num_waves) {
  MockWorkerTimeoutStats stats;
  auto time = std::chrono::steady_clock::now();
  while (num_waves--) {
    for (const auto& req : requests) {
      auto now = time;
      time += 10s;
      auto store = buildStore();
      stats.onCopySent(E::OK, ShardID{req.first, 0}, store, now);
      now += req.second;
      stats.onReply(ShardID{req.first, 0}, store, now);
    }
  }
  return stats;
}
} // namespace

class MockGraylistingTracker : public GraylistingTracker {
 public:
  explicit MockGraylistingTracker(MockWorkerTimeoutStats stats)
      : stats_(std::move(stats)) {}

  virtual WorkerTimeoutStats& getWorkerTimeoutStats() override {
    return stats_;
  }

  virtual const configuration::Nodes& getNodes() const override {
    return nodes_;
  }

  void setStats(MockWorkerTimeoutStats stats) {
    stats_ = std::move(stats);
  }

  std::chrono::seconds getGracePeriod() const override {
    return 5s;
  }

  std::chrono::seconds getGraylistingDuration() const override {
    return 60s;
  }

  double getGraylistNodeThreshold() const override {
    return 0.5;
  }

  size_t getCalculatedMaxGraylistedNodes() const {
    return getMaxGraylistedNodes();
  }

  configuration::Node buildNode() {
    configuration::Node n;
    n.setRole(configuration::NodeRole::STORAGE);
    auto attr = std::make_unique<configuration::StorageNodeAttributes>();
    attr->state = configuration::StorageState::READ_WRITE;
    n.storage_attributes = std::move(attr);
    return n;
  }

 private:
  MockWorkerTimeoutStats stats_;
  configuration::Nodes nodes_{{1, buildNode()},
                              {2, buildNode()},
                              {3, buildNode()},
                              {4, buildNode()},
                              {5, buildNode()},
                              {6, buildNode()}};
};

TEST(GraylistingTrackerTest, OutlierGraylisted) {
  auto stats = buildWorkerStats({{1, 20ms},
                                 {2, 20ms},
                                 {3, 500ms}, // Outlier
                                 {4, 20ms}},
                                10);
  auto now = std::chrono::steady_clock::now();

  MockGraylistingTracker tracker(std::move(stats));
  tracker.updateGraylist(now);
  EXPECT_EQ(0, tracker.getGraylistedNodes().size());

  tracker.updateGraylist(now + 10s);
  EXPECT_THAT(tracker.getGraylistedNodes(), UnorderedElementsAre(3));
}

TEST(GraylistingTrackerTest, MaxGraylistedNodes) {
  auto stats = buildWorkerStats({}, 0);

  MockGraylistingTracker tracker(std::move(stats));
  EXPECT_EQ(3, tracker.getCalculatedMaxGraylistedNodes());
}

TEST(GraylistingTrackerTest, EjectOlderGraylists) {
  auto stats = buildWorkerStats({{1, 20ms},
                                 {2, 20ms},
                                 {3, 500ms},  // Outlier
                                 {4, 500ms},  // Outlier
                                 {5, 500ms}}, // Outlier
                                10);
  auto now = std::chrono::steady_clock::now();

  MockGraylistingTracker tracker(std::move(stats));

  tracker.updateGraylist(now);
  EXPECT_EQ(0, tracker.getGraylistedNodes().size());

  tracker.updateGraylist(now + 10s);
  EXPECT_THAT(tracker.getGraylistedNodes(), UnorderedElementsAre(3, 4, 5));

  // Now detect another outlier
  stats = buildWorkerStats({{1, 20ms},
                            {2, 500ms}, // Outlier
                            {3, 20ms},
                            {4, 20ms},
                            {5, 20ms}},
                           10);
  tracker.setStats(std::move(stats));

  tracker.updateGraylist(now + 20s);
  // N2 is still in the grace period
  EXPECT_THAT(tracker.getGraylistedNodes(), UnorderedElementsAre(3, 4, 5));

  // N3 will get ejected because it's the oldest
  tracker.updateGraylist(now + 30s);
  EXPECT_THAT(tracker.getGraylistedNodes(), UnorderedElementsAre(2, 4, 5));
}

TEST(GraylistingTrackerTest, FixedPotentialOutlier) {
  auto stats = buildWorkerStats({{1, 20ms},
                                 {2, 20ms},
                                 {3, 500ms}, // Outlier
                                 {4, 20ms},
                                 {5, 20ms}},
                                10);
  auto now = std::chrono::steady_clock::now();

  MockGraylistingTracker tracker(std::move(stats));

  tracker.updateGraylist(now);
  EXPECT_EQ(0, tracker.getGraylistedNodes().size());

  // Before the grace period ended, N3 got better
  stats = buildWorkerStats(
      {{1, 20ms}, {2, 20ms}, {3, 20ms}, {4, 20ms}, {5, 20ms}}, 10);
  tracker.setStats(stats);

  tracker.updateGraylist(now + 4s);
  EXPECT_EQ(0, tracker.getGraylistedNodes().size());

  tracker.updateGraylist(now + 11s);
  EXPECT_EQ(0, tracker.getGraylistedNodes().size());
}

TEST(GraylistingTrackerTest, ExpiredGraylists) {
  auto stats = buildWorkerStats({{1, 20ms},
                                 {2, 20ms},
                                 {3, 500ms}, // Outlier
                                 {4, 20ms},
                                 {5, 20ms}},
                                10);
  auto now = std::chrono::steady_clock::now();

  MockGraylistingTracker tracker(std::move(stats));

  tracker.updateGraylist(now);
  tracker.updateGraylist(now + 10s);
  EXPECT_EQ(1, tracker.getGraylistedNodes().size());

  tracker.updateGraylist(now + 20s);
  EXPECT_EQ(1, tracker.getGraylistedNodes().size());

  tracker.updateGraylist(now + 71s);
  EXPECT_EQ(0, tracker.getGraylistedNodes().size());
}

TEST(GraylistingTrackerTest, AllOutlierExceptOne) {
  auto stats = buildWorkerStats({{1, 20ms}, {2, 500ms}, {3, 500ms}}, 10);
  auto now = std::chrono::steady_clock::now();

  MockGraylistingTracker tracker(std::move(stats));

  tracker.updateGraylist(now);
  tracker.updateGraylist(now + 10s);
  EXPECT_THAT(tracker.getGraylistedNodes(), UnorderedElementsAre(2, 3));
}

TEST(GraylistingTrackerTest, AllNotOutlier) {
  auto stats = buildWorkerStats({{1, 20ms}, {2, 20ms}, {3, 20ms}}, 10);
  auto now = std::chrono::steady_clock::now();

  MockGraylistingTracker tracker(std::move(stats));

  tracker.updateGraylist(now);
  tracker.updateGraylist(now + 10s);
  EXPECT_EQ(0, tracker.getGraylistedNodes().size());
}

TEST(GraylistingTrackerTest, NoStoresSent) {
  auto stats = buildWorkerStats({}, 0);
  auto now = std::chrono::steady_clock::now();

  MockGraylistingTracker tracker(std::move(stats));

  tracker.updateGraylist(now);
  EXPECT_EQ(0, tracker.getGraylistedNodes().size());

  tracker.updateGraylist(now + 10s);
  EXPECT_EQ(0, tracker.getGraylistedNodes().size());
}

TEST(GraylistingTrackerTest, ResetGraylist) {
  auto stats = buildWorkerStats({{1, 20ms},
                                 {2, 20ms},
                                 {3, 500ms}, // Outlier
                                 {4, 20ms}},
                                10);
  auto now = std::chrono::steady_clock::now();

  MockGraylistingTracker tracker(std::move(stats));
  tracker.updateGraylist(now);

  tracker.updateGraylist(now + 10s);
  EXPECT_THAT(tracker.getGraylistedNodes(), UnorderedElementsAre(3));

  tracker.resetGraylist();
  EXPECT_EQ(0, tracker.getGraylistedNodes().size());
}
