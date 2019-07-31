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
#include "logdevice/common/test/NodesConfigurationTestUtil.h"
#include "logdevice/common/test/TestUtil.h"

using namespace facebook::logdevice;
using namespace facebook::logdevice::configuration;
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
      : stats_(std::move(stats)) {
    for (node_index_t n : {1, 2, 3, 4, 5, 6}) {
      nodes_.push_back(buildNode(n, "ash.2.08.k.z", n <= 3 ? true : false));
    }
    reComputeNodesConfiguraton();
  }

  WorkerTimeoutStats& getWorkerTimeoutStats() override {
    return stats_;
  }

  std::shared_ptr<const configuration::nodes::NodesConfiguration>
  getNodesConfiguration() const override {
    return nodes_configuration_;
  }

  void setStats(MockWorkerTimeoutStats stats) {
    stats_ = std::move(stats);
  }

  std::chrono::seconds getGracePeriod() const override {
    return 5s; // min ok duration
  }

  std::chrono::seconds getMonitoredPeriod() const override {
    return 3s;
  }

  std::chrono::seconds getGraylistingDuration() const override {
    return 60s;
  }

  double getGraylistNodeThreshold() const override {
    return 0.5;
  }

  NodesConfigurationTestUtil::NodeTemplate buildNode(node_index_t id,
                                                     std::string domain,
                                                     bool metadata = false) {
    return {
        id, NodesConfigurationTestUtil::both_role, domain, 1.0, 2, metadata};
  }

  void addNode(node_index_t id, std::string domain) {
    nodes_.push_back(buildNode(id, domain));
    reComputeNodesConfiguraton();
  }

  void reComputeNodesConfiguraton() {
    nodes_configuration_ = NodesConfigurationTestUtil::provisionNodes(nodes_);
  }

 private:
  MockWorkerTimeoutStats stats_;

  std::vector<NodesConfigurationTestUtil::NodeTemplate> nodes_;

  std::shared_ptr<const configuration::nodes::NodesConfiguration>
      nodes_configuration_;

  FRIEND_TEST(GraylistingTrackerTest, RoundRobinFlat);
  FRIEND_TEST(GraylistingTrackerTest, MaxGraylistedNodes);
  FRIEND_TEST(GraylistingTrackerTest, GroupLatencyPerRegion);
  FRIEND_TEST(GraylistingTrackerTest, PerRegionOutlier);
  FRIEND_TEST(GraylistingTrackerTest, MultiplicativeIncreaseAdditiveDecrease);
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

TEST(GraylistingTrackerTest, MonitoredGraylisted) {
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

  stats = buildWorkerStats({{1, 20ms},
                            {2, 20ms},
                            {3, 20ms}, // Behaves ok
                            {4, 20ms}},
                           10);
  tracker.setStats(std::move(stats));

  // node become ok and we observe that after graylist + grace period
  tracker.updateGraylist(now + 10s + 60s + 1s);
  EXPECT_EQ(0, tracker.getGraylistedNodes().size());

  // Node misbehaves during monitoring period and before grace period
  stats = buildWorkerStats({{1, 20ms},
                            {2, 20ms},
                            {3, 500ms}, // Outlier
                            {4, 20ms}},
                           10);
  tracker.setStats(std::move(stats));
  tracker.updateGraylist(now + 10s + 60s + 2s);
  EXPECT_THAT(tracker.getGraylistedNodes(), UnorderedElementsAre(3));
}

TEST(GraylistingTrackerTest, MonitoredGraylistedExpire) {
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

  stats = buildWorkerStats({{1, 20ms},
                            {2, 20ms},
                            {3, 20ms}, // Behaves ok
                            {4, 20ms}},
                           10);
  tracker.setStats(std::move(stats));

  // node become ok and we observe that after graylist + grace period
  tracker.updateGraylist(now + 10s + 60s + 1s);
  EXPECT_EQ(0, tracker.getGraylistedNodes().size());

  // Node misbehaves after monitoring period and before grace period
  stats = buildWorkerStats({{1, 20ms},
                            {2, 20ms},
                            {3, 500ms}, // Outlier
                            {4, 20ms}},
                           10);
  tracker.setStats(std::move(stats));
  tracker.updateGraylist(now + 10s + 60s + 5s);
  EXPECT_EQ(0, tracker.getGraylistedNodes().size());

  // Nodes keep misbehaving until after the grace period.
  tracker.updateGraylist(now + 10s + 60s + 11s);
  EXPECT_THAT(tracker.getGraylistedNodes(), UnorderedElementsAre(3));
}

TEST(GraylistingTrackerTest, MaxGraylistedNodes) {
  auto stats = buildWorkerStats({}, 0);

  MockGraylistingTracker tracker(std::move(stats));
  EXPECT_EQ(3, tracker.getMaxGraylistedNodes());
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

TEST(GraylistingTrackerTest, MultiplicativeIncreaseAdditiveDecrease) {
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

  // start graylist
  tracker.updateGraylist(now + 6s);
  EXPECT_THAT(tracker.getGraylistedNodes(), UnorderedElementsAre(3, 4, 5));
  auto deadlines = tracker.getGraylistDeadlines();
  for (int i = 3; i <= 5; ++i) {
    EXPECT_EQ(deadlines[i], now + 66s);
  }

  // end graylist
  tracker.updateGraylist(now + 65s);
  EXPECT_THAT(tracker.getGraylistedNodes(), UnorderedElementsAre(3, 4, 5));

  // new grace
  tracker.updateGraylist(now + 66s);
  EXPECT_EQ(0, tracker.getGraylistedNodes().size());

  // new grace end
  tracker.updateGraylist(now + 71s);
  deadlines = tracker.getGraylistDeadlines();
  EXPECT_THAT(tracker.getGraylistedNodes(), UnorderedElementsAre(3, 4, 5));
  for (int i = 3; i <= 5; ++i) {
    // 120s = 2*60s is exponential increase
    // 120s - 5s is  decrease for 5s grace
    EXPECT_EQ(deadlines[i], now + 71s + (120s - 5s)); // 186s
  }

  // set no outliers, wait for additive decrease up until min value
  stats = buildWorkerStats(
      {{1, 20ms}, {2, 20ms}, {3, 20ms}, {4, 20ms}, {5, 20ms}}, 10);
  tracker.setStats(stats);
  tracker.updateGraylist(now + 187s);
  EXPECT_EQ(0, tracker.getGraylistedNodes().size());
  // wait
  tracker.updateGraylist(now + 499s);
  EXPECT_EQ(0, tracker.getGraylistedNodes().size());

  // New set of outliers
  stats = buildWorkerStats({{1, 500ms}, // Outlier
                            {2, 500ms}, // Outlier
                            {3, 500ms}, // Outlier
                            {4, 20ms},
                            {5, 20ms}},
                           10);

  tracker.setStats(stats);
  tracker.updateGraylist(now + 500s);
  EXPECT_EQ(0, tracker.getGraylistedNodes().size());
  tracker.updateGraylist(now + 506s);
  EXPECT_THAT(tracker.getGraylistedNodes(), UnorderedElementsAre(1, 2, 3));
  // deadlines go back to min 60s
  deadlines = tracker.getGraylistDeadlines();
  for (int i = 1; i <= 3; ++i) {
    EXPECT_EQ(deadlines[i], now + 506s + 60s);
  }
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

  stats = buildWorkerStats(
      {{1, 20ms}, {2, 20ms}, {3, 20ms}, {4, 20ms}, {5, 20ms}}, 10);
  tracker.setStats(std::move(stats));

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

TEST(GraylistingTrackerTest, RoundRobinFlat) {
  EXPECT_EQ((std::vector<node_index_t>{0, 1, 2}),
            MockGraylistingTracker::roundRobinFlattenVector({{0, 1, 2}}, 3));
  EXPECT_EQ((std::vector<node_index_t>{0, 1}),
            MockGraylistingTracker::roundRobinFlattenVector({{0, 1, 2}}, 2));
  EXPECT_EQ((std::vector<node_index_t>{}),
            MockGraylistingTracker::roundRobinFlattenVector({{0, 1, 2}}, 0));
  EXPECT_EQ((std::vector<node_index_t>{}),
            MockGraylistingTracker::roundRobinFlattenVector({{}, {}, {}}, 4));
  EXPECT_EQ((std::vector<node_index_t>{}),
            MockGraylistingTracker::roundRobinFlattenVector({}, 4));
  EXPECT_EQ((std::vector<node_index_t>{0, 3, 6, 1, 4}),
            MockGraylistingTracker::roundRobinFlattenVector(
                {{0, 1, 2}, {3, 4, 5}, {6, 7, 8}}, 5));
  EXPECT_EQ((std::vector<node_index_t>{0, 6, 1, 7, 2}),
            MockGraylistingTracker::roundRobinFlattenVector(
                {{0, 1, 2}, {}, {6, 7, 8}}, 5));
  EXPECT_EQ((std::vector<node_index_t>{0, 3, 6, 1, 7}),
            MockGraylistingTracker::roundRobinFlattenVector(
                {{0, 1, 2}, {3}, {6, 7, 8}}, 5));
  EXPECT_EQ((std::vector<node_index_t>{0, 3, 6, 1, 4, 7, 2, 5, 8}),
            MockGraylistingTracker::roundRobinFlattenVector(
                {{0, 1, 2}, {3, 4, 5}, {6, 7, 8}}, 100));
}

TEST(GraylistingTrackerTest, GroupLatencyPerRegion) {
  MockGraylistingTracker tracker(buildWorkerStats({}, 0));

  tracker.addNode(101, "frc.2.08.k.z");
  tracker.addNode(102, "frc.3.08.k.z");
  tracker.addNode(103, "frc.3.09.k.z");
  tracker.addNode(104, "cln.5.09.k.z");
  tracker.addNode(105, "");

  auto got = tracker.groupLatenciesPerRegion({
      {1, 0},
      {2, 0},
      {3, 0},
      {101, 0},
      {102, 0},
      {103, 0},
      {104, 0},
      {105, 0},
  });

  MockGraylistingTracker::PerRegionLatencies expected{
      {"", {{105, 0}}},
      {"ash", {{1, 0}, {2, 0}, {3, 0}}},
      {"cln", {{104, 0}}},
      {"frc", {{101, 0}, {102, 0}, {103, 0}}},
  };
  EXPECT_EQ(expected, got);
}

TEST(GraylistingTrackerTest, PerRegionOutlier) {
  MockGraylistingTracker tracker(buildWorkerStats({}, 0));

  tracker.addNode(101, "frc.2.08.k.z");
  tracker.addNode(102, "frc.3.08.k.z");
  tracker.addNode(103, "frc.2.08.k.y");
  tracker.addNode(104, "cln.3.08.k.z");

  MockGraylistingTracker::PerRegionLatencies regional_outliers{
      {"ash", {{1, 100}, {2, 10}, {3, 12}, {4, 13}, {5, 200}, {6, 300}}},
      {"cln", {{104, 400}}},
      {"frc", {{101, 100}, {102, 300}, {103, 120}}},
  };

  auto got = tracker.findOutlierNodes(regional_outliers);

  EXPECT_EQ((std::vector<node_index_t>{6, 102, 5, 1}), got);
}
