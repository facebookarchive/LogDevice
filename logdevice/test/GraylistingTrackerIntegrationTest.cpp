/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include <cstdlib>

#include "folly/dynamic.h"
#include "folly/json.h"
#include "logdevice/common/stats/Stats.h"
#include "logdevice/lib/ClientImpl.h"
#include "logdevice/test/utils/IntegrationTestBase.h"
#include "logdevice/test/utils/IntegrationTestUtils.h"

using namespace facebook::logdevice;
using namespace facebook::logdevice::IntegrationTestUtils;
using namespace std::literals::chrono_literals;

namespace {
struct Params {
  bool disable_outlier_based_graylisting{false};
  Params& set_disable_outlier_based_graylisting(bool val) {
    disable_outlier_based_graylisting = val;
    return *this;
  }
  std::chrono::seconds graylisting_duration{20s};
  Params& set_graylisting_duration(std::chrono::seconds val) {
    graylisting_duration = val;
    return *this;
  }
  double graylisting_threshold{0.5};
  Params& set_graylisting_threshold(double val) {
    graylisting_threshold = val;
    return *this;
  }
  std::chrono::seconds graylisting_grace_period{10s};
  Params& set_graylisting_grace_period(std::chrono::seconds val) {
    graylisting_grace_period = val;
    return *this;
  }
};

class AppendThread {
 public:
  AppendThread() = default;
  AppendThread(const AppendThread& other) = delete;

  ~AppendThread() {
    stop();
  }

  void start(Client* client, uint64_t from_logid, uint64_t to_logid) {
    ld_check(from_logid <= to_logid);
    stop();

    stop_ = false;
    for (int i = 0; i < 10; i++) {
      append_threads_.emplace_back([=] {
        while (!stop_) {
          for (auto i = from_logid; i <= to_logid; i++) {
            client->appendSync(logid_t{i}, ".");
          }
        }
      });
    }
  }

  void stop() {
    stop_ = true;
    for (auto& t : append_threads_) {
      t.join();
    }
  }

 private:
  std::atomic<bool> stop_{false};
  std::vector<std::thread> append_threads_;
};

class GraylistingTrackerIntegrationTest : public IntegrationTestBase {
 public:
  void initializeCluster(Params params) {
    auto msString = [](std::chrono::milliseconds duration) {
      return std::to_string(duration.count()) + "ms";
    };

    cluster =
        IntegrationTestUtils::ClusterFactory{}
            .useHashBasedSequencerAssignment()
            .setParam("--enable-store-histograms-calculations", "true")
            // Disable old graylisting
            .setParam("--disable-graylisting", "true")
            // Force refresh the graylist faster
            .setParam("--graylisting-refresh-interval", "2s")
            .setParam("--store-histogram-min-samples-per-bucket", "10")
            .setParam("--disable-outlier-based-graylisting",
                      std::to_string(params.disable_outlier_based_graylisting))
            .setParam("--graylisting-grace-period",
                      msString(params.graylisting_grace_period))
            .setParam("--slow-node-retry-interval",
                      msString(params.graylisting_duration))
            .setParam("--gray-list-threshold",
                      std::to_string(params.graylisting_threshold))
            .setParam("--enable-sticky-copysets", "false")
            /* put each node in a separate rack */
            .setNumRacks(5)
            .setNumLogs(NUM_LOGS)
            .create(5);
  }

  std::unordered_map<int, std::unordered_set<node_index_t>>
  getGraylistedNodesPerWorker(node_index_t from) {
    auto resp = cluster->getNode(from).sendCommand("info graylist --json");

    // Remove the trailing \r\nEND\r\n
    resp.erase(resp.end() - 7, resp.end());

    ld_info("%s", resp.c_str());

    auto rows = folly::parseJson(resp)["rows"];

    std::unordered_map<int, std::unordered_set<node_index_t>> ret;
    for (auto row : rows) {
      auto worker = row[0].asInt();
      auto graylisted = row[1].asInt();

      auto it = ret.find(worker);
      if (it == ret.end()) {
        it = ret.emplace(worker, std::unordered_set<node_index_t>{}).first;
      }
      it->second.insert(graylisted);
    }

    return ret;
  }

  std::unordered_set<node_index_t> getGraylistedNodes(node_index_t from) {
    auto graylist = getGraylistedNodesPerWorker(from);
    std::unordered_set<node_index_t> ret;
    for (const auto& g : graylist) {
      ret.insert(g.second.begin(), g.second.end());
    }
    return ret;
  }

  bool
  waitUntillGraylistedNodesEquals(node_index_t on_node,
                                  std::unordered_set<node_index_t> graylist) {
    auto set_to_string = [](std::unordered_set<node_index_t> set) {
      std::string out = "{ ";
      for (const auto& elem : set) {
        out += std::to_string(elem) + " ";
      }
      out += "}";
      return out;
    };
    auto reason = folly::sformat(
        "Graylisted nodes on N{} are {}", on_node, set_to_string(graylist));
    int status =
        wait_until(reason.c_str(),
                   [&]() { return getGraylistedNodes(on_node) == graylist; },
                   std::chrono::steady_clock::now() + 30s);
    return status == 0;
  }

  std::unique_ptr<Cluster> cluster;
  constexpr static int NUM_LOGS = 10;
};
} // namespace

TEST_F(GraylistingTrackerIntegrationTest, HighLatencyGraylisted) {
  initializeCluster(Params{}.set_graylisting_duration(1h));
  auto client = cluster->createClient();

  AppendThread appender;
  // Inject failure in some random node
  cluster->getNode(3).injectShardFault(
      "all", "all", "all", "latency", false, folly::none, (500ms).count());
  appender.start(client.get(), 1, 10);

  EXPECT_TRUE(waitUntillGraylistedNodesEquals(0, {3}));
}

TEST_F(GraylistingTrackerIntegrationTest, GraylistingExpires) {
  initializeCluster(Params{}.set_graylisting_duration(20s));
  auto client = cluster->createClient();

  AppendThread appender;
  // Inject failure in some random node
  cluster->getNode(3).injectShardFault(
      "all", "all", "all", "latency", false, folly::none, (500ms).count());
  appender.start(client.get(), 1, 10);

  EXPECT_TRUE(waitUntillGraylistedNodesEquals(0, {3}));

  // Clear the shard failure
  cluster->getNode(3).injectShardFault(
      "all", "none", "none", "none", false, folly::none, folly::none);

  EXPECT_TRUE(waitUntillGraylistedNodesEquals(0, {}));
}

TEST_F(GraylistingTrackerIntegrationTest, DisablingGraylistWorks) {
  initializeCluster(Params{}.set_graylisting_duration(1h));
  auto client = cluster->createClient();

  AppendThread appender;
  // Inject failure in some random node
  cluster->getNode(3).injectShardFault(
      "all", "all", "all", "latency", false, folly::none, (500ms).count());
  appender.start(client.get(), 1, 10);

  EXPECT_TRUE(waitUntillGraylistedNodesEquals(0, {3}));

  cluster->getNode(0).updateSetting(
      "disable-outlier-based-graylisting", "true");

  EXPECT_TRUE(waitUntillGraylistedNodesEquals(0, {}));
}

TEST_F(GraylistingTrackerIntegrationTest, EnablingGraylistWorks) {
  initializeCluster(Params{}
                        .set_graylisting_duration(1h)
                        .set_disable_outlier_based_graylisting(true));
  auto client = cluster->createClient();

  AppendThread appender;
  // Inject failure in some random node
  cluster->getNode(3).injectShardFault(
      "all", "all", "all", "latency", false, folly::none, (500ms).count());
  appender.start(client.get(), 1, 10);

  std::this_thread::sleep_for(30s);

  EXPECT_EQ(0, getGraylistedNodes(0).size());

  cluster->getNode(0).updateSetting(
      "disable-outlier-based-graylisting", "false");

  EXPECT_TRUE(waitUntillGraylistedNodesEquals(0, {3}));
}

TEST_F(GraylistingTrackerIntegrationTest, NumGraylisted) {
  initializeCluster(Params{}.set_graylisting_duration(1h));
  auto client = cluster->createClient();

  AppendThread appender;
  // Inject failure in some random node
  cluster->getNode(3).injectShardFault(
      "all", "all", "all", "latency", false, folly::none, (500ms).count());
  appender.start(client.get(), 1, 10);

  // After 5 second, inject a failure in another node
  std::this_thread::sleep_for(5s);
  cluster->getNode(2).injectShardFault(
      "all", "all", "all", "latency", false, folly::none, (500ms).count());

  EXPECT_TRUE(waitUntillGraylistedNodesEquals(0, {2, 3}));

  // Only 1 node should get graylisted
  cluster->getNode(0).updateSetting("gray-list-threshold", "0.2");

  std::this_thread::sleep_for(10s);

  auto graylist = getGraylistedNodesPerWorker(0);
  for (const auto& worker : graylist) {
    EXPECT_LE(worker.second.size(), 1);
    if (worker.second.size() == 1) {
      auto graylisted = *worker.second.begin();
      EXPECT_TRUE(graylisted == 2 || graylisted == 3);
    }
  }
}

TEST_F(GraylistingTrackerIntegrationTest, DisablingGraylistByNumNodes) {
  initializeCluster(Params{}.set_graylisting_duration(1h));
  auto client = cluster->createClient();

  AppendThread appender;
  // Inject failure in some random node
  cluster->getNode(3).injectShardFault(
      "all", "all", "all", "latency", false, folly::none, (500ms).count());
  appender.start(client.get(), 1, 10);

  EXPECT_TRUE(waitUntillGraylistedNodesEquals(0, {3}));

  cluster->getNode(0).updateSetting("gray-list-threshold", "0");

  EXPECT_TRUE(waitUntillGraylistedNodesEquals(0, {}));
}

TEST_F(GraylistingTrackerIntegrationTest, NodeRecoversInGracePeriod) {
  initializeCluster(
      Params{}.set_graylisting_duration(1h).set_graylisting_grace_period(30s));
  auto client = cluster->createClient();

  AppendThread appender;
  // Inject failure in some random node
  cluster->getNode(3).injectShardFault(
      "all", "all", "all", "latency", false, folly::none, (500ms).count());
  appender.start(client.get(), 1, 10);

  std::this_thread::sleep_for(10s);

  cluster->getNode(3).injectShardFault(
      "all", "none", "none", "none", false, folly::none, folly::none);

  std::this_thread::sleep_for(30s);

  EXPECT_TRUE(waitUntillGraylistedNodesEquals(0, {}));
}
