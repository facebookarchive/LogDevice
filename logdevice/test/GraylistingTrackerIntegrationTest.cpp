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

using GraylistedNodes = std::unordered_set<node_index_t>;

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
            client->append(logid_t{i}, ".", [](auto, const auto&) {});
            std::this_thread::sleep_for(100ms);
          }
        }
      });
    }
  }

  void stop() {
    if (stop_.exchange(true)) {
      // It's already stopped.
      return;
    }
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
            .setParam("--num-workers", "2")
            /* put each node in a separate rack */
            .setNumRacks(5)
            .setNumLogs(10)
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

  GraylistedNodes getGraylistedNodes(node_index_t from) {
    auto graylist = getGraylistedNodesPerWorker(from);
    GraylistedNodes ret;
    for (const auto& g : graylist) {
      ret.insert(g.second.begin(), g.second.end());
    }
    return ret;
  }

  bool
  waitUntillGraylistOnNode(node_index_t on_node,
                           std::string message,
                           folly::Function<bool(GraylistedNodes)> predicate,
                           std::chrono::seconds duration = 60s) {
    return wait_until(message.c_str(),
                      [&]() { return predicate(getGraylistedNodes(on_node)); },
                      std::chrono::steady_clock::now() + duration) == 0;
  }

  std::unique_ptr<Cluster> cluster;
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

  EXPECT_TRUE(waitUntillGraylistOnNode(
      0, "N3 is graylisted on N0", [&](GraylistedNodes nodes) {
        return nodes.count(3) > 0;
      }));
}

TEST_F(GraylistingTrackerIntegrationTest, GraylistingExpires) {
  initializeCluster(Params{}.set_graylisting_duration(20s));
  auto client = cluster->createClient();

  AppendThread appender;
  // Inject failure in some random node
  cluster->getNode(3).injectShardFault(
      "all", "all", "all", "latency", false, 100, (500ms).count());
  appender.start(client.get(), 1, 10);

  EXPECT_TRUE(waitUntillGraylistOnNode(
      0, "N3 is graylisted on N0", [&](GraylistedNodes nodes) {
        return nodes.count(3) > 0;
      }));

  // Clear the shard failure
  cluster->getNode(3).injectShardFault(
      "all", "none", "none", "none", false, folly::none, folly::none);

  EXPECT_TRUE(waitUntillGraylistOnNode(
      0, "N3 is not graylisted anymore on N0", [&](GraylistedNodes nodes) {
        return nodes.count(3) == 0;
      }));
}

TEST_F(GraylistingTrackerIntegrationTest, DisablingGraylistWorks) {
  initializeCluster(Params{}.set_graylisting_duration(1h));
  auto client = cluster->createClient();

  AppendThread appender;
  // Inject failure in some random node
  cluster->getNode(3).injectShardFault(
      "all", "all", "all", "latency", false, 100, (500ms).count());
  appender.start(client.get(), 1, 10);

  EXPECT_TRUE(waitUntillGraylistOnNode(
      0, "N3 is graylisted on N0", [&](GraylistedNodes nodes) {
        return nodes.count(3) > 0;
      }));

  cluster->getNode(0).updateSetting(
      "disable-outlier-based-graylisting", "true");

  EXPECT_TRUE(waitUntillGraylistOnNode(
      0, "N0 has nothing in the graylist", [&](GraylistedNodes nodes) {
        return nodes.size() == 0;
      }));
}

TEST_F(GraylistingTrackerIntegrationTest, EnablingGraylistWorks) {
  initializeCluster(Params{}
                        .set_graylisting_duration(1h)
                        .set_disable_outlier_based_graylisting(true));
  auto client = cluster->createClient();

  AppendThread appender;
  // Inject failure in some random node
  cluster->getNode(3).injectShardFault(
      "all", "all", "all", "latency", false, 100, (500ms).count());
  appender.start(client.get(), 1, 10);

  EXPECT_FALSE(waitUntillGraylistOnNode(
      0, "Nothing will get graylisted on N0", [&](GraylistedNodes nodes) {
        return nodes.size() > 0;
      }));

  EXPECT_EQ(0, getGraylistedNodes(0).size());

  cluster->getNode(0).updateSetting(
      "disable-outlier-based-graylisting", "false");

  EXPECT_TRUE(waitUntillGraylistOnNode(
      0, "N3 is graylisted on N0", [&](GraylistedNodes nodes) {
        return nodes.count(3) > 0;
      }));
}

TEST_F(GraylistingTrackerIntegrationTest, NumGraylisted) {
  initializeCluster(Params{}.set_graylisting_duration(1h));
  auto client = cluster->createClient();

  // Inject failure in some random node
  cluster->getNode(3).injectShardFault(
      "all", "all", "all", "latency", false, 100, (100ms).count());
  cluster->getNode(2).injectShardFault(
      "all", "all", "all", "latency", false, 100, (100ms).count());

  AppendThread appender;
  appender.start(client.get(), 1, 10);

  EXPECT_TRUE(waitUntillGraylistOnNode(
      0, "N3 & N2 are graylisted on N0", [&](GraylistedNodes nodes) {
        return nodes.count(3) > 0 && nodes.count(2) > 0;
      }));

  // Only 1 node should get graylisted
  cluster->getNode(0).updateSetting("gray-list-threshold", "0.2");

  std::this_thread::sleep_for(10s);

  auto graylist = getGraylistedNodesPerWorker(0);
  bool atleat_one_node_graylisted{false};
  for (const auto& worker : graylist) {
    EXPECT_LE(worker.second.size(), 1);
    if (worker.second.size() == 1) {
      atleat_one_node_graylisted = true;
      break;
    }
  }
  EXPECT_TRUE(atleat_one_node_graylisted);
}

TEST_F(GraylistingTrackerIntegrationTest, DisablingGraylistByNumNodes) {
  initializeCluster(Params{}.set_graylisting_duration(1h));
  auto client = cluster->createClient();

  AppendThread appender;
  // Inject failure in some random node
  cluster->getNode(3).injectShardFault(
      "all", "all", "all", "latency", false, 100, (500ms).count());
  appender.start(client.get(), 1, 10);

  EXPECT_TRUE(waitUntillGraylistOnNode(
      0, "N3 is graylisted on N0", [&](GraylistedNodes nodes) {
        return nodes.count(3) > 0;
      }));

  cluster->getNode(0).updateSetting("gray-list-threshold", "0");

  EXPECT_TRUE(waitUntillGraylistOnNode(
      0, "N0 has nothing in the graylist", [&](GraylistedNodes nodes) {
        return nodes.size() == 0;
      }));
}

TEST_F(GraylistingTrackerIntegrationTest, NodeRecoversInGracePeriod) {
  initializeCluster(
      Params{}.set_graylisting_duration(1h).set_graylisting_grace_period(60s));
  auto client = cluster->createClient();

  AppendThread appender;
  // Inject failure in some random node
  cluster->getNode(3).injectShardFault(
      "all", "all", "all", "latency", false, folly::none, (500ms).count());
  appender.start(client.get(), 1, 10);

  std::this_thread::sleep_for(10s);
  appender.stop();

  EXPECT_FALSE(waitUntillGraylistOnNode(
      0,
      "N0 has nothing in the graylist",
      [&](GraylistedNodes nodes) { return nodes.size() > 0; },
      60s));
}
