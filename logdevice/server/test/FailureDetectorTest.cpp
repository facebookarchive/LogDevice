/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/server/FailureDetector.h"

#include <vector>

#include <folly/Memory.h>
#include <folly/Random.h>
#include <gtest/gtest.h>

#include "logdevice/common/ClusterState.h"
#include "logdevice/common/NoopTraceLogger.h"
#include "logdevice/common/Processor.h"
#include "logdevice/common/Timestamp.h"
#include "logdevice/common/configuration/Configuration.h"
#include "logdevice/common/configuration/LocalLogsConfig.h"
#include "logdevice/common/debug.h"
#include "logdevice/common/protocol/GOSSIP_Message.h"
#include "logdevice/common/settings/GossipSettings.h"
#include "logdevice/common/test/TestUtil.h"
#include "logdevice/server/ServerProcessor.h"
#include "logdevice/server/ServerSettings.h"
#include "logdevice/server/shutdown.h"
#include "logdevice/server/test/TestUtil.h"

using namespace facebook::logdevice;

void shutdown_processor(Processor* processor);

namespace facebook { namespace logdevice {
class MockBoycottTracker : public BoycottTracker {
 public:
  unsigned int getMaxBoycottCount() const override {
    return 0;
  }
  std::chrono::milliseconds getBoycottDuration() const override {
    return std::chrono::milliseconds::zero();
  }

  std::chrono::milliseconds getBoycottSpreadTime() const override {
    return std::chrono::milliseconds::zero();
  }
};

class MockFailureDetector : public FailureDetector {
 public:
  explicit MockFailureDetector(UpdateableSettings<GossipSettings> settings,
                               ServerProcessor* p)
      : FailureDetector(std::move(settings), p),
        my_node_id_(p->getMyNodeID()),
        config_(p->config_->get()->serverConfig()),
        cluster_state_(new ClusterState(
            1000,
            nullptr,
            *config_->getNodesConfigurationFromServerConfigSource()
                 ->getServiceDiscovery())) {}

  // simulate passing of time
  void advanceTime() {
    gossip();
  }

  int sendGossipMessage(NodeID node,
                        std::unique_ptr<GOSSIP_Message> msg) override {
    messages_.emplace_back(node, std::move(msg));
    return 0;
  }
  std::shared_ptr<const configuration::nodes::NodesConfiguration>
  getNodesConfiguration() const override {
    return config_->getNodesConfigurationFromServerConfigSource();
  }
  NodeID getMyNodeID() const override {
    return my_node_id_;
  }
  ClusterState* getClusterState() const override {
    return cluster_state_.get();
  }

  Socket* getServerSocket(node_index_t /*idx*/) override {
    return nullptr;
  }

  std::vector<std::pair<NodeID, std::unique_ptr<GOSSIP_Message>>> messages_;

  int getGossipIntervals(node_index_t nid) {
    return nodes_[nid].gossip_;
  }

  SteadyTimestamp getLastGossipTickTime() {
    return last_gossip_tick_time_;
  }

  void setIsLogsConfigLoaded(bool val) {
    report_logsconfig_loaded_.store(val);
  }

 protected:
  bool isLogsConfigLoaded() override {
    return report_logsconfig_loaded_.load();
  }

 private:
  BoycottTracker& getBoycottTracker() override {
    return mock_tracker_;
  }

  MockBoycottTracker mock_tracker_;
  NodeID my_node_id_;
  std::shared_ptr<ServerConfig> config_;
  std::unique_ptr<ClusterState> cluster_state_;
  std::atomic<bool> report_logsconfig_loaded_{true};
};

}} // namespace facebook::logdevice

namespace {

// generates a dummy config consisting of num_nodes sequencers
std::shared_ptr<ServerConfig> gen_config(size_t num_nodes,
                                         node_index_t this_node) {
  configuration::Nodes nodes;
  for (node_index_t i = 0; i < num_nodes; ++i) {
    auto& node = nodes[i];
    node.address =
        Sockaddr(get_localhost_address_str(), folly::to<std::string>(1337 + i));
    node.generation = 1;
    node.addSequencerRole();
    node.addStorageRole();
  }

  Configuration::NodesConfig nodes_config(std::move(nodes));

  // metadata stored on all nodes with max replication factor 3
  configuration::MetaDataLogsConfig meta_config =
      createMetaDataLogsConfig(nodes_config, nodes_config.getNodes().size(), 3);

  std::shared_ptr<ServerConfig> config =
      ServerConfig::fromDataTest(__FILE__, nodes_config, meta_config);
  return config;
}

std::pair<std::shared_ptr<ServerProcessor>, MockFailureDetector*>
make_processor_with_detector(node_index_t nid,
                             size_t num_nodes,
                             const GossipSettings& gossip_settings) {
  /* setup default settings */
  ServerSettings server_settings = create_default_settings<ServerSettings>();
  Settings main_settings = create_default_settings<Settings>();
  main_settings.num_workers = 1;
  main_settings.max_nodes = 1000;
  main_settings.worker_request_pipe_capacity = 1000000;

  /* make config for this index */
  std::shared_ptr<UpdateableConfig> uconfig =
      std::make_shared<UpdateableConfig>(std::make_shared<Configuration>(
          gen_config(num_nodes, nid),
          std::make_shared<configuration::LocalLogsConfig>()));

  auto processor_builder = TestServerProcessorBuilder{main_settings}
                               .setServerSettings(server_settings)
                               .setGossipSettings(gossip_settings)
                               .setUpdateableConfig(uconfig)
                               .setMyNodeID(NodeID(nid, 1));
  auto p = std::move(processor_builder).build();
  p->setServerInstanceId(SystemTimestamp::now().toMilliseconds().count());
  std::unique_ptr<MockFailureDetector> d =
      std::make_unique<MockFailureDetector>(
          UpdateableSettings<GossipSettings>(gossip_settings), p.get());
  MockFailureDetector* draw = d.get();
  p->failure_detector_ = std::move(d);
  return std::make_pair(std::move(p), draw);
}

using detector_list_t = std::vector<MockFailureDetector*>;

// given a list of FailureDetector objects (one per node) and a set of
// unavailable nodes, runs several iterations of gossiping and verifies that
// those dead nodes are detected
void simulate_single(detector_list_t& detectors,
                     std::unordered_set<node_index_t> dead_nodes) {
  size_t num_nodes = detectors.size();
  std::vector<node_index_t> alive;
  for (node_index_t i = 0; i < num_nodes; ++i) {
    if (dead_nodes.find(i) == dead_nodes.end()) {
      alive.push_back(i);
    }
  }
  ld_check(alive.size() > 0);

  // pretty generous limit on the number of iterations allowed before each
  // node detects that 'dead_nodes' are gone
  const size_t limit = 1000 * num_nodes * num_nodes;
  size_t steps = 0;
  bool detected = false;

  while (steps++ < limit && !detected) {
    // gossip
    for (auto idx : alive) {
      detectors[idx]->advanceTime();
    }

    // send messages
    for (auto idx : alive) {
      auto& messages = detectors[idx]->messages_;
      for (auto& it : messages) {
        if (dead_nodes.find(it.first.index()) != dead_nodes.end()) {
          // skip gossip messages sent to dead nodes
          continue;
        }
        detectors[it.first.index()]->onGossipReceived(*it.second);
      }
      messages.clear();
    }

    // check if all nodes are correctly detected by all other nodes
    detected = true;
    for (size_t j = 0; j < alive.size() && detected; ++j) {
      node_index_t idx = alive[j];

      for (node_index_t i = 0; i < num_nodes; ++i) {
        bool expected_alive = dead_nodes.find(i) == dead_nodes.end();
        if (detectors[idx]->isAlive(i) != expected_alive) {
          detected = false;
          break;
        }
      }
    }
  }

  EXPECT_TRUE(detected);
  ld_info("Detected all %zu/%zu dead nodes after %zu iterations",
          dead_nodes.size(),
          detectors.size(),
          steps);
}

std::tuple<std::vector<std::shared_ptr<ServerProcessor>>, detector_list_t>
create_processors_and_detectors(size_t num_nodes,
                                const GossipSettings& settings) {
  std::vector<std::shared_ptr<ServerProcessor>> processors;
  detector_list_t detectors;

  for (node_index_t i = 0; i < num_nodes; ++i) {
    std::shared_ptr<ServerProcessor> p;
    MockFailureDetector* d;
    std::tie(p, d) = make_processor_with_detector(i, num_nodes, settings);
    processors.push_back(std::move(p));
    detectors.push_back(d);
  }
  return std::make_tuple(std::move(processors), std::move(detectors));
}

// Runs a simulation featuring N nodes gossiping. For each 0 <= i < N/2,
// i randomly selected nodes are marked as down. The rest of the cluster is
// expected to detect that in a limited number of steps.
void simulate(size_t num_nodes, const GossipSettings& settings) {
  folly::ThreadLocalPRNG g;

  for (size_t num_dead = 1; num_dead < num_nodes / 2; ++num_dead) {
    std::vector<node_index_t> indices;
    for (node_index_t i = 0; i < num_nodes; ++i) {
      indices.push_back(i);
    }
    std::shuffle(indices.begin(), indices.end(), g);

    // set representing unavailable nodes
    std::unordered_set<node_index_t> dead(
        indices.begin(), indices.begin() + num_dead);

    std::vector<std::shared_ptr<ServerProcessor>> processors;
    detector_list_t detectors;

    std::tie(processors, detectors) =
        create_processors_and_detectors(num_nodes, settings);

    simulate_single(detectors, dead);
    // Cleanly shutdown the processors since FailureDetector runs on
    // ServerProcessor and its ServerWorkers need to cleanup the server read
    // streams.
    for (auto& processor : processors) {
      shutdown_processor(processor.get());
    }
  }
}

} // namespace

TEST(FailureDetector, RandomGossip) {
  GossipSettings settings = create_default_settings<GossipSettings>();
  settings.mode = GossipSettings::SelectionMode::RANDOM;
  settings.suspect_duration = std::chrono::milliseconds(0);
  simulate(20, settings);
}

TEST(FailureDetector, RoundRobinGossip) {
  GossipSettings settings = create_default_settings<GossipSettings>();
  settings.mode = GossipSettings::SelectionMode::ROUND_ROBIN;
  settings.suspect_duration = std::chrono::milliseconds(0);
  simulate(20, settings);
}

namespace {
// simulates a single round of gossiping between two nodes
void gossip_round(MockFailureDetector* d1, MockFailureDetector* d2) {
  d1->advanceTime();
  d2->advanceTime();

  ASSERT_LT(0, d1->messages_.size());
  ASSERT_LT(0, d2->messages_.size());

  /* drain all messages to get through broadcasts and get at least one actual
   * gossip for this round */
  for (auto& msg : d2->messages_) {
    d1->onGossipReceived(*msg.second);
  }
  for (auto& msg : d1->messages_) {
    d2->onGossipReceived(*msg.second);
  }

  d1->messages_.clear();
  d2->messages_.clear();
}
} // namespace

void shutdown_processor(Processor* processor) {
  // gracefully stop the processor and all its worker threads
  const int nworkers = processor->getAllWorkersCount();
  int nsuccess = post_and_wait(
      processor, [](Worker* worker) { worker->stopAcceptingWork(); });
  ASSERT_EQ(nworkers, nsuccess);
  nsuccess = post_and_wait(
      processor, [](Worker* worker) { worker->finishWorkAndCloseSockets(); });
  ASSERT_EQ(nworkers, nsuccess);
  processor->waitForWorkers(nworkers);
  processor->shutdown();
}
// Simulates a node requesting shard failover. Other nodes are expected to
// immediately treat this node as unavailable, even if it's still gossiping.
TEST(FailureDetector, Failover) {
  const int num_nodes = 2;

  GossipSettings settings = create_default_settings<GossipSettings>();
  settings.failover_blacklist_threshold = 2;
  settings.gossip_failure_threshold = 10;
  settings.suspect_duration = std::chrono::milliseconds(0);
  settings.mode = GossipSettings::SelectionMode::ROUND_ROBIN;
  UpdateableSettings<GossipSettings> updateable(settings);

  std::vector<std::shared_ptr<ServerProcessor>> processors;
  detector_list_t detectors;

  std::tie(processors, detectors) =
      create_processors_and_detectors(num_nodes, settings);

  // gossip until both nodes are aware that the other is alive
  for (int i = 0; i <= settings.min_gossips_for_stable_state; ++i) {
    gossip_round(detectors[0], detectors[1]);
  }

  for (node_index_t idx = 0; idx < num_nodes; ++idx) {
    EXPECT_TRUE(detectors[0]->isAlive(idx));
    EXPECT_TRUE(detectors[1]->isAlive(idx));
  }

  detectors[0]->failover();
  gossip_round(detectors[0], detectors[1]);
  // another round is needed to update gossip_list_
  gossip_round(detectors[0], detectors[1]);

  EXPECT_TRUE(detectors[0]->isAlive(node_index_t(0)));
  EXPECT_TRUE(detectors[0]->isAlive(node_index_t(1)));
  EXPECT_FALSE(detectors[1]->isAlive(node_index_t(0)));

  // "restart" the first node
  shutdown_processor(processors[0].get());
  std::tie(processors[0], detectors[0]) =
      make_processor_with_detector(0, num_nodes, settings);

  gossip_round(detectors[0], detectors[1]);

  // One gossip is enough to bring detectors[0] back to ALIVE in detectors[1]'s
  // view
  EXPECT_TRUE(detectors[1]->isAlive(node_index_t(0)));

  // after minimum number of more rounds, N0 is back in the game
  for (int i = 0; i < settings.min_gossips_for_stable_state; ++i) {
    gossip_round(detectors[0], detectors[1]);
  }
  EXPECT_TRUE(detectors[0]->isAlive(node_index_t(0)));
  EXPECT_TRUE(detectors[1]->isAlive(node_index_t(0)));
  shutdown_processor(processors[0].get());
  shutdown_processor(processors[1].get());
}

// Simulates a node that is stuck not loading Logsconfig. It must be flagged
// correctly as "STARTING".
TEST(FailureDetector, Starting) {
  const int num_nodes = 2;

  GossipSettings settings = create_default_settings<GossipSettings>();
  settings.failover_blacklist_threshold = 2;
  settings.gossip_failure_threshold = 10;
  settings.suspect_duration = std::chrono::milliseconds(0);
  settings.mode = GossipSettings::SelectionMode::ROUND_ROBIN;
  UpdateableSettings<GossipSettings> updateable(settings);

  std::vector<std::shared_ptr<ServerProcessor>> processors;
  detector_list_t detectors;

  std::tie(processors, detectors) =
      create_processors_and_detectors(num_nodes, settings);

  detectors[0]->setIsLogsConfigLoaded(false);

  // propagate state
  for (int i = 0; i <= settings.min_gossips_for_stable_state; ++i) {
    gossip_round(detectors[0], detectors[1]);
  }

  for (node_index_t idx = 0; idx < num_nodes; ++idx) {
    EXPECT_TRUE(detectors[idx]->isAlive(0));
    EXPECT_TRUE(detectors[idx]->getClusterState()->isNodeStarting(0));
    EXPECT_TRUE(detectors[idx]->isAlive(1));
    EXPECT_FALSE(detectors[idx]->getClusterState()->isNodeStarting(1));
  }

  detectors[0]->setIsLogsConfigLoaded(true);

  for (int i = 0; i < 2; ++i) {
    // we need at least two rounds because we only update the node state on the
    // call to FailureDetector::gossip() after we get and up-to-date gossip
    // message.
    gossip_round(detectors[0], detectors[1]);
  }

  for (node_index_t idx = 0; idx < num_nodes; ++idx) {
    EXPECT_TRUE(detectors[idx]->isAlive(0));
    EXPECT_FALSE(detectors[idx]->getClusterState()->isNodeStarting(0));
    EXPECT_TRUE(detectors[idx]->isAlive(1));
    EXPECT_FALSE(detectors[idx]->getClusterState()->isNodeStarting(1));
  }

  for (node_index_t i = 0; i < num_nodes; ++i) {
    shutdown_processor(processors[i].get());
  }
}

TEST(FailureDetector, GossipRetry) {
  const int nodes = 2;

  GossipSettings settings = create_default_settings<GossipSettings>();
  settings.suspect_duration = std::chrono::milliseconds(0);
  UpdateableSettings<GossipSettings> updateable(settings);

  std::vector<std::shared_ptr<ServerProcessor>> processors;
  detector_list_t detectors;

  std::tie(processors, detectors) =
      create_processors_and_detectors(2, settings);

  // gossip until both nodes are aware that the other is alive
  for (int i = 0; i <= settings.min_gossips_for_stable_state; ++i) {
    gossip_round(detectors[0], detectors[1]);
  }

  for (node_index_t idx = 0; idx < nodes; ++idx) {
    EXPECT_TRUE(detectors[0]->isAlive(idx));
    EXPECT_TRUE(detectors[1]->isAlive(idx));
  }

  int n1, n2, n3;

  // gossip
  detectors[0]->advanceTime();
  // get gossip intervals for N1
  n1 = detectors[0]->getGossipIntervals(1);
  // gossip again
  detectors[0]->advanceTime();
  // get gossip intervals again
  n2 = detectors[0]->getGossipIntervals(1);
  // make sure that the gossip intervals did not change because not enough time
  // elapsed between the two gossips
  ASSERT_EQ(n1, n2);

  SteadyTimestamp start = detectors[0]->getLastGossipTickTime();
  // keep gossiping again until the counter increases
  // if for some reason it doesn't happen due to a bug, the test will fail
  // with timeout
  do {
    n3 = detectors[0]->getGossipIntervals(1);
    detectors[0]->advanceTime();
    sleep_until_safe(std::chrono::steady_clock::now() +
                     settings.gossip_interval / 10);
  } while (n3 == n2);
  SteadyTimestamp end = SteadyTimestamp::now();

  ld_info("gossip_interval: %lu - start: %lu - end: %lu",
          settings.gossip_interval.count(),
          start.toMilliseconds().count(),
          end.toMilliseconds().count());
  // make sure the gossip intervals increased by one
  ASSERT_EQ(n2 + 1, n3);
  // make sure only one gossip interval elapsed
  ASSERT_TRUE(end - start >= settings.gossip_interval);
  ASSERT_TRUE(end - start < 2 * settings.gossip_interval);
  shutdown_processor(processors[0].get());
  shutdown_processor(processors[1].get());
}
