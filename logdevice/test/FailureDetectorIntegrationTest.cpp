/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include <cstdlib>
#include <ctime>
#include <map>
#include <string>
#include <unordered_map>
#include <vector>

#include <folly/Conv.h>
#include <gtest/gtest.h>

#include "logdevice/common/ConstructorFailed.h"
#include "logdevice/common/NodeSetSelectorFactory.h"
#include "logdevice/common/configuration/ConfigParser.h"
#include "logdevice/common/configuration/Configuration.h"
#include "logdevice/common/configuration/InternalLogs.h"
#include "logdevice/common/hash.h"
#include "logdevice/common/stats/Stats.h"
#include "logdevice/common/test/TestUtil.h"
#include "logdevice/include/Client.h"
#include "logdevice/lib/ClientImpl.h"
#include "logdevice/test/utils/IntegrationTestBase.h"
#include "logdevice/test/utils/IntegrationTestUtils.h"

using namespace facebook::logdevice;

class FailureDetectorIntegrationTest : public IntegrationTestBase {};

// Verify that gossip list shows DEAD status for cluster nodes
// that are killed and ALIVE status for nodes that exist.
TEST_F(FailureDetectorIntegrationTest, GossipListOnNodeRestarts) {
  std::string key;
  size_t i, num_nodes = 10;
  Configuration::Nodes nodes;

  /* make half of the nodes sequencers, and other half storage nodes */
  for (i = 0; i < num_nodes; ++i) {
    nodes[i].generation = 1;
    nodes[i].addStorageRole(/*num_shards*/ 2);
    if (i < num_nodes / 2) {
      nodes[i].addSequencerRole();
    }
  }

  auto cluster = IntegrationTestUtils::ClusterFactory()
                     .enableMessageErrorInjection()
                     .setNodes(nodes)
                     .useHashBasedSequencerAssignment(10)
                     .create(nodes.size());

  // test liveness of all nodes in gossip list of node `src`
  auto fd_test_alive = [&](node_index_t src, bool alive) {
    bool result = true;
    std::string expected_status = alive ? "ALIVE" : "DEAD";
    std::map<std::string, std::string> gossip_list =
        cluster->getNode(src).gossipInfo();

    for (i = 0; i < nodes.size() && result; ++i) {
      if (i == src)
        continue;
      key = folly::to<std::string>("N", i);
      result = (gossip_list[key] == expected_status);
    }

    return result;
  };

  // wait for all nodes to come up as ALIVE
  wait_until([&]() { return fd_test_alive(0, true); });

  // kill all nodes except first and expect them to be DEAD
  for (i = 1; i < nodes.size(); ++i) {
    cluster->getNode(i).kill();
  }
  wait_until([&]() { return fd_test_alive(0, false); });

  // restart the nodes and expect them to be ALIVE again
  for (i = 1; i < nodes.size(); ++i) {
    cluster->getNode(i).start();
  }
  wait_until([&]() { return fd_test_alive(0, true); });
}

// check if domain isolation dection is enabled
static bool
domainIsolationDetectionEnabled(IntegrationTestUtils::Cluster* cluster,
                                node_index_t index) {
  ld_check(cluster != nullptr);
  ld_check(cluster->getNodes().count(index));
  auto& node = cluster->getNode(index);
  auto result_map = node.domainIsolationInfo();
  return result_map["enabled"] == "true";
}

// check if the local domain in @param scope is marked as isolated or not
// assert domain isolation dection is enabled
static bool checkDomainIsolation(IntegrationTestUtils::Cluster* cluster,
                                 node_index_t index,
                                 NodeLocationScope scope) {
  ld_check(cluster != nullptr);
  ld_check(cluster->getNodes().count(index));
  ld_check(scope != NodeLocationScope::ROOT);
  auto& node = cluster->getNode(index);
  auto result_map = node.domainIsolationInfo();
  EXPECT_EQ("true", result_map["enabled"]);
  auto it = result_map.find(NodeLocation::scopeNames()[scope]);
  EXPECT_NE(result_map.end(), it);
  EXPECT_TRUE(it->second == "ISOLATED" || it->second == "NOT_ISOLATED");
  return it->second == "ISOLATED";
}

static bool isolatedScope(IntegrationTestUtils::Cluster* cluster,
                          node_index_t index,
                          NodeLocationScope iso_scope) {
  for (NodeLocationScope scope = NodeLocationScope::NODE;
       scope < NodeLocationScope::ROOT;
       scope = NodeLocation::nextGreaterScope(scope)) {
    if ((scope < iso_scope && checkDomainIsolation(cluster, index, scope)) ||
        (scope >= iso_scope && !checkDomainIsolation(cluster, index, scope))) {
      return false;
    }
  }
  return true;
}

static bool noIsolatedScope(IntegrationTestUtils::Cluster* cluster,
                            node_index_t index) {
  return isolatedScope(cluster, index, NodeLocationScope::ROOT);
}

// create cluster nodes with six nodes in the cluster;
// the cluster spans across three regions, and has a total of 6 nodes (1+2+3);
// used in following failure domain related tests
static Configuration::Nodes createFailureDomainNodes() {
  Configuration::Nodes nodes;
  for (int i = 0; i < 6; ++i) {
    auto& node = nodes[i];
    std::string domain_string;
    if (i < 1) {
      node.addSequencerRole();
      domain_string = "region0.dc1..."; // node 0 is in region 0
    } else if (i < 3) {
      domain_string = "region1.dc1.cl1.ro1.rk1"; // node 1, 2 is in region 1
    } else {
      domain_string = "region2.dc1.cl1.ro1.rk1"; // node 3-5 is in region 2
    }
    NodeLocation location;
    location.fromDomainString(domain_string);
    node.location = location;
    node.addStorageRole(/*num_shards*/ 2);
  }

  return nodes;
}

TEST_F(FailureDetectorIntegrationTest, DetectDomainIsolation) {
  // node 0 is in region 0
  // node 1, 2 is in the same rack in region 1
  // node 3-5 is in the same rack in region 2
  Configuration::Nodes nodes = createFailureDomainNodes();
  auto cluster =
      IntegrationTestUtils::ClusterFactory()
          .setNodes(nodes)
          .useHashBasedSequencerAssignment() // enable failure detector
          .enableMessageErrorInjection()
          .create(nodes.size());

  for (const auto& it : nodes) {
    node_index_t idx = it.first;
    const std::string reason =
        "domain isolation detection is enabled on N" + std::to_string(idx);
    wait_until(reason.c_str(), [&]() {
      return domainIsolationDetectionEnabled(cluster.get(), idx);
    });
  }

  // for each node, isolation status should eventually be NOT_ISOLATED on
  // all scopes
  for (const auto& it : nodes) {
    node_index_t idx = it.first;
    const std::string reason =
        "isolation status is NOT_ISOLATED for N" + std::to_string(idx);
    wait_until(
        reason.c_str(), [&]() { return noIsolatedScope(cluster.get(), idx); });
  }

  // stop node 0, 3, 4, 5, node 1 and 2 will detect themselve being isolated
  for (auto i : std::vector<int>{0, 3, 4, 5}) {
    cluster->getNode(i).suspend();
  }
  wait_until("N1 detects it is isolated (RACK scope)", [&]() {
    return isolatedScope(cluster.get(), 1, NodeLocationScope::RACK);
  });
  wait_until("N2 detects it is isolated (RACK scope)", [&]() {
    return isolatedScope(cluster.get(), 2, NodeLocationScope::RACK);
  });
  // stop node 1, node 2 will be isolated in NODE scope
  cluster->getNode(1).suspend();
  wait_until("N2 detects it is isolated (NODE scope)", [&]() {
    return isolatedScope(cluster.get(), 2, NodeLocationScope::NODE);
  });

  // resume node 1, node 2 will be isolated in RACK scope again
  cluster->getNode(1).resume();
  wait_until("N2 detects it is isolated (RACK scope again)", [&]() {
    return isolatedScope(cluster.get(), 2, NodeLocationScope::RACK);
  });
  // resume node 4, node 1, 2, 4 will not be isolated
  cluster->getNode(4).resume();
  for (auto i : std::vector<int>{1, 2, 4}) {
    const std::string reason =
        "N" + std::to_string(i) + " no longer isolated after N4 resumed";
    wait_until(
        reason.c_str(), [&]() { return noIsolatedScope(cluster.get(), i); });
  }
  // resume node 0, stop node 1, 2, 4, node 0 should be isolated on NODE scope
  cluster->getNode(0).resume();
  for (auto i : std::vector<int>{1, 2, 4}) {
    cluster->getNode(i).suspend();
  }
  wait_until("N0 detects it is isolated (NODE scope)", [&]() {
    return isolatedScope(cluster.get(), 0, NodeLocationScope::NODE);
  });

  // TODO 7746542: test cluster expansion and config changes
}

TEST_F(FailureDetectorIntegrationTest, ResetStoreTimerAfterIsolation) {
  // node 0 is in region 0
  // node 1-4 is in the same rack in region 1
  // node 5 is in the same rack in region 2
  Configuration::Nodes nodes;
  for (int i = 0; i < 6; ++i) {
    auto& node = nodes[i];
    node.addSequencerRole();
    node.addStorageRole(/*num_shards*/ 2);
    std::string domain_string;
    if (i < 1) {
      domain_string = "region0.dc1.cl1.ro1.rk1";
    } else if (i < 5) {
      domain_string = "region1.dc1.cl1.ro1.rk1";
    } else {
      domain_string = "region2.dc1.cl1.ro1.rk1";
    }
    NodeLocation location;
    location.fromDomainString(domain_string);
    node.location = location;
  }
  logsconfig::LogAttributes log_attrs;
  log_attrs.set_replicationFactor(2);
  log_attrs.set_extraCopies(0);
  log_attrs.set_syncedCopies(0);
  log_attrs.set_maxWritesInFlight(100);
  log_attrs.set_syncReplicationScope(NodeLocationScope::REGION);
  int num_logs = 100;

  // metadata logs are replicated cross-region as well
  Configuration::MetaDataLogsConfig meta_config = createMetaDataLogsConfig(
      /*nodeset=*/{0, 1, 5}, /*replication=*/3, NodeLocationScope::REGION);
  meta_config.nodeset_selector_type = NodeSetSelectorType::SELECT_ALL;

  auto cluster =
      IntegrationTestUtils::ClusterFactory()
          .setNodes(nodes)
          .setLogGroupName("mylogs")
          .setLogAttributes(log_attrs)
          .setNumLogs(num_logs)
          .setMetaDataLogsConfig(std::move(meta_config))
          .useHashBasedSequencerAssignment() // enable failure detector
          .create(nodes.size());

  // look for a log with a sequencer running on node 2
  int log_id = -1;
  std::vector<double> weights(nodes.size(), 1.0);
  for (int i = 0; i < num_logs; i++) {
    auto seq = hashing::weighted_ch(i, weights);
    if (seq == 2) {
      log_id = i;
      break;
    }
  }
  EXPECT_NE(log_id, -1);

  // wait until domain isolation detection is enabled
  for (int i = 0; i < nodes.size(); ++i) {
    wait_until(
        [&]() { return domainIsolationDetectionEnabled(cluster.get(), i); });
  }

  // for each node, isolation status should eventually be NOT_ISOLATED on
  // all scopes
  for (int i = 0; i < nodes.size(); ++i) {
    wait_until([&]() { return noIsolatedScope(cluster.get(), i); });
  }

  // write into the log
  std::atomic<bool> wait_cb(true);
  auto check_status_cb = [&](Status st, const DataRecord& /* unused */) {
    EXPECT_EQ(E::OK, st);
    wait_cb.store(false);
  };

  auto client = cluster->createClient();
  client->append(logid_t(log_id), Payload("hello", 5), check_status_cb);
  while (wait_cb.load()) {
    /* sleep override */
    sleep(1);
  }

  // collect stats for N2
  auto stats_orig = cluster->getNode(2).stats();

  // stop node 0 and 5, to cause the whole region1 to be isolated
  for (auto i : std::vector<int>{0, 5}) {
    cluster->getNode(i).suspend();
  }
  wait_until([&]() {
    return isolatedScope(cluster.get(), 2, NodeLocationScope::RACK);
  });

  // write into the same log again. should not be able to complete right away
  // due to being isolated, but we expect it to succeed eventually
  wait_cb.store(true);
  client->append(logid_t(log_id), Payload("hello", 5), check_status_cb);

  // sleep a little - meanwhile server hits store timeout and retries
  /* sleep override */
  sleep(2);

  // resume node 5, region1 will no longer be isolated - server should reset
  // the store timer and process the append immediately
  cluster->getNode(5).resume();
  wait_until([&]() { return noIsolatedScope(cluster.get(), 2); });

  // append should finish immediately
  while (wait_cb.load()) {
    /* sleep override */
    sleep(1);
  }

  // collect updated stats for N2
  auto stats = cluster->getNode(2).stats();
  EXPECT_GT(
      stats["appender_wave_timedout"], stats_orig["appender_wave_timedout"]);
  EXPECT_GT(stats["appender_store_timer_reset"],
            stats_orig["appender_store_timer_reset"]);
}

TEST_F(FailureDetectorIntegrationTest, MinorityIsolation) {
  // executes an append after 3 nodes out of 5 are killed, to verify that the
  // nodes still alive are declining appends with E::ISOLATED.
  const int num_nodes = 5;
  auto cluster =
      IntegrationTestUtils::ClusterFactory()
          .useHashBasedSequencerAssignment() // enable failure detector
          .enableMessageErrorInjection()
          .create(num_nodes);

  for (size_t idx = 0; idx < num_nodes; ++idx) {
    cluster->getNode(idx).waitUntilAvailable();
  }

  auto client = cluster->createClient();
  for (int i = 0; i < 10; ++i) {
    lsn_t lsn = client->appendSync(logid_t(1), Payload("hello", 5));
    EXPECT_NE(LSN_INVALID, lsn);
  }

  auto node_idx = hashing::weighted_ch(1, std::vector<double>(num_nodes, 1.0));
  ASSERT_TRUE(node_idx != -1 && node_idx < 5);
  ASSERT_EQ(
      "ACTIVE", cluster->getNode(node_idx).sequencerInfo(logid_t(1))["State"]);

  // stop 3 nodes excluding node_idx
  int n = 3;
  std::vector<node_index_t> dead_nodes;
  for (int i = 0; i < num_nodes && n > 0; i++) {
    if (i != node_idx) {
      cluster->getNode(i).suspend();
      std::string key = folly::to<std::string>("N", i);
      // wait until node_idx notices that node i is dead
      wait_until([&]() {
        auto info = cluster->getNode(node_idx).gossipInfo();
        return info[key] == "DEAD";
      });
      dead_nodes.push_back(i);
      --n;
    }
  }

  std::atomic<bool> wait_cb(true);
  auto check_status_cb = [&](Status st, const DataRecord& /* unused */) {
    EXPECT_EQ(E::ISOLATED, st);
    wait_cb.store(false);
  };

  client->append(logid_t(1), Payload("hello", 5), check_status_cb);
  while (wait_cb.load()) {
    /* sleep override */
    sleep(1);
  }

  // resuscitate one node
  auto idx = dead_nodes.front();
  cluster->getNode(idx).resume();
  std::string key = folly::to<std::string>("N", idx);
  wait_until([&]() {
    auto info = cluster->getNode(node_idx).gossipInfo();
    return info[key] == "ALIVE";
  });

  lsn_t lsn = client->appendSync(logid_t(1), Payload("hello", 5));
  EXPECT_NE(LSN_INVALID, lsn);
}

/* checks that GET_CLUSTER_STATE message prevents append timeout */
TEST_F(FailureDetectorIntegrationTest, GetClusterState) {
  const int num_logs = 20;
  const int num_nodes = 5;
  auto cluster = IntegrationTestUtils::ClusterFactory()
                     .useHashBasedSequencerAssignment(100, "10s")
                     .enableMessageErrorInjection()
                     .setNumLogs(num_logs)
                     .create(num_nodes);

  // force client to have only one worker to make sure the same worker is going
  // to exectue all requests.
  std::unique_ptr<ClientSettings> client_settings(ClientSettings::create());
  ASSERT_EQ(0, client_settings->set("num-workers", 1));
  ASSERT_EQ(0, client_settings->set("cluster-state-refresh-interval", "100ms"));

  // look for a log with a sequencer running on node 3
  int log_id = -1;
  std::vector<double> weights(num_nodes, 1.0);
  for (int i = 0; i < num_logs; i++) {
    auto seq = hashing::weighted_ch(i, weights);
    if (seq == 3) {
      log_id = i;
      break;
    }
  }
  ASSERT_NE(log_id, -1);

  lsn_t lsn = LSN_INVALID;
  auto client =
      cluster->createClient(this->testTimeout(), std::move(client_settings));
  // write to log_id (sequencer N3)
  lsn = client->appendSync(logid_t(log_id), Payload("hello", 5));
  ASSERT_NE(LSN_INVALID, lsn);

  // override the timeout to allow it to fail faster.
  client->setTimeout(std::chrono::milliseconds(500));

  Stats stats = checked_downcast<ClientImpl&>(*client).stats()->aggregate();
  auto stats_before = stats.client.cluster_state_updates;
  // now stop N3
  // this should trigger an append timeout and execute a GetClusterStateRequest
  cluster->getNode(3).suspend();
  cluster->waitUntilGossip(/* alive */ false, /* node */ 3);
  lsn = client->appendSync(logid_t(log_id), Payload("hello", 5));
  ASSERT_EQ(LSN_INVALID, lsn);
  ASSERT_EQ(err, E::TIMEDOUT);

  // wait until ClusterState is updated
  wait_until("ClusterState updated", [&]() {
    stats = checked_downcast<ClientImpl&>(*client).stats()->aggregate();
    auto stats_after = stats.client.cluster_state_updates;
    return (stats_before < stats_after);
  });

  // revert timeout value to prevent flakyness
  client->setTimeout(this->testTimeout());
  // kill node to make sure it's not holding the lock on FileEpochStore which
  // would prevent another node from activating the sequencer
  cluster->getNode(3).kill();
  // send another append - no timeout should occur
  lsn = client->appendSync(logid_t(log_id), Payload("hello", 5));
  ASSERT_NE(LSN_INVALID, lsn);
}

// we check whether we correctly detect starting state and if we recover after
// internal logs become available again
TEST_F(FailureDetectorIntegrationTest, StartingState) {
  const int num_nodes = 10;

  auto cluster_factory = IntegrationTestUtils::ClusterFactory();
  logsconfig::LogAttributes log_attrs =
      cluster_factory.createDefaultLogAttributes(num_nodes);
  log_attrs.set_nodeSetSize(num_nodes);

  logsconfig::LogAttributes internal_log_attrs;
  internal_log_attrs.set_replicationFactor(1);
  internal_log_attrs.set_nodeSetSize(1);

  auto cluster = cluster_factory.enableLogsConfigManager()
                     .deferStart()
                     .setLogAttributes(log_attrs)
                     .setConfigLogAttributes(internal_log_attrs)
                     .enableSelfInitiatedRebuilding()
                     .useHashBasedSequencerAssignment(100, "10s")
                     .create(num_nodes);

  ASSERT_EQ(0,
            cluster->provisionEpochMetaData(
                NodeSetSelectorFactory::create(
                    NodeSetSelectorType::CONSISTENT_HASHING),
                false));

  /* here we obtain the only shard that has internal log records */
  auto selector =
      NodeSetSelectorFactory::create(NodeSetSelectorType::CONSISTENT_HASHING);

  const auto config = cluster->getConfig()->get();
  auto selected = selector->getStorageSet(
      configuration::InternalLogs::CONFIG_LOG_DELTAS,
      config.get(),
      *config->getNodesConfigurationFromServerConfigSource(),
      /* target_nodeset_size */ 1,
      /* seed */ 0,
      nullptr);
  ASSERT_EQ(selected.decision, NodeSetSelector::Decision::NEEDS_CHANGE);
  ASSERT_EQ(selected.storage_set.size(), 1);
  auto S = selected.storage_set[0];

  /* start everything but the node that holds logsconfig deltas */
  for (auto& p : cluster->getNodes()) {
    auto node = p.first;
    if (node != S.node()) {
      p.second->start();
    }
  }

  wait_until(
      folly::sformat("All cluster except N{} is STARTING", S.node()).c_str(),
      [&]() {
        for (node_index_t n = 0; n < num_nodes; ++n) {
          if (n == S.node()) {
            continue;
          }
          auto res = cluster->getNode(n).gossipStarting();
          for (node_index_t nid = 0; nid < num_nodes; ++nid) {
            if (nid == S.node()) {
              continue;
            }
            auto key = folly::to<std::string>("N", nid);
            if (res.find(key) == res.end() || !res[key]) {
              return false;
            }
          }
        }
        return true;
      });

  /* we should not be able to create clients if we can't read internal logs */
  std::unique_ptr<ClientSettings> client_settings(ClientSettings::create());
  ASSERT_THROW(
      cluster->createClient(
          /*timeout=*/std::chrono::seconds{2}, std::move(client_settings)),
      ConstructorFailed);

  ld_info("Starting node N%d", S.node());
  cluster->getNode(S.node()).start();

  for (const auto& it : cluster->getNodes()) {
    node_index_t idx = it.first;
    cluster->waitUntilGossip(/* alive */ true, idx);
  }

  wait_until("Nobody is starting", [&]() {
    for (node_index_t n = 0; n < num_nodes; ++n) {
      auto res = cluster->getNode(n).gossipStarting();
      for (node_index_t nid = 0; nid < num_nodes; ++nid) {
        auto key = folly::to<std::string>("N", nid);
        if (res.find(key) != res.end() && res[key]) {
          return false;
        }
      }
    }
    return true;
  });

  cluster->waitForRecovery();

  /* create client */
  std::unique_ptr<ClientSettings> client_settings2(ClientSettings::create());
  auto client = cluster->createIndependentClient(
      this->testTimeout(), std::move(client_settings2));

  /* we should be able to create log groups now */
  auto log_group = client->makeLogGroupSync(
      "/log1",
      logid_range_t(logid_t(1), logid_t(2)),
      client::LogAttributes().with_replicationFactor(2),
      true);
  ASSERT_NE(nullptr, log_group);

  /* we need to wait our own version of LogsConfig catch up to the log_group we
   * just created */
  ASSERT_TRUE(client->syncLogsConfigVersion(log_group->version()));

  /* and append */
  auto lsn = client->appendSync(logid_t(1), Payload("hello", 5));
  ASSERT_NE(LSN_INVALID, lsn);
}

TEST_F(FailureDetectorIntegrationTest, GetClusterStatePeerUnavailable) {
  // 1 sequencer and 2 storage nodes.
  Configuration::Nodes nodes;
  nodes[0].generation = 1;
  nodes[0].addSequencerRole();
  for (node_index_t i = 1; i < 3; ++i) {
    nodes[i].generation = 1;
    nodes[i].addStorageRole(/*num_shards*/ 2);
  }

  auto cluster = IntegrationTestUtils::ClusterFactory()
                     .setNodes(nodes)
                     .useHashBasedSequencerAssignment(10)
                     .enableMessageErrorInjection()
                     .useTcp()
                     .create(3);

  for (size_t idx = 0; idx < 3; ++idx) {
    cluster->getNode(idx).waitUntilAvailable();
  }

  std::unique_ptr<ClientSettings> client_settings(ClientSettings::create());
  ASSERT_EQ(0, client_settings->set("cluster-state-refresh-interval", "1ms"));
  // force N1 to be the only recipinets of GET_CLUSTER_STATE messages
  ASSERT_EQ(0, client_settings->set("test-get-cluster-state-recipients", "1"));
  auto client =
      cluster->createClient(this->testTimeout(), std::move(client_settings));

  // write a first record synchronously to make sure connection is created
  lsn_t lsn = client->appendSync(logid_t(1), Payload("hello", 5));
  ASSERT_NE(LSN_INVALID, lsn);

  // to make sure the append takes long enough, we kill the second
  // storage node so the append cannot complete.
  cluster->getNode(2).kill();

  // now write into the log asynchronously, checking for the expected error code
  Semaphore sem;
  auto check_status_cb = [&](Status st, const DataRecord& /* unused */) {
    // expect peer unavailable
    EXPECT_EQ(E::PEER_UNAVAILABLE, st);
    sem.post();
  };

  // send an append
  client->append(logid_t(1), Payload("hello", 5), check_status_cb);

  // now pause sequencer. the append internal timer should fire, sending a
  // GET_CLUSTER_STATE and upon receiving the reponse, it should close the
  // connection to the sequencer and cancel this pending append with
  // E::PEER_UNAVAILABLE.
  cluster->getNode(0).suspend();

  sem.wait();
}

/*
 * A test to verify that adding a node in the config(by expansion e.g.),
 * without actually bringing up a logdeviced on that node, must not
 * cause existing nodes to treat it as ALIVE/SUSPECT.
 *
 * The new node should stay as DEAD in Failure Detector.
 * This tests the scenario mentioned in #11745547
 *
 * Overview of test:
 * 1. Create a cluster
 * 2. Wait for nodes to become ALIVE in FD
 * 3. expand the cluster by 1 node
 * 4. use "info gossip" to find out DEAD/SUSPECT/ALIVE status of the new node
 *    in existing node's FD.
 */
TEST_F(FailureDetectorIntegrationTest, FDClusterExpandStateTransition) {
  // This number is after expansion, we'll start NUM_NODES-1 initially
  const int NUM_NODES = 3;

  Configuration::Nodes nodes;
  /* make all nodes as sequencers */
  for (int i = 0; i < NUM_NODES - 1; ++i) {
    nodes[i].generation = 1;
    nodes[i].addSequencerRole();
    nodes[i].addStorageRole(/*num_shards*/ 2);
  }

  auto cluster = IntegrationTestUtils::ClusterFactory()
                     .setNodes(nodes)
                     .useHashBasedSequencerAssignment()
                     .setParam("--gossip-interval", "100ms")
                     .setParam("--gossip-threshold", "20")
                     .setParam("--suspect-duration", "3000ms")
                     .create(NUM_NODES - 1);

  auto fd_get_node_state = [&](node_index_t src, node_index_t node_to_inspect) {
    std::map<std::string, std::string> gossip_list =
        cluster->getNode(src).gossipState();

    std::string key_dead_node = folly::to<std::string>("N", node_to_inspect);
    return gossip_list[key_dead_node];
  };

  wait_until("nodes have transitioned to ALIVE",
             [&]() { return "ALIVE" == fd_get_node_state(0, NUM_NODES - 2); });
  /* New node will be absent from "info gossip" output map at this point */

  // Expand the cluster
  ld_info("Adding 1 node to the config");
  int rv = cluster->expand(1, false /*don't start the new node*/);
  ASSERT_EQ(0, rv);

  const node_index_t NEW_NODE = NUM_NODES - 1;
  // Wait for N0 to pick up the new config (until then "info gossip" returns
  // an empty string for the new node)
  wait_until("N0 becomes aware of new node",
             [&] { return fd_get_node_state(0, NEW_NODE) != ""; });

  std::unordered_map<std::string, int> cnt;

  /* test that new node stays in DEAD state */
  for (int i = 0; i < 50; ++i) {
    std::string state = fd_get_node_state(0, NEW_NODE);
    ASSERT_NE(state, "");
    if (state == "DEAD" || state == "SUSPECT" || state == "ALIVE") {
      ++cnt[state];
    } else {
      FAIL() << "Unknown state from \"info gossip\": " << state;
    }
    /* sleep override */
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
  }

  ld_info("DEAD:%d, SUSPECT:%d, ALIVE:%d",
          cnt["DEAD"],
          cnt["SUSPECT"],
          cnt["ALIVE"]);

  /* It takes some time for other nodes to get the new config,
   * so some "info gossips" will return empty string
   */
  EXPECT_GT(cnt["DEAD"], 0);
  EXPECT_EQ(0, cnt["SUSPECT"]);
  EXPECT_EQ(0, cnt["ALIVE"]);
}
