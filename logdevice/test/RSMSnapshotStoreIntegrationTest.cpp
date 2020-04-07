/**
 * Copyright (c) 2019-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "logdevice/common/Processor.h"
#include "logdevice/common/Semaphore.h"
#include "logdevice/common/configuration/InternalLogs.h"
#include "logdevice/common/configuration/logs/FBuffersLogsConfigCodec.h"
#include "logdevice/common/configuration/logs/LogsConfigStateMachine.h"
#include "logdevice/common/replicated_state_machine/LogBasedRSMSnapshotStore.h"
#include "logdevice/common/replicated_state_machine/MessageBasedRSMSnapshotStore.h"
#include "logdevice/common/types_internal.h"
#include "logdevice/include/Client.h"
#include "logdevice/lib/ClientImpl.h"
#include "logdevice/lib/ClientProcessor.h"
#include "logdevice/test/utils/IntegrationTestBase.h"
#include "logdevice/test/utils/IntegrationTestUtils.h"

using namespace facebook::logdevice;
using namespace IntegrationTestUtils;
using namespace testing;

static std::string typeToString(SnapshotStoreType t) {
  std::string cluster_store_type;
  if (t == SnapshotStoreType::LEGACY) {
    cluster_store_type = "legacy";
  } else if (t == SnapshotStoreType::LOG) {
    cluster_store_type = "log";
  } else if (t == SnapshotStoreType::LOCAL_STORE) {
    cluster_store_type = "local-store";
  }
  return cluster_store_type;
}

class LogBasedRSMSnapshotStoreTest : public LogBasedRSMSnapshotStore {
 public:
  explicit LogBasedRSMSnapshotStoreTest(std::string key,
                                        logid_t snapshot_log,
                                        Processor* p,
                                        bool allow_snapshotting, /* writable */
                                        std::shared_ptr<Client> client)
      : LogBasedRSMSnapshotStore(key, snapshot_log, p, allow_snapshotting),
        client_(client) {}

  int extractVersion(const std::string& /* unused */,
                     RSMSnapshotHeader& header_out) const override {
    // Fake a very high base version to avoid getting E::STALE from
    // LogBasedRSMSnapshotStore::onSnapshotRecord()
    header_out.base_version = compose_lsn(epoch_t(1000), esn_t(0));
    return 0;
  }

 private:
  std::shared_ptr<Client> client_;
};

class MessageBasedRSMSnapshotStoreTest : public MessageBasedRSMSnapshotStore {
 public:
  explicit MessageBasedRSMSnapshotStoreTest(std::string key,
                                            std::shared_ptr<Client> client)
      : MessageBasedRSMSnapshotStore(key), client_(client) {}

  int postRequestWithRetrying(std::unique_ptr<Request>& rq) override {
    auto client_impl = std::dynamic_pointer_cast<ClientImpl>(client_);
    return client_impl->getProcessor().postWithRetrying(rq);
  }

 private:
  std::shared_ptr<Client> client_;
};

struct TestMode {
  SnapshotStoreType cluster;
  SnapshotStoreType client;
};

class RSMSnapshotStoreIntegrationTest
    : public IntegrationTestBase,
      public ::testing::WithParamInterface<TestMode> {
 public:
  static const size_t NNODES = 3;
  static const size_t NUM_LOGS = 21;

  void buildCluster(SnapshotStoreType cluster_store_type) {
    std::string cluster_type_str = typeToString(cluster_store_type);
    auto log_attrs = logsconfig::LogAttributes().with_replicationFactor(2);
    cluster = IntegrationTestUtils::ClusterFactory()
                  .setParam("--rsm-snapshot-store-type", cluster_type_str)
                  .setParam("--logsconfig-max-delta-records",
                            "1") // to trigger writeSnapshot() as soon as we
                                 // write a delta record
                  .enableLogsConfigManager()
                  .useHashBasedSequencerAssignment()
                  .setLogGroupName("logrange1")
                  .setLogAttributes(log_attrs)
                  .setNumLogsConfigManagerLogs(NUM_LOGS)
                  .create(NNODES);
    ASSERT_NE(nullptr, cluster);
    cluster->waitForRecovery();

    std::unique_ptr<ClientSettings> client_settings(ClientSettings::create());
    client =
        cluster->createClient(DEFAULT_TEST_TIMEOUT, std::move(client_settings));
    ASSERT_NE(nullptr, client);
  }

  void initClientSnapshotStore(SnapshotStoreType store_type,
                               logid_t delta_log,
                               logid_t snapshot_log) {
    ClientImpl* client_impl = dynamic_cast<ClientImpl*>(client.get());
    switch (store_type) {
      case SnapshotStoreType::LOG:
        ld_info("Creating LogBasedRSMSnapshotStoreTest");
        client_snapshot_store_ = std::make_unique<LogBasedRSMSnapshotStoreTest>(
            folly::to<std::string>(delta_log.val_),
            snapshot_log,
            client_impl->getProcessorPtr().get(),
            true /* writable */,
            client);
        break;
      case SnapshotStoreType::MESSAGE:
        ld_info("Creating MessageBasedRSMSnapshotStore");
        client_snapshot_store_ =
            std::make_unique<MessageBasedRSMSnapshotStoreTest>(
                folly::to<std::string>(delta_log.val_), client);
        break;
      default:
        ld_check(0);
    }
  }

  void triggerSnapshotStoreWrite() {
    logid_t logid_to_create{50};
    for (int i = 1; i <= 1; i++) {
      logid_to_create = logid_t(logid_to_create.val_ + 1);
      std::string dir_name = "/dir-test" + folly::to<std::string>(i);
      ld_info("Create dummy record:%d dir_name:%s", i, dir_name.c_str());
      ASSERT_NE(nullptr,
                client->makeLogGroupSync(
                    dir_name,
                    logid_range_t(logid_to_create, logid_to_create),
                    client::LogAttributes().with_replicationFactor(2),
                    false));
    }
  }

  std::unique_ptr<logsconfig::LogsConfigTree>
  deserializeState(Payload payload,
                   lsn_t version,
                   std::chrono::milliseconds /* unused */) const {
    std::string delimiter = "/";
    auto tree =
        facebook::logdevice::logsconfig::FBuffersLogsConfigCodec::deserialize<
            logsconfig::LogsConfigTree>(payload, delimiter);
    if (tree == nullptr) {
      // The returned tree can be nullptr if deserialization failed. In this
      // case we should ignore this tree when possible
      err = E::BADMSG;
      return nullptr;
    }
    tree->setVersion(version);
    return tree;
  }

  int deserializeStringSnapshot(
      std::string& payload_str,
      RSMSnapshotHeader& header_out,
      std::unique_ptr<logsconfig::LogsConfigTree>& out) const {
    Payload pl(payload_str.data(), payload_str.size());
    const auto header_sz = RSMSnapshotHeader::deserialize(pl, header_out);
    if (header_sz < 0) {
      ld_error("Failed to deserialize header of snapshot");
      err = E::BADMSG;
      return -1;
    }

    const uint8_t* ptr = reinterpret_cast<const uint8_t*>(payload_str.data());
    ptr += header_sz;
    Payload p(ptr, payload_str.size() - header_sz);

    std::chrono::milliseconds ts;
    auto logsconfig_tree =
        deserializeState(p, header_out.base_version, ts /* unused */);
    ld_info("%s deserializeState() for base_version:%s",
            logsconfig_tree ? "Successfully finished" : "Failed during",
            lsn_to_string(header_out.base_version).c_str());
    if (logsconfig_tree) {
      out = std::move(logsconfig_tree);
      return 0;
    } else {
      // err set by `deserializeState`.
      return -1;
    }
  }

  std::unique_ptr<Cluster> cluster;
  std::shared_ptr<Client> client;
  std::unique_ptr<RSMSnapshotStore> snapshot_store_{nullptr};
  std::unique_ptr<RSMSnapshotStore> client_snapshot_store_{nullptr};
};

std::vector<TestMode> cluster_client_types{
    {SnapshotStoreType::LOG, SnapshotStoreType::LOG},
    {SnapshotStoreType::LOG, SnapshotStoreType::MESSAGE},
    {SnapshotStoreType::LOCAL_STORE, SnapshotStoreType::MESSAGE}};
INSTANTIATE_TEST_CASE_P(RSMSnapshotStoreIntegrationTest,
                        RSMSnapshotStoreIntegrationTest,
                        ::testing::ValuesIn(cluster_client_types));

class VerifySequencerOnlyNodes
    : public ::testing::TestWithParam<SnapshotStoreType> {};
INSTANTIATE_TEST_CASE_P(VerifySequencerOnlyNodes,
                        VerifySequencerOnlyNodes,
                        ::testing::Values(SnapshotStoreType::LOG,
                                          SnapshotStoreType::LOCAL_STORE));
TEST_P(VerifySequencerOnlyNodes, CanCatchupToLatestRsmState) {
  int num_nodes = 4;
  auto cluster_snapshot_type = GetParam();
  std::string cluster_store_type = typeToString(cluster_snapshot_type);

  // Configure nodes
  Configuration::Nodes nodes;
  for (int i = 0; i < num_nodes; ++i) {
    auto& node = nodes[i];
    node.generation = 1;
    node.addSequencerRole();
    node.addStorageRole(2);
  }
  // Add a sequencer only node (N4)
  auto& node = nodes[num_nodes];
  node.generation = 1;
  node.addSequencerRole();
  auto factory = IntegrationTestUtils::ClusterFactory()
                     .setParam("--rsm-snapshot-store-type", cluster_store_type)
                     .setParam("--logsconfig-max-delta-records",
                               "1") // to trigger writeSnapshot() as soon as we
                                    // write a delta record
                     .enableLogsConfigManager()
                     .useHashBasedSequencerAssignment()
                     .setNodes(nodes);

  std::unique_ptr<Cluster> cluster;
  std::shared_ptr<Client> client;
  cluster = factory.create(num_nodes + 1);
  cluster->waitUntilAllSequencersQuiescent();
  std::unique_ptr<ClientSettings> client_settings(ClientSettings::create());
  ASSERT_EQ(0, client_settings->set("enable-logsconfig-manager", true));
  client =
      cluster->createClient(DEFAULT_TEST_TIMEOUT, std::move(client_settings));
  ASSERT_NE(nullptr, client);

  /* write something to the delta log */
  auto dir = client->makeDirectorySync("/file1", true);
  ASSERT_NE(nullptr, dir);

  /* Verify that RSMs are at the same lsn everywhere */
  auto tail_lsn =
      client->getTailLSNSync(configuration::InternalLogs::CONFIG_LOG_DELTAS);
  auto tail_lsn_str = lsn_to_string(tail_lsn);
  bool catching_up = true;
  while (catching_up) {
    auto in_mem_versions = cluster->getNode(4).getRsmVersions(
        configuration::InternalLogs::CONFIG_LOG_DELTAS,
        RsmVersionType::IN_MEMORY);
    catching_up = !in_mem_versions.size();
    for (auto x : in_mem_versions) {
      if (tail_lsn_str != x.second) {
        catching_up = true;
        ld_info("N%hu at %s, waiting to catch up till %s",
                x.first,
                x.second.c_str(),
                tail_lsn_str.c_str());
      }
    }
    std::this_thread::sleep_for(std::chrono::seconds(1));
  }
}

// Verify that Durable versions with Local Store Impl will eventually
// be propagated. Also introduce a sequencer only node to verify that
// its Durable version is LSN_INVALID
TEST_F(RSMSnapshotStoreIntegrationTest, LocalStoreDurableVersionCatchesUp) {
  int num_nodes = 4;
  // Configure nodes
  Configuration::Nodes nodes;
  for (int i = 0; i < num_nodes; ++i) {
    auto& node = nodes[i];
    node.generation = 1;
    node.addSequencerRole();
    node.addStorageRole(2);
  }
  // Add a sequencer only node (N4)
  auto& node = nodes[num_nodes];
  node.generation = 1;
  node.addSequencerRole();
  auto factory = IntegrationTestUtils::ClusterFactory()
                     .setParam("--rsm-snapshot-store-type", "local-store")
                     .setParam("--logsconfig-snapshotting-period", "5s")
                     .setParam("--logsconfig-max-delta-records",
                               "1") // to trigger writeSnapshot() as soon as we
                                    // write a delta record
                     .enableLogsConfigManager()
                     .useHashBasedSequencerAssignment()
                     .setNodes(nodes);

  std::unique_ptr<Cluster> cluster;
  std::shared_ptr<Client> client;
  cluster = factory.create(num_nodes + 1);
  cluster->waitUntilAllSequencersQuiescent();
  std::unique_ptr<ClientSettings> client_settings(ClientSettings::create());
  ASSERT_EQ(0, client_settings->set("enable-logsconfig-manager", true));
  client =
      cluster->createClient(DEFAULT_TEST_TIMEOUT, std::move(client_settings));
  ASSERT_NE(nullptr, client);

  /* write something to the delta log */
  auto dir = client->makeDirectorySync("/file1", true);
  ASSERT_NE(nullptr, dir);

  /* Verify that RSMs are at the same lsn everywhere */
  auto tail_lsn =
      client->getTailLSNSync(configuration::InternalLogs::CONFIG_LOG_DELTAS);
  auto tail_lsn_str = lsn_to_string(tail_lsn);
  bool catching_up;
  do {
    catching_up = false;
    auto in_mem_versions = cluster->getNode(4).getRsmVersions(
        configuration::InternalLogs::CONFIG_LOG_DELTAS,
        RsmVersionType::IN_MEMORY);
    for (auto x : in_mem_versions) {
      if (tail_lsn_str != x.second) {
        catching_up = true;
        ld_info("In Memory versions: N%hu at %s, waiting to catch up till %s",
                x.first,
                x.second.c_str(),
                tail_lsn_str.c_str());
      }
    }
    auto durable_versions = cluster->getNode(3).getRsmVersions(
        configuration::InternalLogs::CONFIG_LOG_DELTAS,
        RsmVersionType::DURABLE);
    for (auto d : durable_versions) {
      ld_info("Durable versions: N%hu at %s, waiting to catch up till %s",
              d.first,
              d.second.c_str(),
              tail_lsn_str.c_str());
      if (d.first == 4) {
        ASSERT_EQ(d.second, "LSN_INVALID");
      } else if (tail_lsn_str != d.second) {
        catching_up = true;
      }
    }
    std::this_thread::sleep_for(std::chrono::seconds(1));
  } while (catching_up);

  // Now restart all nodes, and not write anything new.
  // At this stage, verify that
  // a) RSMs bootstrap from local store and
  // b) distribute durable versions
  for (int i = 0; i <= num_nodes; i++) {
    cluster->getNode(i).kill();
  }
  for (int i = 0; i <= num_nodes; i++) {
    cluster->getNode(i).start();
    cluster->getNode(i).waitUntilStarted();
  }

  do {
    catching_up = false;
    auto durable_versions = cluster->getNode(2).getRsmVersions(
        configuration::InternalLogs::CONFIG_LOG_DELTAS,
        RsmVersionType::DURABLE);
    for (auto d : durable_versions) {
      ld_info("Durable versions: N%hu at %s, waiting to catch up till %s",
              d.first,
              d.second.c_str(),
              tail_lsn_str.c_str());
      if (d.first == 4) {
        ASSERT_EQ(d.second, "LSN_INVALID");
      } else if (tail_lsn_str != d.second) {
        catching_up = true;
      }
    }
    std::this_thread::sleep_for(std::chrono::seconds(1));
  } while (catching_up);
}

TEST_F(RSMSnapshotStoreIntegrationTest,
       InavlidateDurableVersionOnShardFailure) {
  int num_nodes = 4;
  // Configure nodes
  Configuration::Nodes nodes;
  for (int i = 0; i < num_nodes; ++i) {
    auto& node = nodes[i];
    node.generation = 1;
    node.addSequencerRole();
    node.addStorageRole(2);
  }
  // Add a sequencer only node (N4)
  auto& node = nodes[num_nodes];
  node.generation = 1;
  node.addSequencerRole();
  auto factory = IntegrationTestUtils::ClusterFactory()
                     .setParam("--rsm-snapshot-store-type", "local-store")
                     .setParam("--logsconfig-snapshotting-period", "5s")
                     .setParam("--logsconfig-max-delta-records",
                               "1") // to trigger writeSnapshot() as soon as we
                                    // write a delta record
                     .enableLogsConfigManager()
                     .useHashBasedSequencerAssignment()
                     .setNodes(nodes);

  std::unique_ptr<Cluster> cluster;
  std::shared_ptr<Client> client;
  cluster = factory.create(num_nodes + 1);
  cluster->waitUntilAllSequencersQuiescent();
  std::unique_ptr<ClientSettings> client_settings(ClientSettings::create());
  ASSERT_EQ(0, client_settings->set("enable-logsconfig-manager", true));
  client =
      cluster->createClient(DEFAULT_TEST_TIMEOUT, std::move(client_settings));
  ASSERT_NE(nullptr, client);

  /* write something to the delta log */
  auto dir = client->makeDirectorySync("/file1", true);
  ASSERT_NE(nullptr, dir);

  /* Verify that RSMs are at the same lsn everywhere */
  auto tail_lsn =
      client->getTailLSNSync(configuration::InternalLogs::CONFIG_LOG_DELTAS);
  auto tail_lsn_str = lsn_to_string(tail_lsn);
  bool catching_up;
  do {
    catching_up = false;
    auto durable_versions = cluster->getNode(2).getRsmVersions(
        configuration::InternalLogs::CONFIG_LOG_DELTAS,
        RsmVersionType::DURABLE);
    for (auto d : durable_versions) {
      ld_info("Durable versions: N%hu at %s, waiting to catch up till %s",
              d.first,
              d.second.c_str(),
              tail_lsn_str.c_str());
      if (d.first == 4) {
        ASSERT_EQ(d.second, "LSN_INVALID");
      } else if (tail_lsn_str != d.second) {
        catching_up = true;
      }
    }
    std::this_thread::sleep_for(std::chrono::seconds(1));
  } while (catching_up);

  ld_info("Injecting io_error in N3:S0 to verify that other nodes know that "
          "N3's durable version is now LSN_INVALID");
  ASSERT_EQ(
      true,
      cluster->getNode(3).injectShardFault("0", "all", "all", "io_error"));
  // Writing to the delta log so that we can have a new snapshot in memory,
  // which will trigger a new snapshot write
  ASSERT_NE(nullptr, client->makeDirectorySync("/file2", true));
  auto new_tail_lsn =
      client->getTailLSNSync(configuration::InternalLogs::CONFIG_LOG_DELTAS);
  auto new_tail_lsn_str = lsn_to_string(new_tail_lsn);
  std::vector<int> nodes_to_check_n3_on{0, 1, 2, 4};
  for (auto n : nodes_to_check_n3_on) {
    bool durable_ver_still_not_invalid = false;
    do {
      auto durable_versions = cluster->getNode(n).getRsmVersions(
          configuration::InternalLogs::CONFIG_LOG_DELTAS,
          RsmVersionType::DURABLE);
      for (auto d : durable_versions) {
        if (d.first != 3) {
          continue;
        }
        ld_info("FD view of N%d -> N%hu(%s), waiting for it to become "
                "LSN_INVALID",
                n,
                d.first,
                d.second.c_str());
        durable_ver_still_not_invalid = d.second != "LSN_INVALID";
      }
      std::this_thread::sleep_for(std::chrono::seconds(1));
    } while (durable_ver_still_not_invalid);
  }
}

// Verifies that a cluster's logsconfig is intact during whole cluster restart
TEST_F(RSMSnapshotStoreIntegrationTest,
       VerifyLogsConfigIsIntactOnClusterRestart) {
  int num_nodes = 5;
  // Configure nodes
  Configuration::Nodes nodes;
  for (int i = 0; i < num_nodes; ++i) {
    auto& node = nodes[i];
    node.generation = 1;
    node.addSequencerRole();
    node.addStorageRole(2);
  }
  auto factory = IntegrationTestUtils::ClusterFactory()
                     .setParam("--rsm-snapshot-store-type", "local-store")
                     .setParam("--logsconfig-snapshotting-period", "5s")
                     .setParam("--logsconfig-max-delta-records",
                               "1") // to trigger writeSnapshot() as soon as we
                                    // write a delta record
                     .enableLogsConfigManager()
                     .useHashBasedSequencerAssignment()
                     .setNodes(nodes);

  std::unique_ptr<Cluster> cluster;
  std::shared_ptr<Client> client;
  cluster = factory.create(num_nodes);
  cluster->waitUntilAllSequencersQuiescent();
  std::unique_ptr<ClientSettings> client_settings(ClientSettings::create());
  ASSERT_EQ(0, client_settings->set("enable-logsconfig-manager", true));
  client =
      cluster->createClient(DEFAULT_TEST_TIMEOUT, std::move(client_settings));
  ASSERT_NE(nullptr, client);

  auto catchup_func = [&](std::string catchup_lsn) {
    bool catching_up;
    do {
      catching_up = false;
      auto durable_versions = cluster->getNode(0).getRsmVersions(
          configuration::InternalLogs::CONFIG_LOG_DELTAS,
          RsmVersionType::DURABLE);
      for (auto d : durable_versions) {
        ld_info("Durable versions: N%hu at %s, waiting to catch up till %s",
                d.first,
                d.second.c_str(),
                catchup_lsn.c_str());
        if (catchup_lsn != d.second) {
          catching_up = true;
        }
      }
      std::this_thread::sleep_for(std::chrono::seconds(1));
    } while (catching_up);
  };

  // write first delta
  ld_info("Writing /file1");
  auto dir = client->makeDirectorySync("/file1", true);
  ASSERT_NE(nullptr, dir);

  // Verify that RSMs are at the same lsn everywhere
  auto tail_lsn =
      client->getTailLSNSync(configuration::InternalLogs::CONFIG_LOG_DELTAS);
  auto tail_lsn_str = lsn_to_string(tail_lsn);
  ld_info("Verifying that rsm is in sync on all nodes, tail_lsn:%s",
          tail_lsn_str.c_str());
  catchup_func(tail_lsn_str);

  // Writing another delta to ensure a new snapshot is created
  // and first delta record is trimmed
  ld_info("Writing /file2");
  dir = client->makeDirectorySync("/file2", true);
  ASSERT_NE(nullptr, dir);
  tail_lsn =
      client->getTailLSNSync(configuration::InternalLogs::CONFIG_LOG_DELTAS);
  tail_lsn_str = lsn_to_string(tail_lsn);
  ld_info("Verifying that rsm is in sync on all nodes, tail_lsn:%s",
          tail_lsn_str.c_str());
  catchup_func(tail_lsn_str);

  ld_info("Shutting down the cluster...");
  std::vector<node_index_t> nodelist{0, 1, 2, 3, 4};
  EXPECT_EQ(0, cluster->shutdownNodes(nodelist));

  ld_info("Starting cluster again...");
  EXPECT_EQ(0, cluster->start(nodelist));
  cluster->waitUntilAllStartedAndPropagatedInGossip();
  // Use a different client to avoid cached results
  std::unique_ptr<ClientSettings> client_settings2(ClientSettings::create());
  ASSERT_EQ(0, client_settings2->set("enable-logsconfig-manager", true));
  ASSERT_EQ(0, client_settings2->set("rsm-snapshot-store-type", "message"));
  auto client2 =
      cluster->createClient(DEFAULT_TEST_TIMEOUT, std::move(client_settings2));
  ASSERT_NE(nullptr, client2);
  ld_info("Verifying that both /file1 and file2 are present...");
  auto dir1 = client2->getDirectorySync("/file1");
  ASSERT_NE(nullptr, dir1);
  auto dir2 = client2->getDirectorySync("/file2");
  ASSERT_NE(nullptr, dir2);
}

TEST_F(RSMSnapshotStoreIntegrationTest, getTrimmableVersion) {
  int num_nodes = 10;
  // Configure nodes
  Configuration::Nodes nodes;
  for (int i = 0; i < num_nodes; ++i) {
    auto& node = nodes[i];
    node.generation = 1;
    node.addSequencerRole();
    node.addStorageRole(2);
  }
  // Add a sequencer only node (N10)
  auto& node = nodes[num_nodes];
  node.generation = 1;
  node.addSequencerRole();

  auto factory = IntegrationTestUtils::ClusterFactory()
                     .setParam("--rsm-snapshot-store-type", "local-store")
                     .setParam("--logsconfig-snapshotting-period", "5s")
                     .setParam("--logsconfig-max-delta-records",
                               "1") // to trigger writeSnapshot() as soon as we
                                    // write a delta record
                     .enableLogsConfigManager()
                     .useHashBasedSequencerAssignment()
                     .setNodes(nodes);

  std::unique_ptr<Cluster> cluster;
  std::shared_ptr<Client> client;
  cluster = factory.create(num_nodes + 1);
  cluster->waitUntilAllSequencersQuiescent();
  std::unique_ptr<ClientSettings> client_settings(ClientSettings::create());
  ASSERT_EQ(0, client_settings->set("enable-logsconfig-manager", true));
  client =
      cluster->createClient(DEFAULT_TEST_TIMEOUT, std::move(client_settings));
  ASSERT_NE(nullptr, client);

  /* write something to the delta log */
  auto dir = client->makeDirectorySync("/file1", true);
  ASSERT_NE(nullptr, dir);

  auto tail_lsn =
      client->getTailLSNSync(configuration::InternalLogs::CONFIG_LOG_DELTAS);
  auto tail_lsn_str = lsn_to_string(tail_lsn);

  // Verify that N10 returns E::NOTSUPPORTED
  auto res = cluster->getNode(num_nodes).getTrimmableVersion(
      configuration::InternalLogs::CONFIG_LOG_DELTAS);
  ASSERT_EQ(res.first, "NOTSUPPORTED");

  bool catching_up;
  do {
    catching_up = false;
    auto n0_res = cluster->getNode(0).getTrimmableVersion(
        configuration::InternalLogs::CONFIG_LOG_DELTAS);
    if (tail_lsn_str != n0_res.second) {
      catching_up = true;
      ld_info("tail_lsn:%s, st:%s, trimmable_version:%s",
              tail_lsn_str.c_str(),
              n0_res.first.c_str(),
              n0_res.second.c_str());
    }
    std::this_thread::sleep_for(std::chrono::seconds(1));
  } while (catching_up);
}

TEST_F(RSMSnapshotStoreIntegrationTest,
       LogImpl_GetSnapshotAfterWritingArbitraryPayload) {
  buildCluster(SnapshotStoreType::LOG);
  Semaphore sem;
  lsn_t lsn_written{LSN_INVALID};
  std::string snapshot_blob_written{"abc"}, snapshot_blob_read;
  logid_t snapshot_log{20}, delta_log{21};
  initClientSnapshotStore(SnapshotStoreType::LOG, delta_log, snapshot_log);

  auto append_cb = [&](Status st, lsn_t lsn) {
    if (st == E::OK) {
      ld_info("write cb. st:%s, Snapshot was assigned LSN %s",
              error_name(st),
              lsn_to_string(lsn).c_str());
      lsn_written = lsn;
      sem.post();
    } else {
      ld_info("Write failed st:%s", error_name(st));
    }
  };
  client_snapshot_store_->writeSnapshot(
      LSN_OLDEST + 1, snapshot_blob_written, append_cb);
  sem.wait();

  auto snapshot_cb = [&](Status st,
                         std::string snapshot_blob_out,
                         RSMSnapshotStore::SnapshotAttributes /* unused */) {
    if (st == E::OK) {
      snapshot_blob_read = std::move(snapshot_blob_out);
    }
    sem.post();
  };

  client_snapshot_store_->getSnapshot(LSN_OLDEST, std::move(snapshot_cb));
  sem.wait();
  ASSERT_EQ(snapshot_blob_written, snapshot_blob_read);
}

TEST_P(RSMSnapshotStoreIntegrationTest,
       WriteFollowedByEventuallyReadingSnapshot) {
  auto test_param = GetParam();
  buildCluster(test_param.cluster);
  initClientSnapshotStore(test_param.client,
                          configuration::InternalLogs::CONFIG_LOG_DELTAS,
                          configuration::InternalLogs::CONFIG_LOG_SNAPSHOTS);
  Semaphore sem;
  std::string snapshot_blob_read;

  // Write to snapshot store
  ld_info("Writing to snapshot store");
  triggerSnapshotStoreWrite();
  auto tail_lsn =
      client->getTailLSNSync(configuration::InternalLogs::CONFIG_LOG_DELTAS);
  ASSERT_GT(tail_lsn, LSN_INVALID);

  Status st_read{E::FAILED};
  do {
    auto snapshot_cb =
        [&](Status st,
            std::string snapshot_blob_out,
            RSMSnapshotStore::SnapshotAttributes snapshot_attrs) {
          ld_info("getSnapshot(tail_lsn:%s) cb -> st:%s, snapshot base_ver:%s, "
                  "snapshot_blob_out:%s, size:%zu",
                  lsn_to_string(tail_lsn).c_str(),
                  error_name(st),
                  lsn_to_string(snapshot_attrs.base_version).c_str(),
                  hexdump_buf(snapshot_blob_out.data(),
                              std::min(30ul, snapshot_blob_out.size()))
                      .c_str(),
                  snapshot_blob_out.size());
          st_read = st;
          if (st == E::OK) {
            snapshot_blob_read = std::move(snapshot_blob_out);
          }
          sem.post();
        };

    ld_info("Reading from snapshot store with min_ver:%s",
            lsn_to_string(tail_lsn).c_str());
    client_snapshot_store_->getSnapshot(tail_lsn, std::move(snapshot_cb));
    sem.wait();
    if (st_read != E::OK) {
      ld_info(
          "Reading from snapshot store was unsuccessful(err:%s). will retry...",
          error_name(st_read));
      std::this_thread::sleep_for(std::chrono::seconds(1));
    }
  } while (st_read != E::OK);

  // Deserialize snapshot blob
  RSMSnapshotHeader snapshot_header_read;
  std::unique_ptr<logsconfig::LogsConfigTree> data_read;
  int rv = deserializeStringSnapshot(
      snapshot_blob_read, snapshot_header_read, data_read);
  EXPECT_EQ(rv, 0);

  auto lookup_dir = data_read->find("/dir-test1");
  ASSERT_NE(lookup_dir, nullptr);
  auto lookup_dir2 = data_read->find("/dir-test2");
  ASSERT_EQ(lookup_dir2, nullptr);
}

class VerifyRSMsSync : public ::testing::TestWithParam<SnapshotStoreType> {};
INSTANTIATE_TEST_CASE_P(VerifyRSMsSync,
                        VerifyRSMsSync,
                        ::testing::Values(SnapshotStoreType::LOG,
                                          SnapshotStoreType::LOCAL_STORE));
TEST_P(VerifyRSMsSync, Basic) {
  static const size_t NUM_LOGS = 20;
  logsconfig::LogAttributes log_attrs;
  log_attrs.with_replicationFactor(2);
  std::string cluster_snapshot_type = typeToString(GetParam());
  auto cluster =
      IntegrationTestUtils::ClusterFactory()
          .enableLogsConfigManager()
          .setParam("--rsm-snapshot-store-type", cluster_snapshot_type)
          .setParam("--disable-rebuilding", "false")
          .useHashBasedSequencerAssignment()
          .create(5);

  std::vector<node_index_t> nodes{0, 1, 2, 3, 4};
  cluster->start(nodes);
  cluster->waitUntilAllAvailable();
  cluster->waitForRecovery();
  auto client = cluster->createClient();
  for (int i = 1; i < NUM_LOGS; ++i) {
    auto logrange_str = "/logrange" + folly::to<std::string>(i);
    auto lg1 = client->makeLogGroupSync(
        logrange_str,
        logid_range_t(logid_t(i), logid_t(i)),
        client::LogAttributes().with_replicationFactor(2));
  }
  auto tail_lsn =
      client->getTailLSNSync(configuration::InternalLogs::CONFIG_LOG_DELTAS);

  cluster->waitUntilRSMSynced("logsconfig_rsm", tail_lsn, nodes);
}

TEST_F(RSMSnapshotStoreIntegrationTest, MessageBasedStore_Reject_Invalid_Rsm) {
  buildCluster(SnapshotStoreType::LOG);
  logid_t snapshot_log{20}, delta_log{21};
  initClientSnapshotStore(SnapshotStoreType::MESSAGE, delta_log, snapshot_log);
  Semaphore sem;
  std::string snapshot_blob_read;
  Status st_read{E::OK};

  auto snapshot_cb = [&](Status st,
                         std::string snapshot_blob_out,
                         RSMSnapshotStore::SnapshotAttributes /* unused */) {
    st_read = st;
    if (st == E::OK) {
      snapshot_blob_read = std::move(snapshot_blob_out);
    }
    sem.post();
  };

  client_snapshot_store_->getSnapshot(LSN_OLDEST, std::move(snapshot_cb));
  sem.wait();
  ASSERT_EQ(st_read, E::NOTSUPPORTED);
}
