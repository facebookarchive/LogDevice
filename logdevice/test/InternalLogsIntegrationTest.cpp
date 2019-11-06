/**
 * Copyright (c) 2017-present, Facebook, Inc.
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
#include "logdevice/common/replicated_state_machine/TrimRSMRequest.h"
#include "logdevice/common/types_internal.h"
#include "logdevice/include/Client.h"
#include "logdevice/lib/ClientImpl.h"
#include "logdevice/test/utils/IntegrationTestBase.h"
#include "logdevice/test/utils/IntegrationTestUtils.h"

using namespace facebook::logdevice;
using namespace IntegrationTestUtils;
using namespace testing;

class InternalLogsIntegrationTest
    : public IntegrationTestBase,
      public ::testing::WithParamInterface<bool /*rsm_trim_up_to_read_ptr*/> {
 public:
  static const size_t NNODES = 3;

  ClusterFactory
  clusterFactory(bool rsm_include_read_pointer_in_snapshot = false) {
    auto factory = IntegrationTestUtils::ClusterFactory()
                       .enableLogsConfigManager()
                       .allowExistingMetaData()
                       .setParam("--loglevel-overrides",
                                 "ReplicatedStateMachine-inl.h:debug")
                       .doPreProvisionEpochMetaData();

    if (rsm_include_read_pointer_in_snapshot) {
      factory.setParam("--rsm-include-read-pointer-in-snapshot", "true");
    } else {
      factory.setParam("--rsm-include-read-pointer-in-snapshot", "false");
    }

    return factory;
  }

  void buildClusterAndClient(ClusterFactory factory) {
    cluster = factory.create(NNODES);

    std::unique_ptr<ClientSettings> client_settings(ClientSettings::create());
    ASSERT_EQ(0, client_settings->set("enable-logsconfig-manager", true));
    client = cluster->createIndependentClient(
        DEFAULT_TEST_TIMEOUT, std::move(client_settings));

    // cast the Client object back to ClientImpl and enable internal log writes
    client_impl = std::dynamic_pointer_cast<ClientImpl>(client);
    ASSERT_NE(nullptr, client);

    cluster->waitForRecovery();
  }

  lsn_t getTrimPointFor(logid_t log) {
    auto head_attr = client->getHeadAttributesSync(
        configuration::InternalLogs::CONFIG_LOG_DELTAS);
    ld_check(head_attr);
    return head_attr->trim_point;
  }

  Status trimLogsconfig(bool trim_everything) {
    Semaphore sem;
    Status res;

    auto cb = [&](Status st) {
      res = st;
      sem.post();
    };

    auto cur_timestamp = std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::high_resolution_clock::now().time_since_epoch());
    logid_t delta_log_id = configuration::InternalLogs::CONFIG_LOG_DELTAS;
    logid_t snapshot_log_id = configuration::InternalLogs::CONFIG_LOG_SNAPSHOTS;

    std::unique_ptr<Request> rq =
        std::make_unique<TrimRSMRequest>(delta_log_id,
                                         snapshot_log_id,
                                         std::chrono::milliseconds::max(),
                                         cb,
                                         worker_id_t{0},
                                         WorkerType::GENERAL,
                                         RSMType::LOGS_CONFIG_STATE_MACHINE,
                                         trim_everything,
                                         client_impl->getTimeout(),
                                         client_impl->getTimeout());

    client_impl->getProcessor().postWithRetrying(rq);

    sem.wait();
    return res;
  }

  std::unique_ptr<Cluster> cluster;
  std::shared_ptr<Client> client;
  std::shared_ptr<ClientImpl> client_impl;
};

INSTANTIATE_TEST_CASE_P(InternalLogsIntegrationTest,
                        InternalLogsIntegrationTest,
                        ::testing::Bool());

// test that Client cannot directly write to internal logs
TEST_F(InternalLogsIntegrationTest, ClientCannotWriteInternalLog) {
  buildClusterAndClient(clusterFactory());

  // Create arbitrary payload. This is safe since we only append to deltas logs
  size_t dataSize = 512;
  std::string data(dataSize, 'x');

  lsn_t lsn = client->appendSync(configuration::InternalLogs::CONFIG_LOG_DELTAS,
                                 Payload((void*)data.c_str(), dataSize));

  ASSERT_EQ(LSN_INVALID, lsn);
  ASSERT_EQ(E::INVALID_PARAM, err);
}

// test that Client can write to internal logs with the right flags
TEST_F(InternalLogsIntegrationTest, ClientWriteInternalLog) {
  buildClusterAndClient(clusterFactory());

  client_impl->allowWriteInternalLog();

  // Create garbage payload. This is safe since we only append to deltas logs
  size_t dataSize = 512;
  std::string data(dataSize, 'x');

  lsn_t lsn =
      client_impl->appendSync(configuration::InternalLogs::CONFIG_LOG_DELTAS,
                              Payload((void*)data.c_str(), dataSize));

  ASSERT_NE(LSN_INVALID, lsn);
  ASSERT_NE(E::INVALID_PARAM, err);

  // verify we can read it back
  auto reader = client->createReader(1);

  reader->setTimeout(std::chrono::seconds(5));
  auto rv =
      reader->startReading(configuration::InternalLogs::CONFIG_LOG_DELTAS, lsn);
  ASSERT_EQ(0, rv);

  std::vector<std::unique_ptr<DataRecord>> records;
  GapRecord gap;
  auto count = reader->read(1, &records, &gap);
  ASSERT_GT(count, 0);
  ASSERT_EQ(records[0]->payload.toString(), data);
}

/**
 * If the `rsm-include-read-pointer-in-snapshot` setting is enabled, our
 * snapshots should allow us to trim up to the delta log read pointer.
 */
TEST_P(InternalLogsIntegrationTest, TrimmingUpToDeltaLogReadPointer) {
  const bool rsm_include_read_pointer_in_snapshot = GetParam();
  buildClusterAndClient(clusterFactory(rsm_include_read_pointer_in_snapshot));

  /* write something to the delta log */
  auto dir = client->makeDirectorySync("/facebok_sneks", true);
  lsn_t last_delta = dir->version();

  /* check we never trimmed */
  ASSERT_EQ(getTrimPointFor(configuration::InternalLogs::CONFIG_LOG_DELTAS),
            LSN_INVALID);

  /* bump epoch a number of times */
  const size_t NUM_EPOCHS_TO_BUMP = 5;
  auto& seq = cluster->getSequencerNode();
  for (size_t t = 0; t < NUM_EPOCHS_TO_BUMP; ++t) {
    seq.upDown(configuration::InternalLogs::CONFIG_LOG_DELTAS);
    cluster->waitForRecovery();
  }

  /* double check that we bumped those epochs */
  auto tail_lsn =
      client->getTailLSNSync(configuration::InternalLogs::CONFIG_LOG_DELTAS);
  ASSERT_EQ(epoch_t(lsn_to_epoch(last_delta).val_ + NUM_EPOCHS_TO_BUMP),
            lsn_to_epoch(tail_lsn));

  /* sync logsconfig everywhere */
  std::vector<node_index_t> all_nodes(NNODES);
  std::iota(all_nodes.begin(), all_nodes.end(), 0);
  cluster->waitUntilLogsConfigSynced(tail_lsn);

  /* take snapshot */
  auto result =
      cluster->getNode(0).sendCommand("rsm write-snapshot logsconfig");
  EXPECT_THAT(result, HasSubstr("Successfully created logsconfig snapshot"));

  ASSERT_EQ(trimLogsconfig(/*trim_everything=*/false), E::OK);

  if (rsm_include_read_pointer_in_snapshot) {
    // then we should have trimmed up to delta log read ptr
    ASSERT_EQ(getTrimPointFor(configuration::InternalLogs::CONFIG_LOG_DELTAS),
              tail_lsn - 1);
  } else {
    // then we should have trimmed up to last applied delta
    ASSERT_EQ(getTrimPointFor(configuration::InternalLogs::CONFIG_LOG_DELTAS),
              last_delta);
  }

  /* now check logsconfig read availability */
  seq.kill();
  seq.start();
  seq.waitUntilStarted();
  seq.waitUntilLogsConfigSynced(tail_lsn);
}

/**
 * The following test checks that if we add new nodes to a cluster and
 * immediately write LogsConfig snapshot to those nodes, we don't get stuck in
 * STARTING state and thus are unable to finish recoveries.
 */
TEST_F(InternalLogsIntegrationTest,
       ShouldBeAbleToFinishRecoveriesAfterExpands) {
  const std::set<node_index_t> FIRST_NODES = {0, 1, 2};
  const std::set<node_index_t> STUCK_NODES = {3, 4, 5};

  const auto NUM_FIRST_NODES = 3;
  const auto NUM_STUCK_NODES = 3;
  const auto NNODES = NUM_FIRST_NODES + NUM_STUCK_NODES;

  Configuration::MetaDataLogsConfig meta_config = createMetaDataLogsConfig(
      /*nodeset=*/{0, 1, 2}, /*replication=*/1, NodeLocationScope::NODE);
  meta_config.sequencers_write_metadata_logs = false;
  meta_config.sequencers_provision_epoch_store = false;
  meta_config.nodeset_selector_type = NodeSetSelectorType::PICK_CURRENT_NODESET;
  auto cluster = IntegrationTestUtils::ClusterFactory()
                     .useHashBasedSequencerAssignment()
                     .setMetaDataLogsConfig(meta_config)
                     .allowExistingMetaData()
                     .setInternalLogsReplicationFactor(1)
                     .enableLogsConfigManager()
                     .setParam("--loglevel-overrides",
                               "ReplicatedStateMachine-inl.h:debug")
                     .setNumDBShards(1)
                     .deferStart()
                     .create(NNODES);

  ASSERT_EQ(0, cluster->provisionEpochMetadataWithShardIDs({0, 1, 2}));

  for (auto nid : FIRST_NODES) {
    cluster->getNode(nid).start();
  }
  cluster->waitUntilStartupComplete(
      std::set<uint64_t>(FIRST_NODES.begin(), FIRST_NODES.end()));

  client = cluster->createIndependentClient(DEFAULT_TEST_TIMEOUT);

  /* Write something to logs config */
  auto dir = client->makeDirectorySync("/ipsum_dolor_sit_amet", true);

  /* we will now provision epoch metadata to be */
  cluster->stop();

  ASSERT_EQ(0, cluster->provisionEpochMetadataWithShardIDs(STUCK_NODES));

  for (auto nid : FIRST_NODES) {
    cluster->getNode(nid).start();
  }
  for (auto nid : STUCK_NODES) {
    auto& node = cluster->getNode(nid);
    node.setParam("--test-hold-logsconfig-in-starting-state", "true");
    node.start();
  }
  cluster->waitUntilAllAvailable();

  cluster->waitForMetaDataLogWrites();
  cluster->waitForRecovery();

  // wait for node 0 to have logsconfig available
  cluster->waitUntilStartupComplete(std::set<uint64_t>{0});

  /* Take a Logsconfig snapshot that should go to one of the nodes stuck in
   * starting state. */
  auto result =
      cluster->getNode(0).sendCommand("rsm write-snapshot logsconfig");
  ASSERT_THAT(result, HasSubstr("Successfully created logsconfig snapshot"));

  /* Let's ensure that should be stuck is stuck */
  auto is_starting = cluster->getNode(0).gossipStarting();
  for (auto nid : STUCK_NODES) {
    auto nids = "N" + std::to_string(nid);
    ld_info("%s %lu", nids.c_str(), is_starting.count(nids));
    ASSERT_TRUE(is_starting.count(nids) && is_starting[nids]);
  }

  /* Now we set Logsconfig unstuck and we expect nodes to eventually move out of
   * starting state. */
  for (auto nid : STUCK_NODES) {
    cluster->getNode(nid).updateSetting(
        "test-hold-logsconfig-in-starting-state", "false");
  }

  /* now we should move out of the STARTING state and finish recoveries */
  cluster->waitUntilStartupComplete();
  cluster->waitForRecovery();
}

/**
 * This test makes sure that if the replicated state machine stalls, and new
 * snapshots are created that do not bump the base version but include a read
 * pointer past the stalling point, the state machine resume reading and
 * recover.
 */
TEST_F(InternalLogsIntegrationTest, RecoverAfterStallingDueToTrimmingDeltaLog) {
  auto factory = clusterFactory(true);

  // Configure nodes
  Configuration::Nodes nodes;
  for (int i = 0; i < NNODES; ++i) {
    auto& node = nodes[i];
    node.generation = 1;
    node.addSequencerRole();
    node.addStorageRole(2);
  }
  // Add one more node that is sequencer-only (so that we can safely isolate it
  // without affecting internal logs reads and writes)
  auto& node = nodes[NNODES];
  node.generation = 1;
  node.addSequencerRole();

  factory.setNodes(nodes).oneConfigPerNode();

  buildClusterAndClient(std::move(factory));

  // Write something to the delta log
  auto dir = client->makeDirectorySync("/dir1", true);
  lsn_t last_delta = dir->version();

  // Check we never trimmed
  ASSERT_EQ(getTrimPointFor(configuration::InternalLogs::CONFIG_LOG_DELTAS),
            LSN_INVALID);

  // Bump epoch a number of times
  const size_t NUM_EPOCHS_TO_BUMP = 5;
  auto& seq = cluster->getSequencerNode();
  for (size_t t = 0; t < NUM_EPOCHS_TO_BUMP; ++t) {
    seq.upDown(configuration::InternalLogs::CONFIG_LOG_DELTAS);
    cluster->waitForRecovery();
  }

  // Check that the tail moved as expected
  auto tail_lsn =
      client->getTailLSNSync(configuration::InternalLogs::CONFIG_LOG_DELTAS);
  ASSERT_EQ(epoch_t(lsn_to_epoch(last_delta).val_ + NUM_EPOCHS_TO_BUMP),
            lsn_to_epoch(tail_lsn));

  // Make sure logsconfig is in sync everywhere
  cluster->waitUntilLogsConfigSynced(tail_lsn);

  // Take snapshot
  auto result =
      cluster->getNode(0).sendCommand("rsm write-snapshot logsconfig");
  EXPECT_THAT(result, HasSubstr("Successfully created logsconfig snapshot"));

  // Trim the delta log up to N0 read pointer
  ASSERT_EQ(trimLogsconfig(/*trim_everything=*/false), E::OK);

  // Check we have trimmed up to delta log read ptr
  ASSERT_EQ(getTrimPointFor(configuration::InternalLogs::CONFIG_LOG_DELTAS),
            tail_lsn - 1);

  // Isolate N<last> so that delta log read stream becomes unhealthy and the
  // replicated state machine stalls
  std::set<int> partition1;
  for (int i = 0; i < NNODES; ++i) {
    partition1.insert(i);
  }
  cluster->partition({partition1, {NNODES}});

  // Write another record
  dir = client->makeDirectorySync("/dir2", true);
  last_delta = dir->version();

  // Bump epoch a few more times to move the tail and cause bridge gaps
  for (size_t t = 0; t < NUM_EPOCHS_TO_BUMP; ++t) {
    seq.upDown(configuration::InternalLogs::CONFIG_LOG_DELTAS);
    cluster->waitForRecovery();
  }

  // Check that the tail moved as expected
  auto new_tail_lsn =
      client->getTailLSNSync(configuration::InternalLogs::CONFIG_LOG_DELTAS);
  ASSERT_EQ(epoch_t(lsn_to_epoch(last_delta).val_ + NUM_EPOCHS_TO_BUMP),
            lsn_to_epoch(new_tail_lsn));
  cluster->getNode(0).waitUntilLogsConfigSynced(new_tail_lsn);

  // Take another snapshot and trim the delta log
  result = cluster->getNode(0).sendCommand("rsm write-snapshot logsconfig");
  EXPECT_THAT(result, HasSubstr("Successfully created logsconfig snapshot"));
  ASSERT_EQ(trimLogsconfig(/*trim_everything=*/false), E::OK);
  ASSERT_EQ(getTrimPointFor(configuration::InternalLogs::CONFIG_LOG_DELTAS),
            new_tail_lsn - 1);

  // While N<last< is still isolated, check that the logsconfig delta log read
  // stream is not healthy
  auto info = cluster->getNode(NNODES).sendJsonCommand(
      "info client_read_streams --json");
  bool checked = false;
  for (auto& rec : info) {
    if (rec["Log ID"] ==
        folly::to<std::string>(
            configuration::InternalLogs::CONFIG_LOG_DELTAS.val())) {
      EXPECT_EQ(rec["Connection health"], "UNHEALTHY");
      checked = true;
    }
  }
  EXPECT_TRUE(checked);

  // Rejoin N<last> to the cluster
  partition1.insert(NNODES);
  cluster->partition({partition1});

  // Check that N<last> is able to sycnhronize the logs config up to the tail.
  cluster->getNode(NNODES).waitUntilLogsConfigSynced(new_tail_lsn);

  // Double check that read stream is now healthy
  info = cluster->getNode(NNODES).sendJsonCommand(
      "info client_read_streams --json");
  checked = false;
  for (auto& rec : info) {
    if (rec["Log ID"] ==
        folly::to<std::string>(
            configuration::InternalLogs::CONFIG_LOG_DELTAS.val())) {
      EXPECT_EQ(rec["Connection health"], "HEALTHY_AUTHORITATIVE_COMPLETE");
      checked = true;
    }
  }
  EXPECT_TRUE(checked);

  // Check logsconfig read availability on sequencer by restarting it
  seq.kill();
  seq.start();
  seq.waitUntilStarted();
  seq.waitUntilLogsConfigSynced(new_tail_lsn);
}

/**
 * This test makes sure that if the replicated state machine stalls,
 * the corresponding stat is bumped.
 */
TEST_F(InternalLogsIntegrationTest, StallingBumpsStat) {
  auto factory = clusterFactory(true);

  // Configure nodes
  Configuration::Nodes nodes;
  for (int i = 0; i < NNODES; ++i) {
    auto& node = nodes[i];
    node.generation = 1;
    node.addSequencerRole();
    node.addStorageRole(2);
  }
  // Add one more node that is sequencer-only (so that we can safely isolate it
  // without affecting internal logs reads and writes)
  auto& node = nodes[NNODES];
  node.generation = 1;
  node.addSequencerRole();

  factory.setNodes(nodes).oneConfigPerNode();

  buildClusterAndClient(std::move(factory));

  // Write something to the delta log
  auto dir = client->makeDirectorySync("/dir1", true);
  lsn_t last_delta = dir->version();

  // Check we never trimmed
  ASSERT_EQ(getTrimPointFor(configuration::InternalLogs::CONFIG_LOG_DELTAS),
            LSN_INVALID);

  // Bump epoch a number of times
  const size_t NUM_EPOCHS_TO_BUMP = 5;
  auto& seq = cluster->getSequencerNode();
  for (size_t t = 0; t < NUM_EPOCHS_TO_BUMP; ++t) {
    seq.upDown(configuration::InternalLogs::CONFIG_LOG_DELTAS);
    cluster->waitForRecovery();
  }

  // Check that the tail moved as expected
  auto tail_lsn =
      client->getTailLSNSync(configuration::InternalLogs::CONFIG_LOG_DELTAS);
  ASSERT_EQ(epoch_t(lsn_to_epoch(last_delta).val_ + NUM_EPOCHS_TO_BUMP),
            lsn_to_epoch(tail_lsn));

  // Make sure logsconfig is in sync everywhere
  cluster->waitUntilLogsConfigSynced(tail_lsn);

  // Take snapshot
  auto result =
      cluster->getNode(0).sendCommand("rsm write-snapshot logsconfig");
  EXPECT_THAT(result, HasSubstr("Successfully created logsconfig snapshot"));

  // Trim the delta log up to N0 read pointer
  ASSERT_EQ(trimLogsconfig(/*trim_everything=*/false), E::OK);

  // Check we have trimmed up to delta log read ptr
  ASSERT_EQ(getTrimPointFor(configuration::InternalLogs::CONFIG_LOG_DELTAS),
            tail_lsn - 1);

  // Isolate N<last> so that delta log read stream becomes unhealthy and the
  // replicated state machine stalls
  std::set<int> partition1;
  for (int i = 0; i < NNODES; ++i) {
    partition1.insert(i);
  }
  cluster->partition({partition1, {NNODES}});

  // Write another record
  dir = client->makeDirectorySync("/dir2", true);
  last_delta = dir->version();

  // Bump epoch a few more times to move the tail and cause bridge gaps
  for (size_t t = 0; t < NUM_EPOCHS_TO_BUMP; ++t) {
    seq.upDown(configuration::InternalLogs::CONFIG_LOG_DELTAS);
    cluster->waitForRecovery();
  }

  // Check that the tail moved as expected
  auto new_tail_lsn =
      client->getTailLSNSync(configuration::InternalLogs::CONFIG_LOG_DELTAS);
  ASSERT_EQ(epoch_t(lsn_to_epoch(last_delta).val_ + NUM_EPOCHS_TO_BUMP),
            lsn_to_epoch(new_tail_lsn));
  cluster->getNode(0).waitUntilLogsConfigSynced(new_tail_lsn);

  // Write an intermediate snapshot
  result = cluster->getNode(0).sendCommand("rsm write-snapshot logsconfig");
  EXPECT_THAT(result, HasSubstr("Successfully created logsconfig snapshot"));

  // Bump epoch a few more times to move the tail and cause bridge gaps
  for (size_t t = 0; t < NUM_EPOCHS_TO_BUMP; ++t) {
    seq.upDown(configuration::InternalLogs::CONFIG_LOG_DELTAS);
    cluster->waitForRecovery();
  }

  // Write another record
  dir = client->makeDirectorySync("/dir3", true);
  last_delta = dir->version();

  // Check that the tail is at the last record
  new_tail_lsn =
      client->getTailLSNSync(configuration::InternalLogs::CONFIG_LOG_DELTAS);
  ASSERT_EQ(epoch_t(lsn_to_epoch(last_delta).val_), lsn_to_epoch(new_tail_lsn));
  cluster->getNode(0).waitUntilLogsConfigSynced(new_tail_lsn);

  // trim the delta log "manually" (bypassing safety checks provided
  // by TrimRSMRequest) without taking a new snapshot
  ASSERT_EQ(client->trimSync(
                configuration::InternalLogs::CONFIG_LOG_DELTAS, new_tail_lsn),
            0);
  ASSERT_EQ(getTrimPointFor(configuration::InternalLogs::CONFIG_LOG_DELTAS),
            new_tail_lsn);

  // Rejoin N<last> to the cluster
  partition1.insert(NNODES);
  cluster->partition({partition1});

  // Check that N<last> is stalled waiting for a newer snapshot
  wait_until("N<last> logsconfig replicated state machine stalls", [&]() {
    auto stats = cluster->getNode(NNODES).stats();
    return stats["num_replicated_state_machines_stalled"] == 1;
  });

  // Write a snapshot
  result = cluster->getNode(0).sendCommand("rsm write-snapshot logsconfig");
  EXPECT_THAT(result, HasSubstr("Successfully created logsconfig snapshot"));

  // Check that N<last> is able to sycnhronize the logs config up to the tail.
  cluster->getNode(NNODES).waitUntilLogsConfigSynced(new_tail_lsn);

  // Check that N<last> is no longer waiting for newer snapshot
  auto stats = cluster->getNode(NNODES).stats();
  EXPECT_EQ(stats["num_replicated_state_machines_stalled"], 0);

  // Check logsconfig read availability on sequencer by restarting it
  seq.kill();
  seq.start();
  seq.waitUntilStarted();
  seq.waitUntilLogsConfigSynced(new_tail_lsn);
}
