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
      public ::testing::WithParamInterface<
          std::tuple<bool /*rsm_trim_up_to_read_ptr*/, SnapshotStoreType>> {
 public:
  static const size_t NNODES = 3;

  ClusterFactory clusterFactory(
      bool rsm_include_read_pointer_in_snapshot = false,
      std::string logsconfig_snapshotting_period =
          "2s", // default in settings is "1h"
      SnapshotStoreType snapshot_store_type = SnapshotStoreType::LEGACY) {
    auto factory = IntegrationTestUtils::ClusterFactory()
                       .useHashBasedSequencerAssignment()
                       .enableLogsConfigManager()
                       .allowExistingMetaData()
                       .setParam("--loglevel-overrides",
                                 "ReplicatedStateMachine-inl.h:debug")
                       .doPreProvisionEpochMetaData();

    factory.setParam("--rsm-include-read-pointer-in-snapshot",
                     rsm_include_read_pointer_in_snapshot ? "true" : "false");
    // take local snapshots more often for tests
    if (logsconfig_snapshotting_period != "default") {
      factory.setParam(
          "--logsconfig-snapshotting-period", logsconfig_snapshotting_period);
    }

    if (snapshot_store_type == SnapshotStoreType::LEGACY) {
      factory.setParam("--rsm-snapshot-store-type", "legacy");
    } else if (snapshot_store_type == SnapshotStoreType::LOG) {
      factory.setParam("--rsm-snapshot-store-type", "log");
    } else if (snapshot_store_type == SnapshotStoreType::LOCAL_STORE) {
      factory.setParam("--rsm-snapshot-store-type", "local-store");
    }

    return factory;
  }

  void buildClusterAndClient(ClusterFactory factory,
                             std::vector<std::pair<std::string, std::string>>
                                 additional_client_settings = {}) {
    cluster = factory.create(NNODES);
    cluster->waitUntilAllSequencersQuiescent();

    std::unique_ptr<ClientSettings> client_settings(ClientSettings::create());
    ASSERT_EQ(0, client_settings->set("enable-logsconfig-manager", true));
    for (auto& p : additional_client_settings) {
      client_settings->set(p.first, p.second);
    }
    client =
        cluster->createClient(DEFAULT_TEST_TIMEOUT, std::move(client_settings));

    // cast the Client object back to ClientImpl and enable internal log writes
    client_impl = std::dynamic_pointer_cast<ClientImpl>(client);
    ASSERT_NE(nullptr, client);
  }

  void take_snapshot() {
    std::string result;
    bool inprogress = true;
    do {
      result = cluster->getNode(0).sendCommand("rsm write-snapshot logsconfig");
      inprogress =
          result.find("Could not create logsconfig snapshot:INPROGRESS") !=
          std::string::npos;
    } while (inprogress);
    if (result.find("Logsconfig snapshot is already uptodate.") ==
            std::string::npos &&
        result.find("Successfully created logsconfig snapshot") ==
            std::string::npos) {
      ld_info("Received unexpected result:(%s)", result.c_str());
      ASSERT_EQ(0, 1);
    }
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
                                         false, /* trim snapshot only */
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

INSTANTIATE_TEST_CASE_P(
    Parametric,
    InternalLogsIntegrationTest,
    ::testing::Combine(
        ::testing::Bool() /* rsm_include_read_pointer_in_snapshot */,
        ::testing::Values(SnapshotStoreType::LEGACY,
                          SnapshotStoreType::LOG,
                          SnapshotStoreType::LOCAL_STORE)));

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
 *
 * This is true for Legacy implementation, but for snapshot store
 * implementation, we only trim upto a durable version, which won't necessarily
 * be same as delta log read pointer.
 */
TEST_P(InternalLogsIntegrationTest, TrimmingUpToDeltaLogReadPointer) {
  const bool rsm_include_read_pointer_in_snapshot = std::get<0>(GetParam());
  const SnapshotStoreType store_type = std::get<1>(GetParam());
  buildClusterAndClient(
      clusterFactory(rsm_include_read_pointer_in_snapshot, "2s", store_type));

  /* write something to the delta log */
  auto dir = client->makeDirectorySync("/facebok_sneks", true);
  lsn_t last_delta = dir->version();

  /* check we never trimmed */
  ASSERT_EQ(getTrimPointFor(configuration::InternalLogs::CONFIG_LOG_DELTAS),
            LSN_INVALID);

  /* bump epoch a number of times */
  const size_t NUM_EPOCHS_TO_BUMP = 5;
  auto sequencer_node_id = cluster->getHashAssignedSequencerNodeId(
      configuration::InternalLogs::CONFIG_LOG_DELTAS, client.get());
  auto& seq = cluster->getNode(sequencer_node_id);
  for (size_t t = 0; t < NUM_EPOCHS_TO_BUMP; ++t) {
    seq.upDown(configuration::InternalLogs::CONFIG_LOG_DELTAS);
    cluster->waitUntilAllSequencersQuiescent();
  }

  /* double check that we bumped those epochs */
  auto tail_lsn =
      client->getTailLSNSync(configuration::InternalLogs::CONFIG_LOG_DELTAS);
  ASSERT_GE(epoch_t(lsn_to_epoch(last_delta).val_ + NUM_EPOCHS_TO_BUMP),
            lsn_to_epoch(tail_lsn));

  /* sync logsconfig everywhere */
  std::vector<node_index_t> all_nodes(NNODES);
  std::iota(all_nodes.begin(), all_nodes.end(), 0);
  cluster->waitUntilLogsConfigSynced(tail_lsn);

  /* take snapshot */
  take_snapshot();

  switch (store_type) {
    case SnapshotStoreType::LEGACY:
      ASSERT_EQ(trimLogsconfig(/*trim_everything=*/false), E::OK);
      break;
    case SnapshotStoreType::LOG:
    case SnapshotStoreType::LOCAL_STORE: {
      bool trimming_can_cause_dataloss = true;
      std::string trim_result;
      while (trimming_can_cause_dataloss) {
        trim_result = cluster->getNode(0).sendCommand("rsm trim logsconfig");
        trimming_can_cause_dataloss =
            trim_result.find("Could not create logsconfig snapshot:DATALOSS") !=
            std::string::npos;
        std::this_thread::sleep_for(std::chrono::seconds(1));
      }
      EXPECT_THAT(trim_result, HasSubstr("Successfully trimmed logsconfig"));
    } break;
    default:
      ld_error("Unsupported store type:%d, call appropriate trimming code when "
               "a new store is added",
               static_cast<int>(store_type));
      ASSERT_EQ(0, 1);
  };

  lsn_t trim_point =
      getTrimPointFor(configuration::InternalLogs::CONFIG_LOG_DELTAS);
  if (rsm_include_read_pointer_in_snapshot) {
    ASSERT_NE(trim_point, LSN_INVALID);
    if (store_type == SnapshotStoreType::LEGACY) {
      // for legacy rsm code we should have trimmed up to delta log read ptr
      ASSERT_EQ(trim_point, tail_lsn - 1);
    } else {
      // Delta trimming is going to be conservative with snapshot store
      // interface, as it only looks at durable version and not the delta log
      // read ptr unlike legacy trim
      ASSERT_LE(trim_point, tail_lsn - 1);
    }
  } else {
    // then we should have trimmed up to last applied delta
    ASSERT_EQ(trim_point, last_delta);
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
  cluster = IntegrationTestUtils::ClusterFactory()
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
  cluster->waitUntilAllStartedAndPropagatedInGossip();

  client = cluster->createClient(DEFAULT_TEST_TIMEOUT);

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
    node.setParam("--block-logsconfig-rsm", "true");
    node.start();
  }
  cluster->waitUntilAllAvailable();
  cluster->waitUntilAllSequencersQuiescent();

  // wait for node 0 to have logsconfig available
  cluster->waitUntilNoOneIsInStartupState(std::set<uint64_t>{0});

  /* Take a Logsconfig snapshot that should go to one of the nodes stuck in
   * starting state. */
  take_snapshot();

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
    cluster->getNode(nid).updateSetting("block-logsconfig-rsm", "false");
  }

  /* now we should move out of the STARTING state and finish recoveries */
  cluster->waitUntilAllStartedAndPropagatedInGossip();
  cluster->waitUntilAllSequencersQuiescent();
}

/**
 * This test makes sure that if the replicated state machine stalls, and new
 * snapshots are created that do not bump the base version but include a read
 * pointer past the stalling point, the state machine resume reading and
 * recover.
 */
TEST_F(InternalLogsIntegrationTest, RecoverAfterStallingDueToTrimmingDeltaLog) {
  auto factory = clusterFactory(true);

  // Configure nodes. N0 is a sequencer+storage node, N1 and N2 are storage
  // nodes.
  Configuration::Nodes nodes;
  for (int i = 0; i < NNODES; ++i) {
    auto& node = nodes[i];
    node.generation = 1;
    node.addStorageRole(2);
  }
  nodes[0].addSequencerRole();

  // Add one more node N3, set up in a weird way to make sure we can safely
  // isolate it without affecting internal logs reads and writes.
  //
  // We configure N0 and N3 as sequencer nodes, while using static sequencer
  // placement. This makes both N0 and N3 activate sequencers for all logs on
  // startup. For each log, random one of the two nodes will win the race and
  // preempt the other. We then manually activate sequencers for all logs on N0
  // to make sure N3 ends up with no useful sequencers at all.
  auto& node = nodes[NNODES];
  node.generation = 1;
  node.addSequencerRole();

  factory.setNodes(nodes);

  // Prevent client's logsconfig API from picking N3 to execute requests, since
  // N3 will be isolated for part of the test.
  buildClusterAndClient(
      std::move(factory), {{"logsconfig-api-blacklist-nodes", "3"}});

  // Write something to the delta log
  auto dir = client->makeDirectorySync("/dir1", true);
  ASSERT_NE(nullptr, dir);
  lsn_t last_delta = dir->version();

  // Check we never trimmed
  ASSERT_EQ(getTrimPointFor(configuration::InternalLogs::CONFIG_LOG_DELTAS),
            LSN_INVALID);

  // Bump epoch a number of times
  const size_t NUM_EPOCHS_TO_BUMP = 5;
  auto& seq = cluster->getNode(0);
  for (size_t t = 0; t < NUM_EPOCHS_TO_BUMP; ++t) {
    seq.upDown(configuration::InternalLogs::CONFIG_LOG_DELTAS);
    cluster->waitUntilAllSequencersQuiescent();
  }
  // Make sure the active snapshots sequencer ends up on N0, not N3.
  seq.upDown(configuration::InternalLogs::CONFIG_LOG_SNAPSHOTS);
  cluster->waitUntilAllSequencersQuiescent();

  // Check that the tail moved as expected
  auto tail_lsn =
      client->getTailLSNSync(configuration::InternalLogs::CONFIG_LOG_DELTAS);
  ASSERT_EQ(epoch_t(lsn_to_epoch(last_delta).val_ + NUM_EPOCHS_TO_BUMP),
            lsn_to_epoch(tail_lsn));

  // Make sure logsconfig is in sync everywhere
  cluster->waitUntilLogsConfigSynced(tail_lsn);
  client->syncLogsConfigVersion(last_delta);

  take_snapshot();

  // Trim the delta log up to our client's read pointer
  ASSERT_EQ(trimLogsconfig(/*trim_everything=*/false), E::OK);

  // Check we have trimmed up to delta log read ptr
  ASSERT_EQ(getTrimPointFor(configuration::InternalLogs::CONFIG_LOG_DELTAS),
            tail_lsn - 1);

  auto get_last_node_reader_health = [&]() -> std::string {
    auto info = cluster->getNode(NNODES).sendJsonCommand(
        "info client_read_streams --json");
    int checked = 0;
    std::string res = "?";
    for (auto& rec : info) {
      if (rec["Log ID"] ==
          folly::to<std::string>(
              configuration::InternalLogs::CONFIG_LOG_DELTAS.val())) {
        res = rec["Connection health"];
        ++checked;
      }
    }
    EXPECT_EQ(1, checked);
    return res;
  };

  wait_until("N3's config log deltas reader is healthy", [&] {
    return get_last_node_reader_health() == "HEALTHY_AUTHORITATIVE_COMPLETE";
  });

  // Isolate N<last> so that delta log read stream becomes unhealthy and the
  // replicated state machine stalls
  std::set<int> partition1;
  for (int i = 0; i < NNODES; ++i) {
    partition1.insert(i);
  }
  cluster->partition({partition1, {NNODES}});

  // Write another record
  dir = client->makeDirectorySync("/dir2", true);
  ASSERT_NE(nullptr, dir);
  last_delta = dir->version();

  // Bump epoch a few more times to move the tail and cause bridge gaps
  for (size_t t = 0; t < NUM_EPOCHS_TO_BUMP; ++t) {
    seq.upDown(configuration::InternalLogs::CONFIG_LOG_DELTAS);
    cluster->waitUntilAllSequencersQuiescent();
  }

  // Check that the tail moved as expected
  auto new_tail_lsn =
      client->getTailLSNSync(configuration::InternalLogs::CONFIG_LOG_DELTAS);
  ASSERT_EQ(epoch_t(lsn_to_epoch(last_delta).val_ + NUM_EPOCHS_TO_BUMP),
            lsn_to_epoch(new_tail_lsn));
  cluster->getNode(0).waitUntilLogsConfigSynced(new_tail_lsn);

  // Take another snapshot and trim the delta log
  take_snapshot();
  ASSERT_EQ(trimLogsconfig(/*trim_everything=*/false), E::OK);
  ASSERT_EQ(getTrimPointFor(configuration::InternalLogs::CONFIG_LOG_DELTAS),
            new_tail_lsn - 1);

  // While N<last> is still isolated, check that the logsconfig delta log read
  // stream is not healthy.
  wait_until("N3's config log deltas reader is unhealthy", [&] {
    std::string s = get_last_node_reader_health();
    return s == "UNHEALTHY" || s == "READING_METADATA";
  });

  // Rejoin N<last> to the cluster
  partition1.insert(NNODES);
  cluster->partition({partition1});

  // Check that N<last> is able to sycnhronize the logs config up to the tail.
  cluster->getNode(NNODES).waitUntilLogsConfigSynced(new_tail_lsn);

  // Double check that read stream is now healthy. Note that the reader may
  // deliver records while unhealthy, so we need to wait here despite the
  // waitUntilLogsConfigSynced() above.
  wait_until("N3's config log deltas reader is healthy again", [&] {
    return get_last_node_reader_health() == "HEALTHY_AUTHORITATIVE_COMPLETE";
  });

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
  // Most of the test is copy-pasted from
  // RecoverAfterStallingDueToTrimmingDeltaLog above. See comments there.

  // More frequent snapshotting shouldn't be used in this test because
  // that means we call processSnapshot(which internally calls
  // cancelStallGracePeriod()) before stallGracePeriodTimer_(default 10s)
  // gets a chance to bump 'num_replicated_state_machines_stalled'.
  auto factory = clusterFactory(true, "default" /* snapshotting period */);

  Configuration::Nodes nodes;
  for (int i = 0; i < NNODES; ++i) {
    auto& node = nodes[i];
    node.generation = 1;
    node.addStorageRole(2);
  }
  nodes[0].addSequencerRole();

  auto& node = nodes[NNODES];
  node.generation = 1;
  node.addSequencerRole();

  factory.setNodes(nodes);

  buildClusterAndClient(
      std::move(factory), {{"logsconfig-api-blacklist-nodes", "3"}});

  // Write something to the delta log
  auto dir = client->makeDirectorySync("/dir1", true);
  ASSERT_NE(nullptr, dir);
  lsn_t last_delta = dir->version();

  // Check we never trimmed
  ASSERT_EQ(getTrimPointFor(configuration::InternalLogs::CONFIG_LOG_DELTAS),
            LSN_INVALID);

  // Bump epoch a number of times
  const size_t NUM_EPOCHS_TO_BUMP = 5;
  auto& seq = cluster->getNode(0);
  for (size_t t = 0; t < NUM_EPOCHS_TO_BUMP; ++t) {
    seq.upDown(configuration::InternalLogs::CONFIG_LOG_DELTAS);
    cluster->waitUntilAllSequencersQuiescent();
  }
  // Make sure the active snapshots sequencer ends up on N0, not N3.
  seq.upDown(configuration::InternalLogs::CONFIG_LOG_SNAPSHOTS);
  cluster->waitUntilAllSequencersQuiescent();

  // Check that the tail moved as expected
  auto tail_lsn =
      client->getTailLSNSync(configuration::InternalLogs::CONFIG_LOG_DELTAS);
  ASSERT_EQ(epoch_t(lsn_to_epoch(last_delta).val_ + NUM_EPOCHS_TO_BUMP),
            lsn_to_epoch(tail_lsn));

  // Make sure logsconfig is in sync everywhere
  cluster->waitUntilLogsConfigSynced(tail_lsn);
  client->syncLogsConfigVersion(last_delta);

  take_snapshot();

  // Trim the delta log up to our client's read pointer
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
  ASSERT_NE(nullptr, dir);
  last_delta = dir->version();

  // Bump epoch a few more times to move the tail and cause bridge gaps
  for (size_t t = 0; t < NUM_EPOCHS_TO_BUMP; ++t) {
    seq.upDown(configuration::InternalLogs::CONFIG_LOG_DELTAS);
    cluster->waitUntilAllSequencersQuiescent();
  }

  // Check that the tail moved as expected
  auto new_tail_lsn =
      client->getTailLSNSync(configuration::InternalLogs::CONFIG_LOG_DELTAS);
  ASSERT_EQ(epoch_t(lsn_to_epoch(last_delta).val_ + NUM_EPOCHS_TO_BUMP),
            lsn_to_epoch(new_tail_lsn));
  cluster->getNode(0).waitUntilLogsConfigSynced(new_tail_lsn);

  // Write an intermediate snapshot
  take_snapshot();

  // Bump epoch a few more times to move the tail and cause bridge gaps
  for (size_t t = 0; t < NUM_EPOCHS_TO_BUMP; ++t) {
    seq.upDown(configuration::InternalLogs::CONFIG_LOG_DELTAS);
    cluster->waitUntilAllSequencersQuiescent();
  }

  // Write another record
  dir = client->makeDirectorySync("/dir3", true);
  ASSERT_NE(nullptr, dir);
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
  take_snapshot();

  // Check that N<last> is able to sycnhronize the logs config up to the tail.
  cluster->getNode(NNODES).waitUntilLogsConfigSynced(new_tail_lsn);

  // Check that N<last> is no longer waiting for newer snapshot
  wait_until("N<last> logsconfig replicated state machine unstalls", [&]() {
    auto stats = cluster->getNode(NNODES).stats();
    return stats["num_replicated_state_machines_stalled"] == 0;
  });

  // Check logsconfig read availability on sequencer by restarting it
  seq.kill();
  seq.start();
  seq.waitUntilStarted();
  seq.waitUntilLogsConfigSynced(new_tail_lsn);
}

TEST_F(InternalLogsIntegrationTest, LCM_VerifyClientCannotTakeSnapshot) {
  // Using a config file to force client to pickup 1s snapshotting period.
  // client_settings->set() doesn't work on SERVER only settings,
  // so with the default setting of 1h, client will never even attempt
  // to write snapshot
  auto config = Configuration::fromJsonFile(
      TEST_CONFIG_FILE("lcm_client_cannot_take_snapshot.conf"));
  ASSERT_NE(nullptr, config);
  cluster = IntegrationTestUtils::ClusterFactory()
                .enableLogsConfigManager()
                .setParam("--file-config-update-interval", "10ms")
                .create(*config);
  std::unique_ptr<ClientSettings> client_settings(ClientSettings::create());
  auto client = cluster->createClient(
      getDefaultTestTimeout(), std::move(client_settings));

  ld_info("Checking N0 stats");
  auto stats = cluster->getNode(0).stats();
  wait_until([&]() {
    return cluster->getNode(0)
               .stats()["logsconfig_manager_snapshot_requested"] > 10;
  });

  ld_info("Checking client stats");
  Stats s = dynamic_cast<ClientImpl*>(client.get())->stats()->aggregate();
  EXPECT_EQ(0, s.logsconfig_manager_snapshot_requested);
}

class VerifyServerSnapshotting : public ::testing::TestWithParam<std::string> {
};
INSTANTIATE_TEST_CASE_P(VerifyServerSnapshotting,
                        VerifyServerSnapshotting,
                        ::testing::Values("legacy", "log", "local-store"));
TEST_P(VerifyServerSnapshotting, Basic) {
  auto snapshot_type = GetParam();
  auto cluster = IntegrationTestUtils::ClusterFactory()
                     .enableLogsConfigManager()
                     .setParam("--rsm-snapshot-store-type", snapshot_type)
                     .setParam("--logsconfig-snapshotting-period", "1s")
                     .useHashBasedSequencerAssignment()
                     .create(4);

  ld_info("Checking N0 stats");
  auto stats = cluster->getNode(0).stats();
  wait_until([&]() {
    return cluster->getNode(0)
               .stats()["logsconfig_manager_snapshot_requested"] > 10;
  });

  for (int i = 1; i < 4; i++) {
    ld_info("Checking N%d stats, store_type:%s", i, snapshot_type.c_str());
    auto num_snapshots_requested =
        cluster->getNode(i).stats()["logsconfig_manager_snapshot_requested"];
    if (snapshot_type == "local-store") {
      EXPECT_GT(num_snapshots_requested, 0);
    } else {
      EXPECT_EQ(num_snapshots_requested, 0);
    }
  }
}
