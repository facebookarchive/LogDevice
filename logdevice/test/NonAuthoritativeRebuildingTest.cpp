/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include <boost/filesystem.hpp>
#include <folly/Memory.h>
#include <gtest/gtest.h>

#include "logdevice/common/DataRecordOwnsPayload.h"
#include "logdevice/common/EpochMetaDataUpdater.h"
#include "logdevice/common/LocalLogStoreRecordFormat.h"
#include "logdevice/common/Metadata.h"
#include "logdevice/common/NodeSetSelectorFactory.h"
#include "logdevice/common/ShardID.h"
#include "logdevice/common/Timer.h"
#include "logdevice/common/client_read_stream/AllClientReadStreams.h"
#include "logdevice/common/configuration/ConfigParser.h"
#include "logdevice/common/configuration/Configuration.h"
#include "logdevice/common/configuration/InternalLogs.h"
#include "logdevice/common/debug.h"
#include "logdevice/common/event_log/EventLogRebuildingSet.h"
#include "logdevice/common/event_log/EventLogRecord.h"
#include "logdevice/common/test/TestUtil.h"
#include "logdevice/common/util.h"
#include "logdevice/include/Client.h"
#include "logdevice/lib/ClientImpl.h"
#include "logdevice/lib/ops/EventLogUtils.h"
#include "logdevice/server/locallogstore/test/StoreUtil.h"
#include "logdevice/test/utils/AppendThread.h"
#include "logdevice/test/utils/IntegrationTestBase.h"
#include "logdevice/test/utils/IntegrationTestUtils.h"
#include "logdevice/test/utils/ReaderThread.h"

/**
 * Contains a suite of tests to verify the behavior of rebuilding state machines
 * and readers under different scenarios involving rebuilding being non
 * authoritative.
 */

using namespace facebook::logdevice;
using namespace facebook::logdevice::IntegrationTestUtils;
using IntegrationTestUtils::requestShardRebuilding;

const logid_t LOG_ID(1);

#define N0 ShardID(0, 0)
#define N1 ShardID(1, 0)
#define N2 ShardID(2, 0)
#define N3 ShardID(3, 0)
#define N4 ShardID(4, 0)
#define N5 ShardID(5, 0)
#define N6 ShardID(6, 0)
#define N7 ShardID(7, 0)
#define N8 ShardID(8, 0)
#define N9 ShardID(9, 0)
#define N10 ShardID(10, 0)
#define N11 ShardID(11, 0)

// Kill test process after this many seconds. These tests are complex and take
// around 25s to execute so giving them a longer timeout.
const std::chrono::seconds TEST_TIMEOUT(DEFAULT_TEST_TIMEOUT * 2);

class NonAuthoritativeRebuildingTest
    : public IntegrationTestBase,
      public ::testing::WithParamInterface<bool /* rebuilding-v2 */> {
 public:
  NonAuthoritativeRebuildingTest() : IntegrationTestBase(TEST_TIMEOUT) {}
  ~NonAuthoritativeRebuildingTest() override {}

 protected:
  /**
   * Set up the environment common to all tests. We want to simulate failures of
   * many nodes to check the effect on read availability on log `LOG_ID`, but at
   * the same time we don't want to affect availability of metadata logs and the
   * event log during the test. This is why we create a cluster of 12 nodes and
   * carefully provision the nodesets of metadata logs and the event log so that
   * tests know what to not do.
   */
  void SetUp() override {
    dbg::currentLevel = dbg::Level::INFO;
    dbg::assertOnData = true;

    logsconfig::LogAttributes log_attrs;
    log_attrs.set_replicationFactor(3);
    log_attrs.set_syncReplicationScope(NodeLocationScope::RACK);
    log_attrs.set_extraCopies(0);
    log_attrs.set_syncedCopies(0);
    log_attrs.set_maxWritesInFlight(30);

    // We want more randomness in the placement of records.
    log_attrs.set_stickyCopySets(false);

    // Place metadata logs on the first 6 nodes in the cluster.
    // Tests may kill an entire rack, but they should not touch the other nodes
    // in the metadata nodeset to affect metadata logs' availability.
    std::vector<node_index_t> nodeset(6);
    std::iota(nodeset.begin(), nodeset.end(), 0);
    Configuration::MetaDataLogsConfig meta_config =
        createMetaDataLogsConfig(nodeset, 4, NodeLocationScope::NODE);
    meta_config.sequencers_provision_epoch_store = false;

    std::shared_ptr<NodeSetSelector> selector =
        NodeSetSelectorFactory::create(NodeSetSelectorType::RANDOM_CROSSDOMAIN);

    cluster_ = IntegrationTestUtils::ClusterFactory()
                   .setParam("--file-config-update-interval", "10ms")
                   .setParam("--disable-rebuilding", "false")
                   // A rebuilding node responds to STOREs with E::DISABLED.
                   // Setting this to 0s makes it so that the sequencer does not
                   // wait for a while before trying to store to that node
                   // again, otherwise the test would timeout.
                   .setParam("--disabled-retry-interval", "0s")
                   .setParam("--seq-state-backoff-time", "10ms..1s")
                   .setParam("--sticky-copysets-block-max-time", "1ms")
                   .setParam("--rebuilding-v2", GetParam() ? "true" : "false")
                   .setNumDBShards(1)
                   .setNumRacks(3)
                   .useHashBasedSequencerAssignment()
                   .setLogGroupName("alog")
                   .setLogAttributes(log_attrs)
                   .setEventLogDeltaAttributes(log_attrs)
                   .setMetaDataLogsConfig(meta_config)
                   .setNumLogs(1)
                   .deferStart()
                   .create(12);

    // Tests are going to work on this log.
    {
      std::vector<std::unique_ptr<EpochMetaData>> epoch_metadata;
      epoch_metadata.emplace_back(new EpochMetaData(
          StorageSet{N2, N3, N4, N6, N7, N8, N9, N10, N11},
          ReplicationProperty(
              {{NodeLocationScope::NODE, 3}, {NodeLocationScope::RACK, 2}}),
          epoch_t(1),
          epoch_t(1)));
      auto meta_provisioner = cluster_->createMetaDataProvisioner();
      auto provisioner = [&](logid_t, std::unique_ptr<EpochMetaData>& info) {
        if (!info) {
          info = std::make_unique<EpochMetaData>();
        }
        *info = *epoch_metadata[0];
        info->h.epoch = epoch_t(1);
        return EpochMetaData::UpdateResult::SUBSTANTIAL_RECONFIGURATION;
      };
      int rv = meta_provisioner->provisionEpochMetaDataForLog(
          LOG_ID,
          std::make_shared<SimpleEpochMetaDataUpdater>(provisioner),
          false /* write_metadata_logs */);
      ASSERT_EQ(0, rv);
    }

    // Provision the event logs with the first 5 nodes. Tests may kill an entire
    // rack, but they should not touch the other nodes in this nodeset to not
    // affect the event log's availability.
    {
      std::vector<std::unique_ptr<EpochMetaData>> epoch_metadata;
      epoch_metadata.emplace_back(new EpochMetaData(
          StorageSet{N0, N1, N2, N3, N4, N5},
          ReplicationProperty(
              {{NodeLocationScope::NODE, 4}, {NodeLocationScope::RACK, 2}}),
          epoch_t(1),
          epoch_t(1)));
      auto meta_provisioner = cluster_->createMetaDataProvisioner();
      auto provisioner = [&](logid_t, std::unique_ptr<EpochMetaData>& info) {
        if (!info) {
          info = std::make_unique<EpochMetaData>();
        }
        *info = *epoch_metadata[0];
        info->h.epoch = epoch_t(1);
        return EpochMetaData::UpdateResult::SUBSTANTIAL_RECONFIGURATION;
      };

      int rv = meta_provisioner->provisionEpochMetaDataForLog(
          configuration::InternalLogs::EVENT_LOG_DELTAS,
          std::make_shared<SimpleEpochMetaDataUpdater>(provisioner),
          false /* write_metadata_logs */);
      ASSERT_EQ(0, rv);
      rv = meta_provisioner->provisionEpochMetaDataForLog(
          configuration::InternalLogs::CONFIG_LOG_DELTAS,
          std::make_shared<SimpleEpochMetaDataUpdater>(provisioner),
          false /* write_metadata_logs */);
      ASSERT_EQ(0, rv);
      rv = meta_provisioner->provisionEpochMetaDataForLog(
          configuration::InternalLogs::CONFIG_LOG_SNAPSHOTS,
          std::make_shared<SimpleEpochMetaDataUpdater>(provisioner),
          false /* write_metadata_logs */);
      ASSERT_EQ(0, rv);
      rv = meta_provisioner->provisionEpochMetaDataForLog(
          configuration::InternalLogs::MAINTENANCE_LOG_SNAPSHOTS,
          std::make_shared<SimpleEpochMetaDataUpdater>(provisioner),
          false /* write_metadata_logs */);
      ASSERT_EQ(0, rv);
      rv = meta_provisioner->provisionEpochMetaDataForLog(
          configuration::InternalLogs::MAINTENANCE_LOG_DELTAS,
          std::make_shared<SimpleEpochMetaDataUpdater>(provisioner),
          false /* write_metadata_logs */);
      ASSERT_EQ(0, rv);
    }

    cluster_->start();
    cluster_->waitForRecovery();
  }

  // Check whether all nodes are reporting or not that we have a non
  // authoritative rebuilding and some shards are still recoverable.  We retry
  // for at most 3s in case stats take some time to propagate.
  void expectClusterIsWaitingForRecoverableShards(size_t num_shards) const {
    auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(3);
    wait_until(
        "checking cluster stats",
        [&]() {
          for (node_index_t nid = 0; nid < 12; ++nid) {
            auto& node = cluster_->getNode(nid);
            if (node.isRunning()) {
              const size_t v =
                  node.stats()["rebuilding_waiting_for_recoverable_shards"];
              if (v != num_shards) {
                return false;
              }
            }
          }
          return true;
        },
        deadline);
  }

  std::unique_ptr<Cluster> cluster_;
};

/**
 * A rack fails, another shard in a different rack fails as well. Rebuilding is
 * started but is not authoritative (ie it misses records that were fully
 * replicated on shards that are unavailable). Readers are expected to stall
 * even if rebuilding completes. When the rack comes back, rebuilding is
 * restarted non authoritatively and readers are unstalled.
 */
TEST_P(NonAuthoritativeRebuildingTest,
       RackPlusAnotherShardFailButWooHooTheRackComesBack) {
  // Create a reader and writer thread to read/write during the whole test.
  auto reader_thread =
      std::make_unique<ReaderThread>(cluster_->createClient(), LOG_ID);
  auto append_thread =
      std::make_unique<AppendThread>(cluster_->createClient(), LOG_ID);
  reader_thread->start();
  append_thread->start();

  auto client = cluster_->createClient();

  std::vector<ShardID> shards_in_rack;
  for (node_index_t nid = 0; nid < 12; ++nid) {
    if (nid % 3 == 0) {
      shards_in_rack.push_back(ShardID(nid, 0));
    }
  }
  ShardID another_shard(11, 0);
  std::vector<ShardID> all_shards = shards_in_rack;
  all_shards.push_back(another_shard);

  // A rack and N11 go down.
  for (ShardID sid : all_shards) {
    cluster_->getNode(sid.node()).kill();
  }

  // Start rebuilding the rack and N11.
  for (ShardID sid : all_shards) {
    lsn_t lsn = requestShardRebuilding(*client, sid.node(), sid.shard());
    EXPECT_NE(LSN_INVALID, lsn);
  }

  // Wait for rebuilding of all these shards to complete, the shards should have
  // authoritative status UNAVAILABLE because rebuilding was not authoritative.
  // This should make readers stall instead of make progress and issue DATALOSS
  // gaps. At the end of this test we'll check that no DATALOSS gaps were
  // issued.
  waitUntilShardsHaveEventLogState(
      client, all_shards, AuthoritativeStatus::UNAVAILABLE, true);
  expectClusterIsWaitingForRecoverableShards(all_shards.size());

  // Wipe the shard on node 11 and restart it.
  // The node should write SHARD_UNRECOVERABLE message.
  cluster_->getNode(another_shard.node()).wipeShard(another_shard.shard());
  ASSERT_EQ(0, cluster_->bumpGeneration(another_shard.node()));
  cluster_->getNode(another_shard.node()).start();
  cluster_->getNode(another_shard.node()).waitUntilStarted();

  // Now that the shard has been marked as unrecoverable, its authoritative
  // status should be changed to UNDERREPLICATION and remain there even though
  // rebuilding completes.
  waitUntilShardHasEventLogState(
      client, another_shard, AuthoritativeStatus::UNDERREPLICATION, true);
  expectClusterIsWaitingForRecoverableShards(all_shards.size());

  // At this point the reader should have realized that its connection is not
  // healthy and stall.
  reader_thread->waitUntilStalled();

  // Restart the rack. The nodes should cancel rebuildings.
  for (ShardID sid : shards_in_rack) {
    cluster_->getNode(sid.node()).start();
  }
  for (ShardID sid : shards_in_rack) {
    cluster_->getNode(sid.node()).waitUntilStarted();
  }

  // The rack coming back should be enough for the readers to unstall.
  reader_thread->syncToTail();
  expectClusterIsWaitingForRecoverableShards(0);

  // After the rack was restarted, rebuilding is restarted authoritatively. Wait
  // until it completes and expect all shards to be FULLY_AUTHORITATIVE.
  waitUntilShardsHaveEventLogState(
      client, all_shards, AuthoritativeStatus::FULLY_AUTHORITATIVE, true);

  // Stop appends, wait for readers to finish reading up to the last record
  // that was appended.
  append_thread->stop();
  reader_thread->syncToTail();
  reader_thread->stop();

  // Readers should have stalled and not see dataloss.
  EXPECT_FALSE(reader_thread->foundDataLoss());

  // Finally, start a reader to read everything from the beginning, it should
  // not see dataloss.
  auto backlog_reader_thread =
      std::make_unique<ReaderThread>(cluster_->createClient(), LOG_ID);
  backlog_reader_thread->start();
  backlog_reader_thread->syncToTail();
  backlog_reader_thread->stop();
  EXPECT_FALSE(backlog_reader_thread->foundDataLoss());
}

// We lose an entire rack plus two other shards in a different rack. This time
// we lose all the data (ie a SHARD_UNRECOVERABLE is written for every single
// shard). The readers should unstall once the non authoritative rebuilding
// completes even though this means they see DATALOSS.
TEST_P(NonAuthoritativeRebuildingTest, LoseRackPlusAnotherShard) {
  // Create a reader and writer thread to read/write during the whole test.
  auto reader_thread =
      std::make_unique<ReaderThread>(cluster_->createClient(), LOG_ID);
  auto append_thread =
      std::make_unique<AppendThread>(cluster_->createClient(), LOG_ID);
  reader_thread->start();
  append_thread->start();

  auto client = cluster_->createClient();

  std::vector<ShardID> shards_in_rack;
  for (node_index_t nid = 0; nid < 12; ++nid) {
    if (nid % 3 == 0) {
      shards_in_rack.push_back(ShardID(nid, 0));
    }
  }
  std::vector<ShardID> two_other_shards{ShardID(8, 0), ShardID(11, 0)};
  std::vector<ShardID> all_shards = shards_in_rack;
  all_shards.insert(
      all_shards.end(), two_other_shards.begin(), two_other_shards.end());
  ;

  // A rack goes down and N8, N11 go down.
  for (ShardID sid : all_shards) {
    cluster_->getNode(sid.node()).kill();
  }

  // Start rebuilding the rack as well as N8 and N11.
  for (ShardID sid : all_shards) {
    lsn_t lsn = requestShardRebuilding(*client, sid.node(), sid.shard());
    EXPECT_NE(LSN_INVALID, lsn);
  }

  // Wait for rebuilding of all these shards to complete, the shards should have
  // authoritative status UNAVAILABLE because rebuilding was not authoritative.
  // This should make readers stall instead of make progress and issue DATALOSS
  // gaps. At the end of this test we'll check that no DATALOSS gaps were
  // issued.
  waitUntilShardsHaveEventLogState(
      client, all_shards, AuthoritativeStatus::UNAVAILABLE, true);
  expectClusterIsWaitingForRecoverableShards(all_shards.size());

  // Wipe the shard on nodes N8 and N11 and restart them.
  // The nodes should write the SHARD_UNRECOVERABLE message.
  for (ShardID sid : two_other_shards) {
    cluster_->getNode(sid.node()).wipeShard(sid.shard());
    ASSERT_EQ(0, cluster_->bumpGeneration(sid.node()));
    cluster_->getNode(sid.node()).start();
  }
  for (ShardID sid : two_other_shards) {
    cluster_->getNode(sid.node()).waitUntilStarted();
  }

  // Now that the two shards have been marked as unrecoverable, their
  // authoritative status should be changed to UNDERREPLICATION and remain there
  // even though rebuilding completes.
  waitUntilShardsHaveEventLogState(
      client, two_other_shards, AuthoritativeStatus::UNDERREPLICATION, true);
  // The authoritative status of the shards in the rack should still be
  // UNAVAILABLE.
  waitUntilShardsHaveEventLogState(
      client, shards_in_rack, AuthoritativeStatus::UNAVAILABLE, true);
  expectClusterIsWaitingForRecoverableShards(all_shards.size());

  // At this point the reader should have realized that its connection is not
  // healthy and stall.
  reader_thread->waitUntilStalled();

  // Now, the rack is not coming back any time soon and the oncall decides to
  // unstall the readers even though this means seeing some dataloss. The oncall
  // marks all the shards in the rack as unrecoverable.
  for (ShardID sid : shards_in_rack) {
    const lsn_t lsn = markShardUnrecoverable(*client, sid.node(), sid.shard());
    ASSERT_NE(LSN_INVALID, lsn);
  }

  // Readers should be able to make progress.
  reader_thread->syncToTail();

  // the cluster should now report that we are not waiting for recoverable
  // shards to come back.
  expectClusterIsWaitingForRecoverableShards(0);

  // Rebuilding should continue non authoritatively, but this time it should
  // complete and all the shards' authoritative status in that rack should be
  // moved to AUTHORITATIVE_EMPTY since there is no possible data to recover.
  waitUntilShardsHaveEventLogState(
      client, shards_in_rack, AuthoritativeStatus::AUTHORITATIVE_EMPTY, true);
  // And the other shard's authoritative status should be moved to
  // FULLY_AUTHORITATIVE since the shard is up and running and should
  // acknowledge rebuilding.
  waitUntilShardsHaveEventLogState(
      client, two_other_shards, AuthoritativeStatus::FULLY_AUTHORITATIVE, true);

  // Restart the rack with their data.
  for (ShardID sid : shards_in_rack) {
    cluster_->getNode(sid.node()).start();
  }
  for (ShardID sid : shards_in_rack) {
    cluster_->getNode(sid.node()).waitUntilStarted();
  }

  // Stop appends, wait for readers to finish reading up to the last record
  // that was appended.
  append_thread->stop();
  reader_thread->syncToTail();
  reader_thread->stop();

  // The rack should have acknowledged rebuilding by then, so everything should
  // be FULLY_AUTHORITATIVE.
  waitUntilShardsHaveEventLogState(
      client, all_shards, AuthoritativeStatus::FULLY_AUTHORITATIVE, true);

  // Finally, start a reader to read everything from the beginning.
  auto backlog_reader_thread =
      std::make_unique<ReaderThread>(cluster_->createClient(), LOG_ID);
  backlog_reader_thread->start();
  backlog_reader_thread->syncToTail();
  backlog_reader_thread->stop();

  // Note: here we are not asserting that backlog_reader_thread found data loss
  // because there is no certainty that a copyset was selected to have a record
  // fully replicated on the nodes that lost data. Hopefully we put on all the
  // right conditions for data loss to hapen.
}

// In the following test, we enable auto mark unrecoverable when in
// non-authoritative state for more than 1s. We then lose an entire rack, and
// wait for shards to enter in AUTHORITATIVE_EMPTY state after ~1s.
TEST_P(NonAuthoritativeRebuildingTest, LoseRackAutoMarkUnrecoverable) {
  // Create a reader and writer thread to read/write during the whole test.
  auto reader_thread =
      std::make_unique<ReaderThread>(cluster_->createClient(), LOG_ID);
  auto append_thread =
      std::make_unique<AppendThread>(cluster_->createClient(), LOG_ID);
  reader_thread->start();
  append_thread->start();

  // activate auto mark unrecoverable
  cluster_->setParam(
      "--auto-mark-unrecoverable-non-authoritative-timeout", "1s");
  cluster_->setParam("--enable-timed-auto-mark-unrecoverable", "true");

  auto client = cluster_->createClient();

  std::vector<ShardID> shards_in_rack;
  for (node_index_t nid = 0; nid < 12; nid += 3) {
    shards_in_rack.push_back(ShardID(nid, 0));
  }

  // Rack goes down
  for (ShardID sid : shards_in_rack) {
    cluster_->getNode(sid.node()).kill();
  }

  // Start rebuilding the rack
  for (ShardID sid : shards_in_rack) {
    lsn_t lsn = requestShardRebuilding(*client, sid.node(), sid.shard());
    EXPECT_NE(LSN_INVALID, lsn);
  }

  // All shards should be marked unrecoverable and enter the
  // AUTHORITATIVE_EMPTY state
  waitUntilShardsHaveEventLogState(
      client, shards_in_rack, AuthoritativeStatus::AUTHORITATIVE_EMPTY, true);

  // the cluster should now report that we are not waiting for recoverable
  // shards to come back.
  expectClusterIsWaitingForRecoverableShards(0);
}

// This test simulates the following conditions:
// Some shards are under-replicated, but one of them has experienced a read IO
// error and logdeviced is still running on that node. The readers should accept
// the dataloss and unstall.
// When a read IO error is detected, logdeviced triggers rebuilding but still
// accepts read streams. So it returns STARTED(OK) in response to the START
// message, but then later send a STARTED(FAILED) when encountering an iterator
// error. The client should consider this node FULLY_AUTHORITATIVE when
// receiving a STARTED(OK) regardless of what the event log say, but then revert
// to previous status (in that case UNDERREPLICATION) if it receives
// STARTED(FAILED) so it can take it into account in f-majority for gap
// detection.
// TODO(T44746268): replace NDEBUG with folly::kIsDebug
// Can not remove now due to the defined functions
#ifndef NDEBUG // Both tests require fault injection.
TEST_P(NonAuthoritativeRebuildingTest, LoseRackPlusAnotherShardAndReadIOError) {
  // Create a reader and writer thread to read/write during the whole test.
  auto reader_thread =
      std::make_unique<ReaderThread>(cluster_->createClient(), LOG_ID);
  auto append_thread =
      std::make_unique<AppendThread>(cluster_->createClient(), LOG_ID);
  reader_thread->start();
  append_thread->start();

  auto client = cluster_->createClient();

  std::vector<ShardID> shards_in_rack;
  for (node_index_t nid = 0; nid < 12; ++nid) {
    if (nid % 3 == 0) {
      shards_in_rack.push_back(ShardID(nid, 0));
    }
  }
  ShardID wiped_shard(8, 0);
  ShardID io_error_shard(11, 0);
  std::vector<ShardID> all_shards = shards_in_rack;
  all_shards.push_back(wiped_shard);
  all_shards.push_back(io_error_shard);

  // A rack goes down as well as N8, request rebuilding for them.
  // and N11 get's a broken disk (read IO error), let it trigger it.
  for (ShardID sid : all_shards) {
    if (sid == io_error_shard) {
      // enable self-initiated rebuilding to let N11 trigger rebuilding for its
      // broken shard
      cluster_->getNode(sid.node())
          .updateSetting("enable-self-initiated-rebuilding", "true");
      ASSERT_TRUE(cluster_->getNode(sid.node())
                      .injectShardFault("0", "data", "read", "io_error"));
    } else {
      cluster_->getNode(sid.node()).kill();
      lsn_t lsn = requestShardRebuilding(*client, sid.node(), sid.shard());
      EXPECT_NE(LSN_INVALID, lsn);
    }
  }

  // Wait for rebuilding of all these shards to complete, the shards should have
  // authoritative status UNAVAILABLE because rebuilding was not authoritative.
  // This should make readers stall instead of make progress and issue DATALOSS
  // gaps. At the end of this test we'll check that no DATALOSS gaps were
  // issued.
  waitUntilShardsHaveEventLogState(
      client, all_shards, AuthoritativeStatus::UNAVAILABLE, true);
  expectClusterIsWaitingForRecoverableShards(all_shards.size());

  ld_info("All shards are UNAVAILABLE and cluster is waiting for "
          "recoverable shards");

  // Wipe the shard on N8 and restart it.
  // The node should write the SHARD_UNRECOVERABLE message.
  cluster_->getNode(wiped_shard.node()).wipeShard(0);
  ASSERT_EQ(0, cluster_->bumpGeneration(wiped_shard.node()));
  cluster_->getNode(wiped_shard.node()).start();
  cluster_->getNode(wiped_shard.node()).waitUntilStarted();

  // Now manually mark shard N11:S0 unrecoverable but keep it running
  markShardUnrecoverable(
      *client, io_error_shard.node(), io_error_shard.shard());

  // Now that the two shards have been marked as unrecoverable, their
  // authoritative status should be changed to UNDERREPLICATION and remain there
  // even though rebuilding completes.
  waitUntilShardsHaveEventLogState(client,
                                   {wiped_shard, io_error_shard},
                                   AuthoritativeStatus::UNDERREPLICATION,
                                   true);
  // The authoritative status of the shards in the rack should still be
  // UNAVAILABLE.
  waitUntilShardsHaveEventLogState(
      client, shards_in_rack, AuthoritativeStatus::UNAVAILABLE, true);
  expectClusterIsWaitingForRecoverableShards(all_shards.size());

  ld_info("wiped shard and io error shard have been marked unrecoverable. "
          "cluster is still waiting for other shards");
  // At this point the reader should have realized that its connection is not
  // healthy and stall.
  reader_thread->waitUntilStalled();

  // Now, mark the shards that are in LOG_ID's nodeset to be unrecoverable to
  // unstall. Note we keep N0 to be unavailable so rebuilding stalls becasue
  // we don't want the shard to transition to being empty
  for (ShardID sid : shards_in_rack) {
    if (sid.node() != 0) {
      const lsn_t lsn =
          markShardUnrecoverable(*client, sid.node(), sid.shard());
      ASSERT_NE(LSN_INVALID, lsn);
    }
  }

  ld_info("all shards but N0:S0 have been marked unrecoverable");
  // At this point, all the broken shards for LOG_ID are unrecoverable. The
  // readers should unstall and possibly issue dataloss.
  // Prior to the fix from T22163714, readers would not unstall here, because
  // the authoritative status of N11 (read IO error) would be overridden by the
  // client to FULLY_AUTHORITATIVE, which prevents having an F-majority to
  // deliver the gap.
  reader_thread->syncToTail();

  // The cluster should now report that we are not waiting for recoverable
  // shards to come back.
  expectClusterIsWaitingForRecoverableShards(0);

  ld_info("reader has unstalled and cluster is no longer waiting for shards");
  // Stop appends, wait for readers to finish reading up to the last record
  // that was appended.
  append_thread->stop();
  reader_thread->syncToTail();
  reader_thread->stop();

  // Finally, start a reader to read everything from the beginning.
  std::unique_ptr<ClientSettings> client_settings{ClientSettings::create()};
  client_settings->set("reader-started-timeout", "1s");
  auto backlog_reader_thread = std::make_unique<ReaderThread>(
      cluster_->createClient(DEFAULT_TEST_TIMEOUT, std::move(client_settings)),
      LOG_ID);
  backlog_reader_thread->start();
  backlog_reader_thread->syncToTail();
  backlog_reader_thread->stop();

  // Note: here we are not asserting that backlog_reader_thread found data loss
  // because there is no certainty that a copyset was selected to have a record
  // fully replicated on the nodes that lost data. Hopefully we put on all the
  // right conditions for data loss to hapen.
}

// Simulate a situation where a shard is AUTHORITATIVE_EMPTY
// and has IO errors. This should not cause an infinite loop of rewinds
// due to the following sequence:
// 1- shard is AUTHORITATIVE_EMPTY
// 2- client sends START
// 3- client receives STARTED(OK) --> overrides shard authoritative status to
//    FULLY_AUTHORITATIVE
// 4- client receives STARTED(FAILED) --> revert shard status to
//    AUTHORITATIVE_EMPTY and rewinds (goes back to 1)
//
// A fix was implemented to mark the log in permanent error and fail
// immediately when a client tries to initiate a read stream. so step 3 is
// removed from that sequence.
TEST_P(NonAuthoritativeRebuildingTest,
       ReadIOErrorAndAuthEmptyStatusRewindLoop) {
  // Create a writer to append data to the log
  auto append_thread =
      std::make_unique<AppendThread>(cluster_->createClient(), LOG_ID);
  append_thread->start();

  auto client = cluster_->createClient();

  std::vector<ShardID> shards_in_rack;
  for (node_index_t nid = 0; nid < 12; ++nid) {
    if (nid % 3 == 0) {
      shards_in_rack.push_back(ShardID(nid, 0));
    }
  }
  ShardID io_error_shard(11, 0);

  // enable self-initiated rebuilding to let N11 trigger rebuilding for its
  // broken shard
  cluster_->getNode(io_error_shard.node())
      .updateSetting("enable-self-initiated-rebuilding", "true");
  // inject read IO errors
  ASSERT_TRUE(cluster_->getNode(io_error_shard.node())
                  .injectShardFault("0", "data", "read", "io_error"));
  // wait for shard to rebuilt
  waitUntilShardsHaveEventLogState(
      client, {io_error_shard}, AuthoritativeStatus::AUTHORITATIVE_EMPTY, true);

  // now loose a rack (technically only two nodes of the same rack would be
  // needed but it's easier to take the whole rack down)
  for (ShardID sid : shards_in_rack) {
    cluster_->getNode(sid.node()).kill();
    lsn_t lsn = requestShardRebuilding(*client, sid.node(), sid.shard());
    EXPECT_NE(LSN_INVALID, lsn);
  }

  // wait for the rack to be rebuilt
  waitUntilShardsHaveEventLogState(
      client, shards_in_rack, AuthoritativeStatus::AUTHORITATIVE_EMPTY, true);

  // Stop appends
  append_thread->stop();

  // Finally, start a reader to read everything from the beginning.
  std::unique_ptr<ClientSettings> client_settings{ClientSettings::create()};
  auto backlog_reader_thread = std::make_unique<ReaderThread>(
      cluster_->createClient(DEFAULT_TEST_TIMEOUT, std::move(client_settings)),
      LOG_ID);
  backlog_reader_thread->start();
  backlog_reader_thread->syncToTail();
  backlog_reader_thread->stop();
}
#endif // NDEBUG

// Rebuild a shard in N11 authoritatively. After the rebuilding completed,
// rebuild a rack. Verify that although N11 is in the rebuilding set and did not
// ack, we consider the rebuilding authoritative.
TEST_P(NonAuthoritativeRebuildingTest, RebuildRackAfterCompletedRebuildShard) {
  // Create a reader and writer thread to read/write during the whole test.
  auto reader_thread =
      std::make_unique<ReaderThread>(cluster_->createClient(), LOG_ID);
  auto append_thread =
      std::make_unique<AppendThread>(cluster_->createClient(), LOG_ID);
  reader_thread->start();
  append_thread->start();

  auto client = cluster_->createClient();

  std::vector<ShardID> shards_in_rack;
  for (node_index_t nid = 0; nid < 12; ++nid) {
    if (nid % 3 == 0) {
      shards_in_rack.push_back(ShardID(nid, 0));
    }
  }
  ShardID another_shard(11, 0);
  std::vector<ShardID> all_shards = shards_in_rack;
  all_shards.push_back(another_shard);

  // N11 goes down and we start rebuilding it.
  cluster_->getNode(another_shard.node()).kill();
  lsn_t lsn = requestShardRebuilding(
      *client, another_shard.node(), another_shard.shard());
  EXPECT_NE(LSN_INVALID, lsn);

  // Reader should not stall.
  reader_thread->syncToTail();
  expectClusterIsWaitingForRecoverableShards(0);

  // Wait until it's rebuilt.
  waitUntilShardHasEventLogState(
      client, another_shard, AuthoritativeStatus::AUTHORITATIVE_EMPTY, true);

  // A rack goes down.
  for (ShardID sid : shards_in_rack) {
    cluster_->getNode(sid.node()).kill();
  }
  // Start rebuilding it.
  for (ShardID sid : shards_in_rack) {
    lsn = requestShardRebuilding(*client, sid.node(), sid.shard());
    EXPECT_NE(LSN_INVALID, lsn);
  }

  // Reader should not stall.
  reader_thread->syncToTail();

  // Rebuilding should be authoritative because N11 was successfully rebuilt
  // before we lost the rack.
  expectClusterIsWaitingForRecoverableShards(0);
  waitUntilShardsHaveEventLogState(
      client, all_shards, AuthoritativeStatus::AUTHORITATIVE_EMPTY, true);

  // Wipe the shard on node 11 and restart it.
  cluster_->getNode(another_shard.node()).wipeShard(another_shard.shard());
  ASSERT_EQ(0, cluster_->bumpGeneration(another_shard.node()));
  cluster_->getNode(another_shard.node()).start();
  cluster_->getNode(another_shard.node()).waitUntilStarted();

  // N11 should now ack and be FULLY_AUTHORITATIVE while the nodes in the rack
  // remain AUTHORITATIVE_EMPTY.
  expectClusterIsWaitingForRecoverableShards(0);
  waitUntilShardHasEventLogState(
      client, another_shard, AuthoritativeStatus::FULLY_AUTHORITATIVE, true);
  waitUntilShardsHaveEventLogState(
      client, shards_in_rack, AuthoritativeStatus::AUTHORITATIVE_EMPTY, true);

  // Stop appends, wait for readers to finish reading up to the last record
  // that was appended.
  append_thread->stop();
  reader_thread->syncToTail();
  reader_thread->stop();

  // Readers should not have seen dataloss.
  EXPECT_FALSE(reader_thread->foundDataLoss());

  // Finally, start a reader to read everything from the beginning, it should
  // not see dataloss.
  auto backlog_reader_thread =
      std::make_unique<ReaderThread>(cluster_->createClient(), LOG_ID);
  backlog_reader_thread->start();
  backlog_reader_thread->syncToTail();
  backlog_reader_thread->stop();
  EXPECT_FALSE(backlog_reader_thread->foundDataLoss());
}

// There is a non authoritative rebuilding hapenning while some nodes are
// AUTHORITATIVE_EMPTY. We verify that the nodes that were already
// AUTHORITATIVE_EMPTY keep that status. We also restart one of the nodes that
// was part of the non authoritative rebuilding set and verify that this causes
// rebuilding to be started authoritatively.
TEST_P(NonAuthoritativeRebuildingTest, Mix1) {
  auto client = cluster_->createClient();

  // Kill N11
  cluster_->getNode(11).kill();
  lsn_t lsn = requestShardRebuilding(*client, 11, 0);
  EXPECT_NE(LSN_INVALID, lsn);
  waitUntilShardHasEventLogState(
      client, ShardID(11, 0), AuthoritativeStatus::AUTHORITATIVE_EMPTY, true);

  // Kill N10
  cluster_->getNode(10).kill();
  lsn = requestShardRebuilding(*client, 10, 0);
  EXPECT_NE(LSN_INVALID, lsn);
  waitUntilShardHasEventLogState(
      client, ShardID(10, 0), AuthoritativeStatus::AUTHORITATIVE_EMPTY, true);

  // Kill N9, N8, N7
  cluster_->getNode(9).kill();
  cluster_->getNode(8).kill();
  cluster_->getNode(7).kill();
  lsn = requestShardRebuilding(*client, 9, 0);
  EXPECT_NE(LSN_INVALID, lsn);
  lsn = requestShardRebuilding(*client, 8, 0);
  EXPECT_NE(LSN_INVALID, lsn);
  lsn = requestShardRebuilding(*client, 7, 0);
  EXPECT_NE(LSN_INVALID, lsn);

  expectClusterIsWaitingForRecoverableShards(3);
  waitUntilShardsHaveEventLogState(
      client,
      {ShardID(9, 0), ShardID(8, 0), ShardID(7, 0)},
      AuthoritativeStatus::UNAVAILABLE,
      true);

  waitUntilShardsHaveEventLogState(client,
                                   {ShardID(11, 0), ShardID(10, 0)},
                                   AuthoritativeStatus::AUTHORITATIVE_EMPTY,
                                   true);

  cluster_->getNode(7).start();

  expectClusterIsWaitingForRecoverableShards(0);
  waitUntilShardsHaveEventLogState(
      client,
      {ShardID(11, 0), ShardID(10, 0), ShardID(9, 0), ShardID(8, 0)},
      AuthoritativeStatus::AUTHORITATIVE_EMPTY,
      true);
  waitUntilShardHasEventLogState(
      client, ShardID(7, 0), AuthoritativeStatus::FULLY_AUTHORITATIVE, true);
}

TEST_P(NonAuthoritativeRebuildingTest, Mix2) {
  auto client = cluster_->createClient();

  // Kill N11
  cluster_->getNode(11).kill();
  lsn_t lsn = requestShardRebuilding(*client, 11, 0);
  EXPECT_NE(LSN_INVALID, lsn);
  waitUntilShardHasEventLogState(
      client, ShardID(11, 0), AuthoritativeStatus::AUTHORITATIVE_EMPTY, true);

  // Kill N10
  cluster_->getNode(10).kill();
  lsn = requestShardRebuilding(*client, 10, 0);
  EXPECT_NE(LSN_INVALID, lsn);
  waitUntilShardHasEventLogState(
      client, ShardID(10, 0), AuthoritativeStatus::AUTHORITATIVE_EMPTY, true);

  // Kill N9, N8, N7
  cluster_->getNode(9).kill();
  cluster_->getNode(8).kill();
  cluster_->getNode(7).kill();
  lsn = requestShardRebuilding(*client, 9, 0);
  EXPECT_NE(LSN_INVALID, lsn);
  lsn = requestShardRebuilding(*client, 8, 0);
  EXPECT_NE(LSN_INVALID, lsn);
  lsn = requestShardRebuilding(*client, 7, 0);
  EXPECT_NE(LSN_INVALID, lsn);

  expectClusterIsWaitingForRecoverableShards(3);
  waitUntilShardsHaveEventLogState(
      client,
      {ShardID(9, 0), ShardID(8, 0), ShardID(7, 0)},
      AuthoritativeStatus::UNAVAILABLE,
      true);

  waitUntilShardsHaveEventLogState(client,
                                   {ShardID(11, 0), ShardID(10, 0)},
                                   AuthoritativeStatus::AUTHORITATIVE_EMPTY,
                                   true);

  // Kill N6
  cluster_->getNode(6).kill();
  lsn = requestShardRebuilding(*client, 6, 0);
  EXPECT_NE(LSN_INVALID, lsn);
  expectClusterIsWaitingForRecoverableShards(4);
  waitUntilShardsHaveEventLogState(
      client,
      {ShardID(9, 0), ShardID(8, 0), ShardID(7, 0), ShardID(6, 0)},
      AuthoritativeStatus::UNAVAILABLE,
      true);
  waitUntilShardsHaveEventLogState(client,
                                   {ShardID(11, 0), ShardID(10, 0)},
                                   AuthoritativeStatus::AUTHORITATIVE_EMPTY,
                                   true);

  cluster_->getNode(7).start();
  cluster_->getNode(7).waitUntilStarted();
  expectClusterIsWaitingForRecoverableShards(3);
  waitUntilShardsHaveEventLogState(
      client, {ShardID(7, 0)}, AuthoritativeStatus::FULLY_AUTHORITATIVE, true);
  waitUntilShardsHaveEventLogState(
      client,
      {ShardID(9, 0), ShardID(8, 0), ShardID(6, 0)},
      AuthoritativeStatus::UNAVAILABLE,
      true);
  waitUntilShardsHaveEventLogState(client,
                                   {ShardID(11, 0), ShardID(10, 0)},
                                   AuthoritativeStatus::AUTHORITATIVE_EMPTY,
                                   true);

  cluster_->getNode(6).start();
  cluster_->getNode(6).waitUntilStarted();
  expectClusterIsWaitingForRecoverableShards(0);
  waitUntilShardsHaveEventLogState(client,
                                   {ShardID(6, 0), ShardID(7, 0)},
                                   AuthoritativeStatus::FULLY_AUTHORITATIVE,
                                   true);
  waitUntilShardsHaveEventLogState(
      client,
      {ShardID(11, 0), ShardID(10, 0), ShardID(9, 0), ShardID(8, 0)},
      AuthoritativeStatus::AUTHORITATIVE_EMPTY,
      true);

  cluster_->getNode(8).start();
  cluster_->getNode(9).start();
  cluster_->getNode(10).start();
  cluster_->getNode(11).start();
  cluster_->getNode(8).waitUntilStarted();
  cluster_->getNode(9).waitUntilStarted();
  cluster_->getNode(10).waitUntilStarted();
  cluster_->getNode(11).waitUntilStarted();
  expectClusterIsWaitingForRecoverableShards(0);
  waitUntilShardsHaveEventLogState(
      client,
      {ShardID(11, 0), ShardID(10, 0), ShardID(9, 0), ShardID(8, 0)},
      AuthoritativeStatus::FULLY_AUTHORITATIVE,
      true);
}

TEST_P(NonAuthoritativeRebuildingTest, Mix3) {
  auto client = cluster_->createClient();

  // Kill N11
  cluster_->getNode(11).kill();
  lsn_t lsn = requestShardRebuilding(*client, 11, 0);
  EXPECT_NE(LSN_INVALID, lsn);
  waitUntilShardHasEventLogState(
      client, ShardID(11, 0), AuthoritativeStatus::AUTHORITATIVE_EMPTY, true);

  // Kill N10
  cluster_->getNode(10).kill();
  lsn = requestShardRebuilding(*client, 10, 0);
  EXPECT_NE(LSN_INVALID, lsn);
  waitUntilShardHasEventLogState(
      client, ShardID(10, 0), AuthoritativeStatus::AUTHORITATIVE_EMPTY, true);

  // Kill N9, N8, N7
  cluster_->getNode(9).kill();
  cluster_->getNode(8).kill();
  cluster_->getNode(7).kill();
  lsn = requestShardRebuilding(*client, 9, 0);
  EXPECT_NE(LSN_INVALID, lsn);
  lsn = requestShardRebuilding(*client, 8, 0);
  EXPECT_NE(LSN_INVALID, lsn);
  lsn = requestShardRebuilding(*client, 7, 0);
  EXPECT_NE(LSN_INVALID, lsn);

  expectClusterIsWaitingForRecoverableShards(3);
  waitUntilShardsHaveEventLogState(
      client,
      {ShardID(9, 0), ShardID(8, 0), ShardID(7, 0)},
      AuthoritativeStatus::UNAVAILABLE,
      true);

  waitUntilShardsHaveEventLogState(client,
                                   {ShardID(11, 0), ShardID(10, 0)},
                                   AuthoritativeStatus::AUTHORITATIVE_EMPTY,
                                   true);

  // Kill N6
  cluster_->getNode(6).kill();
  lsn = requestShardRebuilding(*client, 6, 0);
  EXPECT_NE(LSN_INVALID, lsn);
  expectClusterIsWaitingForRecoverableShards(4);
  waitUntilShardsHaveEventLogState(
      client,
      {ShardID(9, 0), ShardID(8, 0), ShardID(7, 0), ShardID(6, 0)},
      AuthoritativeStatus::UNAVAILABLE,
      true);
  waitUntilShardsHaveEventLogState(client,
                                   {ShardID(11, 0), ShardID(10, 0)},
                                   AuthoritativeStatus::AUTHORITATIVE_EMPTY,
                                   true);

  lsn = markShardUnrecoverable(*client, 6, 0);
  EXPECT_NE(LSN_INVALID, lsn);
  lsn = markShardUnrecoverable(*client, 7, 0);
  EXPECT_NE(LSN_INVALID, lsn);
  lsn = markShardUnrecoverable(*client, 8, 0);
  EXPECT_NE(LSN_INVALID, lsn);
  lsn = markShardUnrecoverable(*client, 9, 0);
  EXPECT_NE(LSN_INVALID, lsn);
  // Note: because we have a non authoritative rebuilding (ie we lost data), we
  // now also require that the oncall would mark N10 and N11 as unrecoverable
  // before we give up and mark all shards as AUTHORITATIVE_EMPTY. This is to
  // give a chance for the oncall to recover data on these shards even though
  // they were rebuilt authoritatively, recovering these shards may help save
  // some records.
  lsn = markShardUnrecoverable(*client, 10, 0);
  EXPECT_NE(LSN_INVALID, lsn);
  lsn = markShardUnrecoverable(*client, 11, 0);
  EXPECT_NE(LSN_INVALID, lsn);

  expectClusterIsWaitingForRecoverableShards(0);
  waitUntilShardsHaveEventLogState(
      client,
      {ShardID(11, 0), ShardID(10, 0), ShardID(9, 0), ShardID(8, 0)},
      AuthoritativeStatus::AUTHORITATIVE_EMPTY,
      true);

  cluster_->getNode(8).start();
  cluster_->getNode(9).start();
  cluster_->getNode(10).start();
  cluster_->getNode(11).start();
  cluster_->getNode(8).waitUntilStarted();
  cluster_->getNode(9).waitUntilStarted();
  cluster_->getNode(10).waitUntilStarted();
  cluster_->getNode(11).waitUntilStarted();

  expectClusterIsWaitingForRecoverableShards(0);
  waitUntilShardsHaveEventLogState(
      client,
      {ShardID(11, 0), ShardID(10, 0), ShardID(9, 0), ShardID(8, 0)},
      AuthoritativeStatus::FULLY_AUTHORITATIVE,
      true);
}

TEST_P(NonAuthoritativeRebuildingTest, Mix4) {
  auto client = cluster_->createClient();

  // Kill N9, N8, N7
  cluster_->getNode(9).kill();
  cluster_->getNode(8).kill();
  cluster_->getNode(7).kill();
  lsn_t lsn = requestShardRebuilding(*client, 9, 0);
  EXPECT_NE(LSN_INVALID, lsn);
  lsn = requestShardRebuilding(*client, 8, 0);
  EXPECT_NE(LSN_INVALID, lsn);
  lsn = requestShardRebuilding(*client, 7, 0);
  EXPECT_NE(LSN_INVALID, lsn);

  expectClusterIsWaitingForRecoverableShards(3);
  waitUntilShardsHaveEventLogState(
      client,
      {ShardID(9, 0), ShardID(8, 0), ShardID(7, 0)},
      AuthoritativeStatus::UNAVAILABLE,
      true);

  lsn = markShardUnrecoverable(*client, 7, 0);
  EXPECT_NE(LSN_INVALID, lsn);
  lsn = markShardUnrecoverable(*client, 8, 0);
  EXPECT_NE(LSN_INVALID, lsn);
  lsn = markShardUnrecoverable(*client, 9, 0);
  EXPECT_NE(LSN_INVALID, lsn);

  expectClusterIsWaitingForRecoverableShards(0);
  waitUntilShardsHaveEventLogState(
      client,
      {ShardID(9, 0), ShardID(8, 0), ShardID(7, 0)},
      AuthoritativeStatus::AUTHORITATIVE_EMPTY,
      true);

  cluster_->getNode(6).kill();
  cluster_->getNode(10).kill();
  cluster_->getNode(11).kill();
  lsn = requestShardRebuilding(*client, 6, 0);
  ASSERT_NE(LSN_INVALID, lsn);
  lsn = requestShardRebuilding(*client, 10, 0);
  ASSERT_NE(LSN_INVALID, lsn);
  lsn = requestShardRebuilding(*client, 11, 0);
  ASSERT_NE(LSN_INVALID, lsn);

  expectClusterIsWaitingForRecoverableShards(3);
  waitUntilShardsHaveEventLogState(
      client,
      {ShardID(6, 0), ShardID(10, 0), ShardID(11, 0)},
      AuthoritativeStatus::UNAVAILABLE,
      true);
  waitUntilShardsHaveEventLogState(
      client,
      {ShardID(9, 0), ShardID(8, 0), ShardID(7, 0)},
      AuthoritativeStatus::AUTHORITATIVE_EMPTY,
      true);

  lsn = markShardUnrecoverable(*client, 6, 0);
  EXPECT_NE(LSN_INVALID, lsn);
  lsn = markShardUnrecoverable(*client, 10, 0);
  EXPECT_NE(LSN_INVALID, lsn);
  lsn = markShardUnrecoverable(*client, 11, 0);
  EXPECT_NE(LSN_INVALID, lsn);

  expectClusterIsWaitingForRecoverableShards(0);
  waitUntilShardsHaveEventLogState(client,
                                   {ShardID(9, 0),
                                    ShardID(8, 0),
                                    ShardID(7, 0),
                                    ShardID(6, 0),
                                    ShardID(10, 0),
                                    ShardID(11, 0)},
                                   AuthoritativeStatus::AUTHORITATIVE_EMPTY,
                                   true);
}

// We drain a node while 3 other nodes are rebuilding in RESTORE mode non
// authoritatively. Because the node is drained in RELOCATE mode, it should not
// prevent the drain from completing (ie its authoritative status to become
// AUTHORITATIVE_EMPTY).
TEST_P(NonAuthoritativeRebuildingTest,
       RackDrainDuringNonAuthoritativeRebuilding) {
  auto client = cluster_->createClient();

  ShardID drained(10, 0);

  std::vector<ShardID> shards_rebuilding;
  for (node_index_t nid : {0, 3, 11, 8}) {
    shards_rebuilding.push_back(ShardID(nid, 0));
  }

  for (ShardID sid : shards_rebuilding) {
    cluster_->getNode(sid.node()).kill();
  }

  const auto flags =
      SHARD_NEEDS_REBUILD_Header::DRAIN | SHARD_NEEDS_REBUILD_Header::RELOCATE;
  lsn_t lsn =
      requestShardRebuilding(*client, drained.node(), drained.shard(), flags);
  EXPECT_NE(LSN_INVALID, lsn);

  for (ShardID sid : shards_rebuilding) {
    lsn_t lsn2 = requestShardRebuilding(*client, sid.node(), sid.shard());
    EXPECT_NE(LSN_INVALID, lsn2);
  }

  expectClusterIsWaitingForRecoverableShards(shards_rebuilding.size());
  waitUntilShardsHaveEventLogState(
      client, shards_rebuilding, AuthoritativeStatus::UNAVAILABLE, true);
  waitUntilShardHasEventLogState(
      client, drained, AuthoritativeStatus::AUTHORITATIVE_EMPTY, true);
}

INSTANTIATE_TEST_CASE_P(P,
                        NonAuthoritativeRebuildingTest,
                        ::testing::Values(false, true));
