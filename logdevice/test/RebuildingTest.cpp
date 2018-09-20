/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include <gtest/gtest.h>

#include <folly/Memory.h>
#include <boost/filesystem.hpp>

#include "logdevice/common/configuration/Configuration.h"
#include "logdevice/common/LocalLogStoreRecordFormat.h"
#include "logdevice/common/Metadata.h"
#include "logdevice/common/configuration/ConfigParser.h"
#include "logdevice/common/debug.h"
#include "logdevice/common/event_log/EventLogRecord.h"
#include "logdevice/common/test/TestUtil.h"
#include "logdevice/common/util.h"
#include "logdevice/server/locallogstore/test/StoreUtil.h"

#include "logdevice/include/Client.h"
#include "logdevice/include/ClientSettings.h"

#include "logdevice/lib/ops/EventLogUtils.h"

#include "logdevice/server/locallogstore/ShardToPathMapping.h"

#include "logdevice/test/utils/IntegrationTestBase.h"
#include "logdevice/test/utils/IntegrationTestUtils.h"

using namespace facebook::logdevice;
using IntegrationTestUtils::markShardUndrained;
using IntegrationTestUtils::markShardUnrecoverable;
using IntegrationTestUtils::requestShardRebuilding;
using IntegrationTestUtils::waitUntilShardHasEventLogState;
using IntegrationTestUtils::waitUntilShardsHaveEventLogState;
namespace fs = boost::filesystem;

const logid_t LOG_ID(1);
const int NUM_DB_SHARDS = 3;

static Configuration::Log logConfig(int replication) {
  Configuration::Log log_config;
  log_config.replicationFactor = replication;
  log_config.rangeName = "mylog";
  log_config.extraCopies = 0;
  log_config.syncedCopies = 0;
  log_config.maxWritesInFlight = 300;
  return log_config;
}

static void commonSetup(IntegrationTestUtils::ClusterFactory& cluster) {
  // TODO enableMessageErrorInjection() once CatchupQueue properly
  //      handles streams waiting for log recoveries that have timed out.
  cluster
      // there is a known issue where purging deletes records that gets surfaced
      // in tests with sequencer-written metadata. See t13850978
      .doPreProvisionEpochMetaData()
      .doNotLetSequencersProvisionEpochMetaData()
      .setRocksDBType(IntegrationTestUtils::RocksDBType::PARTITIONED)
      .setParam("--file-config-update-interval", "10ms")
      .setParam("--disable-rebuilding", "false")
      // A rebuilding node responds to STOREs with E::DISABLED. Setting this to
      // 0s makes it so that the sequencer does not wait for a while before
      // trying to store to that node again, otherwise the test would timeout.
      .setParam("--disabled-retry-interval", "0s")
      .setParam("--seq-state-backoff-time", "10ms..1s")
      .setParam("--rocksdb-partition-data-age-flush-trigger", "1s")
      .setParam("--rocksdb-partition-idle-flush-trigger", "100ms")
      .setParam("--rocksdb-min-manual-flush-interval", "200ms")
      .setParam("--rocksdb-partition-hi-pri-check-period", "50ms")
      .setParam("--rebuilding-store-timeout", "6s..10s")
      // When rebuilding Without WAL, destruction of memtable is used as proxy
      // for memtable being flushed to stable storage. Iterators can pin a
      // memtable preventing its destruction. Low ttl in tests ensures iterators
      // are invalidated and memtable flush notifications are not delayed
      .setParam("--iterator-cache-ttl", "1s")
      .setParam("--rocksdb-partitioned", "true")
      .setNumDBShards(NUM_DB_SHARDS)
      .useDefaultTrafficShapingConfig(false);
}

void writeRecords(Client& client,
                  size_t nrecords,
                  folly::Optional<lsn_t>* first_lsn_out = nullptr) {
  ld_info("Writing %lu records", nrecords);
  // Write some records
  Semaphore sem;
  std::atomic<lsn_t> first_lsn(LSN_MAX);
  auto cb = [&](Status st, const DataRecord& r) {
    EXPECT_EQ(E::OK, st);
    if (st == E::OK) {
      ASSERT_NE(LSN_INVALID, r.attrs.lsn);
      atomic_fetch_min(first_lsn, r.attrs.lsn);
    }
    sem.post();
  };
  for (int i = 1; i <= nrecords; ++i) {
    std::string data("data" + std::to_string(i));
    client.append(LOG_ID, std::move(data), cb);
  }
  for (int i = 1; i <= nrecords; ++i) {
    sem.wait();
  }
  if (first_lsn_out) {
    *first_lsn_out = first_lsn.load();
    ASSERT_NE(LSN_MAX, *first_lsn_out);
  }
}

void createPartition(IntegrationTestUtils::Cluster& cluster,
                     NodeSetIndices nodeset,
                     shard_index_t shard) {
  ld_info("Creating partition on nodes %s", toString(nodeset).c_str());
  cluster.applyToNodes(nodeset, [shard](auto& node) {
    node.sendCommand(folly::format("logsdb create {}", shard).str());
  });
}

void flushPartition(IntegrationTestUtils::Cluster& cluster,
                    NodeSetIndices nodeset,
                    shard_index_t shard,
                    ssize_t relative_partid) {
  auto start_time = std::chrono::steady_clock::now();
  ld_info("Flushing partition %ld on nodes %s",
          relative_partid,
          toString(nodeset).c_str());
  cluster.applyToNodes(nodeset, [shard, relative_partid](auto& node) {
    node.sendCommand(
        folly::format("logsdb flush -r -- {} {}", shard, relative_partid)
            .str());
  });
  auto end_time = std::chrono::steady_clock::now();
  ld_info("Flushed in %.3f seconds",
          std::chrono::duration_cast<std::chrono::duration<double>>(end_time -
                                                                    start_time)
              .count());
}

size_t dirtyNodes(IntegrationTestUtils::Cluster& cluster,
                  Client& client,
                  NodeSetIndices nodeset,
                  shard_index_t shard,
                  folly::Optional<lsn_t>& batch_start) {
  // This is 2 times the partition timestamp granularity so we guarantee
  // that partitions have non-overlapping time ranges.
  auto partition_creation_delay = std::chrono::milliseconds(200);
  size_t nrecords = 0;

  // Create dirty/clean/dirty paritition pattern
  writeRecords(client, 24, &batch_start);
  nrecords += 24;

  /* Provide some time distance between partitions. */
  std::this_thread::sleep_for(partition_creation_delay);
  createPartition(cluster, nodeset, shard);

  writeRecords(client, 18);
  nrecords += 18;

  /* Provide some time distance between partitions. */
  std::this_thread::sleep_for(partition_creation_delay);
  createPartition(cluster, nodeset, shard);
  flushPartition(cluster, nodeset, shard, -1);

  writeRecords(client, 20);
  nrecords += 20;

  /* Provide some time distance between partitions. */
  std::this_thread::sleep_for(partition_creation_delay);
  createPartition(cluster, nodeset, shard);

  for (auto nidx : nodeset) {
    // Have different partition boundaries between nodes so we test dirty
    // regions that span partitions on the donor.
    writeRecords(client, 10);
    nrecords += 10;
    std::this_thread::sleep_for(partition_creation_delay);
    createPartition(cluster, {nidx}, shard);
    flushPartition(cluster, {nidx}, shard, -1);

    // Mix retired and inflight data in the same partition
    writeRecords(client, 15);
    nrecords += 15;
    flushPartition(cluster, {nidx}, shard, 0);
    writeRecords(client, 11);
    nrecords += 11;
  }

  return nrecords;
}

// Wait until all nodes in `nodes` have read the event log up to `sync_lsn`.
static void wait_until_event_log_synced(IntegrationTestUtils::Cluster& cluster,
                                        lsn_t sync_lsn,
                                        std::vector<node_index_t> nodes) {
  const int rv = cluster.waitUntilEventLogSynced(sync_lsn, nodes);
  ASSERT_EQ(0, rv);
}

static fs::path path_for_node_shard(IntegrationTestUtils::Cluster& cluster,
                                    node_index_t node_index,
                                    shard_index_t shard_index) {
  std::string db_path = cluster.getNode(node_index).getDatabasePath();
  std::vector<boost::filesystem::path> out;
  auto num_shards = cluster.getNode(node_index).num_db_shards_;
  int rv = ShardToPathMapping(db_path, num_shards).get(&out);
  if (rv != 0 || shard_index >= int(out.size())) {
    std::abort();
  }
  return out[shard_index];
}

class RebuildingTest
    : public IntegrationTestBase,
      public ::testing::WithParamInterface<bool /*rebuild-store-durability*/> {
 protected:
  enum class NodeFailureMode { REPLACE, KILL };

  /**
   * Create a cluster factory to be used in rolling rebuilding tests.
   * The factory can be further customized by the test case before being
   * passed to the rollingRebuilding() method which executes the rebuilding
   * actions on the cluster.
   *
   * @param nnodes Number of nodes in the cluster. First node is a sequencer.
   * @param r      Replication factor.
   * @param x      Number of extras.
   * @param trim   Should the event log be trimmed?
   */
  IntegrationTestUtils::ClusterFactory
  rollingRebuildingClusterFactory(int /*nnodes*/, int r, int x, bool trim) {
    Configuration::Log log_config;
    log_config.replicationFactor = r;
    log_config.rangeName = "mylog-2";
    log_config.extraCopies = x;
    log_config.syncedCopies = 0;
    log_config.maxWritesInFlight = 30;
    log_config.stickyCopySets = true;

    Configuration::Log event_log = log_config;
    event_log.rangeName = "event-log";

    return IntegrationTestUtils::ClusterFactory()
        .apply(commonSetup)
        .setLogConfig(log_config)
        .setEventLogConfig(event_log)
        .setParam("--disable-event-log-trimming", trim ? "false" : "true")
        .setParam("--byte-offsets")
        .setParam("--event-log-max-delta-records", "5")
        .setParam(
            "--rebuild-store-durability", GetParam() ? "async_write" : "memory")
        .setParam(
            "--message-tracing-types", "RECORD,APPEND,APPENDED,STORE,STORED")
        .setNumLogs(1)
        .eventLogMode(
            IntegrationTestUtils::ClusterFactory::EventLogMode::SNAPSHOTTED);
  }

  void rollingRebuilding(IntegrationTestUtils::Cluster& cluster,
                         node_index_t begin,
                         node_index_t end,
                         NodeFailureMode fmode = NodeFailureMode::REPLACE,
                         IntegrationTestUtils::Cluster::argv_t check_args =
                             IntegrationTestUtils::Cluster::argv_t()) {
    ld_check(begin > 0);
    ld_check(begin <= end);
    ld_check(end < cluster.getNodes().size());

    NodeSetIndices node_set(end - begin + 1);
    std::iota(node_set.begin(), node_set.end(), begin);

    cluster.waitForRecovery();
    cluster.waitForMetaDataLogWrites();

    auto client = cluster.createClient();

    // Write some records
    folly::Optional<lsn_t> first;
    writeRecords(*client, 30, &first);
    size_t nrecords = 30;

    std::vector<uint64_t> correct_byte_offsets;
    // Reading first time will trigger log storage state to get epoch offset
    // by sending GetSeqStateRequest to sequencer. We will try read from
    // beginning until epoch offset is ready.
    wait_until(
        "GetSeqStateRequests are finished and epoch offset is propagated "
        "to storage nodes.",
        [&]() {
          auto reader = client->createReader(1);
          reader->includeByteOffset();
          ld_error("Starting Read at %s", lsn_to_string(first.value()).c_str());
          int rv = reader->startReading(LOG_ID, first.value());
          EXPECT_EQ(0, rv);
          std::vector<std::unique_ptr<DataRecord>> data_out;
          read_records_no_gaps(*reader, nrecords, &data_out);
          if (data_out[0]->attrs.byte_offset != BYTE_OFFSET_INVALID) {
            for (int i = 0; i < nrecords; ++i) {
              EXPECT_NE(BYTE_OFFSET_INVALID, data_out[i]->attrs.byte_offset);
              correct_byte_offsets.push_back(data_out[i]->attrs.byte_offset);
            }
            return true;
          }
          return false;
        });

    for (node_index_t node = begin; node <= end; ++node) {
      ld_info("%s N%u...",
              fmode == NodeFailureMode::REPLACE ? "Replacing" : "Killing",
              node);
      // Kill/Restart or Replace the node
      switch (fmode) {
        case NodeFailureMode::REPLACE:
          ASSERT_EQ(0, cluster.replace(node));

          // Trigger rebuilding of all of its shards.
          ASSERT_NE(LSN_INVALID, requestShardRebuilding(*client, node, 0));
          ASSERT_NE(LSN_INVALID, requestShardRebuilding(*client, node, 1));
          ASSERT_NE(LSN_INVALID, requestShardRebuilding(*client, node, 2));

          // Wait until all shards finish rebuilding.
          cluster.getNode(node).waitUntilAllShardsFullyAuthoritative(client);
          break;
        case NodeFailureMode::KILL:
          // Ensure there is dirty data to rebuild.
          folly::Optional<lsn_t> batch_start;
          size_t batch_records = dirtyNodes(cluster,
                                            *client,
                                            node_set,
                                            /*shard*/ 0,
                                            batch_start);

          // Include added records in our byte offset verifications
          wait_until(
              "GetSeqStateRequests are finished and epoch offset is propagated "
              "to storage nodes.",
              [&]() {
                auto reader = client->createReader(1);
                reader->includeByteOffset();
                ld_error("Starting Read at %s",
                         lsn_to_string(batch_start.value()).c_str());
                int rv = reader->startReading(LOG_ID, batch_start.value());
                EXPECT_EQ(0, rv);
                std::vector<std::unique_ptr<DataRecord>> data_out;
                read_records_no_gaps(*reader, batch_records, &data_out);
                if (data_out[0]->attrs.byte_offset != BYTE_OFFSET_INVALID) {
                  for (int i = 0; i < batch_records; ++i) {
                    EXPECT_NE(
                        BYTE_OFFSET_INVALID, data_out[i]->attrs.byte_offset);
                    correct_byte_offsets.push_back(
                        data_out[i]->attrs.byte_offset);
                  }
                  return true;
                }
                return false;
              });
          nrecords += batch_records;

          // Kill/Restart the node and monitor the event log to ensure
          // rebuilding is triggered and completes.
          EventLogRebuildingSet base_set;
          ASSERT_EQ(EventLogUtils::getRebuildingSet(*client, base_set), 0);

          cluster.getNode(node).kill();
          cluster.getNode(node).start();
          cluster.getNode(node).waitUntilStarted();

          wait_until("self initiated rebuild", [&]() {
            EventLogRebuildingSet polled_set;
            const int rv = EventLogUtils::getRebuildingSet(*client, polled_set);
            if (rv != 0) {
              return false;
            }
            return polled_set.getLastUpdate() > base_set.getLastUpdate();
          });

          wait_until("empty rebuilding set", [&]() {
            EventLogRebuildingSet polled_set;
            const int rv = EventLogUtils::getRebuildingSet(*client, polled_set);
            if (rv != 0) {
              return false;
            }
            return polled_set.empty();
          });
          break;
      }

      // Start a reader to check if the data is still readable.
      // Reading first time will trigger log storage state to get epoch offset
      // by sending GetSeqStateRequest to sequencer. We will try read from
      // beginning until epoch offset is ready.
      wait_until(
          "GetSeqStateRequests are finished and epoch offset is propagated "
          "to storage nodes.",
          [&]() {
            auto reader = client->createReader(1);
            reader->includeByteOffset();
            int rv = reader->startReading(LOG_ID, first.value());
            ld_error(
                "Starting Read at %s", lsn_to_string(first.value()).c_str());
            EXPECT_EQ(0, rv);
            std::vector<std::unique_ptr<DataRecord>> data_out;
            read_records_no_gaps(*reader, nrecords, &data_out);
            EXPECT_EQ(nrecords, data_out.size());
            if (data_out[0]->attrs.byte_offset != BYTE_OFFSET_INVALID) {
              for (int i = 0; i < nrecords; ++i) {
                EXPECT_EQ(
                    correct_byte_offsets[i], data_out[i]->attrs.byte_offset);
              }
              return true;
            }
            return false;
          });
    }

    // Verify that everything is correctly replicated.
    EXPECT_EQ(0, cluster.checkConsistency(check_args));
  }

  /**
   * Perform a rolling rebuiling. Write some data at the beginning and after
   * each step verify that the data is still readable.
   *
   * @param nnodes Number of nodes in the cluster. First node is a sequencer.
   * @param begin  First node to be failed.
   * @param end    Last node to be failed.
   * @param step   Number of nodes to fail simultaneously.
   * @param fmode  Node failure mode.
   */
  void rollingRebuilding(IntegrationTestUtils::ClusterFactory cf,
                         int nnodes,
                         node_index_t begin,
                         node_index_t end,
                         int /*step*/,
                         NodeFailureMode fmode = NodeFailureMode::REPLACE,
                         // there is a known issue where purging deletes records
                         // that gets surfaced in tests with sequencer-written
                         // metadata, which is why we skip checking replication
                         // for bridge records that this may impact.
                         // See t13850978
                         IntegrationTestUtils::Cluster::argv_t check_args = {
                             "--dont-count-bridge-records",
                         }) {
    auto cluster = cf.create(nnodes);
    rollingRebuilding(*cluster, begin, end, fmode, std::move(check_args));
  }
};

// Replace each storage node one by one. No extras.
TEST_P(RebuildingTest, RollingRebuilding) {
  int nnodes = 5;
  int r = 3;
  int x = 0;
  bool trim = true;
  auto cf = rollingRebuildingClusterFactory(nnodes, r, x, trim);
  rollingRebuilding(cf, nnodes, 1, 4, 1);
}

// Same as RollingRebuilding but the event log is not trimmed after each
// rebuilding completes. This should not be an issue as RecordRebuilding is
// supposed to start rebuiling state machines for each SHARD_NEEDS_REBUILD
// message in the event log but immediately abort them if it realizes that the
// rebuilding has been already performed.
TEST_P(RebuildingTest, RollingRebuildingNoTrimming) {
  int nnodes = 5;
  int r = 3;
  int x = 0;
  bool trim = false;
  auto cf = rollingRebuildingClusterFactory(nnodes, r, x, trim);
  rollingRebuilding(cf, nnodes, 1, 4, 1);
}

// Replace N1 twice. The second time it should not re-replicate anything.
TEST_P(RebuildingTest, NodeRebuiltTwice) {
  int nnodes = 5;
  int r = 3;
  int x = 0;
  bool trim = true;
  auto cf = rollingRebuildingClusterFactory(nnodes, r, x, trim);
  rollingRebuilding(cf, nnodes, 1, 1, 1);
  rollingRebuilding(cf, nnodes, 1, 1, 1);
}

TEST_P(RebuildingTest, OnlineDiskRepair) {
  Configuration::Log log_config;
  log_config.replicationFactor = 3;
  log_config.rangeName = "my-test-log";
  log_config.extraCopies = 0;
  log_config.syncedCopies = 0;
  log_config.maxWritesInFlight = 30;

  Configuration::Log event_log = log_config;
  event_log.replicationFactor = 3;
  event_log.rangeName = "my-event-log";
  event_log.extraCopies = 0;
  event_log.syncedCopies = 0;
  event_log.maxWritesInFlight = 30;

  auto cluster = IntegrationTestUtils::ClusterFactory()
                     .apply(commonSetup)
                     .setLogConfig(log_config)
                     .setEventLogConfig(event_log)
                     .setParam("--rebuild-store-durability",
                               GetParam() ? "async_write" : "memory")
                     .setNumDBShards(3)
                     .setNumLogs(1)
                     .create(5);

  cluster->waitForRecovery();

  auto client = cluster->createClient();

  // Write some records..
  folly::Optional<lsn_t> first;
  for (int i = 1; i <= 30; ++i) {
    std::string data("data" + std::to_string(i));
    lsn_t lsn = client->appendSync(LOG_ID, Payload(data.data(), data.size()));
    ASSERT_NE(LSN_INVALID, lsn);
    if (!first.hasValue()) {
      first = lsn;
    }
  }

  // Stop N3...
  ld_info("Stopping N3...");
  EXPECT_EQ(0, cluster->getNode(3).shutdown());

  // In the real world, the disk will be unmounted. Here let's just remove
  // everything and write a marker that instructs ShardedRocksDBLocalLogStore to
  // create a FailingLocalLogStore for the shard.
  fs::path shard_path = path_for_node_shard(*cluster, node_index_t(3), 1);
  for (fs::directory_iterator end_dir_it, it(shard_path); it != end_dir_it;
       ++it) {
    fs::remove_all(it->path());
  }
  auto disable_marker_path = shard_path / fs::path("LOGDEVICE_DISABLED");
  FILE* fp = std::fopen(disable_marker_path.c_str(), "w");
  ASSERT_NE(nullptr, fp);
  std::fclose(fp);

  ld_info("Bumping the generation...");
  ASSERT_EQ(0, cluster->bumpGeneration(3));

  // Restart N3...
  ld_info("Restarting N3...");
  cluster->getNode(3).start();
  cluster->getNode(3).waitUntilStarted();

  // Read data... N3 should respond with E::REBUILDING.
  {
    auto reader = client->createReader(1);
    int rv = reader->startReading(LOG_ID, first.value());
    ASSERT_EQ(0, rv);
    read_records_no_gaps(*reader, 30);
  }

  // Trigger rebuilding of the shard....
  ld_info("Trigger rebuilding...");
  ASSERT_NE(LSN_INVALID, requestShardRebuilding(*client, 3, 1));

  ld_info("Stopping N3...");
  EXPECT_EQ(0, cluster->getNode(3).shutdown());

  ld_info("Removing marker...");
  ASSERT_TRUE(fs::remove(disable_marker_path));

  ld_info("Starting N3 and waiting for rebuilding...");
  cluster->getNode(3).start();
  cluster->getNode(3).waitUntilAllShardsFullyAuthoritative(client);

  // Read data again...
  {
    auto reader = client->createReader(1);
    int rv = reader->startReading(LOG_ID, first.value());
    ASSERT_EQ(0, rv);
    read_records_no_gaps(*reader, 30);
  }

  cluster->waitForMetaDataLogWrites();
  // there is a known issue where purging deletes records that gets surfaced in
  // tests with sequencer-written metadata, which is why we skip checking
  // replication for bridge records that this may impact. See t13850978
  IntegrationTestUtils::Cluster::argv_t check_args = {
      "--dont-count-bridge-records",
  };
  // Verify that everything is correctly replicated.
  ASSERT_EQ(0, cluster->checkConsistency(check_args));
}

// Check that rebuilding completes if all nodes in the cluster are rebuilding
// the same shard.
// Note that this test relies on the event log still being available, ie the
// event log is not stored on the shard being rebuilt.
TEST_P(RebuildingTest, AllNodesRebuildingSameShard) {
  Configuration::Log log_config;
  log_config.replicationFactor = 3;
  log_config.rangeName = "my-test-log";
  log_config.extraCopies = 0;
  log_config.syncedCopies = 0;
  log_config.maxWritesInFlight = 30;

  Configuration::Log event_log = log_config;
  event_log.replicationFactor = 3;
  event_log.rangeName = "my-event-log";
  event_log.extraCopies = 0;
  event_log.syncedCopies = 0;
  event_log.maxWritesInFlight = 30;
  auto cluster = IntegrationTestUtils::ClusterFactory()
                     .apply(commonSetup)
                     .setLogConfig(log_config)
                     .setParam("--rebuild-store-durability",
                               GetParam() ? "async_write" : "memory")
                     .setEventLogConfig(event_log)
                     .setNumLogs(42)
                     .create(5);

  cluster->waitForRecovery();

  // Restart all nodes with an empty disk for shard 1.
  for (node_index_t node = 1; node <= 4; ++node) {
    EXPECT_EQ(0, cluster->getNode(node).shutdown());
    auto shard_path = path_for_node_shard(*cluster, node, 1);
    for (fs::directory_iterator end_dir_it, it(shard_path); it != end_dir_it;
         ++it) {
      fs::remove_all(it->path());
    }
    ASSERT_EQ(0, cluster->bumpGeneration(node));
    cluster->getNode(node).start();
    cluster->getNode(node).waitUntilStarted();
  }

  ld_info("Triggering rebuiling of shard 0 for all nodes in the cluster...");
  auto client = cluster->createClient(std::chrono::hours(1));
  for (node_index_t node = 1; node <= 4; ++node) {
    ASSERT_NE(LSN_INVALID, requestShardRebuilding(*client, node, 1));
  }

  ld_info("Waiting for all nodes to acknowledge rebuilding...");
  IntegrationTestUtils::waitUntilShardsHaveEventLogState(
      client,
      {ShardID(1, 1), ShardID(2, 1), ShardID(3, 1), ShardID(4, 1)},
      AuthoritativeStatus::FULLY_AUTHORITATIVE,
      true);
}

TEST_P(RebuildingTest, RebuildingWithNoAmends) {
  // Higher replication factor for event log and metadata logs.
  Configuration::MetaDataLogsConfig meta_config = createMetaDataLogsConfig(
      /*nodeset=*/{1, 2, 3, 4, 5, 6, 7, 8},
      /*replication=*/5,
      NodeLocationScope::NODE);
  meta_config.sequencers_provision_epoch_store = false;
  meta_config.sequencers_write_metadata_logs = false;

  auto cluster = IntegrationTestUtils::ClusterFactory()
                     .apply(commonSetup)
                     .setLogConfig(logConfig(3))
                     .setEventLogConfig(logConfig(5))
                     .setParam("--rebuild-store-durability",
                               GetParam() ? "async_write" : "memory")
                     .setMetaDataLogsConfig(meta_config)
                     // read quickly when nodes are down
                     .setParam("--gap-grace-period", "10ms")
                     // fall back to non-authoritative quickly
                     .setParam("--event-log-grace-period", "10ms")
                     .setParam("--rebuild-without-amends", "true")
                     .setNumLogs(42)
                     .create(9); // 1 sequencer node + 8 storage nodes

  cluster->waitForRecovery();

  auto client = cluster->createClient();
  client->settings().set("gap-grace-period", "10ms");

  // Write some records.
  for (int i = 1; i <= 1000; ++i) {
    std::string data("data" + std::to_string(i));
    lsn_t lsn = client->appendSync(LOG_ID, Payload(data.data(), data.size()));
    ASSERT_NE(LSN_INVALID, lsn);
  }

  // Kill nodes 1-2 and clean their DBs. They'll be rebuilding later.
  ld_info("Killing nodes.");
  for (node_index_t node = 1; node <= 2; ++node) {
    cluster->getNode(node).kill();
    for (shard_index_t shard = 0; shard < 3; ++shard) {
      auto shard_path = path_for_node_shard(*cluster, node, shard);
      for (fs::directory_iterator end_dir_it, it(shard_path); it != end_dir_it;
           ++it) {
        fs::remove_all(it->path());
      }
    }
  }

  // Writing SHARD_NEEDS_REBUILD for the three nodes to the event log.
  ld_info("Requesting drain of N3");
  auto flags = SHARD_NEEDS_REBUILD_Header::DRAIN;
  ASSERT_NE(
      LSN_INVALID, requestShardRebuilding(*client, node_index_t{3}, 0, flags));
  ASSERT_NE(
      LSN_INVALID, requestShardRebuilding(*client, node_index_t{3}, 1, flags));
  ASSERT_NE(
      LSN_INVALID, requestShardRebuilding(*client, node_index_t{3}, 2, flags));

  for (node_index_t node = 1; node <= 2; ++node) {
    for (shard_index_t shard = 0; shard < 3; ++shard) {
      ASSERT_NE(LSN_INVALID, requestShardRebuilding(*client, node, shard));
      ASSERT_NE(LSN_INVALID, markShardUnrecoverable(*client, node, shard));
    }
  }

  // Read event log to wait for donors 4-8 to finish rebuilding,
  // with nodes 1-3 still down.
  ld_info("Waiting for rebuilding.");
  std::vector<ShardID> to_rebuild;
  for (shard_index_t shard = 0; shard < NUM_DB_SHARDS; ++shard) {
    for (int nid = 1; nid < 4; ++nid) {
      to_rebuild.push_back(ShardID(nid, shard));
    }
  }
  waitUntilShardsHaveEventLogState(
      client, to_rebuild, AuthoritativeStatus::AUTHORITATIVE_EMPTY, true);
}

TEST_P(RebuildingTest, RecoveryWhenManyNodesAreRebuilding) {
  // Higher replication factor for event log and metadata logs.
  Configuration::MetaDataLogsConfig meta_config = createMetaDataLogsConfig(
      /*nodeset=*/{1, 2, 3, 4, 5, 6, 7, 8},
      /*replication=*/5,
      NodeLocationScope::NODE);
  meta_config.sequencers_provision_epoch_store = false;
  meta_config.sequencers_write_metadata_logs = false;

  auto cluster = IntegrationTestUtils::ClusterFactory()
                     .apply(commonSetup)
                     .setLogConfig(logConfig(3))
                     .setEventLogConfig(logConfig(5))
                     .setParam("--rebuild-store-durability",
                               GetParam() ? "async_write" : "memory")
                     .setMetaDataLogsConfig(meta_config)
                     // read quickly when nodes are down
                     .setParam("--gap-grace-period", "10ms")
                     // fall back to non-authoritative quickly
                     .setParam("--event-log-grace-period", "10ms")
                     .setNumLogs(42)
                     .create(9); // 1 sequencer node + 8 storage nodes

  cluster->waitForRecovery();

  auto client = cluster->createClient();
  client->settings().set("gap-grace-period", "10ms");

  // Write some records.
  for (int i = 1; i <= 30; ++i) {
    std::string data("data" + std::to_string(i));
    lsn_t lsn = client->appendSync(LOG_ID, Payload(data.data(), data.size()));
    ASSERT_NE(LSN_INVALID, lsn);
  }

  // Kill nodes 1-3 and clean their DBs. They'll be rebuilding later.
  ld_info("Killing nodes.");
  for (node_index_t node = 1; node <= 3; ++node) {
    cluster->getNode(node).kill();
    for (shard_index_t shard = 0; shard < 3; ++shard) {
      auto shard_path = path_for_node_shard(*cluster, node, shard);
      for (fs::directory_iterator end_dir_it, it(shard_path); it != end_dir_it;
           ++it) {
        fs::remove_all(it->path());
      }
    }
  }

  // Should still have write availability. Write some records.
  ld_info("Writing.");
  for (int i = 1; i <= 30; ++i) {
    std::string data("data" + std::to_string(i));
    lsn_t lsn = client->appendSync(LOG_ID, Payload(data.data(), data.size()));
    ASSERT_NE(LSN_INVALID, lsn);
  }

  // Restart sequencer node. It should get stuck in recovery.
  ld_info("Restarting sequencer.");
  cluster->getNode(0).kill();
  cluster->getNode(0).start();
  cluster->getNode(0).waitUntilStarted();
  int rv = cluster->waitForRecovery(std::chrono::steady_clock::now() +
                                    std::chrono::seconds(1));

  // Not recovered after one second.
  EXPECT_EQ(-1, rv);

  // Writing SHARD_NEEDS_REBUILD for the three nodes to the event log.
  ld_info("Requesting rebuildings.");
  for (node_index_t node = 1; node <= 3; ++node) {
    // Before processing the last node check that recovery still doesn't move.
    if (node == 3) {
      rv = cluster->waitForRecovery(std::chrono::steady_clock::now() +
                                    std::chrono::seconds(1));
      EXPECT_EQ(-1, rv);
    }
    for (shard_index_t shard = 0; shard < 3; ++shard) {
      ASSERT_NE(LSN_INVALID, requestShardRebuilding(*client, node, shard));
      ASSERT_NE(LSN_INVALID, markShardUnrecoverable(*client, node, shard));
    }
  }

  // Now recovery should finish.
  ld_info("Waiting for recovery.");
  cluster->waitForRecovery();

  auto stats = cluster->getNode(0).stats();
  ASSERT_GT(stats["non_auth_recovery_epochs"], 0);

  // Read event log to wait for donors 4-8 to finish rebuilding,
  // with nodes 1-3 still down.
  ld_info("Waiting for rebuilding.");
  std::vector<ShardID> to_rebuild;
  for (shard_index_t shard = 0; shard < NUM_DB_SHARDS; ++shard) {
    for (int nid = 1; nid < 4; ++nid) {
      to_rebuild.push_back(ShardID(nid, shard));
    }
  }
  waitUntilShardsHaveEventLogState(
      client, to_rebuild, AuthoritativeStatus::AUTHORITATIVE_EMPTY, true);

  // Start nodes 1-3 and wait for them to ack the rebuilding.
  ld_info("Starting nodes.");
  for (node_index_t node = 1; node <= 3; ++node) {
    ASSERT_EQ(0, cluster->bumpGeneration(node));
    cluster->getNode(node).start();
  }
  waitUntilShardsHaveEventLogState(
      client, to_rebuild, AuthoritativeStatus::FULLY_AUTHORITATIVE, true);
}

// Verify that rebuilding completes if some new nodes are added to the
// cluster while it is happening. In this test, the new nodes are added before
// the node in the rebuilding set (N3) comes back. So N3 expects the new nodes
// to send SHARD_IS_REBUILT.
// In addition to that, a node N1 was killed and comes back after the expansion
// was done as well. Killing a donor node helps ensure that rebuilding does not
// complete until after we restart it.
TEST_P(RebuildingTest, ClusterExpandedWhileRebuilding) {
  Configuration::Log log_config;
  log_config.replicationFactor = 3;
  log_config.rangeName = "my-test-log";
  log_config.extraCopies = 0;
  log_config.syncedCopies = 0;
  log_config.maxWritesInFlight = 30;

  Configuration::Log event_log = log_config;
  event_log.replicationFactor = 3;
  event_log.rangeName = "my-event-log";
  event_log.extraCopies = 0;
  event_log.syncedCopies = 0;
  event_log.maxWritesInFlight = 30;

  auto cluster = IntegrationTestUtils::ClusterFactory()
                     .apply(commonSetup)
                     .setLogConfig(log_config)
                     .setParam("--rebuild-store-durability",
                               GetParam() ? "async_write" : "memory")
                     .setEventLogConfig(event_log)
                     .setNumLogs(1)
                     .create(6);

  cluster->waitForRecovery();
  auto client = cluster->createClient();

  // Write some records..
  folly::Optional<lsn_t> first;
  for (int i = 1; i <= 30; ++i) {
    std::string data("data" + std::to_string(i));
    lsn_t lsn = client->appendSync(LOG_ID, Payload(data.data(), data.size()));
    ASSERT_NE(LSN_INVALID, lsn);
    if (!first.hasValue()) {
      first = lsn;
    }
  }

  ld_info("Stopping N3...");
  EXPECT_EQ(0, cluster->getNode(3).shutdown());

  ld_info("Stopping N1...");
  EXPECT_EQ(0, cluster->getNode(1).shutdown());

  // Trigger rebuilding of all shards of N3...
  ASSERT_NE(LSN_INVALID, requestShardRebuilding(*client, node_index_t{3}, 0));
  ASSERT_NE(LSN_INVALID, requestShardRebuilding(*client, node_index_t{3}, 1));
  const lsn_t sync = requestShardRebuilding(*client, node_index_t{3}, 2);
  ASSERT_NE(LSN_INVALID, sync);

  // Wait until N2, N4 and N5 realize that they need to rebuild N3's shard. In
  // order to do so we just wait until they read the 3 records we just wrote to
  // the event log.
  wait_until_event_log_synced(*cluster, sync, {2, 4, 5});

  // Now, add two more nodes to the cluster.
  int rv = cluster->expand(2);
  ASSERT_EQ(0, rv);
  // Replace N3
  ASSERT_EQ(0, cluster->replace(3));
  // Restart N1
  cluster->getNode(1).start();
  // Wait for N3 to acknowledge rebuilding of all its shards.
  cluster->getNode(3).waitUntilAllShardsFullyAuthoritative(client);
  cluster->waitForMetaDataLogWrites();
  // there is a known issue where purging deletes records that gets surfaced in
  // tests with sequencer-written metadata, which is why we skip checking
  // replication for bridge records that this may impact. See t13850978.
  // Another reason is that bridge records are also hole records and as such can
  // be rebuilt by multiple donors, leading to copyset divergence
  IntegrationTestUtils::Cluster::argv_t check_args = {
      "--dont-count-bridge-records",
  };
  // Verify that everything is correctly replicated.
  ASSERT_EQ(0, cluster->checkConsistency(check_args));
}

// Same as ClusterExpandedWhileRebuilding, but this time the rebuilding node
// (N3) comes alive after before we do the expansion, and we trigger rebuilding
// of its shards before the expansion. This means N3 will not expect the two new
// nodes to send SHARD_IS_REBUILT messages because they were not present in the
// config when rebuilding started. The two new nodes however will participate
// and send such messages, which N3 will ignore.
TEST_P(RebuildingTest, ClusterExpandedWhileRebuilding2) {
  Configuration::Log log_config;
  log_config.replicationFactor = 3;
  log_config.extraCopies = 0;
  log_config.rangeName = "my-test-log";
  log_config.syncedCopies = 0;
  log_config.maxWritesInFlight = 30;

  Configuration::Log event_log = log_config;
  event_log.replicationFactor = 3;
  event_log.rangeName = "my-event-log";
  event_log.extraCopies = 0;
  event_log.syncedCopies = 0;
  event_log.maxWritesInFlight = 30;
  auto cluster = IntegrationTestUtils::ClusterFactory()
                     .apply(commonSetup)
                     .setLogConfig(log_config)
                     .setParam("--rebuild-store-durability",
                               GetParam() ? "async_write" : "memory")
                     .setEventLogConfig(event_log)
                     .setNumLogs(1)
                     .create(6);

  cluster->waitForRecovery();
  auto client = cluster->createClient();

  // Write some records..
  folly::Optional<lsn_t> first;
  for (int i = 1; i <= 30; ++i) {
    std::string data("data" + std::to_string(i));
    lsn_t lsn = client->appendSync(LOG_ID, Payload(data.data(), data.size()));
    ASSERT_NE(LSN_INVALID, lsn);
    if (!first.hasValue()) {
      first = lsn;
    }
  }

  // Replace N3
  ASSERT_EQ(0, cluster->replace(3));
  // Trigger rebuilding of all shards of N3...
  ASSERT_NE(LSN_INVALID, requestShardRebuilding(*client, node_index_t{3}, 0));
  ASSERT_NE(LSN_INVALID, requestShardRebuilding(*client, node_index_t{3}, 1));
  const lsn_t sync = requestShardRebuilding(*client, node_index_t{3}, 2);
  ASSERT_NE(LSN_INVALID, sync);

  // Wait until N2, N3, N4 and N5 realize that they need to rebuild N3's shard.
  wait_until_event_log_synced(*cluster, sync, {2, 3, 4, 5});

  // Now, add two more nodes to the cluster.
  int rv = cluster->expand(2);
  ASSERT_EQ(0, rv);
  // Wait for N3 to acknowledge rebuilding of all its shards.
  cluster->getNode(3).waitUntilAllShardsFullyAuthoritative(client);
  cluster->waitForMetaDataLogWrites();
  // there is a known issue where purging deletes records that gets surfaced in
  // tests with sequencer-written metadata, which is why we skip checking
  // replication for bridge records that this may impact. See t13850978.
  // Another reason is that bridge records are also hole records and as such can
  // be rebuilt by multiple donors, leading to copyset divergence
  IntegrationTestUtils::Cluster::argv_t check_args = {
      "--dont-count-bridge-records",
  };
  // Verify that everything is correctly replicated.
  ASSERT_EQ(0, cluster->checkConsistency(check_args));
}

// N3's shards are being rebuilt. All donor nodes rebuilt them except for one of
// them (N5), which is removed from the config. Verify that adding N5 to the
// rebuilding set unstalls rebuilding.
TEST_P(RebuildingTest, DonorNodeRemovedFromConfigDuringRestoreRebuilding) {
  Configuration::Log log_config;
  log_config.replicationFactor = 3;
  log_config.extraCopies = 0;
  log_config.rangeName = "my-test-log";
  log_config.syncedCopies = 0;
  log_config.maxWritesInFlight = 30;

  Configuration::Log event_log = log_config;
  event_log.replicationFactor = 3;
  event_log.rangeName = "my-event-log";
  event_log.extraCopies = 0;
  event_log.syncedCopies = 0;
  event_log.maxWritesInFlight = 30;
  // Ensure metadata logs are not stored on the node we are about to remove from
  // the cluster.

  Configuration::MetaDataLogsConfig meta_config =
      createMetaDataLogsConfig({1, 2, 3, 4}, 3, NodeLocationScope::NODE);
  meta_config.sequencers_write_metadata_logs = false;
  meta_config.sequencers_provision_epoch_store = false;

  auto cluster = IntegrationTestUtils::ClusterFactory()
                     .apply(commonSetup)
                     .setLogConfig(log_config)
                     .setParam("--rebuild-store-durability",
                               GetParam() ? "async_write" : "memory")
                     .setEventLogConfig(event_log)
                     .setNumLogs(1)
                     .setMetaDataLogsConfig(meta_config)
                     .create(6);

  cluster->waitForRecovery();
  auto client = cluster->createClient();

  // N5 is the node that we will remove from the cluster.
  cluster->getNode(5).kill();

  // Replace N3
  cluster->getNode(3).kill();
  ASSERT_EQ(0, cluster->replace(3));
  // Trigger rebuilding of all shards of N3...
  ASSERT_NE(LSN_INVALID, requestShardRebuilding(*client, node_index_t{3}, 0));
  ASSERT_NE(LSN_INVALID, requestShardRebuilding(*client, node_index_t{3}, 1));
  const lsn_t sync = requestShardRebuilding(*client, node_index_t{3}, 2);
  ASSERT_NE(LSN_INVALID, sync);

  // Before shrinking, wait until N3 is made aware that its shards are
  // rebuilding and builds its donor sets that include N5.
  wait_until_event_log_synced(*cluster, sync, {3});

  // Now remove N5 from the cluster.
  const int rv = cluster->shrink(1);
  ASSERT_EQ(0, rv);

  // At this point, rebuilding should stall. Add N5 to the rebuilding set.
  ASSERT_NE(LSN_INVALID, requestShardRebuilding(*client, node_index_t{5}, 0));
  ASSERT_NE(LSN_INVALID, requestShardRebuilding(*client, node_index_t{5}, 1));
  ASSERT_NE(LSN_INVALID, requestShardRebuilding(*client, node_index_t{5}, 2));

  // Wait for N3 to acknowledge rebuilding of all its shards.
  cluster->getNode(3).waitUntilAllShardsFullyAuthoritative(client);
}

TEST_P(RebuildingTest, NodeComesBackAfterRebuildingIsComplete) {
  Configuration::Log log_config;
  log_config.replicationFactor = 3;
  log_config.extraCopies = 0;
  log_config.rangeName = "my-test-log";
  log_config.syncedCopies = 0;
  log_config.maxWritesInFlight = 30;

  Configuration::Log event_log = log_config;
  event_log.replicationFactor = 4;
  event_log.rangeName = "my-event-log";
  event_log.extraCopies = 0;
  event_log.syncedCopies = 0;
  event_log.maxWritesInFlight = 30;

  Configuration::MetaDataLogsConfig meta_config =
      createMetaDataLogsConfig({1, 2, 3, 4}, 3, NodeLocationScope::NODE);
  meta_config.sequencers_write_metadata_logs = false;
  meta_config.sequencers_provision_epoch_store = false;

  auto cluster = IntegrationTestUtils::ClusterFactory()
                     .apply(commonSetup)
                     .setLogConfig(log_config)
                     .setEventLogConfig(event_log)
                     .setParam("--rebuild-store-durability",
                               GetParam() ? "async_write" : "memory")
                     .setParam("--disable-event-log-trimming", "true")
                     .setNumLogs(1)
                     .setMetaDataLogsConfig(meta_config)
                     .create(7);

  cluster->waitForRecovery();
  auto client = cluster->createClient();

  // Kill N3
  cluster->getNode(3).kill();
  // Trigger rebuilding of all shards of N3...
  ASSERT_NE(LSN_INVALID, requestShardRebuilding(*client, node_index_t{3}, 0));
  ASSERT_NE(LSN_INVALID, requestShardRebuilding(*client, node_index_t{3}, 1));
  ASSERT_NE(LSN_INVALID, requestShardRebuilding(*client, node_index_t{3}, 2));

  // Wait until others finish rebuilding the node
  cluster->getNode(3).waitUntilAllShardsAuthoritativeEmpty(client);

  // Start N3.
  cluster->getNode(3).start();
  cluster->getNode(3).waitUntilAllShardsFullyAuthoritative(client);
}

TEST_F(RebuildingTest, ShardAckFromNodeAlreadyRebuilt) {
  Configuration::Log log_config;
  log_config.replicationFactor = 3;
  log_config.rangeName = "alog";
  log_config.extraCopies = 0;
  log_config.syncedCopies = 0;
  log_config.maxWritesInFlight = 30;

  // Ensure metadata logs are not stored on the node we are about to remove from
  // the cluster.
  Configuration::MetaDataLogsConfig meta_config =
      createMetaDataLogsConfig({1, 2, 3, 4}, 3, NodeLocationScope::NODE);
  meta_config.sequencers_write_metadata_logs = false;
  meta_config.sequencers_provision_epoch_store = false;

  auto cluster = IntegrationTestUtils::ClusterFactory()
                     .apply(commonSetup)
                     .setLogConfig(log_config)
                     .setEventLogConfig(log_config)
                     .setNumLogs(1)
                     .setMetaDataLogsConfig(meta_config)
                     .create(6);

  cluster->waitForRecovery();
  auto client = cluster->createClient();

  ld_info("Writing.");
  for (int i = 1; i <= 100; ++i) {
    std::string data("data" + std::to_string(i));
    lsn_t lsn = client->appendSync(LOG_ID, Payload(data.data(), data.size()));
    ASSERT_NE(LSN_INVALID, lsn);
  }

  // Kill N3
  cluster->getNode(3).kill();
  // Trigger rebuilding of all shards of N3...
  ASSERT_NE(LSN_INVALID, requestShardRebuilding(*client, node_index_t{3}, 0));
  ASSERT_NE(LSN_INVALID, requestShardRebuilding(*client, node_index_t{3}, 1));
  ASSERT_NE(LSN_INVALID, requestShardRebuilding(*client, node_index_t{3}, 2));

  // Wait until others finish rebuilding the node
  cluster->getNode(3).waitUntilAllShardsAuthoritativeEmpty(client);

  ld_info("Writing.");
  for (int i = 1; i <= 1000; ++i) {
    std::string data("data" + std::to_string(i));
    lsn_t lsn = client->appendSync(LOG_ID, Payload(data.data(), data.size()));
    ASSERT_NE(LSN_INVALID, lsn);
  }

  // Start draining N5
  auto flags =
      SHARD_NEEDS_REBUILD_Header::RELOCATE | SHARD_NEEDS_REBUILD_Header::DRAIN;
  ASSERT_NE(
      LSN_INVALID, requestShardRebuilding(*client, node_index_t{5}, 0, flags));
  ASSERT_NE(
      LSN_INVALID, requestShardRebuilding(*client, node_index_t{5}, 1, flags));
  ASSERT_NE(
      LSN_INVALID, requestShardRebuilding(*client, node_index_t{5}, 2, flags));

  // Now start N3
  ld_info("Starting N3");
  cluster->getNode(3).start();

  // Once the drain completes, the node's authoritative status is changed to
  // AUTHORITATIVE_EMPTY.
  ld_info("Waiting for N5 to be AUTHORITATIVE_EMPTY");
  waitUntilShardsHaveEventLogState(
      client,
      {ShardID(5, 0), ShardID(5, 1), ShardID(5, 2)},
      AuthoritativeStatus::AUTHORITATIVE_EMPTY,
      true);
}

TEST_P(RebuildingTest, NodeDrain) {
  Configuration::Log log_config;
  log_config.replicationFactor = 3;
  log_config.rangeName = "alog";
  log_config.extraCopies = 0;
  log_config.syncedCopies = 0;
  log_config.maxWritesInFlight = 30;

  // Ensure metadata logs are not stored on the node we are about to remove from
  // the cluster.
  Configuration::MetaDataLogsConfig meta_config =
      createMetaDataLogsConfig({1, 2, 3, 4}, 3, NodeLocationScope::NODE);
  meta_config.sequencers_write_metadata_logs = false;
  meta_config.sequencers_provision_epoch_store = false;

  auto cluster = IntegrationTestUtils::ClusterFactory()
                     .apply(commonSetup)
                     .setLogConfig(log_config)
                     .setEventLogConfig(log_config)
                     .setParam("--rebuild-store-durability",
                               GetParam() ? "async_write" : "memory")
                     .setNumLogs(1)
                     .setMetaDataLogsConfig(meta_config)
                     .create(6);

  cluster->waitForRecovery();
  auto client = cluster->createClient();

  // Start draining N5
  auto flags =
      SHARD_NEEDS_REBUILD_Header::RELOCATE | SHARD_NEEDS_REBUILD_Header::DRAIN;
  ASSERT_NE(
      LSN_INVALID, requestShardRebuilding(*client, node_index_t{5}, 0, flags));
  ASSERT_NE(
      LSN_INVALID, requestShardRebuilding(*client, node_index_t{5}, 1, flags));
  ASSERT_NE(
      LSN_INVALID, requestShardRebuilding(*client, node_index_t{5}, 2, flags));

  // Once the drain completes, the node's authoritative status is changed to
  // AUTHORITATIVE_EMPTY.

  waitUntilShardsHaveEventLogState(
      client,
      {ShardID(5, 0), ShardID(5, 1), ShardID(5, 2)},
      AuthoritativeStatus::AUTHORITATIVE_EMPTY,
      true);
}

// Verify that writing SHARD_UNDRAIN to the event log cancels any ongoing drain.
TEST_F(RebuildingTest, NodeDrainCanceled) {
  Configuration::Log log_config;
  log_config.replicationFactor = 3;
  log_config.rangeName = "alog";
  log_config.extraCopies = 0;
  log_config.syncedCopies = 0;
  log_config.maxWritesInFlight = 30;

  // Ensure metadata logs are not stored on the node we are about to remove from
  // the cluster.
  Configuration::MetaDataLogsConfig meta_config =
      createMetaDataLogsConfig({1, 2, 3, 4}, 3, NodeLocationScope::NODE);
  meta_config.sequencers_write_metadata_logs = false;
  meta_config.sequencers_provision_epoch_store = false;

  auto cluster = IntegrationTestUtils::ClusterFactory()
                     .apply(commonSetup)
                     .setLogConfig(log_config)
                     .setEventLogConfig(log_config)
                     .setNumLogs(1)
                     .setMetaDataLogsConfig(meta_config)
                     .create(6);

  cluster->waitForRecovery();
  auto client = cluster->createClient();

  // Stop N3 so that rebuilding stalls.
  cluster->getNode(3).shutdown();

  // Start draining N5
  auto flags =
      SHARD_NEEDS_REBUILD_Header::RELOCATE | SHARD_NEEDS_REBUILD_Header::DRAIN;
  ASSERT_NE(
      LSN_INVALID, requestShardRebuilding(*client, node_index_t{5}, 0, flags));
  ASSERT_NE(
      LSN_INVALID, requestShardRebuilding(*client, node_index_t{5}, 1, flags));
  ASSERT_NE(
      LSN_INVALID, requestShardRebuilding(*client, node_index_t{5}, 2, flags));

  // N5 should remaing fully authoritative because it's a drain.

  bool rebuilding_complete = false; // because N3 cannot participate.
  waitUntilShardsHaveEventLogState(
      client,
      {ShardID(5, 0), ShardID(5, 1), ShardID(5, 2)},
      AuthoritativeStatus::FULLY_AUTHORITATIVE,
      rebuilding_complete);

  // Request we abort draining N5
  ASSERT_NE(LSN_INVALID, markShardUndrained(*client, node_index_t{5}, 0));
  ASSERT_NE(LSN_INVALID, markShardUndrained(*client, node_index_t{5}, 1));
  ASSERT_NE(LSN_INVALID, markShardUndrained(*client, node_index_t{5}, 2));

  // Rebuilding should be aborted.

  rebuilding_complete = true; // because rebuilding was aborted.
  waitUntilShardsHaveEventLogState(
      client,
      {ShardID(5, 0), ShardID(5, 1), ShardID(5, 2)},
      AuthoritativeStatus::FULLY_AUTHORITATIVE,
      rebuilding_complete);
  cluster->getNode(5).waitUntilAllShardsFullyAuthoritative(client);
}

TEST_P(RebuildingTest, NodeDiesAfterDrain) {
  Configuration::Log log_config;
  log_config.replicationFactor = 3;
  log_config.rangeName = "alog";
  log_config.extraCopies = 0;
  log_config.syncedCopies = 0;
  log_config.maxWritesInFlight = 30;

  // Ensure metadata logs are not stored on the node we are about to remove from
  // the cluster.
  Configuration::MetaDataLogsConfig meta_config =
      createMetaDataLogsConfig({1, 2, 3, 4}, 3, NodeLocationScope::NODE);
  meta_config.sequencers_write_metadata_logs = false;
  meta_config.sequencers_provision_epoch_store = false;

  auto cluster = IntegrationTestUtils::ClusterFactory()
                     .apply(commonSetup)
                     .setLogConfig(log_config)
                     .setEventLogConfig(log_config)
                     .setParam("--rebuild-store-durability",
                               GetParam() ? "async_write" : "memory")
                     .setNumLogs(1)
                     .setMetaDataLogsConfig(meta_config)
                     .create(6);

  cluster->waitForRecovery();
  auto client = cluster->createClient();

  // Start draining N5
  auto flags =
      SHARD_NEEDS_REBUILD_Header::RELOCATE | SHARD_NEEDS_REBUILD_Header::DRAIN;
  ASSERT_NE(
      LSN_INVALID, requestShardRebuilding(*client, node_index_t{5}, 0, flags));
  ASSERT_NE(
      LSN_INVALID, requestShardRebuilding(*client, node_index_t{5}, 1, flags));
  ASSERT_NE(
      LSN_INVALID, requestShardRebuilding(*client, node_index_t{5}, 2, flags));

  cluster->getNode(5).waitUntilAllShardsAuthoritativeEmpty(client);

  // Kill N5
  cluster->getNode(5).kill();
  // Kill N3 and start rebuilding N3
  cluster->getNode(3).kill();
  ASSERT_NE(
      LSN_INVALID, requestShardRebuilding(*client, node_index_t{3}, 0, 0));
  ASSERT_NE(
      LSN_INVALID, requestShardRebuilding(*client, node_index_t{3}, 1, 0));
  ASSERT_NE(
      LSN_INVALID, requestShardRebuilding(*client, node_index_t{3}, 2, 0));

  // N5 should not be considered as donor and N3's rebuilding
  // should complete
  cluster->getNode(3).waitUntilAllShardsAuthoritativeEmpty(client);
}

// N5 is being drained, but it is removed from the config before the drain
// completes. We then restart rebuilding of N5 in RELOCATE mode this time.
TEST_P(RebuildingTest, NodeRebuildingInRelocateModeRemovedFromConfig) {
  Configuration::Log log_config;
  log_config.replicationFactor = 3;
  log_config.rangeName = "blog";
  log_config.extraCopies = 0;
  log_config.syncedCopies = 0;
  log_config.maxWritesInFlight = 30;

  // Ensure metadata logs are not stored on the node we are about to remove from
  // the cluster.
  Configuration::MetaDataLogsConfig meta_config =
      createMetaDataLogsConfig({1, 2, 3, 4}, 3, NodeLocationScope::NODE);
  meta_config.sequencers_write_metadata_logs = false;
  meta_config.sequencers_provision_epoch_store = false;

  auto cluster = IntegrationTestUtils::ClusterFactory()
                     .apply(commonSetup)
                     .setLogConfig(log_config)
                     .setEventLogConfig(log_config)
                     .setParam("--rebuild-store-durability",
                               GetParam() ? "async_write" : "memory")
                     .setNumLogs(1)
                     .setMetaDataLogsConfig(meta_config)
                     .create(6);

  cluster->waitForRecovery();
  auto client = cluster->createClient();

  // Stop N3 so that rebuilding stalls.
  cluster->getNode(3).shutdown();

  // Start draining N5
  auto flags = SHARD_NEEDS_REBUILD_Header::RELOCATE;
  ASSERT_NE(
      LSN_INVALID, requestShardRebuilding(*client, node_index_t{5}, 0, flags));
  ASSERT_NE(
      LSN_INVALID, requestShardRebuilding(*client, node_index_t{5}, 1, flags));
  const lsn_t sync = requestShardRebuilding(*client, node_index_t{5}, 2, flags);
  ASSERT_NE(LSN_INVALID, sync);

  // Before shrinking, wait until N4 is made aware that N5's shards are
  // rebuilding in RELOCATE mode.
  wait_until_event_log_synced(*cluster, sync, {4});

  // Now remove N5 from the cluster.
  cluster->getNode(5).shutdown();
  int rv = cluster->shrink(1);
  ASSERT_EQ(0, rv);

  // Start N3.
  cluster->getNode(3).start();
  cluster->getNode(3).waitUntilStarted();

  // At this point, rebuilding should stall. Add N5 to the rebuilding set in
  // RESTORE mode this time to unstall rebuilding.
  ASSERT_NE(LSN_INVALID, requestShardRebuilding(*client, node_index_t{5}, 0));
  ASSERT_NE(LSN_INVALID, requestShardRebuilding(*client, node_index_t{5}, 1));
  ASSERT_NE(LSN_INVALID, requestShardRebuilding(*client, node_index_t{5}, 2));

  // We expect the shards to become fully authoritative because
  // EventLogStateMachine trims the event log when it sees a shard that's
  // AUTHORITATIVE_EMPTY but is not in the config anymore. Readers don't care
  // since the node is not in the config, they treat it as if it was
  // AUTHORITATIVE_EMPTY.
  IntegrationTestUtils::waitUntilShardsHaveEventLogState(
      client,
      {ShardID(5, 0), ShardID(5, 1), ShardID(5, 2)},
      AuthoritativeStatus::FULLY_AUTHORITATIVE,
      true);
}

// A node is removed from the config while it is rebuilding.
// RebuildingCoordinator on donor nodes should abort rebuilding.
TEST_P(RebuildingTest, RebuildingNodeRemovedFromConfig) {
  Configuration::Log log_config;
  log_config.replicationFactor = 3;
  log_config.extraCopies = 0;
  log_config.rangeName = "my-test-log";
  log_config.syncedCopies = 0;
  log_config.maxWritesInFlight = 30;

  Configuration::Log event_log = log_config;
  event_log.replicationFactor = 3;
  event_log.rangeName = "my-event-log";
  event_log.extraCopies = 0;
  event_log.syncedCopies = 0;
  event_log.maxWritesInFlight = 30;

  // Ensure metadata logs are not stored on the node we are about to remove from
  // the cluster.
  Configuration::MetaDataLogsConfig meta_config =
      createMetaDataLogsConfig({1, 2, 3, 4}, 3, NodeLocationScope::NODE);
  meta_config.sequencers_write_metadata_logs = false;
  meta_config.sequencers_provision_epoch_store = false;

  auto cluster = IntegrationTestUtils::ClusterFactory()
                     .apply(commonSetup)
                     .setLogConfig(log_config)
                     .setParam("--rebuild-store-durability",
                               GetParam() ? "async_write" : "memory")
                     .setEventLogConfig(event_log)
                     .setNumLogs(1)
                     .setMetaDataLogsConfig(meta_config)
                     .create(6);

  cluster->waitForRecovery();
  auto client = cluster->createClient();

  // Kill N1 just to prevent rebuilding to complete immediately.
  EXPECT_EQ(0, cluster->getNode(1).shutdown());

  // Replace N5
  cluster->getNode(5).kill();
  ASSERT_EQ(0, cluster->replace(5));
  // Trigger rebuilding of all shards of N5...
  ASSERT_NE(LSN_INVALID, requestShardRebuilding(*client, node_index_t{5}, 0));
  ASSERT_NE(LSN_INVALID, requestShardRebuilding(*client, node_index_t{5}, 1));
  ASSERT_NE(LSN_INVALID, requestShardRebuilding(*client, node_index_t{5}, 2));

  // Now remove N5 from the cluster.
  int rv = cluster->shrink(1);
  ASSERT_EQ(0, rv);

  cluster->getNode(1).start();
}

// Same as RebuildingNodeRemovedFromConfig but the rebuilding node is not the
// only node in the rebuilding set.
TEST_P(RebuildingTest, RebuildingNodeRemovedFromConfigButNotAlone) {
  Configuration::Log log_config;
  log_config.replicationFactor = 3;
  log_config.extraCopies = 0;
  log_config.rangeName = "my-test-log";
  log_config.syncedCopies = 0;
  log_config.maxWritesInFlight = 30;

  Configuration::Log event_log = log_config;
  event_log.replicationFactor = 3;
  event_log.rangeName = "my-event-log";
  event_log.extraCopies = 0;
  event_log.syncedCopies = 0;
  event_log.maxWritesInFlight = 30;

  // Ensure metadata logs are not stored on the node we are about to remove from
  // the cluster.
  Configuration::MetaDataLogsConfig meta_config =
      createMetaDataLogsConfig({1, 2, 3, 4}, 3, NodeLocationScope::NODE);
  meta_config.sequencers_write_metadata_logs = false;
  meta_config.sequencers_provision_epoch_store = false;

  auto cluster = IntegrationTestUtils::ClusterFactory()
                     .apply(commonSetup)
                     .setLogConfig(log_config)
                     .setParam("--rebuild-store-durability",
                               GetParam() ? "async_write" : "memory")
                     .setEventLogConfig(event_log)
                     .setNumLogs(1)
                     .setMetaDataLogsConfig(meta_config)
                     .create(7);

  cluster->waitForRecovery();
  auto client = cluster->createClient();

  // Kill N1 just to prevent rebuilding to complete immediately.
  cluster->getNode(1).shutdown();

  // Replace N6
  cluster->getNode(6).kill();
  ASSERT_EQ(0, cluster->replace(6));
  // Trigger rebuilding of all shards of N6...
  ASSERT_NE(LSN_INVALID, requestShardRebuilding(*client, node_index_t{6}, 0));
  ASSERT_NE(LSN_INVALID, requestShardRebuilding(*client, node_index_t{6}, 1));
  ASSERT_NE(LSN_INVALID, requestShardRebuilding(*client, node_index_t{6}, 2));

  // Replace N5
  cluster->getNode(5).kill();
  ASSERT_EQ(0, cluster->replace(5));
  // Trigger rebuilding of all shards of N5...
  ASSERT_NE(LSN_INVALID, requestShardRebuilding(*client, node_index_t{5}, 0));
  ASSERT_NE(LSN_INVALID, requestShardRebuilding(*client, node_index_t{5}, 1));
  ASSERT_NE(LSN_INVALID, requestShardRebuilding(*client, node_index_t{5}, 2));

  // Now remove N6 from the cluster.
  int rv = cluster->shrink(1);
  ASSERT_EQ(0, rv);

  cluster->getNode(1).start();

  // Wait until N5 is rebuilt.
  cluster->getNode(5).waitUntilAllShardsFullyAuthoritative(client);
}

// Test a scenario where an f-majority of nodes is in the nodeset. This makes it
// impossible for RecordRebuilding state machines to make progress so it should
// try to re-replicate as much as possible but records will remain
// under-replicated. In this test we want to verify that this situation does not
// cause rebuilding to stall.
TEST_P(RebuildingTest, FMajorityInRebuildingSet) {
  Configuration::Log log_config;
  log_config.replicationFactor = 3;
  log_config.extraCopies = 0;
  log_config.rangeName = "my-test-log";
  log_config.syncedCopies = 0;
  log_config.maxWritesInFlight = 30;
  log_config.backlogDuration = std::chrono::hours{6};

  Configuration::Log event_log = log_config;
  event_log.replicationFactor = 3;
  event_log.rangeName = "my-event-log";
  event_log.extraCopies = 0;
  event_log.syncedCopies = 0;
  event_log.maxWritesInFlight = 30;
  event_log.backlogDuration.clear();

  auto cluster = IntegrationTestUtils::ClusterFactory()
                     .apply(commonSetup)
                     .setParam("--rebuild-store-durability",
                               GetParam() ? "async_write" : "memory")
                     .setLogConfig(log_config)
                     .setEventLogConfig(event_log)
                     .setNumLogs(42)
                     .create(6);

  // Write some records
  auto client = cluster->createClient();
  for (int i = 1; i <= 100; ++i) {
    std::string data("data" + std::to_string(i));
    lsn_t lsn = client->appendSync(LOG_ID, Payload(data.data(), data.size()));
    ASSERT_NE(LSN_INVALID, lsn);
  }

  cluster->waitForMetaDataLogWrites();
  cluster->waitForRecovery();

  for (node_index_t node = 1; node <= 4; ++node) {
    EXPECT_EQ(0, cluster->getNode(node).shutdown());
    auto shard_path = path_for_node_shard(*cluster, node, 1);
    for (fs::directory_iterator end_dir_it, it(shard_path); it != end_dir_it;
         ++it) {
      fs::remove_all(it->path());
    }
    ASSERT_EQ(0, cluster->bumpGeneration(node));
    cluster->getNode(node).start();
    cluster->getNode(node).waitUntilStarted();
  }

  // 4 nodes in the rebuilding set is too much, it's impossible to rebuild the
  // records that have a replication factor of 3 since there are 6-4=2 nodes not
  // in the set. LogRebuilding should skip the epochs and rebuilding should not
  // stall.
  for (node_index_t node = 1; node <= 4; ++node) {
    ASSERT_NE(LSN_INVALID, requestShardRebuilding(*client, node, 1));
  }

  IntegrationTestUtils::waitUntilShardsHaveEventLogState(
      client,
      {ShardID(1, 1), ShardID(2, 1), ShardID(3, 1), ShardID(4, 1)},
      AuthoritativeStatus::FULLY_AUTHORITATIVE,
      true);
}

// Rebuild using a local window of 5ms
// Note that in this test, some logs have a backlog and some logs don't have one
// (event log). Logs that have a backlog are put in the wakeupQueue with an
// estimate of their `nextTimestamp` equal to `now() - backlog` while logs that
// don't have a backlog are put on the wakeupQueue with a `nextTimestamp` equal
// to -inf. This mechanism increases the chances that the first time we read a
// batch of records for a log that has a backlog duration configured, we
// actually read some data instead of reading nothing and just updating
// `nextTimestamp`.
// In order to exercise conditions where both logs with a backlog duration and
// logs without one are scheduled for rebuilding, we rebuild shard 0 which
// includes the event log.
TEST_P(RebuildingTest, LocalWindow) {
  Configuration::Log log_config;
  log_config.replicationFactor = 3;
  log_config.extraCopies = 0;
  log_config.syncedCopies = 0;
  log_config.rangeName = "test-log-group";
  log_config.maxWritesInFlight = 30;
  log_config.backlogDuration = std::chrono::hours{6};

  Configuration::Log event_log_config;
  event_log_config.replicationFactor = 3;
  event_log_config.extraCopies = 0;
  event_log_config.syncedCopies = 0;
  event_log_config.rangeName = "event_log";
  event_log_config.maxWritesInFlight = 30;

  auto cluster = IntegrationTestUtils::ClusterFactory()
                     .apply(commonSetup)
                     .setParam("--rebuild-store-durability",
                               GetParam() ? "async_write" : "memory")
                     .setLogConfig(log_config)
                     .setEventLogConfig(event_log_config)
                     .setNumLogs(42)
                     .create(5);

  // Write some records
  auto client = cluster->createClient();
  for (int i = 1; i <= 100; ++i) {
    std::string data("data" + std::to_string(i));
    lsn_t lsn = client->appendSync(LOG_ID, Payload(data.data(), data.size()));
    /* sleep override */
    std::this_thread::sleep_for(std::chrono::milliseconds(1));
    ASSERT_NE(LSN_INVALID, lsn);
  }

  cluster->waitForRecovery();

  EXPECT_EQ(0, cluster->getNode(1).shutdown());
  auto shard_path = path_for_node_shard(*cluster, node_index_t(1), 0);
  for (fs::directory_iterator end_dir_it, it(shard_path); it != end_dir_it;
       ++it) {
    fs::remove_all(it->path());
  }
  ASSERT_EQ(0, cluster->bumpGeneration(1));
  cluster->getNode(1).start();
  cluster->getNode(1).waitUntilStarted();

  ASSERT_NE(LSN_INVALID, requestShardRebuilding(*client, 1, 0));

  cluster->getNode(1).waitUntilAllShardsFullyAuthoritative(client);
  cluster->waitForMetaDataLogWrites();
  // there is a known issue where purging deletes records that gets surfaced in
  // tests with sequencer-written metadata, which is why we skip checking
  // replication for bridge records that this may impact. See t13850978
  IntegrationTestUtils::Cluster::argv_t check_args = {
      "--dont-count-bridge-records",
  };
  // Verify that everything is correctly replicated.
  ASSERT_EQ(0, cluster->checkConsistency(check_args));
}

// Same as `LocalWindow` but there is also a 30ms global window.
TEST_P(RebuildingTest, LocalAndGlobalWindow) {
  Configuration::Log log_config;
  log_config.replicationFactor = 3;
  log_config.extraCopies = 0;
  log_config.rangeName = "test-log";
  log_config.syncedCopies = 0;
  log_config.maxWritesInFlight = 30;
  log_config.backlogDuration = std::chrono::hours{6};

  Configuration::Log event_log_config;
  event_log_config.replicationFactor = 3;
  event_log_config.extraCopies = 0;
  event_log_config.rangeName = "event-log";
  event_log_config.syncedCopies = 0;
  event_log_config.maxWritesInFlight = 30;

  auto cluster = IntegrationTestUtils::ClusterFactory()
                     .apply(commonSetup)
                     .setParam("--rebuild-store-durability",
                               GetParam() ? "async_write" : "memory")
                     .setLogConfig(log_config)
                     .setEventLogConfig(event_log_config)
                     .setParam("--rebuilding-local-window", "5ms")
                     .setParam("--rebuilding-global-window", "30ms")
                     .setNumLogs(42)
                     .create(5);

  // Write some records
  auto client = cluster->createClient();
  for (int i = 1; i <= 100; ++i) {
    std::string data("data" + std::to_string(i));
    lsn_t lsn = client->appendSync(LOG_ID, Payload(data.data(), data.size()));
    /* sleep override */
    std::this_thread::sleep_for(std::chrono::milliseconds(1));
    ASSERT_NE(LSN_INVALID, lsn);
  }

  cluster->waitForRecovery();

  EXPECT_EQ(0, cluster->getNode(1).shutdown());
  cluster->getNode(1).wipeShard(0);
  ASSERT_EQ(0, cluster->bumpGeneration(1));
  cluster->getNode(1).start();
  cluster->getNode(1).waitUntilStarted();

  ASSERT_NE(LSN_INVALID, requestShardRebuilding(*client, 1, 0));

  cluster->getNode(1).waitUntilAllShardsFullyAuthoritative(client);
  cluster->waitForMetaDataLogWrites();
  // there is a known issue where purging deletes records that gets surfaced in
  // tests with sequencer-written metadata, which is why we skip checking
  // replication for bridge records that this may impact. See t13850978
  IntegrationTestUtils::Cluster::argv_t check_args = {
      "--dont-count-bridge-records",
  };
  // Verify that everything is correctly replicated.
  ASSERT_EQ(0, cluster->checkConsistency(check_args));
}

// Kill/Restart each storage node one by one. No extras.
TEST_P(RebuildingTest, RollingMiniRebuilding) {
  int nnodes = 5;
  int r = 3;
  int x = 0;
  bool trim = true;
  // Because the failed node may still hame some copies of the data,
  // tolerate copyset divergence.
  IntegrationTestUtils::Cluster::argv_t check_args = {
      "--dont-fail-on",
      "DIFFERENT_COPYSET_IN_LATEST_WAVE,"
      "NO_ACCURATE_COPYSET,"
      "BAD_REPLICATION_LAST_WAVE",
      // there is a known issue where purging deletes records that gets surfaced
      // in tests with sequencer-written metadata, which is why we skip checking
      // replication for bridge records that this may impact. See t13850978
      "--dont-count-bridge-records"};
  auto cf = rollingRebuildingClusterFactory(nnodes, r, x, trim);
  cf.setParam("--append-store-durability", "memory")
      // Writes with ASYNC_WRITE durability (the default) may be lost even
      // though they are appended to the WAL. Use SYNC_WRITE durability for
      // rebuilding stores so that we can rely on them being available even if
      // a node is killed. We are only testing recovery from loss of appends.
      .setParam("--rebuild-store-durability", "sync_write")
      // Amends rely on pre-existing append data. Avoid false-positive
      // dataloss since WALless rebuilding is not enabled and the
      // rebuilding code will only attempt to rebuild append data.
      .setParam("--rebuild-without-amends", "true")
      // Set min flush trigger intervals and partition duration high
      // so that only the test is creating/retiring partitions.
      .setParam("--rocksdb-min-manual-flush-interval", "900s")
      .setParam("--rocksdb-partition-duration", "900s")
      // Decrease the timestamp granularity so that we can minimize the
      // amount of wall clock delay required for this test to create adjacent
      // partitions with non-overlapping time ranges.
      .setParam("--rocksdb-partition-timestamp-granularity", "100ms")
      // To ensure that all nodes receive at least some data when we dirty
      // them, adjust the copyset block size so we get a copyset shuffle
      // every ~6 records.
      .setParam("--sticky-copysets-block-size", "128")
      // Use only a single shard so that partition creation/flushing commands
      // can be unambiguously targetted.
      .setNumDBShards(1);

  rollingRebuilding(cf, nnodes, 1, 4, 1, NodeFailureMode::KILL, check_args);
}

// Verify that Mini-Rebuildings do not prevent a rebuild from being
// considered authoritative.
TEST_P(RebuildingTest, MiniRebuildingIsAuthoritative) {
  Configuration::Log log_config;
  log_config.replicationFactor = 3;
  log_config.rangeName = "my-test-log";
  log_config.extraCopies = 0;
  log_config.syncedCopies = 0;
  log_config.maxWritesInFlight = 30;

  Configuration::Log event_log = log_config;
  event_log.replicationFactor = 3;
  event_log.rangeName = "my-event-log";
  event_log.extraCopies = 0;
  event_log.syncedCopies = 0;
  event_log.maxWritesInFlight = 30;

  NodeSetIndices node_set(5);
  std::iota(node_set.begin(), node_set.end(), 0);
  auto cluster =
      IntegrationTestUtils::ClusterFactory()
          .apply(commonSetup)
          .setLogConfig(log_config)
          .setEventLogConfig(event_log)
          .setParam("--append-store-durability", "memory")
          // Set min flush trigger intervals and partition duration high
          // so that only the test is creating/retiring partitions.
          .setParam("--rocksdb-min-manual-flush-interval", "900s")
          .setParam("--rocksdb-partition-duration", "900s")
          // Decrease the timestamp granularity so that we can minimize the
          // amount of wall clock delay required for this test to create
          // adjacent partitions with non-overlapping time ranges.
          .setParam("--rocksdb-partition-timestamp-granularity", "100ms")
          // To ensure that all nodes receive at least some data when we dirty
          // them, adjust the copyset block size so we get a copyset shuffle
          // every ~6 records.
          .setParam("--sticky-copysets-block-size", "128")
          // Use only a single shard so that partition creation/flushing
          // commands can be unambiguously targetted.
          .setNumDBShards(1)
          .create(5);

  cluster->waitForRecovery();

  auto client = cluster->createClient();

  // Write some records..
  folly::Optional<lsn_t> batch_start;
  dirtyNodes(*cluster, *client, node_set, /*shard*/ 0, batch_start);

  // Kill all nodes
  for (auto node : node_set) {
    cluster->getNode(node).kill();
  }

  // Restart all nodes
  for (auto node : node_set) {
    cluster->getNode(node).start();
  }

  EventLogRebuildingSet base_set;
  wait_until("self initiated rebuild", [&]() {
    EventLogRebuildingSet polled_set;
    const int rv = EventLogUtils::getRebuildingSet(*client, polled_set);
    if (rv != 0) {
      return false;
    }
    return polled_set.getLastUpdate() > base_set.getLastUpdate();
  });

  wait_until("empty rebuilding set", [&]() {
    EventLogRebuildingSet polled_set;
    const int rv = EventLogUtils::getRebuildingSet(*client, polled_set);
    if (rv != 0) {
      return false;
    }
    return polled_set.empty();
  });
}

// We shouldn't have to explicitly mark dirty-nodes unrecoverable
// when they cause other shards to be rebuilt non-authoritatively.
TEST_F(RebuildingTest, MiniRebuildingAlwaysNonRecoverable) {
  // Higher replication factor for event log and metadata logs.
  Configuration::MetaDataLogsConfig meta_config = createMetaDataLogsConfig(
      /*nodeset=*/{1, 2, 3, 4, 5, 6, 7, 8},
      /*replication=*/5,
      NodeLocationScope::NODE);
  meta_config.sequencers_provision_epoch_store = false;
  meta_config.sequencers_write_metadata_logs = false;

  auto cluster =
      IntegrationTestUtils::ClusterFactory()
          .apply(commonSetup)
          .setLogConfig(logConfig(3))
          .setEventLogConfig(logConfig(5))
          .setParam("--rebuild-store-durability", "async_write")
          .setMetaDataLogsConfig(meta_config)
          // read quickly when nodes are down
          .setParam("--gap-grace-period", "10ms")
          // fall back to non-authoritative quickly
          .setParam("--event-log-grace-period", "10ms")
          .setNumLogs(42)
          // Enable appends without the WAL.
          .setParam("--append-store-durability", "memory")
          // Set min flush trigger intervals and partition duration high
          // so that only the test is creating/retiring partitions.
          .setParam("--rocksdb-min-manual-flush-interval", "900s")
          .setParam("--rocksdb-partition-duration", "900s")
          // Decrease the timestamp granularity so that we can minimize the
          // amount of wall clock delay required for this test to create
          // adjacent partitions with non-overlapping time ranges.
          .setParam("--rocksdb-partition-timestamp-granularity", "100ms")
          // To ensure that all nodes receive at least some data when we dirty
          // them, adjust the copyset block size so we get a copyset shuffle
          // on every record.
          .setParam("--sticky-copysets-block-size", "10")
          // Use only a single shard so that partition creation/flushing
          // commands can be unambiguously targetted.
          .setNumDBShards(1)
          .create(9); // 1 sequencer node + 8 storage nodes

  cluster->waitForRecovery();

  auto client = cluster->createClient();
  EventLogRebuildingSet base_set;
  ASSERT_EQ(EventLogUtils::getRebuildingSet(*client, base_set), 0);
  ASSERT_TRUE(base_set.empty());

  NodeSetIndices full_node_set(8);
  std::iota(full_node_set.begin(), full_node_set.end(), 1);

  // First three require full shard rebuilding.
  NodeSetIndices unrecoverable_node_set(
      full_node_set.begin(), full_node_set.begin() + 3);
  // The remainder get a crash restart.
  NodeSetIndices dirty_node_set(full_node_set.begin() + 3, full_node_set.end());

  folly::Optional<lsn_t> batch_start;
  dirtyNodes(*cluster, *client, full_node_set, /*shard*/ 0, batch_start);

  // Kill unrecoverable_nodes and clean their DBs. They'll be be marked
  // unrecoverable later.
  ld_info("Killing and removing data from nodes.");
  for (node_index_t node : unrecoverable_node_set) {
    cluster->getNode(node).kill();
    auto shard_path = path_for_node_shard(*cluster, node, /*shard*/ 0);
    for (fs::directory_iterator end_dir_it, it(shard_path); it != end_dir_it;
         ++it) {
      fs::remove_all(it->path());
    }
  }

  // Kill dirty_nodes and clean their DBs. They'll be be marked
  // unrecoverable later.
  ld_info("Killing and restarting nodes.");
  for (node_index_t node : dirty_node_set) {
    cluster->getNode(node).kill();
    cluster->getNode(node).start();
  }

  wait_until("self initiated rebuild", [&]() {
    EventLogRebuildingSet polled_set;
    const int rv = EventLogUtils::getRebuildingSet(*client, polled_set);
    if (rv != 0) {
      return false;
    }
    return polled_set.getLastUpdate() > base_set.getLastUpdate();
  });

  // Should still have write availability. Write some records.
  ld_info("Writing.");
  for (int i = 1; i <= 30; ++i) {
    std::string data("data" + std::to_string(i));
    lsn_t lsn = client->appendSync(LOG_ID, Payload(data.data(), data.size()));
    ASSERT_NE(LSN_INVALID, lsn);
  }

  // Restart sequencer node. It should get stuck in recovery.
  ld_info("Restarting sequencer.");
  cluster->getNode(0).kill();
  cluster->getNode(0).start();
  cluster->getNode(0).waitUntilStarted();
  {
    const int rv = cluster->waitForRecovery(std::chrono::steady_clock::now() +
                                            std::chrono::seconds(1));
    // Not recovered after one second.
    EXPECT_EQ(-1, rv);
  }

  // Dirty nodes were rebuilt non-authoritatively and so should still
  // be in the rebuilding set.
  ASSERT_EQ(EventLogUtils::getRebuildingSet(*client, base_set), 0);
  ASSERT_FALSE(base_set.empty());
  ld_error("RebuildingSet now %s", base_set.toString().c_str());

  ASSERT_EQ(EventLogUtils::getRebuildingSet(*client, base_set), 0);
  for (node_index_t node : dirty_node_set) {
    const auto* node_info = base_set.getNodeInfo(node, /*shard*/ 0);
    ASSERT_NE(node_info, nullptr);
    ASSERT_FALSE(node_info->dc_dirty_ranges.empty());
  }

  // Writing SHARD_NEEDS_REBUILD for the unrecoverable nodes to the event log.
  ld_info("Requesting rebuildings.");
  for (node_index_t node : unrecoverable_node_set) {
    // Before processing the last node check that recovery still doesn't move.
    if (node == 3) {
      const int rv = cluster->waitForRecovery(std::chrono::steady_clock::now() +
                                              std::chrono::seconds(1));
      EXPECT_EQ(-1, rv);
    }
    ASSERT_NE(LSN_INVALID, requestShardRebuilding(*client, node, /*shard*/ 0));
    ASSERT_NE(LSN_INVALID, markShardUnrecoverable(*client, node, /*shard*/ 0));
  }

  // Now recovery should finish.
  ld_info("Waiting for recovery.");
  cluster->waitForRecovery();

  auto stats = cluster->getNode(0).stats();
  ASSERT_GT(stats["non_auth_recovery_epochs"], 0);

  // Read event log to wait for donors 4-8 to finish rebuilding,
  // with nodes 1-3 still down.
  ld_info("Waiting for rebuilding.");
  std::vector<ShardID> to_rebuild;
  for (node_index_t node : unrecoverable_node_set) {
    to_rebuild.push_back(ShardID(node, /*shard*/ 0));
  }
  waitUntilShardsHaveEventLogState(
      client, to_rebuild, AuthoritativeStatus::AUTHORITATIVE_EMPTY, true);

  // Start nodes 1-3 and wait for them to ack the rebuilding.
  ld_info("Starting nodes.");
  for (node_index_t node = 1; node <= 3; ++node) {
    ASSERT_EQ(0, cluster->bumpGeneration(node));
    cluster->getNode(node).start();
  }
  waitUntilShardsHaveEventLogState(
      client, to_rebuild, AuthoritativeStatus::FULLY_AUTHORITATIVE, true);

  wait_until("empty rebuilding set", [&]() {
    EventLogRebuildingSet polled_set;
    const int rv = EventLogUtils::getRebuildingSet(*client, polled_set);
    if (rv != 0) {
      return false;
    }
    ld_error("RebuildingSet now %s", polled_set.toString().c_str());
    return polled_set.empty();
  });
}

TEST_F(RebuildingTest, RebuildingWithDifferentDurabilities) {
  Configuration::Log log_config;
  log_config.replicationFactor = 4;
  log_config.extraCopies = 0;
  log_config.syncedCopies = 0;
  log_config.rangeName = "test-log-group";
  log_config.maxWritesInFlight = 30;
  log_config.backlogDuration = std::chrono::hours{6};

  Configuration::Log event_log_config;
  event_log_config.replicationFactor = 3;
  event_log_config.extraCopies = 0;
  event_log_config.syncedCopies = 0;
  event_log_config.rangeName = "event_log";
  event_log_config.maxWritesInFlight = 30;

  auto cluster = IntegrationTestUtils::ClusterFactory()
                     .apply(commonSetup)
                     .setParam("--rebuild-store-durability", "async_write")
                     .setLogConfig(log_config)
                     .setEventLogConfig(event_log_config)
                     .setNumLogs(42)
                     .create(5);

  // Write some records
  auto client = cluster->createClient();
  for (int i = 1; i <= 1000; ++i) {
    std::string data("data" + std::to_string(i));
    lsn_t lsn = client->appendSync(LOG_ID, Payload(data.data(), data.size()));
    /* sleep override */
    std::this_thread::sleep_for(std::chrono::milliseconds(1));
    ASSERT_NE(LSN_INVALID, lsn);
  }

  cluster->waitForRecovery();

  cluster->getNode(2).sendCommand("set rebuild-store-durability memory");
  cluster->getNode(3).sendCommand("set rebuild-store-durability memory");

  EXPECT_EQ(0, cluster->getNode(1).shutdown());
  auto shard_path = path_for_node_shard(*cluster, node_index_t(1), 0);
  for (fs::directory_iterator end_dir_it, it(shard_path); it != end_dir_it;
       ++it) {
    fs::remove_all(it->path());
  }
  ASSERT_EQ(0, cluster->bumpGeneration(1));
  cluster->getNode(1).start();
  cluster->getNode(1).waitUntilStarted();

  ASSERT_NE(LSN_INVALID, requestShardRebuilding(*client, 1, 0));

  cluster->getNode(1).waitUntilAllShardsFullyAuthoritative(client);
  cluster->waitForMetaDataLogWrites();
  // there is a known issue where purging deletes records that gets surfaced in
  // tests with sequencer-written metadata, which is why we skip checking
  // replication for bridge records that this may impact. See t13850978
  IntegrationTestUtils::Cluster::argv_t check_args = {
      "--dont-count-bridge-records",
  };
  // Verify that everything is correctly replicated.
  ASSERT_EQ(0, cluster->checkConsistency(check_args));
}

// Delete some logs, then replace each storage node one by one, re-add them and
// check that their metadata is still intact
TEST_P(RebuildingTest, RebuildMetaDataLogsOfDeletedLogs) {
  int nnodes = 5;
  auto cf = rollingRebuildingClusterFactory(nnodes, 3, 0, true).setNumLogs(2);
  auto cluster = cf.create(nnodes);
  auto get_metadata_record_count = [&](logid_t log_id) {
    std::shared_ptr<Client> client = cluster->createIndependentClient();
    lsn_t until_lsn =
        client->getTailLSNSync(MetaDataLog::metaDataLogID(log_id));
    ld_info("Reading metadata log for %lu until lsn %s",
            log_id.val(),
            lsn_to_string(until_lsn).c_str());
    auto reader = client->createReader(1);

    reader->setTimeout(DEFAULT_TEST_TIMEOUT);
    int rv = reader->startReading(
        MetaDataLog::metaDataLogID(log_id), lsn_t(1), until_lsn);
    ld_check(rv == 0);

    std::vector<std::unique_ptr<DataRecord>> records;
    GapRecord gap;

    // number of metadata log records
    size_t rec_count = 0;
    ssize_t count;
    do {
      count = reader->read(1, &records, &gap);
      if (count == -1) {
        EXPECT_EQ(E::GAP, err);
        EXPECT_NE(GapType::NOTINCONFIG, gap.type);
      } else {
        rec_count += count;
      }
    } while (count != 0);
    return rec_count;
  };

  ASSERT_EQ(1, get_metadata_record_count(logid_t(2)));

  auto change_logs_config = [&](logid_range_t expected_range,
                                logid_range_t new_range) {
    auto logs_config_changed =
        cluster->getConfig()->getLocalLogsConfig()->copyLocal();
    auto& logs =
        const_cast<logsconfig::LogMap&>(logs_config_changed->getLogMap());
    auto log_in_directory = logs.begin()->second;
    ASSERT_EQ(expected_range, log_in_directory.log_group->range());
    ASSERT_TRUE(logs_config_changed->replaceLogGroup(
        log_in_directory.getFullyQualifiedName(),
        log_in_directory.log_group->withRange(new_range)));
    auto& tree = const_cast<logsconfig::LogsConfigTree&>(
        logs_config_changed->getLogsConfigTree());
    // Setting the newer tree version so the config gets actually written
    tree.setVersion(tree.version() + 1);

    cluster->writeLogsConfig(logs_config_changed.get());
    cluster->waitForConfigUpdate();
  };

  ld_info("Changing config with removed log_id");
  change_logs_config(logid_range_t(logid_t(1), logid_t(2)),
                     logid_range_t(logid_t(1), logid_t(1)));

  // TODO: T23153817, T13850978
  // Remove this once we have a fool-proof
  // solution against purging deleteing records
  IntegrationTestUtils::Cluster::argv_t check_args = {
      "--dont-count-bridge-records",
  };
  rollingRebuilding(
      *cluster, 1, nnodes - 1, NodeFailureMode::REPLACE, check_args);

  ld_info("Changing config with re-added log_id");
  change_logs_config(logid_range_t(logid_t(1), logid_t(1)),
                     logid_range_t(logid_t(1), logid_t(2)));

  ASSERT_EQ(1, get_metadata_record_count(logid_t(2)));
}

// Create under-replicated regions of the log store on a node and
// verify that a reader can successfully read without seeing
// spurious dataloss gaps.
TEST_F(RebuildingTest, UnderReplicatedRegions) {
  Configuration::Log log_config;
  log_config.replicationFactor = 3;
  log_config.rangeName = "my-test-log";
  log_config.extraCopies = 0;
  log_config.syncedCopies = 0;
  log_config.maxWritesInFlight = 30;

  Configuration::Log event_log = log_config;
  event_log.replicationFactor = 3;
  event_log.rangeName = "my-event-log";
  event_log.extraCopies = 0;
  event_log.syncedCopies = 0;
  event_log.maxWritesInFlight = 30;

  NodeSetIndices node_set(5);
  std::iota(node_set.begin(), node_set.end(), 0);
  auto cluster =
      IntegrationTestUtils::ClusterFactory()
          .apply(commonSetup)
          .setLogConfig(log_config)
          .setEventLogConfig(event_log)
          .setParam("--append-store-durability", "memory")
          // Set min flush trigger intervals and partition duration high
          // so that only the test is creating/retiring partitions.
          .setParam("--rocksdb-min-manual-flush-interval", "900s")
          .setParam("--rocksdb-partition-duration", "900s")
          // Decrease the timestamp granularity so that we can minimize the
          // amount of wall clock delay required for this test to create
          // adjacent partitions with non-overlapping time ranges.
          .setParam("--rocksdb-partition-timestamp-granularity", "100ms")
          // To ensure that all nodes receive at least some data when we dirty
          // them, adjust the copyset block size so we get a copyset shuffle
          // every ~6 records.
          .setParam("--sticky-copysets-block-size", "128")
          // Don't request rebuilding of dirty shards so that they stay dirty
          // while clients read data.
          .setParam("--rebuild-dirty-shards", "false")
          // Use only a single shard so that partition creation/flushing
          // commands can be unambiguously targetted.
          .setNumDBShards(1)
          .create(5);

  cluster->waitForRecovery();

  auto client = cluster->createClient();
  client->settings().set("gap-grace-period", "0ms");

  // Write some records..
  folly::Optional<lsn_t> batch_start;
  size_t nrecords = 0;
  nrecords = dirtyNodes(*cluster, *client, node_set, /*shard*/ 0, batch_start);

  // Kill node and restart node 2
  cluster->getNode(2).kill();
  cluster->getNode(2).start();
  cluster->getNode(2).waitUntilStarted();

  cluster->waitForRecovery();

  // Write more records.
  nrecords += dirtyNodes(*cluster, *client, node_set, /*shard*/ 0, batch_start);

  auto reader = client->createReader(1);
  int rv = reader->startReading(LOG_ID, LSN_OLDEST);
  ASSERT_EQ(rv, 0);

  size_t total_read = 0;
  std::vector<std::unique_ptr<DataRecord>> data;
  reader->setTimeout(std::chrono::seconds(1));
  while (1) {
    GapRecord gap;
    int nread = reader->read(nrecords - total_read, &data, &gap);
    if (nread < 0) {
      EXPECT_EQ(err, E::GAP);
      EXPECT_EQ(gap.type, GapType::BRIDGE);
      continue;
    }
    if (nread == 0) {
      break;
    }
    total_read += nread;
    if (total_read >= nrecords) {
      break;
    }
  }
  EXPECT_EQ(total_read, nrecords);

  cluster->shutdownNodes(node_set);
}

// Do a rebuilding that doesn't have anything to rebuild but doesn't know it in
// advance. Check that it skips all the records using csi and does so using
// near-minimum number of seeks/nexts of csi iterator.
TEST_P(RebuildingTest, SkipEverything) {
  Configuration::Log log_config;
  log_config.replicationFactor = 2;
  log_config.rangeName = "a";
  log_config.extraCopies = 0;
  log_config.syncedCopies = 0;
  log_config.maxWritesInFlight = 1000;

  Configuration::Log event_log = log_config;
  event_log.replicationFactor = 2;
  event_log.rangeName = "ev";
  event_log.extraCopies = 0;
  event_log.syncedCopies = 0;
  event_log.maxWritesInFlight = 30;

  Configuration::MetaDataLogsConfig meta_config = createMetaDataLogsConfig(
      /*nodeset=*/{0, 1},
      /*replication=*/2,
      NodeLocationScope::NODE);

  Configuration::Nodes nodes(3);
  for (int i = 0; i < 3; ++i) {
    nodes[i].num_shards = 1;
    nodes[i].generation = 1;
    nodes[i].sequencer_weight = 1.0;
    nodes[i].storage_state = i == 2 ? configuration::StorageState::READ_ONLY
                                    : configuration::StorageState::READ_WRITE;
  }

  ld_info("Creating cluster");
  auto cluster = IntegrationTestUtils::ClusterFactory()
                     .apply(commonSetup)
                     .setLogConfig(log_config)
                     .setEventLogConfig(event_log)
                     .setMetaDataLogsConfig(meta_config)
                     .setNodes(nodes)
                     .setNumDBShards(1)
                     .useHashBasedSequencerAssignment()
                     .setParam("--rocksdb-new-partition-timestamp-margin", "0s")
                     .create(3);

  cluster->waitForRecovery();

  ld_info("Creating client");
  auto client = cluster->createClient();
  ld_info("Writing records");
  for (int i = 0; i < 20; ++i) {
    writeRecords(*client, 20);
    createPartition(*cluster, {0, 1}, 0);
    // Make sure that all partitions have different timestamps (they have
    // millisecond granularity).
    /* sleep override */
    std::this_thread::sleep_for(std::chrono::milliseconds(1));
  }

  ld_info("Requesting drain of N2");
  ASSERT_NE(
      LSN_INVALID,
      requestShardRebuilding(*client, 2, 0, SHARD_NEEDS_REBUILD_Header::DRAIN));

  ld_info("Waiting for rebuilding of N2");
  waitUntilShardHasEventLogState(
      client, ShardID(2, 0), AuthoritativeStatus::AUTHORITATIVE_EMPTY, true);

  ld_info("Checking stats");
  for (int n = 0; n <= 1; ++n) {
    auto stats = cluster->getNode(n).stats();
    auto check_stat = [&](const char* name, int64_t min, int64_t max) {
      SCOPED_TRACE(std::to_string(n) + ' ' + name);
      ASSERT_EQ(1, stats.count(name));
      int64_t val = folly::to<int64_t>(stats.at(name));
      ld_info("N%d: %s = %ld", n, name, val);
      EXPECT_GE(val, min);
      EXPECT_LE(val, max);
    };
    check_stat("read_streams_rocksdb_locallogstore_csi_next_reads", 300, 600);
    check_stat("read_streams_rocksdb_locallogstore_csi_seek_reads", 1, 200);
    check_stat("read_streams_rocksdb_locallogstore_record_next_reads", 0, 200);
    check_stat("read_streams_rocksdb_locallogstore_record_seek_reads", 0, 200);
  }
}

TEST_P(RebuildingTest, DerivedStats) {
  ld_info("Creating cluster");
  auto cluster =
      IntegrationTestUtils::ClusterFactory()
          .apply(commonSetup)
          .setNumDBShards(1)
          .useHashBasedSequencerAssignment()
          .setInternalLogsReplicationFactor(3)
          // A hack to prevent self-initiated rebuilding of empty shards.
          .setParam("--event-log-grace-period", "120s")
          .create(5);

  auto stats = cluster->getNode(1).stats();
  EXPECT_EQ(0, stats["shards_waiting_for_non_started_restore"]);
  EXPECT_EQ(0, stats["non_empty_shards_in_restore"]);

  ld_info("Shutting down N1 and N2");
  EXPECT_EQ(0, cluster->shutdownNodes({1, 2}));
  // Wipe N1 and start it.
  ld_info("Wiping N1");
  cluster->getNode(1).wipeShard(0);
  ASSERT_EQ(0, cluster->bumpGeneration(1));
  ld_info("Starting N1");
  cluster->getNode(1).start();
  cluster->getNode(1).waitUntilStarted();

  stats = cluster->getNode(1).stats();
  EXPECT_EQ(1, stats["shards_waiting_for_non_started_restore"]);
  EXPECT_EQ(0, stats["non_empty_shards_in_restore"]);

  ld_info("Creating Client");
  auto client = cluster->createClient();
  ld_info("Requesting rebuilding of N1");
  lsn_t event_lsn =
      requestShardRebuilding(*client, /* node */ 1, /* shard */ 0);
  ASSERT_NE(LSN_INVALID, event_lsn);

  ld_info("Waiting for event to propagate to N1");
  wait_until_event_log_synced(*cluster, event_lsn, /* nodes */ {1});

  // Rebuilding should be stuck because N2 is down.
  stats = cluster->getNode(1).stats();
  EXPECT_EQ(0, stats["shards_waiting_for_non_started_restore"]);
  EXPECT_EQ(0, stats["non_empty_shards_in_restore"]);

  // Start N2.
  ld_info("Starting N2");
  cluster->getNode(2).start();

  // Rebuilding should be able to complete now.
  ld_info("Waiting for rebuilding of N1");
  cluster->getNode(1).waitUntilAllShardsFullyAuthoritative(client);

  stats = cluster->getNode(1).stats();
  EXPECT_EQ(0, stats["shards_waiting_for_non_started_restore"]);
  EXPECT_EQ(0, stats["non_empty_shards_in_restore"]);

  // Stop N2 again to make the next rebuilding stall.
  ld_info("Shutting down N2");
  cluster->getNode(2).shutdown();

  // Now let's drain N3.
  ld_info("Requesting drain of N3");
  ASSERT_NE(LSN_INVALID,
            requestShardRebuilding(*client,
                                   /* node */ 3,
                                   /* shard */ 0,
                                   SHARD_NEEDS_REBUILD_Header::DRAIN));

  stats = cluster->getNode(3).stats();
  EXPECT_EQ(0, stats["shards_waiting_for_non_started_restore"]);
  EXPECT_EQ(0, stats["non_empty_shards_in_restore"]);

  ld_info("Starting N2");
  cluster->getNode(2).start();

  ld_info("Waiting for drain of N3 to complete");
  event_lsn = cluster->getNode(3).waitUntilAllShardsAuthoritativeEmpty(client);

  stats = cluster->getNode(3).stats();
  EXPECT_EQ(0, stats["shards_waiting_for_non_started_restore"]);
  EXPECT_EQ(0, stats["non_empty_shards_in_restore"]);

  // Wipe N3.
  ld_info("Shutting down N3");
  EXPECT_EQ(0, cluster->getNode(3).shutdown());
  ld_info("Wiping N3");
  cluster->getNode(3).wipeShard(0);
  ASSERT_EQ(0, cluster->bumpGeneration(3));
  ld_info("Starting N3");
  cluster->getNode(3).start();
  cluster->getNode(3).waitUntilStarted();

  // Should still be authoritative empty and not need restore rebuilding.
  ld_info("Waiting for N3 to catch up in event log");
  wait_until_event_log_synced(*cluster, event_lsn, /* nodes */ {3});

  stats = cluster->getNode(3).stats();
  EXPECT_EQ(0, stats["shards_waiting_for_non_started_restore"]);
  EXPECT_EQ(0, stats["non_empty_shards_in_restore"]);

  // Undrain N3 and wait for it to ack.
  ld_info("Marking N3 undrained");
  event_lsn = markShardUndrained(*client, /* node */ 3, /* shard */ 0);
  ASSERT_NE(LSN_INVALID, event_lsn);
  ld_info("Waiting for N3 to ack");
  cluster->getNode(3).waitUntilAllShardsFullyAuthoritative(client);

  stats = cluster->getNode(3).stats();
  EXPECT_EQ(0, stats["shards_waiting_for_non_started_restore"]);
  EXPECT_EQ(0, stats["non_empty_shards_in_restore"]);
}

// During rebuilding records are underreplicated.
// Make sure replication_checker doesn't raise errors about it.
TEST_P(RebuildingTest, ReplicationCheckerDuringRebuilding) {
  ld_info("Creating cluster");
  auto cluster = IntegrationTestUtils::ClusterFactory()
                     .apply(commonSetup)
                     .setNumDBShards(1)
                     .useHashBasedSequencerAssignment()
                     .setLogConfig(logConfig(2))
                     .setInternalLogsReplicationFactor(2)
                     .setParam("--test-stall-rebuilding", "true")
                     .create(3);

  // Append a record, making sure that N2 is in the copyset.
  ld_info("Updating setting");
  for (node_index_t n : {0, 1, 2}) {
    cluster->getNode(n).updateSetting("test-do-not-pick-in-copysets", "0");
  }

  ld_info("Creating Client");
  auto client = cluster->createClient();
  ld_info("Appending record");
  std::string data("hello");
  lsn_t lsn = client->appendSync(LOG_ID, Payload(data.data(), data.size()));
  ASSERT_NE(LSN_INVALID, lsn);

  ld_info("Un-updating setting");
  for (node_index_t n : {0, 1, 2}) {
    cluster->getNode(n).unsetSetting("test-do-not-pick-in-copysets");
  }

  ld_info("Shutting down N2");
  EXPECT_EQ(0, cluster->shutdownNodes({2}));

  ld_info("Requesting rebuilding of N2");
  lsn_t event_lsn =
      requestShardRebuilding(*client, /* node */ 2, /* shard */ 0);
  ASSERT_NE(LSN_INVALID, event_lsn);
  ld_info("Marking N2 unrecoverable");
  event_lsn = markShardUnrecoverable(*client, /* node */ 2, /* shard */ 0);
  ASSERT_NE(LSN_INVALID, event_lsn);

  // Read the record back to make sure it's released.
  ld_info("Reading");
  auto reader = client->createReader(1);
  ASSERT_EQ(0, reader->startReading(LOG_ID, lsn));
  std::vector<std::unique_ptr<DataRecord>> data_out;
  GapRecord gap_out;
  ASSERT_EQ(1, reader->read(1, &data_out, &gap_out));
  ASSERT_EQ(lsn, data_out[0]->attrs.lsn);

  ld_info("Running checker");
  EXPECT_EQ(0, cluster->checkConsistency());

  // Now make sure that checker works at all.
  // Wipe N2 and start it without rebuilding (keep generation 1).
  ld_info("Wiping N2");
  cluster->getNode(2).wipeShard(0);
  ld_info("Starting N2");
  cluster->getNode(2).start();
  cluster->getNode(2).waitUntilStarted();

  ld_info("Waiting for N2 to abort its rebuilding");
  event_lsn = cluster->getNode(2).waitUntilAllShardsFullyAuthoritative(client);

  ld_info("Waiting for nodes to catch up in event log");
  wait_until_event_log_synced(*cluster, event_lsn, /* nodes */ {0, 1, 2});

  // Make a new client to discard any stale authoritative status.
  ld_info("Re-creating client");
  client = cluster->createClient();

  ld_info("Running checker again, expecting errors");
  EXPECT_NE(0, cluster->checkConsistency());
}

INSTANTIATE_TEST_CASE_P(RebuildingTest,
                        RebuildingTest,
                        ::testing::Values(false, true));

// TODO(#8570293): write at test where we use a cross-domain copyset selector.
// TODO(#8570293): once we support rebuilding with extras, write a test.
