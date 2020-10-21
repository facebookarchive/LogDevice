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

#include "logdevice/common/LocalLogStoreRecordFormat.h"
#include "logdevice/common/Metadata.h"
#include "logdevice/common/Timestamp.h"
#include "logdevice/common/configuration/ConfigParser.h"
#include "logdevice/common/configuration/Configuration.h"
#include "logdevice/common/debug.h"
#include "logdevice/common/event_log/EventLogRecord.h"
#include "logdevice/common/test/TestUtil.h"
#include "logdevice/common/util.h"
#include "logdevice/include/Client.h"
#include "logdevice/include/ClientSettings.h"
#include "logdevice/lib/ops/EventLogUtils.h"
#include "logdevice/server/locallogstore/test/StoreUtil.h"
#include "logdevice/test/utils/AdminAPITestUtils.h"
#include "logdevice/test/utils/IntegrationTestBase.h"
#include "logdevice/test/utils/IntegrationTestUtils.h"

using namespace facebook::logdevice;
using IntegrationTestUtils::waitUntilShardHasEventLogState;
using IntegrationTestUtils::waitUntilShardsHaveEventLogState;

// Kill test process after this many seconds. These tests are complex and take
// around 25s to execute so giving them a longer timeout.
const std::chrono::seconds TEST_TIMEOUT(DEFAULT_TEST_TIMEOUT * 3);

namespace fs = boost::filesystem;

namespace {

struct TestMode {};

const int NUM_DB_SHARDS = 2;
// More logs than shards, so that at least one shard gets multiple logs.
const std::vector<logid_t> LOG_IDS = {logid_t(1), logid_t(2), logid_t(3)};

logsconfig::LogAttributes logAttributes(int replication) {
  auto log_attrs = logsconfig::LogAttributes()
                       .with_replicationFactor(replication)
                       .with_syncedCopies(0)
                       .with_maxWritesInFlight(300);
  return log_attrs;
}

void createPartition(IntegrationTestUtils::Cluster& cluster,
                     NodeSetIndices nodeset,
                     shard_index_t shard) {
  ld_info("Creating partition on nodes %s", toString(nodeset).c_str());
  cluster.applyToNodes(
      nodeset, [shard](auto& node) { node.createPartition(shard); });
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

struct RecordsInfo {
  size_t num_records = 0;
  lsn_t min_lsn = LSN_MAX;
  lsn_t max_lsn = LSN_INVALID;

  void merge(const RecordsInfo& rhs) {
    num_records += rhs.num_records;
    min_lsn = std::min(min_lsn, rhs.min_lsn);
    max_lsn = std::max(max_lsn, rhs.max_lsn);
  }
};

void writeRecords(Client& client,
                  size_t nrecords,
                  std::vector<RecordsInfo>* out_info_per_log = nullptr,
                  const std::vector<logid_t>& logs = LOG_IDS,
                  std::chrono::milliseconds sleep_between_writes =
                      std::chrono::milliseconds(0)) {
  ld_info("Writing %lu records", nrecords);

  // Write some records, keeping a window of at most 20 appends in flight.
  // All tests that use writeRecords() should have maxWritesInFlight >= 20.
  //
  // Note that, unfortunately, it's not enough to limit number of appends
  // in flight - we may still get a SEQNOBUFS if the first few appends are
  // slow (e.g. because some storage nodes haven't fully started yet), while the
  // next appends are fast (e.g. if they picked a more lucky copyset). So we
  // have to keep a window of in-flight appends rather than just a counter.

  const size_t max_in_flight = std::min(nrecords, 20ul);
  std::mutex mutex;
  std::set<int> in_flight;
  std::condition_variable cv;
  std::vector<RecordsInfo> temp_info_per_log;
  if (out_info_per_log == nullptr) {
    out_info_per_log = &temp_info_per_log;
  }
  out_info_per_log->resize(logs.size());

  std::unique_lock lock(mutex);
  for (int i = 1; i <= nrecords; ++i) {
    while (!in_flight.empty() && i - *in_flight.begin() >= max_in_flight) {
      cv.wait(lock);
    }
    std::string data("data" + std::to_string(i));
    int log_idx = folly::Random::rand32() % logs.size();
    int rv =
        client.append(logs[log_idx],
                      std::move(data),
                      [&mutex, &cv, &in_flight, out_info_per_log, i, log_idx](
                          Status st, const DataRecord& r) {
                        std::unique_lock lock2(mutex);
                        EXPECT_EQ(E::OK, st);
                        if (st == E::OK) {
                          ASSERT_NE(LSN_INVALID, r.attrs.lsn);
                          auto& info = out_info_per_log->at(log_idx);
                          ++info.num_records;
                          info.min_lsn = std::min(info.min_lsn, r.attrs.lsn);
                          info.max_lsn = std::max(info.max_lsn, r.attrs.lsn);
                        }
                        bool erased = in_flight.erase(i);
                        ld_check(erased);
                        cv.notify_one();
                      });
    if (rv == 0) {
      in_flight.insert(i);
    } else {
      ADD_FAILURE() << "append failed synchronously: " << error_name(err);
    }
    if (sleep_between_writes.count() != 0) {
      lock.unlock();
      /* sleep override */
      std::this_thread::sleep_for(sleep_between_writes);
      lock.lock();
    }
  }
  while (!in_flight.empty()) {
    cv.wait(lock);
  }
}

void writeRecordsToMultiplePartitions(
    IntegrationTestUtils::Cluster& cluster,
    Client& client,
    size_t nrecords,
    std::vector<RecordsInfo>* out_info_per_log = nullptr,
    const std::vector<logid_t>& logs = LOG_IDS,
    std::chrono::milliseconds sleep_between_writes =
        std::chrono::milliseconds(0)) {
  // ~5 nodes x 2 shards per node x ~3 partitions per shard.
  const size_t total_partitions = 30;
  for (size_t i = 0; i < total_partitions; ++i) {
    if (i != 0) {
      // Create a new partition in a random shard on a random node.
      // We don't do it on all nodes at once because we want different partition
      // boundaries on different nodes. But we could do it in batches: a few
      // random shards at a time, in parallel; that would probably speed this up
      // a little; this is not implemented.

      std::vector<node_index_t> nodes = cluster.getRunningStorageNodes();
      ld_check(!nodes.empty());
      auto& node =
          cluster.getNode(nodes[folly::Random::rand32() % nodes.size()]);
      shard_index_t shard = folly::Random::rand32() % node.num_db_shards_;
      node.createPartition(shard);

      // Make sure that all partitions have different timestamps (they have
      // millisecond granularity).
      /* sleep override */
      std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }

    // Careful rounding to make sure we write exactly nrecords records in total.
    size_t records_to_write =
        nrecords * (i + 1) / total_partitions - nrecords * i / total_partitions;

    writeRecords(
        client, records_to_write, out_info_per_log, logs, sleep_between_writes);
  }
}

void dirtyNodes(IntegrationTestUtils::Cluster& cluster,
                Client& client,
                NodeSetIndices nodeset,
                shard_index_t shard,
                std::vector<RecordsInfo>* out_info_per_log = nullptr) {
  // This is 4 times the partition timestamp granularity so we guarantee
  // that partitions have non-overlapping time ranges.
  //
  // Why 4 instead of 2? Reopening the DB effectively doubles the timestamp
  // margins for dirty partitions, see "Expand partition bounds to cover the
  // dirty time range" in PartitionedRocksDBStore::open(), and
  // PartitionedRocksDBStore::dirtyTimeInterval().
  auto partition_creation_delay = std::chrono::milliseconds(400);

  // Create dirty/clean/dirty paritition pattern
  writeRecords(client, 24, out_info_per_log);

  /* Provide some time distance between partitions. */
  std::this_thread::sleep_for(partition_creation_delay);
  createPartition(cluster, nodeset, shard);

  writeRecords(client, 18, out_info_per_log);

  /* Provide some time distance between partitions. */
  std::this_thread::sleep_for(partition_creation_delay);
  createPartition(cluster, nodeset, shard);
  flushPartition(cluster, nodeset, shard, -1);

  writeRecords(client, 20, out_info_per_log);

  /* Provide some time distance between partitions. */
  std::this_thread::sleep_for(partition_creation_delay);
  createPartition(cluster, nodeset, shard);

  for (auto nidx : nodeset) {
    // Have different partition boundaries between nodes so we test dirty
    // regions that span partitions on the donor.
    writeRecords(client, 10, out_info_per_log);
    std::this_thread::sleep_for(partition_creation_delay);
    createPartition(cluster, {nidx}, shard);
    flushPartition(cluster, {nidx}, shard, -1);

    // Mix retired and inflight data in the same partition
    writeRecords(client, 15, out_info_per_log);
    flushPartition(cluster, {nidx}, shard, 0);
    writeRecords(client, 11, out_info_per_log);
  }
}

} // anonymous namespace

class RebuildingTest : public IntegrationTestBase,
                       public ::testing::WithParamInterface<TestMode> {
 public:
  RebuildingTest() : IntegrationTestBase(TEST_TIMEOUT) {}
  ~RebuildingTest() override {}

 protected:
  enum class NodeFailureMode { REPLACE, KILL };

  std::function<void(IntegrationTestUtils::ClusterFactory& cluster)>
  commonSetup() {
    // TODO enableMessageErrorInjection() once CatchupQueue properly
    //      handles streams waiting for log recoveries that have timed out.
    return [this](IntegrationTestUtils::ClusterFactory& cluster) {
      auto test_param = GetParam();
      cluster
          // there is a known issue where purging deletes records that gets
          // surfaced in tests with sequencer-written metadata. See t13850978
          .doPreProvisionEpochMetaData()
          .doNotLetSequencersProvisionEpochMetaData()
          .setParam("--file-config-update-interval", "10ms")
          .setParam("--disable-rebuilding", "false")
          // A rebuilding node responds to STOREs with E::DISABLED. Setting this
          // to 0s makes it so that the sequencer does not wait for a while
          // before trying to store to that node again, otherwise the test would
          // timeout.
          .setParam("--disabled-retry-interval", "0s")
          .setParam("--gossip-enabled", "true")
          .setParam("--seq-state-backoff-time", "10ms..1s")
          .setParam("--rocksdb-partition-data-age-flush-trigger", "1s")
          .setParam("--rocksdb-partition-idle-flush-trigger", "100ms")
          .setParam("--rocksdb-min-manual-flush-interval", "200ms")
          .setParam("--rocksdb-partition-hi-pri-check-period", "50ms")
          .setParam("--rebuilding-store-timeout", "6s..10s")
          // When rebuilding Without WAL, destruction of memtable is used as
          // proxy for memtable being flushed to stable storage. Iterators can
          // pin a memtable preventing its destruction. Low ttl in tests ensures
          // iterators are invalidated and memtable flush notifications are not
          // delayed
          .setParam("--iterator-cache-ttl", "1s")
          .setParam("--rocksdb-partitioned", "true")
          // Make newly created partitions receive new records immediately.
          .setParam("--rocksdb-new-partition-timestamp-margin", "0")
          .setParam("--enable-cluster-maintenance-state-machine", "true")
          .setParam("--min-gossips-for-stable-state", "1")
          .setParam("--disable-event-log-trimming", "true")
          .setParam(
              "--maintenance-manager-metadata-nodeset-update-period", "10800s")
          .setParam("--event-log-grace-period", "1ms")
          .setParam("--nodes-configuration-manager-intermediary-shard-state-"
                    "timeout",
                    "1s")
          .setNumDBShards(NUM_DB_SHARDS)
          .setNumLogs(3)
          .useStandaloneAdminServer(true)
          .useDefaultTrafficShapingConfig(false)
          // Reduce the copyset block size so we get a copyset shuffle every ~6
          // records.
          .setParam("--sticky-copysets-block-size", "128");
    };
  }

  /**
   * Create a cluster factory to be used in rolling rebuilding tests.
   * The factory can be further customized by the test case before being
   * passed to the rollingRebuilding() method which executes the rebuilding
   * actions on the cluster.
   *
   * @param nnodes Number of nodes in the cluster. First node is a sequencer.
   * @param r      Replication factor.
   * @param trim   Should the event log be trimmed?
   */
  IntegrationTestUtils::ClusterFactory
  rollingRebuildingClusterFactory(int /*nnodes*/, int r, bool trim) {
    auto log_attrs = logsconfig::LogAttributes()
                         .with_replicationFactor(r)
                         .with_syncedCopies(0)
                         .with_maxWritesInFlight(30)
                         .with_stickyCopySets(true);

    return IntegrationTestUtils::ClusterFactory()
        .apply(commonSetup())
        .setLogGroupName("mylog-2")
        .setLogAttributes(log_attrs)
        .setMaintenanceLogAttributes(log_attrs)
        .setEventLogAttributes(log_attrs)
        .enableSelfInitiatedRebuilding()
        .setParam("--disable-event-log-trimming", trim ? "false" : "true")
        .setParam("--byte-offsets")
        .setParam("--event-log-max-delta-records", "5")
        .setParam(
            "--message-tracing-types", "RECORD,APPEND,APPENDED,STORE,STORED")
        .setNumLogs(3)
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

    cluster.waitUntilAllSequencersQuiescent();

    auto client = cluster.createClient();

    // Write some records
    std::vector<RecordsInfo> info_per_log;
    writeRecordsToMultiplePartitions(cluster, *client, 100, &info_per_log);

    std::vector<std::vector<OffsetMap>> correct_offsets(LOG_IDS.size());
    // Reading first time will trigger log storage state to get epoch offset
    // by sending GetSeqStateRequest to sequencer. We will try read from
    // beginning until epoch offset is ready.
    wait_until(
        "GetSeqStateRequests are finished and epoch offset is propagated "
        "to storage nodes.",
        [&]() {
          auto reader = client->createReader(1);
          reader->includeByteOffset();
          std::vector<std::vector<OffsetMap>> new_offsets(LOG_IDS.size());
          for (size_t log_idx = 0; log_idx < LOG_IDS.size(); ++log_idx) {
            logid_t log = LOG_IDS.at(log_idx);
            const auto& info = info_per_log.at(log_idx);
            if (info.num_records == 0) {
              continue;
            }
            ld_info("Starting Read of log %lu at %s",
                    log.val(),
                    lsn_to_string(info.min_lsn).c_str());
            int rv = reader->startReading(log, info.min_lsn, info.max_lsn);
            EXPECT_EQ(0, rv);
            std::vector<std::unique_ptr<DataRecord>> data_out;
            read_records_no_gaps(*reader, info.num_records, &data_out);
            for (int i = 0; i < info.num_records; ++i) {
              if (!data_out.at(i)->attrs.offsets.isValid()) {
                // No offsets yet, keep waiting.
                return false;
              }
              EXPECT_NE(RecordOffset(), data_out[i]->attrs.offsets);
              OffsetMap offsets =
                  OffsetMap::fromRecord(std::move(data_out[i]->attrs.offsets));
              new_offsets.at(log_idx).push_back(offsets);
            }
          }
          for (size_t log_idx = 0; log_idx < LOG_IDS.size(); ++log_idx) {
            correct_offsets.at(log_idx).insert(correct_offsets[log_idx].end(),
                                               new_offsets.at(log_idx).begin(),
                                               new_offsets.at(log_idx).end());
          }
          return true;
        });

    cluster.getAdminServer()->waitUntilFullyLoaded();
    auto admin_client = cluster.getAdminServer()->createAdminClient();
    for (node_index_t node = begin; node <= end; ++node) {
      ld_info("%s N%u...",
              fmode == NodeFailureMode::REPLACE ? "Replacing" : "Killing",
              node);
      // Kill/Restart or Replace the node
      switch (fmode) {
        case NodeFailureMode::REPLACE:
          ASSERT_EQ(0, cluster.replaceViaAdminServer(*admin_client, node));
          ASSERT_TRUE(
              wait_until_shards_enabled_and_healthy(cluster, node, {0, 1}));
          cluster.getNode(node).waitUntilAllShardsFullyAuthoritative(client);
          break;
        case NodeFailureMode::KILL:
          // Ensure there is dirty data to rebuild.
          std::vector<RecordsInfo> batch_info;
          dirtyNodes(cluster,
                     *client,
                     node_set,
                     /*shard*/ 0,
                     &batch_info);
          for (size_t log_idx = 0; log_idx < LOG_IDS.size(); ++log_idx) {
            info_per_log[log_idx].merge(batch_info.at(log_idx));
          }

          // Include added records in our byte offset verifications
          wait_until(
              "GetSeqStateRequests are finished and epoch offset is propagated "
              "to storage nodes.",
              [&]() {
                auto reader = client->createReader(1);
                reader->includeByteOffset();
                std::vector<std::vector<OffsetMap>> new_offsets(LOG_IDS.size());
                for (size_t log_idx = 0; log_idx < LOG_IDS.size(); ++log_idx) {
                  logid_t log = LOG_IDS.at(log_idx);
                  const auto& info = batch_info.at(log_idx);
                  if (info.num_records == 0) {
                    continue;
                  }
                  ld_info("Starting Read of log %lu at %s",
                          log.val(),
                          lsn_to_string(info.min_lsn).c_str());
                  int rv =
                      reader->startReading(log, info.min_lsn, info.max_lsn);
                  EXPECT_EQ(0, rv);
                  std::vector<std::unique_ptr<DataRecord>> data_out;
                  read_records_no_gaps(*reader, info.num_records, &data_out);
                  for (int i = 0; i < info.num_records; ++i) {
                    if (!data_out.at(i)->attrs.offsets.isValid()) {
                      // No offsets yet, keep waiting.
                      return false;
                    }
                    EXPECT_NE(RecordOffset(), data_out[i]->attrs.offsets);
                    OffsetMap offsets = OffsetMap::fromRecord(
                        std::move(data_out[i]->attrs.offsets));
                    new_offsets.at(log_idx).push_back(offsets);
                  }
                }
                for (size_t log_idx = 0; log_idx < LOG_IDS.size(); ++log_idx) {
                  correct_offsets.at(log_idx).insert(
                      correct_offsets[log_idx].end(),
                      new_offsets.at(log_idx).begin(),
                      new_offsets.at(log_idx).end());
                }
                return true;
              });

          // Kill/Restart the node and monitor the event log to ensure
          // rebuilding is triggered and completes.
          EventLogRebuildingSet base_set;
          ASSERT_EQ(EventLogUtils::getRebuildingSet(*client, base_set), 0);

          cluster.getNode(node).kill();
          cluster.getNode(node).start();
          cluster.getNode(node).waitUntilStarted();

          ld_info("Waiting for self-initiated rebuild");
          EventLogUtils::tailEventLog(*client,
                                      nullptr,
                                      [&](const EventLogRebuildingSet& set,
                                          const EventLogRecord*,
                                          lsn_t) {
                                        return set.getLastUpdate() <=
                                            base_set.getLastUpdate();
                                      });

          ld_info("Waiting for empty rebuilding set");
          EventLogUtils::tailEventLog(*client,
                                      nullptr,
                                      [&](const EventLogRebuildingSet& set,
                                          const EventLogRecord*,
                                          lsn_t) { return !set.empty(); });
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
            for (size_t log_idx = 0; log_idx < LOG_IDS.size(); ++log_idx) {
              logid_t log = LOG_IDS.at(log_idx);
              const auto& info = info_per_log.at(log_idx);
              if (info.num_records == 0) {
                continue;
              }
              ld_info("Starting Read of log %lu at %s",
                      log.val(),
                      lsn_to_string(info.min_lsn).c_str());
              int rv = reader->startReading(log, info.min_lsn, info.max_lsn);
              EXPECT_EQ(0, rv);
              std::vector<std::unique_ptr<DataRecord>> data_out;
              read_records_no_gaps(*reader, info.num_records, &data_out);
              for (int i = 0; i < info.num_records; ++i) {
                if (!data_out.at(i)->attrs.offsets.isValid()) {
                  // No offsets yet, keep waiting.
                  return false;
                }
                EXPECT_NE(RecordOffset(), data_out[i]->attrs.offsets);
                OffsetMap offsets = OffsetMap::fromRecord(
                    std::move(data_out[i]->attrs.offsets));
                EXPECT_EQ(correct_offsets[log_idx].at(i), offsets)
                    << log.val() << ' ' << i;
              }
            }
            return true;
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
  bool trim = true;
  auto cf = rollingRebuildingClusterFactory(nnodes, r, trim);
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
  bool trim = false;
  auto cf = rollingRebuildingClusterFactory(nnodes, r, trim);
  rollingRebuilding(cf, nnodes, 1, 4, 1);
}

// Replace N1 twice. The second time it should not re-replicate anything.
TEST_P(RebuildingTest, NodeRebuiltTwice) {
  int nnodes = 5;
  int r = 3;
  bool trim = true;
  auto cf = rollingRebuildingClusterFactory(nnodes, r, trim);
  rollingRebuilding(cf, nnodes, 1, 1, 1);
  rollingRebuilding(cf, nnodes, 1, 1, 1);
}

TEST_P(RebuildingTest, OnlineDiskRepair) {
  auto log_attrs = logsconfig::LogAttributes()
                       .with_replicationFactor(3)
                       .with_syncedCopies(0)
                       .with_maxWritesInFlight(30);

  auto cluster = IntegrationTestUtils::ClusterFactory()
                     .apply(commonSetup())
                     .setLogGroupName("my-test-log")
                     .setLogAttributes(log_attrs)
                     .setEventLogAttributes(log_attrs)
                     .setMaintenanceLogAttributes(log_attrs)
                     .setNumDBShards(3)
                     .setNumLogs(1)
                     .create(5);

  cluster->waitUntilAllSequencersQuiescent();

  auto client = cluster->createClient();
  cluster->getAdminServer()->waitUntilFullyLoaded();
  auto admin_client = cluster->getAdminServer()->createAdminClient();

  // Write some records to shard 1.
  std::vector<RecordsInfo> info_per_log;
  writeRecordsToMultiplePartitions(
      *cluster, *client, 50, &info_per_log, {logid_t(1)});

  // Stop N3...
  ld_info("Stopping N3...");
  EXPECT_EQ(0, cluster->getNode(3).shutdown());

  // In the real world, the disk will be unmounted. Here let's just remove
  // everything and write a marker that instructs ShardedRocksDBLocalLogStore to
  // create a FailingLocalLogStore for the shard.
  fs::path shard_path = cluster->getNode(3).getShardPath(1);
  for (fs::directory_iterator end_dir_it, it(shard_path); it != end_dir_it;
       ++it) {
    fs::remove_all(it->path());
  }
  auto disable_marker_path = shard_path / fs::path("LOGDEVICE_DISABLED");
  FILE* fp = std::fopen(disable_marker_path.c_str(), "w");
  ASSERT_NE(nullptr, fp);
  std::fclose(fp);

  ld_info("Bumping the generation...");
  ASSERT_EQ(0, cluster->bumpGeneration(*admin_client, 3));

  // Restart N3...
  ld_info("Restarting N3...");
  // This will NOT trigger automatic internal maintenance since rebuilding
  // supervisor is disabled here.
  cluster->getNode(3).start();
  cluster->getNode(3).waitUntilStarted();

  // Read data... N3 should respond with E::REBUILDING.
  {
    auto reader = client->createReader(1);
    int rv = reader->startReading(logid_t(1), info_per_log.at(0).min_lsn);
    ASSERT_EQ(0, rv);
    read_records_no_gaps(*reader, info_per_log.at(0).num_records);
  }

  // Trigger rebuilding of the shard....
  ld_info("Trigger rebuilding...");
  cluster->applyInternalMaintenance(
      *client, 3, 1, "Requesting rebuilding from the test case");

  ld_info("Stopping N3...");
  EXPECT_EQ(0, cluster->getNode(3).shutdown());

  ld_info("Removing marker...");
  ASSERT_TRUE(fs::remove(disable_marker_path));

  ld_info("Starting N3 and waiting for rebuilding...");
  cluster->getNode(3).start();
  ASSERT_TRUE(wait_until_enabled_and_healthy(*cluster, 3));
  cluster->getNode(3).waitUntilAllShardsFullyAuthoritative(client);

  // Read data again...
  {
    auto reader = client->createReader(1);
    int rv = reader->startReading(logid_t(1), info_per_log.at(0).min_lsn);
    ASSERT_EQ(0, rv);
    read_records_no_gaps(*reader, info_per_log.at(0).num_records);
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
  auto log_attrs = logsconfig::LogAttributes()
                       .with_replicationFactor(3)
                       .with_syncedCopies(0)
                       .with_maxWritesInFlight(30);

  auto cluster = IntegrationTestUtils::ClusterFactory()
                     .apply(commonSetup())
                     .setLogGroupName("my-test-log")
                     .setLogAttributes(log_attrs)
                     .setEventLogAttributes(log_attrs)
                     .setMaintenanceLogAttributes(log_attrs)
                     .setNumLogs(42)
                     .setNumDBShards(2)
                     .create(5);
  cluster->waitUntilAllSequencersQuiescent();

  cluster->getAdminServer()->waitUntilFullyLoaded();
  auto admin_client = cluster->getAdminServer()->createAdminClient();

  // We use 2 shards, and event has odd ID => event log and its metadata log
  // live in shard 1 => it's safe to wipe shard 0.
  const shard_index_t SHARD = 0;

  // Restart all nodes with an empty disk for shard 0.
  for (node_index_t node = 1; node <= 4; ++node) {
    EXPECT_EQ(0, cluster->getNode(node).shutdown());
    auto shard_path = cluster->getNode(node).getShardPath(SHARD);
    for (fs::directory_iterator end_dir_it, it(shard_path); it != end_dir_it;
         ++it) {
      fs::remove_all(it->path());
    }
    ASSERT_EQ(0, cluster->bumpGeneration(*admin_client, node));
    cluster->getNode(node).start();
    cluster->getNode(node).waitUntilStarted();
  }

  ld_info("Triggering rebuiling of shard 0 for all nodes in the cluster...");
  auto client = cluster->createClient(std::chrono::hours(1));
  for (node_index_t node = 1; node <= 4; ++node) {
    cluster->applyInternalMaintenance(
        *client, node, SHARD, "Requesting rebuilding from the test case");
  }

  ld_info("Waiting for all nodes to acknowledge rebuilding...");
  IntegrationTestUtils::waitUntilShardsHaveEventLogState(
      client,
      {ShardID(1, SHARD),
       ShardID(2, SHARD),
       ShardID(3, SHARD),
       ShardID(4, SHARD)},
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
                     .apply(commonSetup())
                     .setLogGroupName("mylog")
                     .setLogAttributes(logAttributes(3))
                     .setEventLogAttributes(logAttributes(5))
                     .setMaintenanceLogAttributes(logAttributes(5))
                     .setMetaDataLogsConfig(meta_config)
                     // read quickly when nodes are down
                     .setParam("--gap-grace-period", "10ms")
                     .setParam("--rebuilding-restarts-grace-period", "1ms")
                     .setParam("--rebuild-without-amends", "true")
                     .setNumLogs(42)
                     .create(9); // 1 sequencer node + 8 storage nodes

  cluster->waitUntilAllSequencersQuiescent();

  auto client = cluster->createClient();
  client->settings().set("gap-grace-period", "10ms");
  cluster->getAdminServer()->waitUntilFullyLoaded();
  auto admin_client = cluster->getAdminServer()->createAdminClient();

  // Write some records.
  writeRecordsToMultiplePartitions(*cluster, *client, 1000);

  // Kill nodes 1-2 and clean their DBs. They'll be rebuilding later.
  ld_info("Killing nodes.");
  for (node_index_t node = 1; node <= 2; ++node) {
    cluster->getNode(node).kill();
    for (shard_index_t shard = 0; shard < NUM_DB_SHARDS; ++shard) {
      auto shard_path = cluster->getNode(node).getShardPath(shard);
      for (fs::directory_iterator end_dir_it, it(shard_path); it != end_dir_it;
           ++it) {
        fs::remove_all(it->path());
      }
    }
  }

  // Writing SHARD_NEEDS_REBUILD for the three nodes to the event log.
  ld_info("Requesting drain of N3");
  ld_info(
      "Maintenance for N3:S0 created: %s",
      cluster->applyMaintenance(
                 *admin_client, 3, 0, "testing", /* drain =*/true)
          .c_str());
  ld_info(
      "Maintenance for N3:S1 created: %s",
      cluster
          ->applyMaintenance(*admin_client, 3, 1, "testing", /* drain = */ true)
          .c_str());

  for (node_index_t node = 1; node <= 2; ++node) {
    for (shard_index_t shard = 0; shard < NUM_DB_SHARDS; ++shard) {
      cluster->applyInternalMaintenance(
          *client, node, shard, "Requesting rebuilding from the test case");
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
                     .apply(commonSetup())
                     .setLogGroupName("mylog")
                     .setLogAttributes(logAttributes(3))
                     .setEventLogAttributes(logAttributes(5))
                     .setMaintenanceLogAttributes(logAttributes(5))
                     .setMetaDataLogsConfig(meta_config)
                     // read quickly when nodes are down
                     .setParam("--gap-grace-period", "10ms")
                     .setNumLogs(42)
                     .create(9); // 1 sequencer node + 8 storage nodes

  cluster->waitUntilAllSequencersQuiescent();

  auto client = cluster->createClient();
  client->settings().set("gap-grace-period", "10ms");

  cluster->getAdminServer()->waitUntilFullyLoaded();
  auto admin_client = cluster->getAdminServer()->createAdminClient();

  // Write some records.
  writeRecordsToMultiplePartitions(*cluster, *client, 100);

  // Kill nodes 1-3 and clean their DBs. They'll be rebuilding later.
  ld_info("Killing nodes.");
  for (node_index_t node = 1; node <= 3; ++node) {
    cluster->getNode(node).kill();
    for (shard_index_t shard = 0; shard < NUM_DB_SHARDS; ++shard) {
      auto shard_path = cluster->getNode(node).getShardPath(shard);
      for (fs::directory_iterator end_dir_it, it(shard_path); it != end_dir_it;
           ++it) {
        fs::remove_all(it->path());
      }
    }
  }

  // Should still have write availability. Write some records.
  writeRecordsToMultiplePartitions(*cluster, *client, 50);

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
    for (shard_index_t shard = 0; shard < NUM_DB_SHARDS; ++shard) {
      cluster->applyInternalMaintenance(
          *client, node, shard, "Requesting rebuilding from the test case");
    }
  }

  // Now recovery should finish.
  ld_info("Waiting for recovery.");
  cluster->waitForRecovery();

  auto stats = cluster->getNode(0).stats();
  ASSERT_GT(stats["non_auth_recovery_epochs"], 0);

  // Marking the shards unrecoverable
  thrift::MarkAllShardsUnrecoverableRequest request;
  request.set_user("test");
  request.set_reason("test");
  thrift::MarkAllShardsUnrecoverableResponse response;
  admin_client->sync_markAllShardsUnrecoverable(response, request);

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
    ASSERT_EQ(0, cluster->bumpGeneration(*admin_client, node));
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
  auto log_attrs = logsconfig::LogAttributes()
                       .with_replicationFactor(3)
                       .with_syncedCopies(0)
                       .with_maxWritesInFlight(30);

  auto cluster = IntegrationTestUtils::ClusterFactory()
                     .apply(commonSetup())
                     .setLogGroupName("my-test-log")
                     .setLogAttributes(log_attrs)
                     .setEventLogAttributes(log_attrs)
                     .setMaintenanceLogAttributes(log_attrs)
                     .setNumLogs(3)
                     .create(6);

  cluster->waitForRecovery();
  auto client = cluster->createClient();

  cluster->getAdminServer()->waitUntilFullyLoaded();
  auto admin_client = cluster->getAdminServer()->createAdminClient();

  // Write some records..
  writeRecordsToMultiplePartitions(*cluster, *client, 50);

  ld_info("Stopping N3...");
  EXPECT_EQ(0, cluster->getNode(3).shutdown());

  ld_info("Stopping N1...");
  EXPECT_EQ(0, cluster->getNode(1).shutdown());

  // Trigger rebuilding of all shards of N3...
  cluster->applyInternalMaintenance(
      *client, 3, 0, "Requesting rebuilding from the test case");
  cluster->applyInternalMaintenance(
      *client, 3, 1, "Requesting rebuilding from the test case");

  cluster->getNode(3).waitUntilInternalMaintenances(
      *admin_client,
      [&](const auto& maintenances) { return !maintenances.empty(); },
      "Internal maintenances must not be empty");

  // Wait until N3 is performing rebuildnig.
  cluster->getNode(3).waitUntilShardState(
      *admin_client,
      0,
      [](const thrift::ShardState& shard) {
        return shard.get_storage_state() ==
            membership::thrift::StorageState::DATA_MIGRATION;
      },
      "Shard must be in DATA_MIGRATION");

  // Now, add two more nodes to the cluster.
  int rv = cluster->expandViaAdminServer(*admin_client, 2);
  ASSERT_EQ(0, rv);
  // Replace N3
  ASSERT_EQ(0, cluster->replaceViaAdminServer(*admin_client, 3));
  // Restart N1
  cluster->getNode(1).start();
  // Wait for N3 to acknowledge rebuilding of all its shards.
  ASSERT_TRUE(wait_until_enabled_and_healthy(*cluster, 3));
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
  auto log_attrs = logsconfig::LogAttributes()
                       .with_replicationFactor(3)
                       .with_syncedCopies(0)
                       .with_maxWritesInFlight(30);

  auto cluster = IntegrationTestUtils::ClusterFactory()
                     .apply(commonSetup())
                     .setLogGroupName("my-test-log")
                     .setLogAttributes(log_attrs)
                     .setEventLogAttributes(log_attrs)
                     .setMaintenanceLogAttributes(log_attrs)
                     .setNumLogs(3)
                     .create(6);

  cluster->waitForRecovery();
  auto client = cluster->createClient();

  cluster->getAdminServer()->waitUntilFullyLoaded();
  auto admin_client = cluster->getAdminServer()->createAdminClient();

  // Write some records..
  writeRecordsToMultiplePartitions(*cluster, *client, 50);

  // Replace N3
  ASSERT_EQ(0, cluster->replaceViaAdminServer(*admin_client, 3));
  // Trigger rebuilding of all shards of N3...
  cluster->applyInternalMaintenance(
      *client, 3, 0, "Requesting rebuilding from the test case");
  cluster->applyInternalMaintenance(
      *client, 3, 1, "Requesting rebuilding from the test case");
  cluster->getNode(3).waitUntilShardState(
      *admin_client,
      0,
      [](const thrift::ShardState& shard) {
        return (shard.get_storage_state() ==
                    membership::thrift::StorageState::DATA_MIGRATION ||
                shard.get_storage_state() ==
                    membership::thrift::StorageState::NONE);
      },
      "Shard must be in DATA_MIGRATION | NONE");

  // Now, add two more nodes to the cluster.
  int rv = cluster->expandViaAdminServer(*admin_client, 2);
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

TEST_P(RebuildingTest, NodeComesBackAfterRebuildingIsComplete) {
  auto log_attrs = logsconfig::LogAttributes()
                       .with_replicationFactor(3)
                       .with_syncedCopies(0)
                       .with_maxWritesInFlight(30);

  logsconfig::LogAttributes event_log_attrs =
      log_attrs.with_replicationFactor(4);

  Configuration::MetaDataLogsConfig meta_config =
      createMetaDataLogsConfig({1, 2, 3, 4}, 3, NodeLocationScope::NODE);
  meta_config.sequencers_write_metadata_logs = false;
  meta_config.sequencers_provision_epoch_store = false;

  auto cluster = IntegrationTestUtils::ClusterFactory()
                     .apply(commonSetup())
                     .setLogGroupName("my-test-log")
                     .setLogAttributes(log_attrs)
                     .setEventLogAttributes(event_log_attrs)
                     .setMaintenanceLogAttributes(event_log_attrs)
                     .setNumLogs(3)
                     .setMetaDataLogsConfig(meta_config)
                     .create(7);

  cluster->waitForRecovery();
  auto client = cluster->createClient();

  cluster->getAdminServer()->waitUntilFullyLoaded();
  auto admin_client = cluster->getAdminServer()->createAdminClient();

  // Kill N3
  cluster->getNode(3).kill();
  // Trigger rebuilding of all shards of N3...
  cluster->applyInternalMaintenance(
      *client, 3, 0, "Requesting rebuilding from the test case");
  cluster->applyInternalMaintenance(
      *client, 3, 1, "Requesting rebuilding from the test case");

  // Wait until others finish rebuilding the node
  cluster->getNode(3).waitUntilAllShardsAuthoritativeEmpty(client);

  // Start N3.
  cluster->getNode(3).start();
  cluster->getNode(3).waitUntilAllShardsFullyAuthoritative(client);
}

TEST_P(RebuildingTest, ShardAckFromNodeAlreadyRebuilt) {
  auto log_attrs = logsconfig::LogAttributes()
                       .with_replicationFactor(3)
                       .with_syncedCopies(0)
                       .with_maxWritesInFlight(30);

  // Ensure metadata logs are not stored on the node we are about to remove from
  // the cluster.
  Configuration::MetaDataLogsConfig meta_config =
      createMetaDataLogsConfig({1, 2, 3, 4}, 3, NodeLocationScope::NODE);
  meta_config.sequencers_write_metadata_logs = false;
  meta_config.sequencers_provision_epoch_store = false;

  auto cluster = IntegrationTestUtils::ClusterFactory()
                     .apply(commonSetup())
                     .setLogGroupName("alog")
                     .setLogAttributes(log_attrs)
                     .setEventLogAttributes(log_attrs)
                     .setMaintenanceLogAttributes(log_attrs)
                     .setNumLogs(3)
                     .setMetaDataLogsConfig(meta_config)
                     .create(6);

  cluster->waitUntilAllSequencersQuiescent();
  auto client = cluster->createClient();

  cluster->getAdminServer()->waitUntilFullyLoaded();
  auto admin_client = cluster->getAdminServer()->createAdminClient();

  writeRecordsToMultiplePartitions(*cluster, *client, 100);

  // Kill N3
  cluster->getNode(3).kill();
  // Trigger rebuilding of all shards of N3...
  cluster->applyInternalMaintenance(
      *client, 3, 0, "Requesting rebuilding from the test case");
  cluster->applyInternalMaintenance(
      *client, 3, 1, "Requesting rebuilding from the test case");

  // Wait until others finish rebuilding the node
  cluster->getNode(3).waitUntilAllShardsAuthoritativeEmpty(client);

  writeRecordsToMultiplePartitions(*cluster, *client, 1000);

  // Start draining N5
  ld_info(
      "Maintenance for N5:S0 created: %s",
      cluster->applyMaintenance(
                 *admin_client, 5, 0, "testing", /* drain =*/true)
          .c_str());
  ld_info(
      "Maintenance for N5:S1 created: %s",
      cluster
          ->applyMaintenance(*admin_client, 5, 1, "testing", /* drain = */ true)
          .c_str());

  // Now start N3
  ld_info("Starting N3");
  cluster->getNode(3).start();

  // Once the drain completes, the node's authoritative status is changed to
  // AUTHORITATIVE_EMPTY.
  ld_info("Waiting for N5 to be AUTHORITATIVE_EMPTY");
  waitUntilShardsHaveEventLogState(client,
                                   {ShardID(5, 0), ShardID(5, 1)},
                                   AuthoritativeStatus::AUTHORITATIVE_EMPTY,
                                   true);
}

TEST_P(RebuildingTest, NodeDrain) {
  auto log_attrs = logsconfig::LogAttributes()
                       .with_replicationFactor(3)
                       .with_syncedCopies(0)
                       .with_maxWritesInFlight(30);

  // Ensure metadata logs are not stored on the node we are about to remove from
  // the cluster.
  Configuration::MetaDataLogsConfig meta_config =
      createMetaDataLogsConfig({1, 2, 3, 4}, 3, NodeLocationScope::NODE);
  meta_config.sequencers_write_metadata_logs = false;
  meta_config.sequencers_provision_epoch_store = false;

  auto cluster = IntegrationTestUtils::ClusterFactory()
                     .apply(commonSetup())
                     .setLogGroupName("alog")
                     .setLogAttributes(log_attrs)
                     .setEventLogAttributes(log_attrs)
                     .setNumLogs(3)
                     .setMetaDataLogsConfig(meta_config)
                     .create(6);

  cluster->waitForRecovery();
  auto client = cluster->createClient();

  cluster->getAdminServer()->waitUntilFullyLoaded();
  auto admin_client = cluster->getAdminServer()->createAdminClient();

  // Start draining N5
  ld_info("Maintenance for N5:S0 created: %s",
          cluster->applyMaintenance(*admin_client, 5, 0, "testing").c_str());
  ld_info("Maintenance for N5:S1 created: %s",
          cluster->applyMaintenance(*admin_client, 5, 1, "testing").c_str());

  // Once the drain completes, the node's authoritative status is changed to
  // AUTHORITATIVE_EMPTY.
  waitUntilShardsHaveEventLogState(client,
                                   {ShardID(5, 0), ShardID(5, 1)},
                                   AuthoritativeStatus::AUTHORITATIVE_EMPTY,
                                   true);
}

// Verify that writing SHARD_UNDRAIN to the event log cancels any ongoing drain.
TEST_P(RebuildingTest, NodeDrainCanceled) {
  auto log_attrs = logsconfig::LogAttributes()
                       .with_replicationFactor(3)
                       .with_syncedCopies(0)
                       .with_maxWritesInFlight(30);

  // Ensure metadata logs are not stored on the node we are about to remove from
  // the cluster.
  Configuration::MetaDataLogsConfig meta_config =
      createMetaDataLogsConfig({1, 2, 3, 4}, 3, NodeLocationScope::NODE);
  meta_config.sequencers_write_metadata_logs = false;
  meta_config.sequencers_provision_epoch_store = false;

  auto cluster = IntegrationTestUtils::ClusterFactory()
                     .apply(commonSetup())
                     .setLogGroupName("alog")
                     .setLogAttributes(log_attrs)
                     .setEventLogAttributes(log_attrs)
                     .setMaintenanceLogAttributes(log_attrs)
                     .setNumLogs(3)
                     .setMetaDataLogsConfig(meta_config)
                     .create(6);

  cluster->waitForRecovery();
  auto client = cluster->createClient();
  cluster->getAdminServer()->waitUntilFullyLoaded();
  auto admin_client = cluster->getAdminServer()->createAdminClient();

  // Stop N3 so that rebuilding stalls.
  cluster->getNode(3).shutdown();

  // Start draining N5
  auto maintenance1 = cluster->applyMaintenance(*admin_client, 5, 0, "testing");
  auto maintenance2 = cluster->applyMaintenance(*admin_client, 5, 1, "testing");

  // N5 should remaing fully authoritative because it's a drain.
  bool rebuilding_complete = false; // because N3 cannot participate.
  waitUntilShardsHaveEventLogState(client,
                                   {ShardID(5, 0), ShardID(5, 1)},
                                   AuthoritativeStatus::FULLY_AUTHORITATIVE,
                                   rebuilding_complete);

  // We should get stuck at DATA_MIGRATION. This maintenance will be blocked on
  // the donor (3) not completing.
  cluster->getNode(5).waitUntilShardState(
      *admin_client,
      0,
      [](const thrift::ShardState& shard) {
        return shard.get_storage_state() ==
            membership::thrift::StorageState::DATA_MIGRATION;
      },
      "Shard must be in DATA_MIGRATION");

  cluster->getNode(5).waitUntilShardState(
      *admin_client,
      1,
      [](const thrift::ShardState& shard) {
        return shard.get_storage_state() ==
            membership::thrift::StorageState::DATA_MIGRATION;
      },
      "Shard must be in DATA_MIGRATION");

  // Request we abort draining N5
  {
    thrift::RemoveMaintenancesRequest req;
    thrift::MaintenancesFilter filter;
    filter.set_group_ids({maintenance1, maintenance2});
    req.set_user("test");
    req.set_filter(filter);
    thrift::RemoveMaintenancesResponse resp;
    admin_client->sync_removeMaintenances(resp, req);
    ASSERT_EQ(2, resp.get_maintenances().size());
  }
  ASSERT_TRUE(wait_until_enabled_and_healthy(*cluster, 5));

  // Rebuilding should be aborted.

  rebuilding_complete = true; // because rebuilding was aborted.
  waitUntilShardsHaveEventLogState(client,
                                   {ShardID(5, 0), ShardID(5, 1)},
                                   AuthoritativeStatus::FULLY_AUTHORITATIVE,
                                   rebuilding_complete);
  cluster->getNode(5).waitUntilAllShardsFullyAuthoritative(client);
}

TEST_P(RebuildingTest, NodeDiesAfterDrain) {
  auto log_attrs = logsconfig::LogAttributes()
                       .with_replicationFactor(3)
                       .with_syncedCopies(0)
                       .with_maxWritesInFlight(30);

  // Ensure metadata logs are not stored on the node we are about to remove from
  // the cluster.
  Configuration::MetaDataLogsConfig meta_config =
      createMetaDataLogsConfig({1, 2, 3, 4}, 3, NodeLocationScope::NODE);
  meta_config.sequencers_write_metadata_logs = false;
  meta_config.sequencers_provision_epoch_store = false;

  auto cluster = IntegrationTestUtils::ClusterFactory()
                     .apply(commonSetup())
                     .setParam("--use-force-restore-rebuilding-flag", "true")
                     .setLogGroupName("alog")
                     .setLogAttributes(log_attrs)
                     .setEventLogAttributes(log_attrs)
                     .setMaintenanceLogAttributes(log_attrs)
                     .setNumLogs(3)
                     .setMetaDataLogsConfig(meta_config)
                     .create(6);

  cluster->waitForRecovery();
  auto client = cluster->createClient();

  cluster->getAdminServer()->waitUntilFullyLoaded();
  auto admin_client = cluster->getAdminServer()->createAdminClient();

  // Start draining N5 in RESTORE mode.
  cluster->applyMaintenance(*admin_client,
                            5,
                            0,
                            "testing",
                            /* drain = */ true,
                            /* force_restore = */ true);
  cluster->applyMaintenance(*admin_client,
                            5,
                            1,
                            "testing",
                            /* drain = */ true,
                            /* force_restore = */ true);

  cluster->getNode(5).waitUntilAllShardsAuthoritativeEmpty(client);

  // Kill N5
  cluster->getNode(5).kill();
  // Kill N3 and start rebuilding N3
  cluster->getNode(3).kill();
  cluster->applyInternalMaintenance(*client, 3, 0, "testing");
  cluster->applyInternalMaintenance(*client, 3, 1, "testing");

  // N5 should not be considered as donor and N3's rebuilding
  // should complete
  cluster->getNode(3).waitUntilAllShardsAuthoritativeEmpty(client);
}

// Kill/Restart each storage node one by one. No extras.
TEST_P(RebuildingTest, RollingMiniRebuilding) {
  int nnodes = 5;
  int r = 3;
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
  auto cf = rollingRebuildingClusterFactory(nnodes, r, trim);
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
      // Use only a single shard so that partition creation/flushing commands
      // can be unambiguously targeted.
      .setNumDBShards(1);

  rollingRebuilding(cf, nnodes, 1, 4, 1, NodeFailureMode::KILL, check_args);
}

// Verify that Mini-Rebuildings do not prevent a rebuild from being
// considered authoritative.
TEST_P(RebuildingTest, MiniRebuildingIsAuthoritative) {
  auto log_attrs = logsconfig::LogAttributes()
                       .with_replicationFactor(3)
                       .with_syncedCopies(0)
                       .with_maxWritesInFlight(30);

  NodeSetIndices node_set(5);
  std::iota(node_set.begin(), node_set.end(), 0);
  auto cluster =
      IntegrationTestUtils::ClusterFactory()
          .apply(commonSetup())
          .setLogGroupName("my-test-log")
          .setLogAttributes(log_attrs)
          .setEventLogAttributes(log_attrs)
          .enableSelfInitiatedRebuilding()
          .setMaintenanceLogAttributes(log_attrs)
          .setParam("--append-store-durability", "memory")
          // Set min flush trigger intervals and partition duration high
          // so that only the test is creating/retiring partitions.
          .setParam("--rocksdb-min-manual-flush-interval", "900s")
          .setParam("--rocksdb-partition-duration", "900s")
          // Decrease the timestamp granularity so that we can minimize the
          // amount of wall clock delay required for this test to create
          // adjacent partitions with non-overlapping time ranges.
          .setParam("--rocksdb-partition-timestamp-granularity", "100ms")
          // Use only a single shard so that partition creation/flushing
          // commands can be unambiguously targeted.
          .setNumDBShards(1)
          .create(5);

  cluster->waitForRecovery();

  auto client = cluster->createClient();

  // Write some records..
  dirtyNodes(*cluster, *client, node_set, /*shard*/ 0);

  EventLogRebuildingSet base_set;
  ASSERT_EQ(EventLogUtils::getRebuildingSet(*client, base_set), 0);
  ASSERT_TRUE(base_set.empty());

  // Kill all nodes
  for (auto node : node_set) {
    cluster->getNode(node).kill();
  }

  // Restart all nodes
  for (auto node : node_set) {
    cluster->getNode(node).start();
  }

  ld_info("Waiting for self-initiated rebuild");
  EventLogUtils::tailEventLog(
      *client,
      nullptr,
      [&](const EventLogRebuildingSet& set, const EventLogRecord*, lsn_t) {
        return set.getLastUpdate() <= base_set.getLastUpdate();
      });
  ld_info("Waiting for empty rebuilding set");
  EventLogUtils::tailEventLog(
      *client,
      nullptr,
      [&](const EventLogRebuildingSet& set, const EventLogRecord*, lsn_t) {
        return !set.empty();
      });
}

// We shouldn't have to explicitly mark dirty-nodes unrecoverable
// when they cause other shards to be rebuilt non-authoritatively.
TEST_P(RebuildingTest, MiniRebuildingAlwaysNonRecoverable) {
  // Higher replication factor for event log and metadata logs.
  Configuration::MetaDataLogsConfig meta_config = createMetaDataLogsConfig(
      /*nodeset=*/{1, 2, 3, 4, 5, 6, 7, 8},
      /*replication=*/5,
      NodeLocationScope::NODE);
  meta_config.sequencers_provision_epoch_store = false;
  meta_config.sequencers_write_metadata_logs = false;

  auto cluster =
      IntegrationTestUtils::ClusterFactory()
          .apply(commonSetup())
          .setLogGroupName("mylog")
          .setLogAttributes(logAttributes(3))
          .setEventLogAttributes(logAttributes(5))
          .setMaintenanceLogAttributes(logAttributes(5))
          .setParam("--rebuild-store-durability", "async_write")
          .setMetaDataLogsConfig(meta_config)
          // read quickly when nodes are down
          .setParam("--gap-grace-period", "10ms")
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
          // commands can be unambiguously targeted.
          .setNumDBShards(1)
          .create(9); // 1 sequencer node + 8 storage nodes

  cluster->waitForRecovery();

  auto client = cluster->createClient();
  cluster->getAdminServer()->waitUntilFullyLoaded();
  auto admin_client = cluster->getAdminServer()->createAdminClient();

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

  dirtyNodes(*cluster, *client, full_node_set, /*shard*/ 0);

  // Kill unrecoverable_nodes and clean their DBs. They'll be be marked
  // unrecoverable later.
  ld_info("Killing and removing data from nodes.");
  for (node_index_t node : unrecoverable_node_set) {
    cluster->getNode(node).kill();
    auto shard_path = cluster->getNode(node).getShardPath(0);
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

  ld_info("Waiting for self-initiated rebuild");
  EventLogUtils::tailEventLog(
      *client,
      nullptr,
      [&](const EventLogRebuildingSet& set, const EventLogRecord*, lsn_t) {
        for (node_index_t node : dirty_node_set) {
          if (set.getNodeInfo(node, /*shard*/ 0) == nullptr) {
            return true;
          }
        }
        return false;
      });

  // Should still have write availability. Write some records.
  writeRecordsToMultiplePartitions(*cluster, *client, 30);

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
    cluster->applyInternalMaintenance(*client, node, 0, "testing");
  }
  // Waiting until all nodes are in MIGRATING_DATA state
  for (node_index_t node : unrecoverable_node_set) {
    cluster->getNode(node).waitUntilShardState(
        *admin_client,
        0,
        [](const thrift::ShardState& shard) {
          return shard.get_storage_state() ==
              membership::thrift::StorageState::DATA_MIGRATION;
        },
        "Shard must be in DATA_MIGRATION");
  }
  // Marking the data unrecoverable
  thrift::MarkAllShardsUnrecoverableRequest request;
  request.set_user("test");
  request.set_reason("test");
  thrift::MarkAllShardsUnrecoverableResponse response;
  admin_client->sync_markAllShardsUnrecoverable(response, request);

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
    ASSERT_EQ(0, cluster->bumpGeneration(*admin_client, node));
    cluster->getNode(node).start();
  }
  waitUntilShardsHaveEventLogState(
      client, to_rebuild, AuthoritativeStatus::FULLY_AUTHORITATIVE, true);

  ld_info("Waiting for empty rebuilding set");
  EventLogUtils::tailEventLog(
      *client,
      nullptr,
      [&](const EventLogRebuildingSet& set, const EventLogRecord*, lsn_t) {
        return !set.empty();
      });
}

TEST_P(RebuildingTest, RebuildingWithDifferentDurabilities) {
  auto log_attrs = logsconfig::LogAttributes()
                       .with_replicationFactor(4)
                       .with_syncedCopies(0)
                       .with_maxWritesInFlight(30)
                       .with_backlogDuration(std::chrono::seconds{6 * 3600});

  auto event_log_attrs = logsconfig::LogAttributes()
                             .with_replicationFactor(3)
                             .with_syncedCopies(0)
                             .with_maxWritesInFlight(30);

  ld_info("Creating cluster");
  auto cluster = IntegrationTestUtils::ClusterFactory()
                     .apply(commonSetup())
                     .setParam("--rebuild-store-durability", "async_write")
                     .setParam("--enable-self-initiated-rebuilding", "true")
                     .setLogGroupName("test-log-group")
                     .setLogAttributes(log_attrs)
                     .setEventLogAttributes(event_log_attrs)
                     .setMaintenanceLogAttributes(event_log_attrs)
                     .setNumLogs(42)
                     .create(6);

  // Write some records
  ld_info("Creating client");
  auto client = cluster->createClient();
  cluster->getAdminServer()->waitUntilFullyLoaded();
  auto admin_client = cluster->getAdminServer()->createAdminClient();
  // Write to shard 1 only.
  // Note: I'm not sure why there's a 1ms sleep between records here.
  // This may just be a bad copypasta.
  writeRecordsToMultiplePartitions(*cluster,
                                   *client,
                                   1000,
                                   nullptr,
                                   {logid_t(1), logid_t(3)},
                                   std::chrono::milliseconds(1));

  ld_info("Waiting for recovery");
  cluster->waitForRecovery();

  ld_info("Changing settings");
  cluster->getNode(2).sendCommand("set rebuild-store-durability memory");
  cluster->getNode(3).sendCommand("set rebuild-store-durability memory");

  ld_info("Shutting down N1");
  EXPECT_EQ(0, cluster->getNode(1).shutdown());
  ld_info("Wiping N1");
  cluster->getNode(1).wipeShard(0);
  cluster->getNode(1).wipeShard(1);
  ld_info("Bumping generation");
  ASSERT_EQ(0, cluster->bumpGeneration(*admin_client, 1));
  ld_info("Starting N1");
  cluster->getNode(1).start();
  cluster->getNode(1).waitUntilStarted();

  ld_info("Waiting for rebuilding");
  cluster->getNode(1).waitUntilInternalMaintenances(
      *admin_client,
      [&](const auto& maintenances) { return maintenances.empty(); },
      "Internal maintenances must be removed");

  ASSERT_TRUE(wait_until_enabled_and_healthy(*cluster, 1));
  cluster->getNode(1).waitUntilAllShardsFullyAuthoritative(client);
  ld_info("Waiting for metadata log writes");
  cluster->waitForMetaDataLogWrites();
  // there is a known issue where purging deletes records that gets surfaced in
  // tests with sequencer-written metadata, which is why we skip checking
  // replication for bridge records that this may impact. See t13850978
  IntegrationTestUtils::Cluster::argv_t check_args = {
      "--dont-count-bridge-records",
  };
  // Verify that everything is correctly replicated.
  ld_info("Running checker");
  ASSERT_EQ(0, cluster->checkConsistency(check_args));
}

// Delete some logs, then replace each storage node one by one, re-add them and
// check that their metadata is still intact
TEST_P(RebuildingTest, RebuildMetaDataLogsOfDeletedLogs) {
  int nnodes = 5;
  auto cf = rollingRebuildingClusterFactory(nnodes, 3, true).setNumLogs(4);
  auto cluster = cf.create(nnodes);
  auto get_metadata_record_count = [&](logid_t log_id) {
    std::shared_ptr<Client> client = cluster->createClient();
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

  ASSERT_EQ(1, get_metadata_record_count(logid_t(4)));

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
    cluster->waitForServersToPartiallyProcessConfigUpdate();
  };

  ld_info("Changing config with removed log_id");
  change_logs_config(logid_range_t(logid_t(1), logid_t(4)),
                     logid_range_t(logid_t(1), logid_t(3)));

  // TODO: T23153817, T13850978
  // Remove this once we have a fool-proof
  // solution against purging deleteing records
  IntegrationTestUtils::Cluster::argv_t check_args = {
      "--dont-count-bridge-records",
  };
  rollingRebuilding(
      *cluster, 1, nnodes - 1, NodeFailureMode::REPLACE, check_args);

  ld_info("Changing config with re-added log_id");
  change_logs_config(logid_range_t(logid_t(1), logid_t(3)),
                     logid_range_t(logid_t(1), logid_t(4)));

  ASSERT_EQ(1, get_metadata_record_count(logid_t(4)));
}

// Create under-replicated regions of the log store on a node and
// verify that a reader can successfully read without seeing
// spurious dataloss gaps.
TEST_P(RebuildingTest, UnderReplicatedRegions) {
  auto log_attrs = logsconfig::LogAttributes()
                       .with_replicationFactor(3)
                       .with_syncedCopies(0)
                       .with_maxWritesInFlight(30);

  NodeSetIndices node_set(5);
  std::iota(node_set.begin(), node_set.end(), 0);
  auto cluster =
      IntegrationTestUtils::ClusterFactory()
          .apply(commonSetup())
          .setLogGroupName("my-test-log")
          .setLogAttributes(log_attrs)
          .setEventLogAttributes(log_attrs)
          .setMaintenanceLogAttributes(log_attrs)
          .setParam("--append-store-durability", "memory")
          // Set min flush trigger intervals and partition duration high
          // so that only the test is creating/retiring partitions.
          .setParam("--rocksdb-min-manual-flush-interval", "900s")
          .setParam("--rocksdb-partition-duration", "900s")
          // Decrease the timestamp granularity so that we can minimize the
          // amount of wall clock delay required for this test to create
          // adjacent partitions with non-overlapping time ranges.
          .setParam("--rocksdb-partition-timestamp-granularity", "100ms")
          // Don't request rebuilding of dirty shards so that they stay dirty
          // while clients read data.
          .setParam("--rebuild-dirty-shards", "false")
          // Use only a single shard so that partition creation/flushing
          // commands can be unambiguously targeted.
          .setNumDBShards(1)
          .create(5);

  cluster->waitForRecovery();

  auto client = cluster->createClient();
  client->settings().set("gap-grace-period", "0ms");

  // Write some records..
  std::vector<RecordsInfo> info_per_log;
  dirtyNodes(*cluster, *client, node_set, /*shard*/ 0, &info_per_log);

  // Kill node and restart node 2
  cluster->getNode(2).kill();
  cluster->getNode(2).start();
  cluster->getNode(2).waitUntilStarted();

  cluster->waitForRecovery();

  // Write more records.
  dirtyNodes(*cluster, *client, node_set, /*shard*/ 0, &info_per_log);

  auto reader = client->createReader(LOG_IDS.size());
  size_t total_records = 0;
  for (size_t i = 0; i < LOG_IDS.size(); ++i) {
    int rv =
        reader->startReading(LOG_IDS[i], LSN_OLDEST, info_per_log[i].max_lsn);
    ASSERT_EQ(rv, 0);
    total_records += info_per_log[i].num_records;
  }

  size_t total_read = 0;
  std::vector<std::unique_ptr<DataRecord>> data;
  reader->setTimeout(std::chrono::seconds(1));
  while (reader->isReadingAny()) {
    GapRecord gap;
    int nread = reader->read(1, &data, &gap);
    if (nread < 0) {
      EXPECT_EQ(err, E::GAP);
      EXPECT_EQ(gap.type, GapType::BRIDGE);
      continue;
    }
    if (nread == 0) {
      break;
    }
    total_read += nread;
  }
  EXPECT_EQ(total_read, total_records);

  cluster->shutdownNodes(node_set);
}

// Do a rebuilding that doesn't have anything to rebuild but doesn't know it in
// advance. Check that it skips all the records using csi and does so using
// near-minimum number of seeks/nexts of csi iterator.
TEST_P(RebuildingTest, SkipEverything) {
  auto log_attrs = logsconfig::LogAttributes()
                       .with_replicationFactor(2)
                       .with_syncedCopies(0)
                       .with_maxWritesInFlight(1000);

  auto event_log_attrs = logsconfig::LogAttributes()
                             .with_replicationFactor(2)
                             .with_syncedCopies(0)
                             .with_maxWritesInFlight(30);

  Configuration::MetaDataLogsConfig meta_config = createMetaDataLogsConfig(
      /*nodeset=*/{0, 1},
      /*replication=*/2,
      NodeLocationScope::NODE);
  meta_config.sequencers_provision_epoch_store = false;
  meta_config.sequencers_write_metadata_logs = false;

  Configuration::Nodes nodes(3);
  for (int i = 0; i < 3; ++i) {
    nodes[i].setName("Node-" + std::to_string(i));
    nodes[i].addSequencerRole();
    nodes[i].addStorageRole(/*num_shards*/ 1);
    nodes[i].generation = 1;
  }

  auto nodes_configuration =
      NodesConfigurationTestUtil::provisionNodes(std::move(nodes));
  /*
   *nodes_configuration = nodes_configuration->applyUpdate(
   *    NodesConfigurationTestUtil::setStorageMembershipUpdate(
   *        *nodes_configuration,
   *        {ShardID(2, -1)},
   *        membership::StorageState::READ_ONLY,
   *        folly::none));
   */

  ld_info("Creating cluster");
  auto cluster = IntegrationTestUtils::ClusterFactory()
                     .apply(commonSetup())
                     .setLogGroupName("a")
                     .setLogAttributes(log_attrs)
                     .setEventLogAttributes(event_log_attrs)
                     .setMaintenanceLogAttributes(event_log_attrs)
                     .setMetaDataLogsConfig(meta_config)
                     .setNodes(std::move(nodes_configuration))
                     .setNumDBShards(1)
                     .useHashBasedSequencerAssignment()
                     .create(3);

  cluster->waitUntilAllStartedAndPropagatedInGossip();

  ld_info("Creating client");
  auto client = cluster->createClient();
  cluster->getAdminServer()->waitUntilFullyLoaded();
  auto admin_client = cluster->getAdminServer()->createAdminClient();
  // N2 needs to be READ_ONLY
  cluster->applyMaintenance(
      *admin_client, 2, 0, /* user = */ "testing", /* drain = */ false);

  cluster->getNode(2).waitUntilShardState(
      *admin_client,
      0,
      [](const thrift::ShardState& shard) {
        return shard.get_storage_state() ==
            membership::thrift::StorageState::READ_ONLY;
      },
      "Shard N2:S0 must be READ_ONLY");

  ld_info("Writing records");
  writeRecordsToMultiplePartitions(*cluster, *client, 400);

  ld_info("Requesting drain of N2");
  ld_info(
      "Maintenance for N2:S0 created: %s",
      cluster
          ->applyMaintenance(
              *admin_client, 2, 0, /* user = */ "testing2", /* drain = */ true)
          .c_str());

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
    // Each node has at least 400 records in the data log, and rebuilding should
    // read a CSI entry for each record at least once.
    check_stat("read_streams_rocksdb_locallogstore_csi_next_reads", 300, 600);
    check_stat("read_streams_rocksdb_locallogstore_csi_seek_reads", 1, 200);
    check_stat("read_streams_rocksdb_locallogstore_record_next_reads", 0, 200);
    check_stat("read_streams_rocksdb_locallogstore_record_seek_reads", 0, 200);
  }
}

TEST_P(RebuildingTest, DerivedStats) {
  ld_info("Creating cluster");
  auto cluster = IntegrationTestUtils::ClusterFactory()
                     .apply(commonSetup())
                     .setNumDBShards(1)
                     .useHashBasedSequencerAssignment()
                     .setInternalLogsReplicationFactor(3)
                     // Prevent self-initiated rebuilding of empty shards.
                     .setParam("--enable-self-initiated-rebuilding", "false")
                     .create(5);

  ld_info("Creating Client");
  auto client = cluster->createClient();
  cluster->getAdminServer()->waitUntilFullyLoaded();

  auto admin_client = cluster->getAdminServer()->createAdminClient();

  auto stats = cluster->getNode(1).stats();
  EXPECT_EQ(0, stats["shards_waiting_for_non_started_restore"]);
  EXPECT_EQ(0, stats["non_empty_shards_in_restore"]);

  ld_info("Shutting down N1 and N2");
  EXPECT_EQ(0, cluster->shutdownNodes({1, 2}));
  // Wipe N1 and start it.
  ld_info("Wiping N1");
  cluster->getNode(1).wipeShard(0);
  ASSERT_EQ(0, cluster->bumpGeneration(*admin_client, 1));
  ld_info("Starting N1");
  cluster->getNode(1).start();
  cluster->getNode(1).waitUntilStarted();

  stats = cluster->getNode(1).stats();
  EXPECT_EQ(1, stats["shards_waiting_for_non_started_restore"]);
  EXPECT_EQ(0, stats["non_empty_shards_in_restore"]);

  ld_info("Requesting rebuilding of N1");
  cluster->applyInternalMaintenance(
      *client, 1, 0, "Requesting rebuilding from the test case");

  ld_info("Waiting for maintenance to be applied");
  cluster->getNode(1).waitUntilInternalMaintenances(
      *admin_client,
      [&](const auto& maintenances) { return !maintenances.empty(); },
      "Internal maintenances must not be empty");

  // Rebuilding should be stuck because N2 is down.
  // The event propagation inside the server is asynchronous, even after the
  // waitUntilEventLogSynced() (RebuildingCoordinator uses a zero-delay
  // timer and a storage task for restarting and acking rebuilding
  // correspondingly).
  wait_until("shards_waiting_for_non_started_restore", [&cluster, &stats] {
    stats = cluster->getNode(1).stats();
    return 0 == stats["shards_waiting_for_non_started_restore"];
  });
  EXPECT_EQ(0, stats["non_empty_shards_in_restore"]);

  // Start N2.
  ld_info("Starting N2");
  cluster->getNode(2).start();

  // Rebuilding should be able to complete now.
  ld_info("Waiting for rebuilding of N1");
  ASSERT_TRUE(wait_until_enabled_and_healthy(*cluster, 1));
  cluster->getNode(1).waitUntilAllShardsFullyAuthoritative(client);

  stats = cluster->getNode(1).stats();
  EXPECT_EQ(0, stats["shards_waiting_for_non_started_restore"]);

  // Logically, non_empty_shards_in_restore should stay zero for the whole
  // duration of the test. But due to a race condition, it may change to 1
  // for a brief moment, then back to 0:
  // RebuildingCoordinator::notifyProcessorShardRebuilt() sets
  // shard_missing_all_data to false, while full_restore_set_contains_myself
  // remains true until a SHARD_ACK_REBUILT is written and read back.
  wait_until("non_empty_shards_in_restore", [&cluster, &stats] {
    stats = cluster->getNode(1).stats();
    return 0 == stats["non_empty_shards_in_restore"];
  });

  // Stop N2 again to make the next rebuilding stall.
  ld_info("Shutting down N2");
  cluster->getNode(2).shutdown();

  // Now let's drain N3.
  ld_info("Requesting drain of N3");
  auto maintenance_id = cluster->applyMaintenance(
      *admin_client, 3, 0, "testing", /* drain = */ true);

  stats = cluster->getNode(3).stats();
  EXPECT_EQ(0, stats["shards_waiting_for_non_started_restore"]);
  EXPECT_EQ(0, stats["non_empty_shards_in_restore"]);

  ld_info("Starting N2");
  cluster->getNode(2).start();

  ld_info("Waiting for drain of N3 to complete");
  auto event_lsn =
      cluster->getNode(3).waitUntilAllShardsAuthoritativeEmpty(client);

  stats = cluster->getNode(3).stats();
  EXPECT_EQ(0, stats["shards_waiting_for_non_started_restore"]);
  EXPECT_EQ(0, stats["non_empty_shards_in_restore"]);

  // Wipe N3.
  ld_info("Shutting down N3");
  EXPECT_EQ(0, cluster->getNode(3).shutdown());
  ld_info("Wiping N3");
  cluster->getNode(3).wipeShard(0);
  ASSERT_EQ(0, cluster->bumpGeneration(*admin_client, 3));
  ld_info("Starting N3");
  cluster->getNode(3).start();
  cluster->getNode(3).waitUntilStarted();

  // Should still be authoritative empty and not need restore rebuilding.
  ld_info("Waiting for N3 to catch up in event log");
  EXPECT_EQ(0, cluster->waitUntilEventLogSynced(event_lsn, {3}));

  stats = cluster->getNode(3).stats();
  // RebuildingCoordinator does things asynchronously even after
  // waitUntilEventLogSynced(). Wait for it to apply the event log update.
  wait_until("shards_waiting_for_non_started_restore", [&cluster, &stats] {
    stats = cluster->getNode(1).stats();
    return 0 == stats["shards_waiting_for_non_started_restore"];
  });
  EXPECT_EQ(0, stats["non_empty_shards_in_restore"]);

  // Undrain N3 and wait for it to ack.
  ld_info("Marking N3 undrained");
  // Request we abort draining N3
  {
    thrift::RemoveMaintenancesRequest req;
    thrift::MaintenancesFilter filter;
    filter.set_group_ids({maintenance_id});
    req.set_user("test");
    req.set_filter(filter);
    thrift::RemoveMaintenancesResponse resp;
    admin_client->sync_removeMaintenances(resp, req);
    ASSERT_EQ(1, resp.get_maintenances().size());
  }
  ld_info("Waiting for N3 to ack");
  ASSERT_TRUE(wait_until_enabled_and_healthy(*cluster, 3));
  cluster->getNode(3).waitUntilAllShardsFullyAuthoritative(client);

  wait_until("non_empty_shards_in_restore", [&cluster, &stats] {
    stats = cluster->getNode(3).stats();
    return 0 == stats["non_empty_shards_in_restore"];
  });
  EXPECT_EQ(0, stats["shards_waiting_for_non_started_restore"]);
}

// During rebuilding records are underreplicated.
// Make sure replication_checker doesn't raise errors about it.
TEST_P(RebuildingTest, ReplicationCheckerDuringRebuilding) {
  ld_info("Creating cluster");
  auto cluster = IntegrationTestUtils::ClusterFactory()
                     .apply(commonSetup())
                     .setNumDBShards(1)
                     .useHashBasedSequencerAssignment()
                     .setLogGroupName("mylog")
                     .setLogAttributes(logAttributes(2))
                     .setInternalLogsReplicationFactor(2)
                     .setParam("--test-stall-rebuilding", "true")
                     .create(3);
  cluster->waitUntilAllStartedAndPropagatedInGossip();

  // Append a record, making sure that N2 is in the copyset.
  ld_info("Updating setting");
  for (node_index_t n : {0, 1, 2}) {
    cluster->getNode(n).updateSetting("test-do-not-pick-in-copysets", "0");
  }

  ld_info("Creating Client");
  auto client = cluster->createClient();
  cluster->getAdminServer()->waitUntilFullyLoaded();
  auto admin_client = cluster->getAdminServer()->createAdminClient();
  ld_info("Appending record");
  std::string data("hello");
  lsn_t lsn = client->appendSync(LOG_IDS[0], Payload(data.data(), data.size()));
  ASSERT_NE(LSN_INVALID, lsn);

  ld_info("Un-updating setting");
  for (node_index_t n : {0, 1, 2}) {
    cluster->getNode(n).unsetSetting("test-do-not-pick-in-copysets");
  }

  ld_info("Shutting down N2");
  EXPECT_EQ(0, cluster->shutdownNodes({2}));

  ld_info("Requesting rebuilding of N2");
  cluster->applyInternalMaintenance(
      *client, 2, 0, "Requesting rebuilding from the test case");

  cluster->getNode(2).waitUntilShardState(
      *admin_client,
      0,
      [](const thrift::ShardState& shard) {
        return shard.get_data_health() == thrift::ShardDataHealth::UNAVAILABLE;
      },
      "Shard data health must be UNAVAILABLE");

  // Marking the shards unrecoverable
  thrift::MarkAllShardsUnrecoverableRequest request;
  request.set_user("test");
  request.set_reason("test");
  thrift::MarkAllShardsUnrecoverableResponse response;
  admin_client->sync_markAllShardsUnrecoverable(response, request);

  // Read the record back to make sure it's released.
  ld_info("Reading");
  auto reader = client->createReader(1);
  ASSERT_EQ(0, reader->startReading(LOG_IDS[0], lsn));
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
  auto event_lsn =
      cluster->getNode(2).waitUntilAllShardsFullyAuthoritative(client);

  ld_info("Waiting for nodes to catch up in event log");
  EXPECT_EQ(0, cluster->waitUntilEventLogSynced(event_lsn, {0, 1, 2}));

  // Make a new client to discard any stale authoritative status.
  ld_info("Re-creating client");
  client = cluster->createClient();

  ld_info("Running checker again, expecting errors");
  EXPECT_NE(0, cluster->checkConsistency());
}

/*
 * Test the disable-data-log-rebuilding setting.
 * This feature is tested with the following 4 test cases:
 * - Shards are wiped before a failed node comes back:
 * DisableDataLogRebuildShardsWiped
 * - Shards come back in good condition: DisableDataLogRebuildShardsWiped
 * - Shards (node) never comes back: DisableDataLogRebuildNodeFailed
 * - Shards that need rebuild had no data.
 *
 * Each test runs two iterations. The second iteration
 * is to ensure that there is no incorrectly left-over rebuilding state,
 * from the previous iteration, that impacts future
 * rebuilds.
 */

// Case: shards come back wiped.
TEST_P(RebuildingTest, DisableDataLogRebuildShardsWiped) {
  // FIXME: Need to add a mix of retentions.
  // Shorter than half of the test duration.
  std::chrono::seconds maxBacklogDuration(20);

  ld_info("Creating cluster");

  auto log_attrs = logsconfig::LogAttributes()
                       .with_replicationFactor(3)
                       .with_syncedCopies(0)
                       .with_maxWritesInFlight(30)
                       .with_backlogDuration(maxBacklogDuration);

  auto event_log_attrs = logsconfig::LogAttributes()
                             .with_replicationFactor(3)
                             .with_syncedCopies(0)
                             .with_maxWritesInFlight(30);

  auto cluster =
      IntegrationTestUtils::ClusterFactory()
          .apply(commonSetup())
          .setParam("--rebuild-store-durability", "async_write")
          .setParam("--disable-data-log-rebuilding", "true")
          .setParam("--shard-is-rebuilt-msg-delay", "0s..2s")
          .setParam("--rebuilding-restarts-grace-period", "1ms")
          .setLogGroupName("test-log-group")
          .setLogAttributes(log_attrs)
          .setEventLogAttributes(event_log_attrs)
          .setMaintenanceLogAttributes(event_log_attrs)
          .eventLogMode(
              IntegrationTestUtils::ClusterFactory::EventLogMode::SNAPSHOTTED)
          .setNumLogs(10)
          .create(7);

  cluster->getAdminServer()->waitUntilFullyLoaded();
  auto admin_client = cluster->getAdminServer()->createAdminClient();
  // Run two iterations of each test to make sure that
  // there is no incorrectly left over state from the previous
  // iterations that impacts future rebuilds.
  int id = 1;
  int maxIters = 2;
  for (int iter = 0; iter < maxIters; iter++) {
    int numRecords = 100;

    // Write some records
    ld_info("Writing records");
    auto client = cluster->createClient();
    while (numRecords--) {
      std::string data("data" + std::to_string(id++));
      lsn_t lsn = client->appendSync(
          logid_t(numRecords % 10 + 1), Payload(data.data(), data.size()));
      /* sleep override */
      std::this_thread::sleep_for(std::chrono::milliseconds(1));
      ASSERT_NE(LSN_INVALID, lsn);
    }

    ld_info("Waiting for recovery");
    cluster->waitForRecovery();

    // Wipe some of the shards.
    ld_info("Wiping shards and restarting nodes");
    int nodeToFail = 1;
    EXPECT_EQ(0, cluster->getNode(nodeToFail).shutdown());
    cluster->getNode(nodeToFail).wipeShard(0);
    cluster->getNode(nodeToFail).wipeShard(1);
    ASSERT_EQ(0, cluster->bumpGeneration(*admin_client, nodeToFail));
    cluster->getNode(nodeToFail).start();
    cluster->getNode(nodeToFail).waitUntilStarted();

    // Ask to rebuild the shards.
    ld_info("Requesting rebuilding");
    cluster->applyInternalMaintenance(
        *client, nodeToFail, 0, "Requesting rebuilding from the test case");
    cluster->applyInternalMaintenance(
        *client, nodeToFail, 1, "Requesting rebuilding from the test case");

    // Wait a little and fail the same shard on another node
    ld_info("Waiting, wiping more shards, restarting nodes");
    nodeToFail = 4;
    std::this_thread::sleep_for(std::chrono::seconds(1));
    EXPECT_EQ(0, cluster->getNode(nodeToFail).shutdown());
    cluster->getNode(nodeToFail).wipeShard(0);
    cluster->getNode(nodeToFail).wipeShard(1);
    ASSERT_EQ(0, cluster->bumpGeneration(*admin_client, nodeToFail));
    cluster->getNode(nodeToFail).start();
    cluster->getNode(nodeToFail).waitUntilStarted();

    // The max time we expect to wait is from the latest
    // SHARD_NEEDS_REBUILD message.
    auto tstart = RecordTimestamp::now().toSeconds();

    // Ask to rebuild the shards.
    ld_info("Requesting rebuilding");
    cluster->applyInternalMaintenance(
        *client, nodeToFail, 0, "Requesting rebuilding from the test case");
    cluster->applyInternalMaintenance(
        *client, nodeToFail, 1, "Requesting rebuilding from the test case");

    // Wait a little and restart one of the other nodes. We want to
    // make sure that donor restarts don't impact the expected outcome.
    ld_info("Waiting and restarting nodes");
    nodeToFail = 2;
    std::this_thread::sleep_for(std::chrono::seconds(1));
    EXPECT_EQ(0, cluster->getNode(nodeToFail).shutdown());
    cluster->getNode(nodeToFail).start();
    cluster->getNode(nodeToFail).waitUntilStarted();

    ld_info("Waiting for fully authoritative");
    cluster->getNode(1).waitUntilAllShardsFullyAuthoritative(client);
    cluster->getNode(4).waitUntilAllShardsFullyAuthoritative(client);

    // The failed shards must be FA only after all its original data expired.
    auto elapsed = RecordTimestamp::now().toSeconds() - tstart;
    ASSERT_LE(maxBacklogDuration.count(), elapsed.count());
  }

  ld_info("All done");
}

// Case: shards come back good.
TEST_P(RebuildingTest, DisableDataLogRebuildShardsAborted) {
  // Longer than test duration.
  std::chrono::seconds maxBacklogDuration(300);

  auto log_attrs = logsconfig::LogAttributes()
                       .with_replicationFactor(3)
                       .with_syncedCopies(0)
                       .with_maxWritesInFlight(30)
                       .with_backlogDuration(maxBacklogDuration);

  auto event_log_attrs = logsconfig::LogAttributes()
                             .with_replicationFactor(3)
                             .with_syncedCopies(0)
                             .with_maxWritesInFlight(60);

  auto cluster =
      IntegrationTestUtils::ClusterFactory()
          .apply(commonSetup())
          .setParam("--rebuild-store-durability", "async_write")
          .setParam("--disable-data-log-rebuilding", "true")
          .setParam("--shard-is-rebuilt-msg-delay", "0s..2s")
          .setLogGroupName("test-log-group")
          .setLogAttributes(log_attrs)
          .setEventLogAttributes(event_log_attrs)
          .setMaintenanceLogAttributes(event_log_attrs)
          .eventLogMode(
              IntegrationTestUtils::ClusterFactory::EventLogMode::SNAPSHOTTED)
          .setNumLogs(42)
          .create(5);

  cluster->getAdminServer()->waitUntilFullyLoaded();
  auto admin_client = cluster->getAdminServer()->createAdminClient();
  // Run two iterations of each test to make sure that
  // there is no incorrectly left over state from the previous
  // iterations that impacts future rebuilds.
  int id = 1;
  int maxIters = 2;
  for (int iter = 0; iter < maxIters; iter++) {
    // Write some records
    auto client = cluster->createClient();
    writeRecordsToMultiplePartitions(*cluster,
                                     *client,
                                     1000,
                                     nullptr,
                                     LOG_IDS,
                                     std::chrono::milliseconds(1));

    cluster->waitForRecovery();

    EXPECT_EQ(0, cluster->getNode(1).shutdown());
    auto tstart = RecordTimestamp::now().toSeconds();

    // Ask to rebuild the shards.
    int numShards = cluster->getNode(1).num_db_shards_;
    int nodeToFail = 1;
    for (shard_index_t shard = 0; shard < numShards; shard++) {
      cluster->applyInternalMaintenance(
          *client,
          nodeToFail,
          shard,
          "Requesting rebuilding from the test case");
    }

    // Sleep for a little and re-enable the node. The nodes data is available
    // again.
    std::this_thread::sleep_for(std::chrono::seconds(4));
    cluster->getNode(nodeToFail).start();
    cluster->getNode(nodeToFail).waitUntilStarted();

    // Wait a little and restart one of the other nodes. We want to
    // make sure that donor restarts don't impact the expected outcome.
    nodeToFail = 2;
    std::this_thread::sleep_for(std::chrono::seconds(1));
    EXPECT_EQ(0, cluster->getNode(nodeToFail).shutdown());
    std::this_thread::sleep_for(std::chrono::seconds(1));
    cluster->getNode(nodeToFail).start();
    cluster->getNode(nodeToFail).waitUntilStarted();

    cluster->getNode(nodeToFail).waitUntilAllShardsFullyAuthoritative(client);

    // Rebuilding should have aborted and all shards must
    // be FA without waiting for the maxBacklogDuration.
    auto elapsed = RecordTimestamp::now().toSeconds() - tstart;
    ASSERT_GT(maxBacklogDuration.count(), elapsed.count());
  }
}

// Case: shards never come back.
TEST_P(RebuildingTest, DisableDataLogRebuildNodeFailed) {
  // Shorter than half of the test duration.
  std::chrono::seconds maxBacklogDuration(15);

  auto log_attrs = logsconfig::LogAttributes()
                       .with_replicationFactor(3)
                       .with_syncedCopies(0)
                       .with_maxWritesInFlight(30)
                       .with_backlogDuration(maxBacklogDuration);

  auto event_log_attrs = logsconfig::LogAttributes()
                             .with_replicationFactor(3)
                             .with_syncedCopies(0)
                             .with_maxWritesInFlight(30);

  auto cluster =
      IntegrationTestUtils::ClusterFactory()
          .apply(commonSetup())
          .setParam("--rebuild-store-durability", "async_write")
          .setParam("--disable-data-log-rebuilding", "true")
          .setParam("--shard-is-rebuilt-msg-delay", "0s..2s")
          .setLogGroupName("test-log-group")
          .setLogAttributes(log_attrs)
          .setEventLogAttributes(event_log_attrs)
          .setMaintenanceLogAttributes(event_log_attrs)
          .eventLogMode(
              IntegrationTestUtils::ClusterFactory::EventLogMode::SNAPSHOTTED)
          .setNumLogs(42)
          .create(5);

  cluster->waitForRecovery();
  cluster->getAdminServer()->waitUntilFullyLoaded();
  auto admin_client = cluster->getAdminServer()->createAdminClient();

  // Run two iterations of each test to make sure that
  // there is no incorrectly left over state from the previous
  // iterations that impacts future rebuilds.
  int id = 1;
  int maxIters = 2;
  for (int iter = 0; iter < maxIters; iter++) {
    // Write some records
    auto client = cluster->createClient();
    writeRecordsToMultiplePartitions(*cluster,
                                     *client,
                                     1000,
                                     nullptr,
                                     LOG_IDS,
                                     std::chrono::milliseconds(1));

    cluster->waitForRecovery();

    // Fail node and ask to rebuild its shards.
    int nodeToFail = 1;
    EXPECT_EQ(0, cluster->getNode(nodeToFail).shutdown());

    auto tstart = RecordTimestamp::now().toSeconds();
    int numShards = cluster->getNode(nodeToFail).num_db_shards_;
    std::vector<ShardID> rebuildingShards;
    for (shard_index_t shard = 0; shard < numShards; shard++) {
      cluster->applyInternalMaintenance(
          *client,
          nodeToFail,
          shard,
          "Requesting rebuilding from the test case");
      rebuildingShards.push_back(ShardID(nodeToFail, shard));
    }

    waitUntilShardsHaveEventLogState(client,
                                     rebuildingShards,
                                     AuthoritativeStatus::AUTHORITATIVE_EMPTY,
                                     true);

    // The failed shards can be AE only after all its original data has expired.
    auto elapsed = RecordTimestamp::now().toSeconds() - tstart;
    ASSERT_LT(maxBacklogDuration.count(), elapsed.count());

    // Restart the node. It should become FA.
    cluster->getNode(nodeToFail).start();
    cluster->getNode(nodeToFail).waitUntilStarted();
    ASSERT_TRUE(wait_until_enabled_and_healthy(*cluster, nodeToFail));
  }
}

TEST_P(RebuildingTest, DirtyRangeAdminCommands) {
  // Longer than test duration.
  std::chrono::seconds maxBacklogDuration(300);

  auto log_attrs = logsconfig::LogAttributes()
                       .with_replicationFactor(3)
                       .with_syncedCopies(0)
                       .with_maxWritesInFlight(30)
                       .with_backlogDuration(maxBacklogDuration);

  auto event_log_attrs = logsconfig::LogAttributes()
                             .with_replicationFactor(3)
                             .with_syncedCopies(0)
                             .with_maxWritesInFlight(30);

  NodeSetIndices node_set(5);
  std::iota(node_set.begin(), node_set.end(), 0);
  auto cluster =
      IntegrationTestUtils::ClusterFactory()
          .apply(commonSetup())
          .setLogGroupName("test-log-group")
          .setLogAttributes(log_attrs)
          .setEventLogAttributes(event_log_attrs)
          .setParam("--append-store-durability", "memory")
          // Set min flush trigger intervals and partition duration high
          // so that only the test is creating/retiring partitions.
          .setParam("--rocksdb-partition-idle-flush-trigger", "900s")
          .setParam("--rocksdb-partition-data-age-flush-trigger", "900s")
          .setParam("--rocksdb-partition-flush-check-period", "900s")
          .setParam("--rocksdb-min-manual-flush-interval", "900s")
          .setParam("--rocksdb-partition-duration", "900s")
          // Decrease the timestamp granularity so that we can minimize the
          // amount of wall clock delay required for this test to create
          // adjacent partitions with non-overlapping time ranges.
          .setParam("--rocksdb-partition-timestamp-granularity", "100ms")
          // Print all flush events to server's log, for debugging.
          .setParam("--rocksdb-print-details", "true")
          // Disable rebuilding so dirty time ranges are only retired if
          // they expire by retention, or are explicitly cleared by this
          // test.
          .setParam("--disable-data-log-rebuilding")
          .setParam("--shard-is-rebuilt-msg-delay", "0s..2s")
          // Use only a single shard so that partition creation/flushing
          // commands can be unambiguously targeted.
          .setNumDBShards(1)
          .useHashBasedSequencerAssignment()
          .create(5);

  cluster->waitForRecovery();

  auto client = cluster->createClient();

  // Write records and generate partitions...
  dirtyNodes(*cluster, *client, node_set, /*shard*/ 0);
  // And a few more partitions to ensure we have enough to work with.
  dirtyNodes(*cluster, *client, node_set, /*shard*/ 0);

  auto& node1 = cluster->getNode(1);

  // Crash the node so dirty partition state is persisted to the
  // RebuildingRanges metadata.
  node1.kill();
  node1.start();
  node1.waitUntilStarted();

  // ----------------------------- Helpers ------------------------------------
  using namespace std::chrono_literals;

  auto get_partitions = [&node1] {
    return node1.partitionsInfo(/*shard*/ 0, /*level*/ 2);
  };

  auto count_dirty_partitions = [&]() {
    auto partitions = get_partitions();
    return std::count_if(partitions.begin(), partitions.end(), [](auto p) {
      return p["Under Replicated"] == "1";
    });
  };

  auto find_partition = [&](bool dirty) {
    auto partitions = get_partitions();
    for (auto partition : partitions) {
      if (partition["Under Replicated"] == (dirty ? "1" : "0")) {
        return partition;
      }
    }
    // Test invariants broken. Log a failure.
    EXPECT_FALSE(true);
    return partitions.front();
  };

  auto find_partition_by_id = [&](std::string id) {
    auto partitions = get_partitions();
    for (auto partition : partitions) {
      if (partition["ID"] == id) {
        return partition;
      }
    }
    // Test invariants broken. Log a failure.
    EXPECT_FALSE(true);
    return partitions.front();
  };

  // Ensure there's a mix of clean and dirty partitions to work with.
  auto assert_good_partition_mix = [&]() {
    ASSERT_GT(get_partitions().size(), 5);
    ASSERT_GE(count_dirty_partitions(), 1);
    ASSERT_NE(get_partitions().size(), count_dirty_partitions());
    EXPECT_FALSE(node1.dirtyShardInfo().empty());
  };

  auto send_cmd = [&](std::string dirty_or_clean, auto min, auto max) {
    auto cmd_str =
        folly::format("rebuilding mark_{} 0 --time-from='{}' --time-to='{}'",
                      dirty_or_clean,
                      min,
                      max)
            .str();
    ld_info("Sending command %s", cmd_str.c_str());
    std::string response = node1.sendCommand(cmd_str);
    ASSERT_EQ(response, "Done.\r\n");
  };

  auto start_time = [](auto partition) {
    return RecordTimestamp(
        std::chrono::milliseconds(std::stoull(partition["Start Time"])));
  };

  // Take care in case the partition has not seen any records:
  // min == RecordTimestamp::max().
  auto min_time = [&](auto partition) {
    auto min = std::chrono::milliseconds(std::stoll(partition["Min Time"]));
    return (min == RecordTimestamp::max().time_since_epoch())
        ? start_time(partition)
        : RecordTimestamp(min);
  };

  // Take care in case the partition has not seen any records:
  // max == RecordTimestamp::min().
  auto max_time = [&](auto partition) {
    auto max = std::chrono::milliseconds(std::stoll(partition["Max Time"]));
    return (max == RecordTimestamp::min().time_since_epoch())
        ? start_time(partition)
        : RecordTimestamp(max);
  };

  // --------------------------- Test Cases -----------------------------------
  // NOTE: Test case order matters. Some early test cases assume the
  //       time ranges listed in dirtyShardInfo() match the min/max ranges
  //       of under-replicated partitions. That stops being true after
  //       test cases that clear only part of a partition's time range.

  // Clearing a time range that is before all partitions should have no effect.
  {
    assert_good_partition_mix();

    auto base_dirty_info = node1.dirtyShardInfo();
    auto base_dirty_pariritions = count_dirty_partitions();
    auto partitions = get_partitions();
    auto& partition0 = partitions.front();
    auto min = min_time(partition0);

    send_cmd("clean", (min - 6000ms).toString(), (min - 1000ms).toString());

    EXPECT_EQ(base_dirty_pariritions, count_dirty_partitions());
    EXPECT_EQ(toString(node1.dirtyShardInfo()), toString(base_dirty_info));
  }

  // Clearing a time range that is after all partitions should have no effect.
  {
    assert_good_partition_mix();

    auto base_dirty_info = node1.dirtyShardInfo();
    auto base_dirty_pariritions = count_dirty_partitions();
    auto partitions = get_partitions();
    auto& last_partition = partitions.back();
    auto max = max_time(last_partition);

    send_cmd("clean", max + 1000ms, max + 6000ms);

    EXPECT_EQ(base_dirty_pariritions, count_dirty_partitions());
    EXPECT_EQ(toString(node1.dirtyShardInfo()), toString(base_dirty_info));
  }

  // Dirtying a range before all partitions should update the dirty shard
  // metadata, but not find any partitions to mark dirty.
  {
    assert_good_partition_mix();

    auto base_dirty_info = node1.dirtyShardInfo();
    auto base_dirty_pariritions = count_dirty_partitions();
    auto partitions = get_partitions();
    auto& partition0 = partitions.front();
    auto min = min_time(partition0);

    send_cmd("dirty", min - 30s, min - 20s);

    EXPECT_EQ(base_dirty_pariritions, count_dirty_partitions());
    EXPECT_NE(toString(node1.dirtyShardInfo()), toString(base_dirty_info));
  }

  // Dirtying a range after all partitions should update the dirty shard
  // metadata, but not find any partitions to mark dirty.
  {
    assert_good_partition_mix();

    auto base_dirty_info = node1.dirtyShardInfo();
    auto base_dirty_pariritions = count_dirty_partitions();
    auto partitions = get_partitions();
    auto& last_partition = partitions.back();
    auto max = max_time(last_partition);

    send_cmd("dirty", max + 1000ms, max + 6000ms);

    EXPECT_EQ(base_dirty_pariritions, count_dirty_partitions());
    EXPECT_NE(toString(node1.dirtyShardInfo()), toString(base_dirty_info));
  }

  // Add a dirty range that matches an existing dirty partition.
  // Should be a no-op.
  {
    assert_good_partition_mix();

    auto base_dirty_info = node1.dirtyShardInfo();
    auto base_partitions = get_partitions();
    auto partition = find_partition(/*dirty*/ true);
    auto min = min_time(partition);
    auto max = max_time(partition);
    ASSERT_GT((max - min).count(), 1);

    send_cmd("dirty", min.toString(), max.toString());

    auto partitions = get_partitions();
    EXPECT_EQ(partitions, base_partitions);
    EXPECT_EQ(toString(node1.dirtyShardInfo()), toString(base_dirty_info));
  }

  // Removing part of a partition's time range should update the
  // RebuildignRanges metadata, but leave the partition as under-replicated.
  {
    assert_good_partition_mix();

    auto base_dirty_info = node1.dirtyShardInfo();
    auto partition = find_partition(/*dirty*/ true);
    auto min = min_time(partition);
    auto max = max_time(partition);
    ASSERT_GT((max - min).count(), 1);

    send_cmd("clean", min, min + 1ms);

    partition = find_partition_by_id(partition["ID"]);
    EXPECT_EQ(partition["Under Replicated"], "1");
    EXPECT_NE(toString(node1.dirtyShardInfo()), toString(base_dirty_info));
  }

  // Removing all of a partition's time range should update the
  // RebuildignRanges metadata, and clear the partition's under-replicated
  // status.
  {
    assert_good_partition_mix();

    auto base_dirty_info = node1.dirtyShardInfo();
    auto partition = find_partition(/*dirty*/ true);
    EXPECT_EQ(partition["Under Replicated"], "1");

    send_cmd("clean", min_time(partition), max_time(partition));

    partition = find_partition_by_id(partition["ID"]);
    EXPECT_EQ(partition["Under Replicated"], "0");
    EXPECT_NE(toString(node1.dirtyShardInfo()), toString(base_dirty_info));
  }

  // Adding a dirty range that spans part of two adjoining, clean
  // partitions, should mark both partitions as under-replicated.
  {
    assert_good_partition_mix();

    auto base_dirty_info = node1.dirtyShardInfo();
    auto partitions = get_partitions();
    for (auto it = partitions.begin();; ++it) {
      auto next_it = std::next(it);
      if (next_it == partitions.end()) {
        ld_error("Unable to find two adjoining clean partitions");
        FAIL();
        break;
      }
      if ((*it)["Under Replicated"] == "1" ||
          (*next_it)["Under Replicated"] == "1") {
        continue;
      }

      send_cmd("dirty", max_time(*it), min_time(*next_it));

      EXPECT_EQ(find_partition_by_id((*it)["ID"])["Under Replicated"], "1");
      EXPECT_EQ(
          find_partition_by_id((*next_it)["ID"])["Under Replicated"], "1");
      EXPECT_NE(toString(node1.dirtyShardInfo()), toString(base_dirty_info));
      break;
    }
  }

  // Verify that nothing is left dirty if all ranges are cleared.
  {
    assert_good_partition_mix();

    auto base_dirty_info = node1.dirtyShardInfo();
    ASSERT_FALSE(base_dirty_info.empty());

    send_cmd("clean",
             RecordTimestamp::zero(),
             RecordTimestamp::now() + std::chrono::hours(1));

    EXPECT_EQ(count_dirty_partitions(), 0);
    EXPECT_TRUE(node1.dirtyShardInfo().empty());

    // Update base for next test.
    base_dirty_info = node1.dirtyShardInfo();
  }
}

TEST_P(RebuildingTest, UndrainDeadNode) {
  auto cluster =
      IntegrationTestUtils::ClusterFactory()
          .apply(commonSetup())
          .setLogAttributes(
              logsconfig::DefaultLogAttributes().with_replicationFactor(3))
          .setNumDBShards(1)
          .create(6);

  auto client = cluster->createClient();
  cluster->getAdminServer()->waitUntilFullyLoaded();
  auto admin_client = cluster->getAdminServer()->createAdminClient();

  auto check_still_auth_empty = [&](bool drain) {
    EventLogRebuildingSet set;
    ASSERT_EQ(0, EventLogUtils::getRebuildingSet(*client, set));
    auto node = set.getNodeInfo(1, 0);
    ASSERT_NE(nullptr, node);
    EXPECT_EQ(AuthoritativeStatus::AUTHORITATIVE_EMPTY, node->auth_status);
    EXPECT_EQ(drain, node->drain);
  };

  // Drain N1:S0.
  auto maintenance_id = cluster->applyMaintenance(
      *admin_client, 1, 0, "testing", /* drain = */ true);

  // Wait for rebuilding.
  waitUntilShardsHaveEventLogState(
      client, {ShardID(1, 0)}, AuthoritativeStatus::AUTHORITATIVE_EMPTY, true);
  cluster->getNode(1).waitUntilShardState(
      *admin_client,
      0,
      [](const thrift::ShardState& shard) {
        return shard.get_storage_state() ==
            membership::thrift::StorageState::NONE;
      },
      "Shard must be in NONE");
  {
    SCOPED_TRACE("");
    check_still_auth_empty(true);
  }

  // Stop N1.
  cluster->getNode(1).shutdown();

  // Undrain N1:S0.
  {
    thrift::RemoveMaintenancesRequest req;
    thrift::MaintenancesFilter filter;
    filter.set_group_ids({maintenance_id});
    req.set_user("test");
    req.set_filter(filter);
    thrift::RemoveMaintenancesResponse resp;
    admin_client->sync_removeMaintenances(resp, req);
    ASSERT_EQ(1, resp.get_maintenances().size());
  }

  {
    SCOPED_TRACE("");
    // The node will remain drained because it's DEAD.
    check_still_auth_empty(true);
  }

  // Drain N1:S0 and N2:S0.
  auto maintenance_id1 = cluster->applyMaintenance(
      *admin_client, 1, 0, "testing", /* drain = */ true);
  {
    SCOPED_TRACE("");
    check_still_auth_empty(true);
  }

  auto maintenance_id2 = cluster->applyMaintenance(
      *admin_client, 2, 0, "testing", /* drain = */ true);
  {
    SCOPED_TRACE("");
    check_still_auth_empty(true);
  }

  // Wait for rebuilding.
  cluster->getNode(2).waitUntilShardState(
      *admin_client,
      0,
      [](const thrift::ShardState& shard) {
        return shard.get_storage_state() ==
            membership::thrift::StorageState::NONE;
      },
      "Shard must be in NONE");

  // Undrain N2:S0.
  {
    thrift::RemoveMaintenancesRequest req;
    thrift::MaintenancesFilter filter;
    filter.set_group_ids({maintenance_id2});
    req.set_user("test");
    req.set_filter(filter);
    thrift::RemoveMaintenancesResponse resp;
    admin_client->sync_removeMaintenances(resp, req);
    ASSERT_EQ(1, resp.get_maintenances().size());
  }

  // Wait for ack.
  cluster->getNode(2).waitUntilShardState(
      *admin_client,
      0,
      [](const thrift::ShardState& shard) {
        return shard.get_storage_state() ==
            membership::thrift::StorageState::READ_WRITE;
      },
      "Shard must be in READ_WRITE");
  waitUntilShardsHaveEventLogState(
      client, {ShardID(2, 0)}, AuthoritativeStatus::FULLY_AUTHORITATIVE, true);

  {
    SCOPED_TRACE("");
    // N1:S0 should remain drained
    check_still_auth_empty(true);
  }
}

std::vector<TestMode> test_params{{}};
INSTANTIATE_TEST_CASE_P(RebuildingTest,
                        RebuildingTest,
                        ::testing::ValuesIn(test_params));
