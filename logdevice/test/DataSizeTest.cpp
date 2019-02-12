/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include <gtest/gtest.h>

#include "logdevice/common/configuration/Configuration.h"
#include "logdevice/common/debug.h"
#include "logdevice/common/test/TestUtil.h"
#include "logdevice/common/util.h"
#include "logdevice/include/Client.h"
#include "logdevice/include/ClientSettings.h"
#include "logdevice/include/NodeLocationScope.h"
#include "logdevice/server/locallogstore/PartitionedRocksDBStore.h"
#include "logdevice/test/utils/IntegrationTestBase.h"
#include "logdevice/test/utils/IntegrationTestUtils.h"

using namespace facebook::logdevice;

namespace {

int NUM_NODES = 8; // must be > 1
int NUM_LOGS = 4;
const int MAX_IN_FLIGHT = 500;

struct DataSizeResult {
  uint64_t log_id;
  Status status;
  size_t size_range_lo;
  size_t size_range_hi;
  std::chrono::milliseconds time_range_lo = std::chrono::milliseconds::min();
  std::chrono::milliseconds time_range_hi = std::chrono::milliseconds::max();
};

static NodeSetIndices getFullNodeSet() {
  NodeSetIndices full_node_set(NUM_NODES);
  if (NUM_NODES > 1) {
    std::iota(++full_node_set.begin(), full_node_set.end(), 1);
  }
  return full_node_set;
}

static void commonSetup(IntegrationTestUtils::ClusterFactory& cluster) {
  logsconfig::LogAttributes log_attrs;
  log_attrs.set_replicationFactor(std::min(3, NUM_NODES - 1));
  log_attrs.set_extraCopies(0);
  log_attrs.set_syncedCopies(0);
  log_attrs.set_maxWritesInFlight(MAX_IN_FLIGHT);

  logsconfig::LogAttributes event_log_attrs;
  event_log_attrs.set_replicationFactor(std::min(4, NUM_NODES - 1));
  event_log_attrs.set_extraCopies(0);
  event_log_attrs.set_syncedCopies(0);
  event_log_attrs.set_maxWritesInFlight(100);

  Configuration::MetaDataLogsConfig meta_config;
  {
    const size_t nodeset_size = std::min(6, NUM_NODES - 1);
    std::vector<node_index_t> nodeset(nodeset_size);
    std::iota(nodeset.begin(), nodeset.end(), 1);
    meta_config = createMetaDataLogsConfig(
        nodeset, std::min(4ul, nodeset_size), NodeLocationScope::NODE);
  }
  meta_config.sequencers_write_metadata_logs = true;
  meta_config.sequencers_provision_epoch_store = true;

  cluster.setParam("--rocksdb-partition-duration", "900s")
      .setParam("--rocksdb-partition-timestamp-granularity", "0ms")
      .setParam("--rocksdb-new-partition-timestamp-margin", "0ms")
      // Make sure memtables are lost on crash
      .setParam("--append-store-durability", "memory")
      .setParam("--disable-rebuilding", "false")
      .setParam("--rocksdb-min-manual-flush-interval", "0")
      // Disable sticky copysets to make records more evenly distributed
      .setParam("--enable-sticky-copysets", "false")
      // Don't compress or batch data
      .setParam("--rocksdb-compression-type", "none")
      .setParam("--sequencer-batching", "false")
      // Don't have overlapping partition timestamps.
      .setParam("--rocksdb-new-partition-timestamp-margin", "0s")
      // To keep the test simple, make sure no appends time out, e.g. during
      // stress testing, since each wave will dump the dataSize counter.
      .setParam("--store-timeout", "30s")
      .setNumDBShards(1)
      .setLogAttributes(log_attrs)
      .setLogGroupName("my-test-log")
      .setEventLogAttributes(event_log_attrs)
      .setMetaDataLogsConfig(meta_config)
      .setNumLogs(NUM_LOGS);
}

class DataSizeTest : public IntegrationTestBase {
 public:
  // Initializes a Cluster object with the desired log config
  void init();

  // Checks whether dataSize returns the indicated expected values;
  // returns false and prints mismatch if any is found.
  bool dataSizeResultsMatch(std::vector<DataSizeResult> expected_results);

  // Creates a partition on each node.
  void createPartition();

  // Drops partitions up to the given partition on all nodes.
  void dropPartition(partition_id_t partition);
  // Same but for the given node/s.
  void dropPartition(partition_id_t partition, std::vector<node_index_t> nodes);

  // Write the given number of records to the given log.
  void writeRecordsToSingleLog(uint64_t log_id,
                               size_t nrecords = 25,
                               size_t payload_size = 10);
  void writeRecords(std::vector<uint64_t> log_ids,
                    size_t nrecords = 25,
                    size_t payload_size = 10);
  void writeRecordsToNewPartition(std::vector<uint64_t> log_ids,
                                  size_t nrecords = 25,
                                  size_t payload_size = 10);

  std::unique_ptr<IntegrationTestUtils::Cluster> cluster_;
  std::shared_ptr<Client> client_;
  partition_id_t latest_partition_ = PARTITION_INVALID;
};

void DataSizeTest::init() {
  ld_check_gt(NUM_NODES, 1);
  cluster_ = IntegrationTestUtils::ClusterFactory()
                 .apply(commonSetup)
                 .create(NUM_NODES);

  latest_partition_ = PartitionedRocksDBStore::INITIAL_PARTITION_ID;

  auto client_settings =
      std::unique_ptr<ClientSettings>(ClientSettings::create());
  // Increase output buffer size so we can send larger records
  client_settings->set("--outbuf-kb", "200000000");
  client_ = cluster_->createClient(
      getDefaultTestTimeout(), std::move(client_settings));
}

bool DataSizeTest::dataSizeResultsMatch(
    std::vector<DataSizeResult> expected_results) {
  ld_check(client_);
  std::atomic<bool> all_matched(true);
  Semaphore sem;

  for (DataSizeResult& expected : expected_results) {
    int rv = client_->dataSize(
        logid_t(expected.log_id),
        expected.time_range_lo,
        expected.time_range_hi,
        DataSizeAccuracy::APPROXIMATE,
        [&](Status st, size_t size) {
          if (st != expected.status || size < expected.size_range_lo ||
              size > expected.size_range_hi) {
            ld_error(
                "DataSize[%lu]: expected %s, size in [%lu,%lu]; got %s, %lu",
                expected.log_id,
                error_name(expected.status),
                expected.size_range_lo,
                expected.size_range_hi,
                error_name(st),
                size);
            all_matched.store(false);
          }
          sem.post();
        });
    if (rv != 0) {
      ld_error("Failed to call dataSize for log %lu, time range [%lu,%lu] "
               "(err: %s)",
               expected.log_id,
               expected.time_range_lo.count(),
               expected.time_range_hi.count(),
               error_name(err));
      all_matched.store(false);
      sem.post();
    }
  }

  for (int i = 0; i < expected_results.size(); i++) {
    sem.wait();
  }

  return all_matched.load();
}

void DataSizeTest::createPartition() {
  auto nodeset = getFullNodeSet();
  ld_info("Creating partition on nodes %s", toString(nodeset).c_str());
  cluster_->applyToNodes(
      nodeset, [](auto& node) { node.sendCommand("logsdb create 0"); });
  ++latest_partition_;
}

void DataSizeTest::dropPartition(partition_id_t partition) {
  auto nodeset = getFullNodeSet();
  ld_info("Dropping partition %lu on nodes %s",
          partition,
          toString(nodeset).c_str());

  cluster_->applyToNodes(nodeset, [partition](auto& node) {
    node.sendCommand(folly::format("logsdb drop 0 {}", partition).str());
  });
}

void DataSizeTest::dropPartition(partition_id_t partition,
                                 std::vector<node_index_t> nodes) {
  ld_info(
      "Dropping partition %lu on nodes %s", partition, toString(nodes).c_str());

  for (node_index_t node_idx : nodes) {
    cluster_->getNode(node_idx).sendCommand(
        folly::format("logsdb drop 0 {}", partition).str());
  }
}

void DataSizeTest::writeRecordsToSingleLog(uint64_t log_id,
                                           size_t num_records,
                                           size_t payload_size) {
  ld_check_le(num_records, MAX_IN_FLIGHT);
  ld_info("Writing %lu records", num_records);
  // Write some records
  Semaphore sem;
  std::atomic<lsn_t> first_lsn(LSN_MAX);
  auto cb = [&](Status st, const DataRecord& r) {
    ASSERT_EQ(E::OK, st);
    if (st == E::OK) {
      ASSERT_NE(LSN_INVALID, r.attrs.lsn);
      atomic_fetch_min(first_lsn, r.attrs.lsn);
    }
    sem.post();
  };
  for (int i = 1; i <= num_records; ++i) {
    std::string data(payload_size, 'x');
    client_->append(logid_t(log_id), std::move(data), cb);
  }
  for (int i = 1; i <= num_records; ++i) {
    sem.wait();
  }
  ASSERT_NE(LSN_MAX, first_lsn);
}

void DataSizeTest::writeRecords(std::vector<uint64_t> log_ids,
                                size_t num_records,
                                size_t payload_size) {
  for (uint64_t log_id : log_ids) {
    writeRecordsToSingleLog(log_id, num_records, payload_size);
  }
}

void DataSizeTest::writeRecordsToNewPartition(std::vector<uint64_t> log_ids,
                                              size_t num_records,
                                              size_t payload_size) {
  createPartition();
  writeRecords(log_ids, num_records, payload_size);
};

// Check that dataSize is 0 on startup, as bridge records have no payload.
TEST_F(DataSizeTest, Startup) {
  init();

  // Wait for recoveries to finish, which'll write bridge records for all the
  // logs. These should be ignored, and all logs correctly declared empty.
  cluster_->waitForRecovery();
  ASSERT_TRUE(dataSizeResultsMatch({
      /* log_id, status, size_range_lo, size_range_hi,
         time_range_lo (default: min), time_range_hi (default: max) */
      {1, E::OK, 0, 0},
      {2, E::OK, 0, 0},
      {3, E::OK, 0, 0},
      {4, E::OK, 0, 0},
  }));
}

TEST_F(DataSizeTest, DataTrimmedAway) {
  NUM_NODES = 2; // 1 sequencer node, 1 storage node which we'll be restarting.
  init();

  cluster_->waitForRecovery();
  // Depend on Startup test to pass
  ASSERT_TRUE(dataSizeResultsMatch({
      /* log_id, status, size_range_lo, size_range_hi,
         time_range_lo (default: min), time_range_hi (default: max) */
      {1, E::OK, 0, 0},
      {2, E::OK, 0, 0},
      {3, E::OK, 0, 0},
      {4, E::OK, 0, 0},
  }));

  // Write 10 records to log 1, each with 100 bytes payload.
  writeRecords({1}, /*num_records=*/10, /*payload_size=*/100);
  ASSERT_TRUE(dataSizeResultsMatch({
      /* log_id, status, size_range_lo, size_range_hi,
         time_range_lo (default: min), time_range_hi (default: max) */
      {1, E::OK, 800, 1200},
      {2, E::OK, 0, 0},
      {3, E::OK, 0, 0},
      {4, E::OK, 0, 0},
  }));

  // Write some data to a newer partition for log 2.
  writeRecordsToNewPartition({2},
                             /*num_records=*/40,
                             /*payload_size=*/800);
  ASSERT_TRUE(dataSizeResultsMatch({
      /* log_id, status, size_range_lo, size_range_hi,
         time_range_lo (default: min), time_range_hi (default: max) */
      {1, E::OK, 800, 1200},
      {2, E::OK, 30000, 34000},
      {3, E::OK, 0, 0},
      {4, E::OK, 0, 0},
  }));

  // Write some data for multiple new partitions for log 2, and drop the first
  // partition, which held some data for log 1. Log 2 should now be the only
  // non-empty log, and remain so on restart.
  partition_id_t drop_up_to = latest_partition_;
  writeRecordsToNewPartition({2}, /*num_records=*/5, /*payload_size=*/1);
  dropPartition(drop_up_to);
  ASSERT_TRUE(dataSizeResultsMatch({
      /* log_id, status, size_range_lo, size_range_hi,
         time_range_lo (default: min), time_range_hi (default: max) */
      {1, E::OK, 0, 0},
      {2, E::OK, 30000, 34000},
      {3, E::OK, 0, 0},
      {4, E::OK, 0, 0},
  }));
}

// Check that the answer is the same when a node is restarted.
TEST_F(DataSizeTest, RestartNode) {
  NUM_NODES = 2; // 1 sequencer node, 1 storage node which we'll be restarting.
  init();

  cluster_->waitForRecovery();
  // Depend on Startup test to pass
  ASSERT_TRUE(dataSizeResultsMatch({
      /* log_id, status, size_range_lo, size_range_hi,
         time_range_lo (default: min), time_range_hi (default: max) */
      {1, E::OK, 0, 0},
      {2, E::OK, 0, 0},
      {3, E::OK, 0, 0},
      {4, E::OK, 0, 0},
  }));

  // Write 10 records to log 1, each with 100 bytes payload.
  writeRecords({1}, /*num_records=*/10, /*payload_size=*/100);
  ASSERT_TRUE(dataSizeResultsMatch({
      /* log_id, status, size_range_lo, size_range_hi,
         time_range_lo (default: min), time_range_hi (default: max) */
      {1, E::OK, 1040, 1040},
      {2, E::OK, 0, 0},
      {3, E::OK, 0, 0},
      {4, E::OK, 0, 0},
  }));

  // Write some data to a newer partition for log 2.
  writeRecordsToNewPartition({2},
                             /*num_records=*/40,
                             /*payload_size=*/800);
  ASSERT_TRUE(dataSizeResultsMatch({
      /* log_id, status, size_range_lo, size_range_hi,
         time_range_lo (default: min), time_range_hi (default: max) */
      {1, E::OK, 1040, 1040},
      {2, E::OK, 32160, 32160},
      {3, E::OK, 0, 0},
      {4, E::OK, 0, 0},
  }));

  // Check that the above holds after restarting the node.
  cluster_->getNode(1).restart(/*graceful=*/true,
                               /*wait_until_available=*/false);
  cluster_->getNode(1).waitUntilStarted();
  ASSERT_TRUE(dataSizeResultsMatch({
      /* log_id, status, size_range_lo, size_range_hi,
         time_range_lo (default: min), time_range_hi (default: max) */
      {1, E::OK, 1040, 1040},
      {2, E::OK, 32160, 32160},
      {3, E::OK, 0, 0},
      {4, E::OK, 0, 0},
  }));

  // Write some data for multiple new partitions for log 2, and drop the first
  // partition, which held some data for log 1. We should now have just as much
  // data for log 1, and
  partition_id_t drop_up_to = latest_partition_;
  writeRecordsToNewPartition({2}, /*num_records=*/5, /*payload_size=*/1);
  dropPartition(drop_up_to);
  ASSERT_TRUE(dataSizeResultsMatch({
      /* log_id, status, size_range_lo, size_range_hi,
         time_range_lo (default: min), time_range_hi (default: max) */
      {1, E::OK, 0, 0},
      {2, E::OK, 32185, 32185},
      {3, E::OK, 0, 0},
      {4, E::OK, 0, 0},
  }));
  cluster_->getNode(1).restart(/*graceful=*/true,
                               /*wait_until_available=*/false);
  cluster_->getNode(1).waitUntilStarted();
  ASSERT_TRUE(dataSizeResultsMatch({
      /* log_id, status, size_range_lo, size_range_hi,
         time_range_lo (default: min), time_range_hi (default: max) */
      {1, E::OK, 0, 0},
      {2, E::OK, 32185, 32185},
      {3, E::OK, 0, 0},
      {4, E::OK, 0, 0},
  }));
}

// Test that requests with time ranges provided give reasonable estimates.
TEST_F(DataSizeTest, TimeRanges) {
  std::chrono::milliseconds SECOND(1000);
  std::chrono::milliseconds MINUTE(600000);

  auto get_current_time = []() -> std::chrono::milliseconds {
    return std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::system_clock::now().time_since_epoch());
  };

  // Use more nodes, which will give us a more accurate estimate since we've
  // got more data points when reaching an f-majority.
  NUM_NODES = 18;
  auto start_time = get_current_time();
  init();
  cluster_->waitForRecovery();

  // Depend on Startup test to pass
  ASSERT_TRUE(dataSizeResultsMatch({
      /* log_id, status, size_range_lo, size_range_hi,
         time_range_lo (default: min), time_range_hi (default: max) */
      {1, E::OK, 0, 0},
      {2, E::OK, 0, 0},
      {3, E::OK, 0, 0},
      {4, E::OK, 0, 0},
  }));

  // For our expected values, let's estimate the space used by other things we
  // add to the blob we store, as up to 30 bytes per record.

  // Write 200kb of data to log 1 for the given partition. This should be
  // enough that the load will be decently spread out, and the estimate should
  // be pretty accurate. Run in batches to not keep too many in flight.
  writeRecords({1}, /*num_records=*/500, /*payload_size=*/200);
  writeRecords({1}, /*num_records=*/500, /*payload_size=*/200);
  ASSERT_TRUE(dataSizeResultsMatch({
      /* log_id, status, size_range_lo, size_range_hi,
         time_range_lo (default: min), time_range_hi (default: max) */
      // real: ~200-230 KB
      {1, E::OK, 140000, 380000},
      {2, E::OK, 0, 0},
      {3, E::OK, 0, 0},
      {4, E::OK, 0, 0},
  }));

  // Outside time range should give at most a small result. Test that
  // non-overlapping time ranges are considered invalid.
  ASSERT_TRUE(dataSizeResultsMatch({
      /* log_id, status, size_range_lo, size_range_hi,
         time_range_lo (default: min), time_range_hi (default: max) */
      {1, E::OK, 0, 500, start_time - 12 * MINUTE, start_time - 28 * SECOND},
      {2, E::INVALID_PARAM, 0, 0, start_time, std::chrono::milliseconds::min()},
      {3, E::OK, 0, 0, std::chrono::milliseconds::min(), start_time},
      {4, E::INVALID_PARAM, 0, 0, start_time, start_time - 28 * SECOND},
  }));

  // A few more similar cases; non-existing logs and metadata logs should be
  // considered invalid parameters.
  ASSERT_TRUE(dataSizeResultsMatch({
      /* log_id, status, size_range_lo, size_range_hi,
         time_range_lo (default: min), time_range_hi (default: max) */
      {1, E::OK, 0, 500, start_time + 2 * MINUTE, start_time + 17 * MINUTE},
      {2, E::OK, 0, 0, start_time, start_time + 2 * SECOND},
      {777, E::INVALID_PARAM, 0, 0},
      {9223372036854775809ul, E::INVALID_PARAM, 0, 0},
      {9223372036854775841ul, E::INVALID_PARAM, 0, 0},
  }));

  auto t1 = get_current_time();

  // Write 100kb to log2 for a new partition.
  writeRecordsToNewPartition({2}, /*num_records=*/500, /*payload_size=*/200);
  ASSERT_TRUE(dataSizeResultsMatch({
      /* log_id, status, size_range_lo, size_range_hi,
         time_range_lo (default: min), time_range_hi (default: max) */
      // real: ~200-230 KB
      {1, E::OK, 140000, 380000},
      // real: ~100-115 KB
      {2, E::OK, 75000, 225000},
      {3, E::OK, 0, 0},
  }));

  // Write ~200k to the partition for log1.
  writeRecords({1}, /*num_records=*/500, /*payload_size=*/200);
  writeRecords({1}, /*num_records=*/500, /*payload_size=*/200);
  ASSERT_TRUE(dataSizeResultsMatch({
      /* log_id, status, size_range_lo, size_range_hi,
         time_range_lo (default: min), time_range_hi (default: max) */
      // real: ~400-460 KB
      {1, E::OK, 325000, 630000},
      // real: ~100-115 KB
      {2, E::OK, 75000, 225000},
      {3, E::OK, 0, 0},
  }));

  // Write to a new partition for a different log just to make the time stop
  // ticking for that previous partition.
  writeRecordsToNewPartition({3}, /*num_records=*/1, /*payload_size=*/1);
  auto t2 = get_current_time();

  // These are short time ranges, so let's give generous bounds, yet check that
  // results are somewhat reasonable. More precise tests can be found for the
  // LogsDB part in PartitionedRocksDBStoreTest.
  ASSERT_TRUE(dataSizeResultsMatch({
      /* log_id, status, size_range_lo, size_range_hi,
         time_range_lo (default: min), time_range_hi (default: max) */
      // real: ~200-230 KB
      {1, E::OK, 140000, 380000, start_time - MINUTE, t1},
      // real: ~400-460 KB
      {1, E::OK, 325000, 630000, start_time - SECOND, t2 + 2 * SECOND},
      // real: ~200-230 KB
      {1, E::OK, 145000, 380000, t1, t2 + MINUTE},
      // real: ~400-460 KB
      {1, E::OK, 325000, 625000, start_time, t2 + 3 * SECOND},
  }));
  ASSERT_TRUE(dataSizeResultsMatch({
      /* log_id, status, size_range_lo, size_range_hi,
         time_range_lo (default: min), time_range_hi (default: max) */
      // real: 0, but timestamp of next partition might not be exact
      {2, E::OK, 0, 15000, start_time, t1},
      // real: ~100-115 KB
      {2, E::OK, 75000, 250000, start_time, t2},
      // real: ~100-115 KB
      {2, E::OK, 75000, 225000, t1, t2},
      {3, E::OK, 0, 1000},
      {4, E::OK, 0, 0},
  }));
}

} // namespace
