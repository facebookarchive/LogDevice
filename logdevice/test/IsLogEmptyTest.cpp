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

struct IsLogEmptyResult {
  uint64_t log_id;
  Status status;
  bool empty;
};

class IsLogEmptyTest : public IntegrationTestBase {
 public:
  NodeSetIndices getFullNodeSet();
  void commonSetup(IntegrationTestUtils::ClusterFactory& cluster);

  // Initializes a Cluster object with the desired log config
  void init();

  // Checks whether isLogEmpty returns the indicated expected values;
  // returns false and prints mismatch if any is found.
  bool isLogEmptyResultsMatch(std::vector<IsLogEmptyResult> expected_results);
  bool isLogEmptyResultsMatch(std::vector<IsLogEmptyResult> expected_results,
                              bool with_grace_period,
                              bool skip_extra_runs = false);

  // Checks whether isLogEmpty results are consistent when doing num_runs runs
  // for each given log. If with_grace_period is not specified, run once
  // without, and once with it, requiring both to succeed.
  bool isLogEmptyResultsConsistent(std::vector<uint64_t> logs, int num_runs);
  bool isLogEmptyResultsConsistent(std::vector<uint64_t> logs,
                                   int num_runs,
                                   bool with_grace_period);

  // Creates a partition on each node.
  void createPartition();

  // Drops partitions up to the given partition on all nodes.
  void dropPartition(partition_id_t partition);
  // Same but for the given node/s.
  void dropPartition(partition_id_t partition, std::vector<node_index_t> nodes);

  // Write the given number of records to the given log.
  void writeRecordsToSingleLog(uint64_t log_id, size_t nrecords);
  void writeRecords(std::vector<uint64_t> log_ids, size_t nrecords = 25);
  void writeRecordsToNewPartition(std::vector<uint64_t> log_ids,
                                  size_t nrecords = 25);

  std::unique_ptr<IntegrationTestUtils::Cluster> cluster_;
  std::shared_ptr<Client> client_no_grace_period_;
  std::shared_ptr<Client> client_with_grace_period_;
  partition_id_t latest_partition_ = PARTITION_INVALID;

  int num_nodes = 4; // must be > 1
  int num_logs = 4;
};

NodeSetIndices IsLogEmptyTest::getFullNodeSet() {
  NodeSetIndices full_node_set(num_nodes);
  if (num_nodes > 1) {
    std::iota(++full_node_set.begin(), full_node_set.end(), 1);
  }
  return full_node_set;
}

void IsLogEmptyTest::commonSetup(
    IntegrationTestUtils::ClusterFactory& cluster) {
  logsconfig::LogAttributes log_attrs;
  log_attrs.set_replicationFactor(std::min(num_nodes - 1, 2));
  log_attrs.set_extraCopies(0);
  log_attrs.set_syncedCopies(0);
  log_attrs.set_maxWritesInFlight(250);

  logsconfig::LogAttributes event_log_attrs;
  event_log_attrs.set_replicationFactor(std::min(num_nodes - 1, 2));
  event_log_attrs.set_extraCopies(0);
  event_log_attrs.set_syncedCopies(0);
  event_log_attrs.set_maxWritesInFlight(250);

  Configuration::MetaDataLogsConfig meta_config;
  {
    const size_t nodeset_size = std::min(6, num_nodes - 1);
    std::vector<node_index_t> nodeset(nodeset_size);
    std::iota(nodeset.begin(), nodeset.end(), 1);
    meta_config = createMetaDataLogsConfig(
        nodeset, std::min(4ul, nodeset_size), NodeLocationScope::NODE);
  }
  meta_config.sequencers_write_metadata_logs = true;
  meta_config.sequencers_provision_epoch_store = true;

  cluster.setParam("--rocksdb-partition-duration", "900s")
      .setParam("--rocksdb-partition-timestamp-granularity", "0ms")
      // Use bridge records, which previously tricked isLogEmpty
      .setParam("--bridge-record-in-empty-epoch", "true")
      .setParam("--rocksdb-new-partition-timestamp-margin", "0ms")
      .setParam("--disable-rebuilding", "true")
      .setParam("--rocksdb-min-manual-flush-interval", "0")
      // Disable sticky copysets to make records more randomly distributed
      .setParam("--enable-sticky-copysets", "false")
      // Make sure no appends time out.
      .setParam("--store-timeout", "30s")
      .setNumDBShards(1)
      .setLogGroupName("my_test_logs")
      .setLogAttributes(log_attrs)
      .setEventLogAttributes(event_log_attrs)
      .setMetaDataLogsConfig(meta_config)
      .setNumLogs(num_logs);
}

void IsLogEmptyTest::init() {
  ld_check_gt(num_nodes, 1);
  cluster_ = IntegrationTestUtils::ClusterFactory()
                 .apply([this](IntegrationTestUtils::ClusterFactory& cluster) {
                   commonSetup(cluster);
                 })
                 .create(num_nodes);

  latest_partition_ = PartitionedRocksDBStore::INITIAL_PARTITION_ID;

  // Set timeouts to something very high, so we're certain that timeouts won't
  // be a source of flakiness even in stress testing.

  auto client_settings_no_grace_period =
      std::unique_ptr<ClientSettings>(ClientSettings::create());
  client_settings_no_grace_period->set(
      "client-is-log-empty-grace-period", "0s");
  client_settings_no_grace_period->set("meta-api-timeout", "180s");
  client_no_grace_period_ = cluster_->createClient(
      getDefaultTestTimeout(), std::move(client_settings_no_grace_period));

  auto client_settings_with_grace_period =
      std::unique_ptr<ClientSettings>(ClientSettings::create());
  client_settings_with_grace_period->set(
      "client-is-log-empty-grace-period", "10s");
  client_settings_with_grace_period->set("meta-api-timeout", "180s");
  client_with_grace_period_ = cluster_->createClient(
      getDefaultTestTimeout(), std::move(client_settings_with_grace_period));
}

bool IsLogEmptyTest::isLogEmptyResultsMatch(
    std::vector<IsLogEmptyResult> expected_results) {
  bool match_with_grace_period =
      isLogEmptyResultsMatch(expected_results, true, false);
  ld_info(
      "Match with grace period: %s", match_with_grace_period ? "OK" : "FAILED");
  bool match_without_grace_period =
      isLogEmptyResultsMatch(expected_results, false, false);
  ld_info("Match without grace period: %s",
          match_without_grace_period ? "OK" : "FAILED");
  return match_with_grace_period && match_without_grace_period;
}
bool IsLogEmptyTest::isLogEmptyResultsMatch(
    std::vector<IsLogEmptyResult> expected_results,
    bool with_grace_period,
    bool skip_extra_runs) {
  ld_check(client_no_grace_period_ && client_with_grace_period_);
  std::atomic<bool> all_matched(true);
  Semaphore sem;
  auto* client = with_grace_period ? client_with_grace_period_.get()
                                   : client_no_grace_period_.get();

  for (IsLogEmptyResult& expected : expected_results) {
    int rv = client->isLogEmpty(
        logid_t(expected.log_id), [&](Status st, bool empty) {
          if (st != expected.status || empty != expected.empty) {
            ld_error("IsLogEmpty[%lu]: expected %s, %s; got %s, %s",
                     expected.log_id,
                     error_name(expected.status),
                     expected.empty ? "Y" : "N",
                     error_name(st),
                     empty ? "Y" : "N");
            all_matched.store(false);
          }
          sem.post();
        });
    if (rv != 0) {
      ld_error("Failed to call IsLogEmpty for log %lu (err: %s)",
               expected.log_id,
               error_name(err));
      ld_check(false); // TODO(T34744712): remove this check
      all_matched.store(false);
      sem.post();
    }
  }

  for (int i = 0; i < expected_results.size(); i++) {
    sem.wait();
  }

  if (!all_matched.load()) {
    return false;
  }

  if (skip_extra_runs) {
    return true;
  }

  // All matched; verify that results do not vary.
  std::vector<uint64_t> log_ids;
  for (IsLogEmptyResult& expected : expected_results) {
    log_ids.push_back(expected.log_id);
  }
  return isLogEmptyResultsConsistent(log_ids,
                                     /*num_runs=*/5,
                                     with_grace_period);
}

bool IsLogEmptyTest::isLogEmptyResultsConsistent(std::vector<uint64_t> log_ids,
                                                 int num_runs) {
  bool result_without_grace_period =
      isLogEmptyResultsConsistent(log_ids, num_runs, false);
  ld_info("Results without grace period: %s",
          result_without_grace_period ? "OK" : "FAILED");
  bool result_with_grace_period =
      isLogEmptyResultsConsistent(log_ids, num_runs, true);
  ld_info("Results without grace period: %s",
          result_with_grace_period ? "OK" : "FAILED");
  return result_without_grace_period && result_with_grace_period;
}

bool IsLogEmptyTest::isLogEmptyResultsConsistent(std::vector<uint64_t> log_ids,
                                                 int num_runs,
                                                 bool with_grace_period) {
  ld_check(client_no_grace_period_ && client_with_grace_period_);
  Semaphore sem;
  std::mutex result_mutex;
  std::unordered_map<uint64_t, IsLogEmptyResult> previous_result;
  std::unordered_map<uint64_t, bool> result_varied;
  std::unordered_map<uint64_t, int> failures;

  for (auto log_id : log_ids) {
    result_varied[log_id] = false;
    failures[log_id] = 0;
  }

  for (int i = 0; i < num_runs; i++) {
    for (auto& log_id : log_ids) {
      auto* client = with_grace_period ? client_with_grace_period_.get()
                                       : client_no_grace_period_.get();
      int rv = client->isLogEmpty(logid_t(log_id), [&](Status st, bool empty) {
        {
          std::lock_guard<std::mutex> lock(result_mutex);
          ld_info("IsLogEmpty[%lu]: %sempty", log_id, empty ? "" : "non-");

          if (previous_result.count(log_id) == 0) {
            // This is the first result recorded. Just remember it.
            previous_result[log_id] = {log_id, st, empty};
          } else {
            // There was some previous result/s: check if they differ, and if
            // so, remember that the results for this log varied.
            IsLogEmptyResult prev = previous_result[log_id];
            ld_check_eq(prev.log_id, log_id);

            if (prev.status != st || prev.empty != empty) {
              result_varied[log_id] = true;
            }
          }
        }

        sem.post();
      });

      if (rv != 0) {
        ld_error("Failed to call IsLogEmpty for log %lu (err: %s)",
                 log_id,
                 error_name(err));
        ld_check(false); // TODO(T34744712): remove this check
        ++failures[log_id];
        sem.post();
      }
    }
  }

  for (int i = 0; i < log_ids.size() * num_runs; i++) {
    sem.wait();
  }

  for (uint64_t log_id : log_ids) {
    if (failures[log_id] != 0) {
      ld_error("isLogEmpty for log %lu failed!", log_id);
      return false;
    } else if (result_varied[log_id]) {
      ld_error("Results for log %lu varied!", log_id);
      return false;
    }
  }

  return true;
}

void IsLogEmptyTest::createPartition() {
  auto nodeset = getFullNodeSet();
  ld_info("Creating partition on nodes %s", toString(nodeset).c_str());
  cluster_->applyToNodes(
      nodeset, [](auto& node) { node.sendCommand("logsdb create 0"); });
  ++latest_partition_;
}

void IsLogEmptyTest::dropPartition(partition_id_t partition) {
  auto nodeset = getFullNodeSet();
  ld_info("Dropping partition %lu on nodes %s",
          partition,
          toString(nodeset).c_str());

  cluster_->applyToNodes(nodeset, [partition](auto& node) {
    node.sendCommand(folly::format("logsdb drop 0 {}", partition).str());
  });
}

void IsLogEmptyTest::dropPartition(partition_id_t partition,
                                   std::vector<node_index_t> nodes) {
  ld_info(
      "Dropping partition %lu on nodes %s", partition, toString(nodes).c_str());

  for (node_index_t node_idx : nodes) {
    cluster_->getNode(node_idx).sendCommand(
        folly::format("logsdb drop 0 {}", partition).str());
  }
}

void IsLogEmptyTest::writeRecordsToSingleLog(uint64_t log_id,
                                             size_t nrecords = 25) {
  ld_info("Writing %lu records", nrecords);
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
  for (int i = 1; i <= nrecords; ++i) {
    std::string data("data" + std::to_string(i));
    client_no_grace_period_->append(logid_t(log_id), std::move(data), cb);
  }
  for (int i = 1; i <= nrecords; ++i) {
    sem.wait();
  }
  ASSERT_NE(LSN_MAX, first_lsn);
}

void IsLogEmptyTest::writeRecords(std::vector<uint64_t> log_ids,
                                  size_t nrecords) {
  for (uint64_t log_id : log_ids) {
    writeRecordsToSingleLog(log_id, nrecords);
  }
}

void IsLogEmptyTest::writeRecordsToNewPartition(std::vector<uint64_t> log_ids,
                                                size_t nrecords) {
  createPartition();
  writeRecords(log_ids, nrecords);
};

// Check that isLogEmpty is no longer tripped by bridge records.
TEST_F(IsLogEmptyTest, Startup) {
  init();

  // Wait for recoveries to finish, which'll write bridge records for all the
  // logs. These should be ignored, and all logs correctly declared empty.
  cluster_->waitForRecovery();
  ASSERT_TRUE(isLogEmptyResultsMatch({
      /*log_id, status, empty, run_with_grace_period(default: true)*/
      {1, E::OK, true},
      {2, E::OK, true},
      {3, E::OK, true},
      {4, E::OK, true},
  }));
}

// Check that empty/non-empty status of a log is preserved when a server is
// restarted, whether the log has bridge records and/xor/nor real data.
// This is mainly to test handling of the PSEUDORECORDS_ONLY flag used on
// logsdb directory entries to signify that all records in that partition for
// that log are pseudorecords, such as bridge records.
TEST_F(IsLogEmptyTest, RestartNode) {
  // Override node count, so that we have 1 sequencer node and 1 storage node
  // which we'll be restarting.
  num_nodes = 2;
  init();

  // Wait for recoveries to finish, which'll write bridge records for all the
  // logs. These should be ignored, and all logs correctly declared empty.
  cluster_->waitForRecovery();
  ASSERT_TRUE(isLogEmptyResultsMatch({
      /*log_id, status, empty, run_with_grace_period(default: true)*/
      {1, E::OK, true},
      {2, E::OK, true},
  }));

  // Check that log 1 is the only non-empty log both before and restarting the
  // storage node.
  writeRecords({1});
  ASSERT_TRUE(isLogEmptyResultsMatch({
      /*log_id, status, empty, run_with_grace_period(default: true)*/
      {1, E::OK, false},
      {2, E::OK, true},
  }));
  cluster_->getNode(1).shutdown();
  cluster_->getNode(1).start();
  cluster_->getNode(1).waitUntilStarted();
  ASSERT_TRUE(isLogEmptyResultsMatch({
      /*log_id, status, empty, run_with_grace_period(default: true)*/
      {1, E::OK, false},
      {2, E::OK, true},
      {3, E::OK, true},
  }));

  // Write some data for multiple new partitions for log 2, and drop the first
  // partition, which held bridge records for all logs and some data for log 1.
  // Log 2 should now be the only non-empty log, and remain so on restart.
  writeRecordsToNewPartition({2});
  partition_id_t drop_up_to = latest_partition_;
  writeRecordsToNewPartition({2});
  writeRecordsToNewPartition({2});
  dropPartition(drop_up_to);
  ASSERT_TRUE(isLogEmptyResultsMatch({
      /*log_id, status, empty, run_with_grace_period(default: true)*/
      {1, E::OK, true},
      {2, E::OK, false},
      {3, E::OK, true},
  }));
  cluster_->getNode(1).shutdown();
  cluster_->getNode(1).start();
  cluster_->getNode(1).waitUntilStarted();
  ASSERT_TRUE(isLogEmptyResultsMatch({
      /*log_id, status, empty, run_with_grace_period(default: true)*/
      {1, E::OK, true},
      {2, E::OK, false},
      {3, E::OK, true},
  }));
}

TEST_F(IsLogEmptyTest, LogsTrimmedAway) {
  init();
  cluster_->waitForRecovery();

  // Write a bunch of records to log 1. It should be the only non-empty log.
  writeRecords({1});
  ASSERT_TRUE(isLogEmptyResultsMatch({
      /*log_id, status, empty, run_with_grace_period(default: true)*/
      {1, E::OK, false},
      {2, E::OK, true},
      {3, E::OK, true},
  }));

  // Write some records to log 2; expect 1 and 2 to be non-empty.
  writeRecords({2});
  ASSERT_TRUE(isLogEmptyResultsMatch({
      /*log_id, status, empty, run_with_grace_period(default: true)*/
      {1, E::OK, false},
      {2, E::OK, false},
      {3, E::OK, true},
      {4, E::OK, true},
  }));

  // Now, let's write some more for log 1, and later we'll trim away whatever's
  // only for log 2.
  writeRecords({1});
  writeRecordsToNewPartition({1});
  partition_id_t drop_up_to = latest_partition_;
  writeRecordsToNewPartition({1});

  // Logs 1, 2 should be non-empty
  ASSERT_TRUE(isLogEmptyResultsMatch({
      /*log_id, status, empty, run_with_grace_period(default: true)*/
      {1, E::OK, false},
      {2, E::OK, false},
      {3, E::OK, true},
      {4, E::OK, true},
  }));

  // We should now have one partition for which log 1 and 2 have data, and then
  // a bunch where log 1 has data. This drop should make log 2 empty.
  dropPartition(drop_up_to);
  ASSERT_TRUE(isLogEmptyResultsMatch({
      /*log_id, status, empty, run_with_grace_period(default: true)*/
      {1, E::OK, false},
      {2, E::OK, true},
      {3, E::OK, true},
      {4, E::OK, true},
  }));

  // Now, let's write to 3 and check that client trim calls are reflected by
  // isLogEmpty. Log 2 taking writes again should make it non-empty.
  writeRecords({2, 3});
  ASSERT_TRUE(isLogEmptyResultsMatch({
      /*log_id, status, empty, run_with_grace_period(default: true)*/
      {1, E::OK, false},
      {2, E::OK, false},
      {3, E::OK, false},
      {4, E::OK, true},
  }));

  // Trim away all data for log 1, and expect only logs 2, 3 to be non-empty.
  writeRecordsToNewPartition({2, 3});
  dropPartition(latest_partition_);
  ASSERT_TRUE(isLogEmptyResultsMatch({
      /*log_id, status, empty, run_with_grace_period(default: true)*/
      {1, E::OK, true},
      {2, E::OK, false},
      {3, E::OK, false},
      {4, E::OK, true},
  }));
}

TEST_F(IsLogEmptyTest, SimpleGracePeriod) {
  init();
  cluster_->waitForRecovery();

  // Write a single record to log 1. Since it will be replicated to three
  // nodes, the answer will vary between 'non-empty' and PARTIAL if we don't
  // use a grace period. However, when we use a grace period, we should always
  // get 'non-empty'.
  writeRecords({1}, 1);
  ASSERT_TRUE(isLogEmptyResultsMatch(
      {
          /*log_id, status, empty*/
          {1, E::OK, false},
          {2, E::OK, true},
          {3, E::OK, true},
          {4, E::OK, true},
      },
      /*with_grace_period=*/true));
}

TEST_F(IsLogEmptyTest, PartialResult) {
  num_nodes = 5; // 4 storage nodes
  init();
  cluster_->waitForRecovery();

  // Write some records to log 1. It should be the only non-empty log, and
  // there should be some records on every storage node (N1-N4, that is), since
  // we've disabled sticky copysets. That means that the result should not
  // depend on having a non-zero grace period.
  writeRecords({1});
  ASSERT_TRUE(isLogEmptyResultsMatch({
      /*log_id, status, empty*/
      {1, E::OK, false},
      {2, E::OK, true},
  }));

  // Now, let's create a new partition and write some for a different log.
  writeRecordsToNewPartition({3});
  ASSERT_TRUE(isLogEmptyResultsMatch({
      /*log_id, status, empty*/
      {1, E::OK, false},
      {2, E::OK, true},
  }));

  // Make N1 lose all its data for log 1: should still be impossible to get an
  // f-majority without a full copyset of data for log 1.
  dropPartition(latest_partition_, /*nodes=*/{1});
  ASSERT_TRUE(isLogEmptyResultsMatch({
      /*log_id, status, empty*/
      {1, E::OK, false},
      {2, E::OK, true},
  }));

  // With only N4 remaining non-empty, isLogEmpty will sometimes respond
  // 'empty', and other times return PARTIAL, when the grace period is too
  // short or zero. If we kill N3, we should consistently get PARTIAL results.
  // This should be the case regardless of grace period. Let's not bother to
  // drop data on N3 since it will be down.
  dropPartition(latest_partition_, /*nodes=*/{2});

  // Kill N3 so it can't respond
  cluster_->getNode(3).kill();

  ASSERT_TRUE(isLogEmptyResultsMatch(
      {
          /*log_id, status, empty*/
          {1, E::PARTIAL, false},
          {2, E::OK, true},
      },
      /*with_grace_period=*/true,
      /*skip_extra_runs=*/false)); // just run once since grace period is long

  // If we drop data from N4, we'll have an empty f-majority despite N3 being
  // down.
  dropPartition(latest_partition_, /*nodes=*/{4});

  ASSERT_TRUE(isLogEmptyResultsMatch(
      {
          /*log_id, status, empty*/
          {1, E::OK, true},
          {2, E::OK, true},
      },
      /*with_grace_period=*/true));

  // If we even just write a single new record, that should be enough to make
  // the log non-empty again, so long as we use a grace period.
  writeRecords({1}, /*nrecords=*/1);
  ASSERT_TRUE(isLogEmptyResultsMatch(
      {
          /*log_id, status, empty*/
          {1, E::OK, false},
          {2, E::OK, true},
      },
      /*with_grace_period=*/true));
}

// Check that isLogEmpty responds to failures as expected.
TEST_F(IsLogEmptyTest, Failures) {
  init();

  // Wait for recoveries to finish, which'll write bridge records for all the
  // logs. These should be ignored, and all logs correctly declared empty.
  cluster_->waitForRecovery();
  ASSERT_TRUE(isLogEmptyResultsMatch({
      /*log_id, status, empty, run_with_grace_period(default: true)*/
      {9223372036854775809ul,
       E::INVALID_PARAM,
       false}, // metadata log for log 1
  }));
  ASSERT_TRUE(isLogEmptyResultsMatch({
      /*log_id, status, empty, run_with_grace_period(default: true)*/
      {9223372036854775813ul,
       E::INVALID_PARAM,
       false}, // metadata log for non-existing log 5
  }));
  ASSERT_TRUE(isLogEmptyResultsMatch({
      /*log_id, status, empty, run_with_grace_period(default: true)*/
      {((uint64_t)num_logs) + 1, E::INVALID_PARAM, false}, // non-existing log
  }));
}

} // namespace
