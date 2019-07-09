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
#include "logdevice/lib/ClientImpl.h"
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

class IsLogEmptyV2Test : public IntegrationTestBase {
 public:
  NodeSetIndices getFullNodeSet();
  void commonSetup(IntegrationTestUtils::ClusterFactory& cluster);

  // Initializes a Cluster object with the desired log config
  void init();

  // Checks whether isLogEmpty returns the indicated expected values;
  // returns false and prints mismatch if any is found.
  bool isLogEmptyResultsMatch(std::vector<IsLogEmptyResult> expected_results);

  // Write the given number of records to the given log.
  void writeRecordsToSingleLog(uint64_t log_id, size_t nrecords);
  void writeRecords(std::vector<uint64_t> log_ids, size_t nrecords = 25);

  bool trimRecords(std::vector<uint64_t> log_ids, std::vector<lsn_t> lsns);
  bool getTailLSNs(std::vector<uint64_t> log_ids,
                   std::vector<lsn_t>& tail_lsns_out);
  void waitForTrimPointUpdate() {
    /* sleep override */
    std::this_thread::sleep_for(
        std::chrono::seconds(get_trim_point_wait_seconds));
  }

  std::unique_ptr<IntegrationTestUtils::Cluster> cluster_;
  std::shared_ptr<Client> client_;

  static const int NUM_NODES = 4;
  static const int NUM_LOGS = 4;
  int meta_api_timeout_sec = 180;
  int get_trim_point_interval_seconds = 5;
  int get_trim_point_wait_seconds = get_trim_point_interval_seconds + 2;
  bool use_dynamic_placement = false;
};

void IsLogEmptyV2Test::commonSetup(
    IntegrationTestUtils::ClusterFactory& cluster) {
  logsconfig::LogAttributes log_attrs;
  log_attrs.set_replicationFactor(std::min(NUM_NODES - 1, 2));
  log_attrs.set_extraCopies(0);
  log_attrs.set_syncedCopies(0);
  log_attrs.set_maxWritesInFlight(250);

  logsconfig::LogAttributes event_log_attrs;
  event_log_attrs.set_replicationFactor(std::min(NUM_NODES - 1, 2));
  event_log_attrs.set_extraCopies(0);
  event_log_attrs.set_syncedCopies(0);
  event_log_attrs.set_maxWritesInFlight(250);

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

  cluster
      // Use bridge records, which previously tricked isLogEmpty
      .setParam("--bridge-record-in-empty-epoch", "true")
      // Disable sticky copysets to make records more randomly distributed
      .setParam("--enable-sticky-copysets", "false")
      // Make sure no appends time out.
      .setParam("--store-timeout", "30s")
      .setParam("--get-trimpoint-interval",
                folly::sformat("{}s", get_trim_point_interval_seconds))
      .setNumDBShards(2)
      .setLogGroupName("my_test_logs")
      .setLogAttributes(log_attrs)
      .setEventLogAttributes(event_log_attrs)
      .setMetaDataLogsConfig(meta_config)
      .setNumLogs(NUM_LOGS);
  if (use_dynamic_placement) {
    cluster.useHashBasedSequencerAssignment();
  }
}

void IsLogEmptyV2Test::init() {
  ld_check_gt(NUM_NODES, 1);
  cluster_ = IntegrationTestUtils::ClusterFactory()
                 .apply([this](IntegrationTestUtils::ClusterFactory& cluster) {
                   commonSetup(cluster);
                 })
                 .create(NUM_NODES);

  auto client_settings =
      std::unique_ptr<ClientSettings>(ClientSettings::create());
  client_settings->set(
      "meta-api-timeout", folly::sformat("{}s", meta_api_timeout_sec));
  client_ = cluster_->createClient(
      getDefaultTestTimeout(), std::move(client_settings));
}

void IsLogEmptyV2Test::writeRecordsToSingleLog(uint64_t log_id,
                                               size_t nrecords = 25) {
  ld_info("Writing %lu records to log %lu", nrecords, log_id);
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
    client_->append(logid_t(log_id), std::move(data), cb);
  }
  for (int i = 1; i <= nrecords; ++i) {
    sem.wait();
  }
  ASSERT_NE(LSN_MAX, first_lsn);
}

void IsLogEmptyV2Test::writeRecords(std::vector<uint64_t> log_ids,
                                    size_t nrecords) {
  for (uint64_t log_id : log_ids) {
    writeRecordsToSingleLog(log_id, nrecords);
  }
}

bool IsLogEmptyV2Test::trimRecords(std::vector<uint64_t> log_ids,
                                   std::vector<lsn_t> lsns) {
  ld_check_eq(log_ids.size(), lsns.size());
  Semaphore sem;
  std::atomic<bool> failed(false);

  for (int i = 0; i < log_ids.size(); i++) {
    auto cb = [&sem, &failed](Status st) {
      if (st != E::OK) {
        failed = true;
      }
      sem.post();
    };

    ld_info("Log %lu: trimming up to %s",
            log_ids[i],
            lsn_to_string(lsns[i]).c_str());
    int rv = client_->trim(logid_t(log_ids[i]), lsns[i], cb);

    if (rv != 0) {
      ld_check(false); // TODO(T34744712): remove this check
      failed = true;
      sem.post();
    }
  }

  // Wait for requests to finish.
  for (int i = 0; i < log_ids.size(); i++) {
    sem.wait();
  }

  return !failed;
}

bool IsLogEmptyV2Test::getTailLSNs(std::vector<uint64_t> log_ids,
                                   std::vector<lsn_t>& tail_lsns_out) {
  Semaphore sem;
  std::atomic<bool> failed(false);
  tail_lsns_out = std::vector<lsn_t>(log_ids.size(), LSN_INVALID);

  for (int i = 0; i < log_ids.size(); i++) {
    auto cb = [&sem, i, &failed, &tail_lsns_out](Status st, lsn_t tail_lsn) {
      if (st != E::OK) {
        failed = true;
      }
      tail_lsns_out[i] = tail_lsn;
      sem.post();
    };

    int rv = client_->getTailLSN(logid_t(log_ids[i]), cb);

    if (rv != 0) {
      ld_check(false); // TODO(T34744712): remove this check
      failed = true;
      sem.post();
    }
  }

  // Wait for requests to finish.
  for (int i = 0; i < log_ids.size(); i++) {
    sem.wait();
  }

  return !failed;
}

bool IsLogEmptyV2Test::isLogEmptyResultsMatch(
    std::vector<IsLogEmptyResult> expected_results) {
  ld_check(client_);
  std::atomic<bool> all_matched(true);
  Semaphore sem;

  for (IsLogEmptyResult& expected : expected_results) {
    int rv =
        static_cast<ClientImpl*>(client_.get())
            ->isLogEmptyV2(
                logid_t(expected.log_id), [&](Status st, bool empty) {
                  if (st != expected.status || empty != expected.empty) {
                    ld_error("IsLogEmptyV2[%lu]: expected %s, %s; got %s, %s",
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
      ld_error("Failed to call IsLogEmptyV2 for log %lu (err: %s)",
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

  // All matched; verify that results do not vary.
  std::vector<uint64_t> log_ids;
  for (IsLogEmptyResult& expected : expected_results) {
    log_ids.push_back(expected.log_id);
  }

  return true;
}

TEST_F(IsLogEmptyV2Test, NonExistingLogsStaticSeqPlacement) {
  // Make timeout 10s so test doesn't time out.
  meta_api_timeout_sec = 20;
  init();

  // Wait for recoveries to finish, which'll write bridge records for all the
  // logs. These should be ignored, and all logs correctly declared empty.
  cluster_->waitForRecovery();

  // Existing logs should be empty, non-existing logs finish with NOSEQUENCER
  // when using static sequencer placement, and metadata logs should cause
  // INVALID_PARAM.
  ASSERT_TRUE(isLogEmptyResultsMatch({
      /*log_id, status, empty, run_with_grace_period(default: true)*/
      {1, E::OK, true},
      {2, E::OK, true},
      {3, E::OK, true},
      {4, E::OK, true},
      {NUM_LOGS + 1, E::NOSEQUENCER, false},
      {13223372036854775808ul, E::INVALID_PARAM, false},
  }));
}

TEST_F(IsLogEmptyV2Test, NonExistingLogsDynamicSeqPlacement) {
  // Make timeout 10s so test doesn't time out.
  meta_api_timeout_sec = 20;
  // Use dynamic sequencer placement.
  use_dynamic_placement = true;
  init();

  // Wait for recoveries to finish, which'll write bridge records for all the
  // logs. These should be ignored, and all logs correctly declared empty.
  cluster_->waitForRecovery();

  // Existing logs should be empty, non-existing logs should finish with
  // NOTFOUND, metadata logs should cause INVALID_PARAM.
  ASSERT_TRUE(isLogEmptyResultsMatch({
      /*log_id, status, empty, run_with_grace_period(default: true)*/
      {1, E::OK, true},
      {2, E::OK, true},
      {3, E::OK, true},
      {4, E::OK, true},
      {NUM_LOGS + 1, E::NOTFOUND, false},
      {13223372036854775808ul, E::INVALID_PARAM, false},
  }));
}

TEST_F(IsLogEmptyV2Test, SingleEpochTest) {
  init();

  // Wait for recoveries to finish, which'll write bridge records for all the
  // logs. These should be ignored, and all logs correctly declared empty.
  cluster_->waitForRecovery();

  // Nothing written, should be empty.
  ASSERT_TRUE(isLogEmptyResultsMatch({
      /*log_id, status, empty, run_with_grace_period(default: true)*/
      {1, E::OK, true},
      {2, E::OK, true},
      {3, E::OK, true},
      {4, E::OK, true},
  }));

  // Write records to log 1, 2 and make sure they're non-empty.
  writeRecords({1, 2});
  ASSERT_TRUE(isLogEmptyResultsMatch({
      /*log_id, status, empty, run_with_grace_period(default: true)*/
      {1, E::OK, false},
      {2, E::OK, false},
      {3, E::OK, true},
      {4, E::OK, true},
  }));

  // Trim away the record from log 1, make sure log 1 becomes empty again.
  std::vector<lsn_t> tail_lsns;
  ASSERT_TRUE(getTailLSNs({1}, tail_lsns));
  ASSERT_TRUE(trimRecords({1}, tail_lsns));
  waitForTrimPointUpdate();

  ASSERT_TRUE(isLogEmptyResultsMatch({
      /*log_id, status, empty, run_with_grace_period(default: true)*/
      {1, E::OK, true},
      {2, E::OK, false},
      {3, E::OK, true},
      {4, E::OK, true},
  }));

  // Write records to logs 2, 3 and make sure they are found non-empty.
  writeRecords({2, 3});
  ASSERT_TRUE(isLogEmptyResultsMatch({
      /*log_id, status, empty, run_with_grace_period(default: true)*/
      {1, E::OK, true},
      {2, E::OK, false},
      {3, E::OK, false},
      {4, E::OK, true},
  }));
}

TEST_F(IsLogEmptyV2Test, FewEpochTest) {
  init();

  // Wait for recoveries to finish, which'll write bridge records for all the
  // logs. These should be ignored, and all logs correctly declared empty.
  cluster_->waitForRecovery();

  // Nothing written, should be empty.
  ASSERT_TRUE(isLogEmptyResultsMatch({
      /*log_id, status, empty, run_with_grace_period(default: true)*/
      {1, E::OK, true},
      {2, E::OK, true},
      {3, E::OK, true},
      {4, E::OK, true},
  }));

  // Restart sequencer, should still be recognized as empty.
  cluster_->getNode(0).shutdown();
  cluster_->getNode(0).start();
  cluster_->getNode(0).waitUntilStarted();
  cluster_->waitForRecovery();

  ASSERT_TRUE(isLogEmptyResultsMatch({
      /*log_id, status, empty, run_with_grace_period(default: true)*/
      {1, E::OK, true},
      {2, E::OK, true},
      {3, E::OK, true},
      {4, E::OK, true},
  }));

  // Write records to log 1, 2 and make sure they're non-empty.
  writeRecords({1, 2});
  ASSERT_TRUE(isLogEmptyResultsMatch({
      /*log_id, status, empty, run_with_grace_period(default: true)*/
      {1, E::OK, false},
      {2, E::OK, false},
      {3, E::OK, true},
      {4, E::OK, true},
  }));
  waitForTrimPointUpdate();
  ASSERT_TRUE(isLogEmptyResultsMatch({
      /*log_id, status, empty, run_with_grace_period(default: true)*/
      {1, E::OK, false},
      {2, E::OK, false},
      {3, E::OK, true},
      {4, E::OK, true},
  }));

  // Restart sequencer, should still be recognized as non-empty.
  cluster_->getNode(0).shutdown();
  cluster_->getNode(0).start();
  cluster_->getNode(0).waitUntilStarted();
  cluster_->waitForRecovery();
  ASSERT_TRUE(isLogEmptyResultsMatch({
      /*log_id, status, empty, run_with_grace_period(default: true)*/
      {1, E::OK, false},
      {2, E::OK, false},
      {3, E::OK, true},
      {4, E::OK, true},
  }));

  // Trim away the record from log 1, make sure it becomes empty again.
  std::vector<lsn_t> tail_lsns;
  ASSERT_TRUE(getTailLSNs({1}, tail_lsns));
  ASSERT_TRUE(trimRecords({1}, tail_lsns));

  // Restart sequencer, should still be recognized as empty.
  cluster_->getNode(0).shutdown();
  cluster_->getNode(0).start();
  cluster_->getNode(0).waitUntilStarted();
  cluster_->waitForRecovery();
  ASSERT_TRUE(isLogEmptyResultsMatch({
      /*log_id, status, empty, run_with_grace_period(default: true)*/
      {1, E::OK, true},
      {2, E::OK, false},
      {3, E::OK, true},
      {4, E::OK, true},
  }));

  // Write records to logs 2, 3 and make sure they are found non-empty.
  writeRecords({2, 3});
  ASSERT_TRUE(isLogEmptyResultsMatch({
      /*log_id, status, empty, run_with_grace_period(default: true)*/
      {1, E::OK, true},
      {2, E::OK, false},
      {3, E::OK, false},
      {4, E::OK, true},
  }));
}

TEST_F(IsLogEmptyV2Test, ManyEpochTest) {
  init();

  // Wait for recoveries to finish, which'll write bridge records for all the
  // logs. These should be ignored, and all logs correctly declared empty.
  cluster_->waitForRecovery();

  // Nothing written, should be empty.
  ASSERT_TRUE(isLogEmptyResultsMatch({
      /*log_id, status, empty, run_with_grace_period(default: true)*/
      {1, E::OK, true},
      {2, E::OK, true},
      {3, E::OK, true},
      {4, E::OK, true},
  }));

  // Restart sequencer a bunch of times, should still be recognized as empty.
  for (int i = 0; i < 3; i++) {
    cluster_->getNode(0).shutdown();
    cluster_->getNode(0).start();
    cluster_->getNode(0).waitUntilStarted();
    cluster_->waitForRecovery();
  }

  ASSERT_TRUE(isLogEmptyResultsMatch({
      /*log_id, status, empty, run_with_grace_period(default: true)*/
      {1, E::OK, true},
      {2, E::OK, true},
      {3, E::OK, true},
      {4, E::OK, true},
  }));

  // Write records to log 1, 2 and make sure they're non-empty.
  writeRecords({1, 2});
  ASSERT_TRUE(isLogEmptyResultsMatch({
      /*log_id, status, empty, run_with_grace_period(default: true)*/
      {1, E::OK, false},
      {2, E::OK, false},
      {3, E::OK, true},
      {4, E::OK, true},
  }));

  // Restart sequencer a bunch of times, should still be recognized as
  // non-empty.
  for (int i = 0; i < 3; i++) {
    cluster_->getNode(0).shutdown();
    cluster_->getNode(0).start();
    cluster_->getNode(0).waitUntilStarted();
    cluster_->waitForRecovery();
  }

  ASSERT_TRUE(isLogEmptyResultsMatch({
      /*log_id, status, empty, run_with_grace_period(default: true)*/
      {1, E::OK, false},
      {2, E::OK, false},
      {3, E::OK, true},
      {4, E::OK, true},
  }));

  // Trim away the record from log 1, make sure it becomes empty again.
  std::vector<lsn_t> tail_lsns;
  ASSERT_TRUE(getTailLSNs({1}, tail_lsns));
  ASSERT_TRUE(trimRecords({1}, tail_lsns));

  // Restart sequencer a bunch of times, should still be recognized as empty.
  for (int i = 0; i < 3; i++) {
    cluster_->getNode(0).shutdown();
    cluster_->getNode(0).start();
    cluster_->getNode(0).waitUntilStarted();
    cluster_->waitForRecovery();
  }

  ASSERT_TRUE(isLogEmptyResultsMatch({
      /*log_id, status, empty, run_with_grace_period(default: true)*/
      {1, E::OK, true},
      {2, E::OK, false},
      {3, E::OK, true},
      {4, E::OK, true},
  }));
}

} // namespace
