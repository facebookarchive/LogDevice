/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/server/storage/PurgeSingleEpoch.h"

#include <chrono>
#include <memory>
#include <vector>

#include <folly/Memory.h>
#include <gtest/gtest.h>

#include "logdevice/common/Timer.h"
#include "logdevice/common/debug.h"
#include "logdevice/common/stats/Stats.h"
#include "logdevice/common/test/MockBackoffTimer.h"
#include "logdevice/common/test/NodeSetTestUtil.h"
#include "logdevice/common/util.h"
#include "logdevice/server/locallogstore/test/StoreUtil.h"
#include "logdevice/server/locallogstore/test/TemporaryLogStore.h"
#include "logdevice/server/storage/PurgeUncleanEpochs.h"

using namespace facebook::logdevice;
using namespace facebook::logdevice::NodeSetTestUtil;

#define N0S0 ShardID(0, 0)
#define N1S0 ShardID(1, 0)
#define N2S0 ShardID(2, 0)
#define N3S0 ShardID(3, 0)
#define N4S0 ShardID(4, 0)

class MockPurgeSingleEpoch;

const logid_t LOG_ID(2333333);

class PurgeSingleEpochTest : public ::testing::Test {
 public:
  void setUp();

  lsn_t lsn(uint32_t epoch, uint32_t esn) {
    return compose_lsn(epoch_t(epoch), esn_t(esn));
  }

  PurgeDeleteRecordsStorageTask createDeleteTask(logid_t log_id,
                                                 epoch_t epoch,
                                                 esn_t start_esn,
                                                 esn_t end_esn) {
    return PurgeDeleteRecordsStorageTask(
        log_id, epoch, start_esn, end_esn, WeakRef<PurgeSingleEpoch>());
  }

  std::vector<lsn_t> getLsnsForLog(logid_t log_id, LocalLogStore& store) {
    std::vector<lsn_t> found_lsns;
    auto it =
        store.read(log_id, LocalLogStore::ReadOptions("LocalLogStoreChanges"));
    it->seek(lsn_t(0));
    for (it->seek(lsn_t(0)); it->state() == IteratorState::AT_RECORD;
         it->next()) {
      found_lsns.push_back(it->getLSN());
    }
    ld_check_eq(it->state(), IteratorState::AT_END);
    return found_lsns;
  }

  std::vector<std::unique_ptr<StorageTask>> tasks_;
  std::shared_ptr<Configuration> config_;
  std::unique_ptr<MockPurgeSingleEpoch> purge_;
  logid_t log_id_{1234};
  shard_index_t shard_{0};
  epoch_t purge_to_{10};
  epoch_t epoch_{1};
  std::shared_ptr<EpochMetaData> metadata_;
  esn_t local_lng_{0};
  esn_t local_last_record_{0};
  Status status_{E::UNKNOWN};
  EpochRecoveryMetadata erm_;
  ReplicationProperty replication_{{NodeLocationScope::NODE, 3}};
  StorageSet nodes_{N0S0, N1S0, N2S0, N3S0, N4S0};
  bool complete_{false};
  bool GetERMRequestPosted_{false};
  TailRecord tail_record;
  OffsetMap epoch_size_map;
  OffsetMap epoch_end_offsets;
};

class MockPurgeSingleEpoch : public PurgeSingleEpoch {
 public:
  explicit MockPurgeSingleEpoch(PurgeSingleEpochTest* test)
      : PurgeSingleEpoch(test->log_id_,
                         test->shard_,
                         test->purge_to_,
                         test->epoch_,
                         test->metadata_,
                         test->local_lng_,
                         test->local_last_record_,
                         test->status_,
                         test->erm_,
                         nullptr),
        test_(test) {}

  StatsHolder* getStats() override {
    return nullptr;
  }

  std::unique_ptr<BackoffTimer> createRetryTimer() override {
    return std::make_unique<MockBackoffTimer>();
  }

  void startStorageTask(std::unique_ptr<StorageTask>&& task) override {
    test_->tasks_.push_back(std::move(task));
  }

  void postGetEpochRecoveryMetadataRequest() override {
    ld_check(!test_->GetERMRequestPosted_);
    test_->GetERMRequestPosted_ = true;
  }

  void deferredComplete() override {
    ASSERT_FALSE(test_->complete_);
    test_->complete_ = true;
  }

  PurgeSingleEpochTest* test_;
};

void PurgeSingleEpochTest::setUp() {
  dbg::currentLevel = dbg::Level::DEBUG;
  dbg::assertOnData = true;

  Configuration::Nodes nodes;
  addNodes(&nodes, 1, 1, "....", 1);
  Configuration::NodesConfig nodes_config(std::move(nodes));
  auto logs_config = std::make_shared<configuration::LocalLogsConfig>();
  addLog(logs_config.get(), log_id_, 1, 0, 1, {});
  config_ = std::make_shared<Configuration>(
      ServerConfig::fromDataTest(
          "purge_single_epoch_test", std::move(nodes_config)),
      std::move(logs_config));

  metadata_ = std::make_shared<EpochMetaData>(nodes_, replication_);
  purge_ = std::make_unique<MockPurgeSingleEpoch>(this);
}

#define CHECK_STORAGE_TASK(name)                         \
  do {                                                   \
    ASSERT_FALSE(tasks_.empty());                        \
    if (folly::kIsDebug) {                               \
      auto task_ptr = tasks_.back().get();               \
      ASSERT_NE(nullptr, dynamic_cast<name*>(task_ptr)); \
    }                                                    \
    tasks_.clear();                                      \
  } while (0)

TEST_F(PurgeSingleEpochTest, Basic) {
  epoch_ = epoch_t(8);
  local_lng_ = esn_t(10);
  local_last_record_ = esn_t(20);
  setUp();
  purge_->start();
  ASSERT_TRUE(GetERMRequestPosted_);
  epoch_size_map.setCounter(BYTE_OFFSET, 0);
  epoch_end_offsets.setCounter(BYTE_OFFSET, 0);
  tail_record.offsets_map_.setCounter(BYTE_OFFSET, 0);
  EpochRecoveryMetadata md(epoch_t(9),
                           esn_t(10),
                           esn_t(11),
                           0,
                           tail_record,
                           epoch_size_map,
                           epoch_end_offsets);
  EpochRecoveryStateMap map{{8, {E::OK, md}}};
  purge_->onGetEpochRecoveryMetadataComplete(E::OK, map);
  CHECK_STORAGE_TASK(PurgeDeleteRecordsStorageTask);
  purge_->onPurgeRecordsTaskDone(E::OK);
  CHECK_STORAGE_TASK(PurgeWriteEpochRecoveryMetadataStorageTask);
  purge_->onWriteEpochRecoveryMetadataDone(E::OK);
  ASSERT_TRUE(complete_);
}

TEST_F(PurgeSingleEpochTest, ERMKnown) {
  epoch_ = epoch_t(8);
  local_lng_ = esn_t(10);
  local_last_record_ = esn_t(20);
  epoch_size_map.setCounter(BYTE_OFFSET, 0);
  epoch_end_offsets.setCounter(BYTE_OFFSET, 0);
  tail_record.offsets_map_.setCounter(BYTE_OFFSET, 0);
  erm_ = EpochRecoveryMetadata(epoch_t(9),
                               esn_t(10),
                               esn_t(11),
                               0,
                               tail_record,
                               epoch_size_map,
                               epoch_end_offsets);
  status_ = E::OK;
  setUp();
  purge_->start();
  ASSERT_FALSE(GetERMRequestPosted_);
  CHECK_STORAGE_TASK(PurgeDeleteRecordsStorageTask);
  purge_->onPurgeRecordsTaskDone(E::OK);
  CHECK_STORAGE_TASK(PurgeWriteEpochRecoveryMetadataStorageTask);
  purge_->onWriteEpochRecoveryMetadataDone(E::OK);
  ASSERT_TRUE(complete_);
}

TEST_F(PurgeSingleEpochTest, EpochEmptyLocally) {
  epoch_ = epoch_t(8);
  local_lng_ = esn_t(0);
  local_last_record_ = esn_t(0);
  epoch_size_map.setCounter(BYTE_OFFSET, 0);
  tail_record.offsets_map_.setCounter(BYTE_OFFSET, 0);
  epoch_end_offsets.setCounter(BYTE_OFFSET, 0);
  erm_ = EpochRecoveryMetadata(epoch_t(9),
                               esn_t(10),
                               esn_t(11),
                               0,
                               tail_record,
                               epoch_size_map,
                               epoch_end_offsets);
  status_ = E::OK;
  setUp();
  purge_->start();
  ASSERT_FALSE(GetERMRequestPosted_);
  CHECK_STORAGE_TASK(PurgeWriteEpochRecoveryMetadataStorageTask);
  purge_->onWriteEpochRecoveryMetadataDone(E::OK);
  ASSERT_TRUE(complete_);
}

TEST_F(PurgeSingleEpochTest, EpochEmptyGlobally) {
  epoch_ = epoch_t(8);
  local_lng_ = esn_t(0);
  local_last_record_ = esn_t(0);
  erm_ = EpochRecoveryMetadata();
  status_ = E::EMPTY;
  setUp();
  purge_->start();
  ASSERT_FALSE(GetERMRequestPosted_);
  ASSERT_TRUE(complete_);
}

/// Test the delete storage task used by PurgeSingleEpoch
TEST_F(PurgeSingleEpochTest, DeleteRecordsByKey) {
  TemporaryRocksDBStore store;
  StatsHolder stats(StatsParams().setIsServer(true));

  std::vector<TestRecord> test_data = {
      TestRecord(LOG_ID, lsn(1, 2), esn_t(1)),

      TestRecord(LOG_ID, lsn(2, 1), esn_t(0)),
      TestRecord(LOG_ID, lsn(2, 2), esn_t(1)),
      TestRecord(LOG_ID, lsn(2, 10), esn_t(1)),
      TestRecord(LOG_ID, lsn(2, 12), esn_t(1)),
      TestRecord(LOG_ID, lsn(2, 15), esn_t(1)),
      TestRecord(LOG_ID, lsn(2, 99), esn_t(1)),
      TestRecord(LOG_ID, lsn(2, 140), esn_t(10)),
      TestRecord(LOG_ID, lsn(2, 221), esn_t(10)),
      TestRecord(LOG_ID, lsn(2, 222), esn_t(10)),
      TestRecord(LOG_ID, lsn(2, 360), esn_t(10)),
      TestRecord(LOG_ID, lsn(2, 1020), esn_t(225)),
      TestRecord(LOG_ID, lsn(2, 2034), esn_t(225)),
      TestRecord(LOG_ID, lsn(2, 3033), esn_t(225)),

      TestRecord(LOG_ID, lsn(3, 1), esn_t(0)),
  };

  store_fill(store, test_data);
  // number of keys smaller than PURGE_DELETE_BY_KEY_THRESHOLD
  auto task = createDeleteTask(LOG_ID, epoch_t(2), esn_t(2), esn_t(3900));
  auto before_time = std::chrono::steady_clock::now();
  task.executeImpl(store, &stats, nullptr);
  auto after_time = std::chrono::steady_clock::now();
  auto usec = std::chrono::duration_cast<std::chrono::microseconds>(after_time -
                                                                    before_time)
                  .count();
  ld_info("Task took %lu us.", usec);

  const std::vector<lsn_t> expected_lsns = {
      lsn(1, 2),
      lsn(2, 1),
      lsn(3, 1),
  };
  ASSERT_EQ(expected_lsns, getLsnsForLog(LOG_ID, store));
  ASSERT_EQ(1, stats.get().purging_v2_delete_by_keys);
  ASSERT_EQ(0, stats.get().purging_v2_delete_by_reading_data);
}

TEST_F(PurgeSingleEpochTest, DeleteRecordsByReadingData) {
  TemporaryRocksDBStore store;
  StatsHolder stats(StatsParams().setIsServer(true));

  std::vector<TestRecord> test_data = {
      TestRecord(LOG_ID, lsn(1, 2), esn_t(1)),

      TestRecord(LOG_ID, lsn(2, 2), esn_t(1)),
      TestRecord(LOG_ID, lsn(2, 3988765544), esn_t(225)),

      TestRecord(LOG_ID, lsn(3, 1), esn_t(0)),
  };

  store_fill(store, test_data);
  // number of keys smaller than PURGE_DELETE_BY_KEY_THRESHOLD
  auto task = createDeleteTask(LOG_ID, epoch_t(2), esn_t(3), ESN_MAX);
  auto before_time = std::chrono::steady_clock::now();
  task.executeImpl(store, &stats, nullptr);
  auto after_time = std::chrono::steady_clock::now();
  auto usec = std::chrono::duration_cast<std::chrono::microseconds>(after_time -
                                                                    before_time)
                  .count();
  ld_info("Task took %lu us.", usec);

  const std::vector<lsn_t> expected_lsns = {
      lsn(1, 2),
      lsn(2, 2),
      lsn(3, 1),
  };
  ASSERT_EQ(expected_lsns, getLsnsForLog(LOG_ID, store));
  ASSERT_EQ(0, stats.get().purging_v2_delete_by_keys);
  ASSERT_EQ(1, stats.get().purging_v2_delete_by_reading_data);
}
