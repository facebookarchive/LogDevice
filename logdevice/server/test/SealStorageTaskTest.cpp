/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/server/storage/SealStorageTask.h"

#include <gtest/gtest.h>

#include "logdevice/common/OffsetMap.h"
#include "logdevice/server/locallogstore/test/StoreUtil.h"
#include "logdevice/server/locallogstore/test/TemporaryLogStore.h"
#include "logdevice/server/read_path/LogStorageStateMap.h"

namespace facebook { namespace logdevice {

const shard_index_t SHARD_IDX = 0;

using Context = SealStorageTask::Context;
using EpochInfo = SealStorageTask::EpochInfo;
using EpochInfoMap = SealStorageTask::EpochInfoMap;

class TestSealStorageTask : public SealStorageTask {
 public:
  TestSealStorageTask(logid_t log_id,
                      epoch_t last_clean,
                      Seal seal,
                      const Address& reply_to,
                      bool tail_optimized)
      : SealStorageTask(log_id, last_clean, seal, reply_to, tail_optimized) {}

  TestSealStorageTask(logid_t log_id,
                      epoch_t last_clean,
                      Seal seal,
                      WeakRef<PurgeUncleanEpochs> driver)
      : SealStorageTask(log_id, last_clean, seal, driver) {}
  shard_index_t getShardIdx() const override {
    return SHARD_IDX;
  }
};

static TestSealStorageTask create_task(logid_t log_id,
                                       epoch_t seal_epoch,
                                       epoch_t last_clean,
                                       bool tail_optimized = false) {
  return TestSealStorageTask(log_id,
                             last_clean,
                             Seal(seal_epoch, NodeID(0, 1)),
                             Address(NodeID()),
                             tail_optimized);
}

static TestSealStorageTask create_task_purging(logid_t log_id,
                                               epoch_t seal_epoch,
                                               epoch_t last_clean) {
  return TestSealStorageTask(log_id,
                             last_clean,
                             Seal(seal_epoch, NodeID(0, 1)),
                             WeakRef<PurgeUncleanEpochs>());
}

static lsn_t lsn(int epoch, int esn) {
  return compose_lsn(epoch_t(epoch), esn_t(esn));
}

TEST(SealStorageTaskTest, UpdateSeal) {
  TemporaryRocksDBStore store;
  LogStorageStateMap map(1);

  {
    TestSealStorageTask task =
        create_task(logid_t(1), epoch_t(2), EPOCH_INVALID);
    ASSERT_EQ(E::OK, task.executeImpl(store, map));
  }

  {
    TestSealStorageTask task =
        create_task(logid_t(1), epoch_t(1), EPOCH_INVALID);
    ASSERT_EQ(E::PREEMPTED, task.executeImpl(store, map));
  }
}

TEST(SealStorageTaskTest, UpdateSealSmaller) {
  TemporaryRocksDBStore store;
  LogStorageStateMap map(1);

  // local log store already contains a seal record with epoch 2
  store.writeLogMetadata(logid_t(1),
                         SealMetadata(Seal(epoch_t(2), NodeID(0, 1))),
                         LocalLogStore::WriteOptions());

  TestSealStorageTask task = create_task(logid_t(1), epoch_t(1), EPOCH_INVALID);
  EXPECT_EQ(E::PREEMPTED, task.executeImpl(store, map));

  folly::Optional<Seal> seal =
      map.get(logid_t(1), SHARD_IDX).getSeal(LogStorageState::SealType::NORMAL);
  ASSERT_TRUE(seal.hasValue());
  EXPECT_EQ(epoch_t(2), seal.value().epoch);
}

TEST(SealStorageTaskTest, LastKnownGood) {
  TemporaryRocksDBStore store;
  LogStorageStateMap map(1);

  std::vector<TestRecord> records = {
      TestRecord(logid_t(1), lsn(1, 1), ESN_INVALID),
      TestRecord(logid_t(1), lsn(1, 2), esn_t(1)),
      TestRecord(logid_t(1), lsn(1, 3), esn_t(1)),
      TestRecord(logid_t(1), lsn(1, 4), esn_t(3)),
      TestRecord(logid_t(1), lsn(2, 1), ESN_INVALID),
      TestRecord(logid_t(1), lsn(2, 2), esn_t(1)),
      TestRecord(logid_t(1), lsn(4, 1), ESN_INVALID),
      TestRecord(logid_t(1), lsn(4, 4), esn_t(1)),
      TestRecord(logid_t(1), lsn(EPOCH_MAX.val_, 1), ESN_INVALID),
      TestRecord(logid_t(1), lsn(EPOCH_MAX.val_, 2), esn_t(1)),
  };

  store_fill(store, records);

  // get LNG values for all epochs in <0, 5]
  TestSealStorageTask task1 = create_task(logid_t(1), epoch_t(5), epoch_t(0));

  ASSERT_EQ(E::OK, task1.executeImpl(store, map));
  std::vector<lsn_t> epoch_lng;
  std::vector<OffsetMap> epoch_offset_map;
  std::vector<uint64_t> last_timestamp;
  std::vector<lsn_t> max_seen_lsn;
  task1.getAllEpochInfo(
      epoch_lng, epoch_offset_map, last_timestamp, max_seen_lsn);
  EXPECT_EQ(std::vector<lsn_t>({
                lsn(1, 3),
                lsn(2, 1),
                lsn(3, ESN_INVALID.val_),
                lsn(4, 1),
                lsn(5, ESN_INVALID.val_),
            }),
            epoch_lng);

  EXPECT_EQ(std::vector<lsn_t>({
                lsn(1, 4),
                lsn(2, 2),
                lsn(3, ESN_INVALID.val_),
                lsn(4, 4),
                lsn(5, ESN_INVALID.val_),
            }),
            max_seen_lsn);

  // tail records: [(1, 3), (2, 1), (4, 1)]
  ASSERT_EQ(3, task1.tail_records_.size());
  ASSERT_EQ(lsn(1, 3), task1.tail_records_[0].header.lsn);
  ASSERT_EQ(lsn(2, 1), task1.tail_records_[1].header.lsn);
  ASSERT_EQ(lsn(4, 1), task1.tail_records_[2].header.lsn);

  TestSealStorageTask task2 =
      create_task(logid_t(1), EPOCH_MAX, epoch_t(EPOCH_MAX.val_ - 1));

  ASSERT_EQ(E::OK, task2.executeImpl(store, map));
  task2.getAllEpochInfo(
      epoch_lng, epoch_offset_map, last_timestamp, max_seen_lsn);
  EXPECT_EQ(std::vector<lsn_t>({
                lsn(EPOCH_MAX.val_, 1),
            }),
            epoch_lng);

  EXPECT_EQ(std::vector<lsn_t>({
                lsn(EPOCH_MAX.val_, 2),
            }),
            max_seen_lsn);

  // tail records: [(EPOCH_MAX, 1)]
  ASSERT_EQ(1, task2.tail_records_.size());
  ASSERT_EQ(lsn(EPOCH_MAX.val_, 1), task2.tail_records_[0].header.lsn);
}

// SealStorageTask can be preempted by soft seals but never modify them.
// it also recovers soft seals if it is not in memory
TEST(SealStorageTaskTest, SoftSeal) {
  TemporaryRocksDBStore store;
  LogStorageStateMap map(1);

  {
    // local log store already contains a normal seal record with epoch 2
    store.writeLogMetadata(logid_t(1),
                           SealMetadata(Seal(epoch_t(2), NodeID(0, 1))),
                           LocalLogStore::WriteOptions());
    // local log store already contains a soft seal record with epoch 3
    store.writeLogMetadata(logid_t(1),
                           SoftSealMetadata(Seal(epoch_t(3), NodeID(5, 1))),
                           LocalLogStore::WriteOptions());
    // try to seal epoch 2
    TestSealStorageTask task =
        create_task(logid_t(1), epoch_t(2), EPOCH_INVALID);
    EXPECT_EQ(E::PREEMPTED, task.executeImpl(store, map));
    // preempted by soft seal
    EXPECT_EQ(Seal(epoch_t(3), NodeID(5, 1)), task.getSeal());

    folly::Optional<Seal> soft_seal =
        map.get(logid_t(1), SHARD_IDX).getSeal(LogStorageState::SealType::SOFT);
    ASSERT_TRUE(soft_seal.hasValue());
    EXPECT_EQ(Seal(epoch_t(3), NodeID(5, 1)), soft_seal.value());
  }

  {
    ////// log 2
    // local log store already contains a normal seal record with epoch 3
    store.writeLogMetadata(logid_t(2),
                           SealMetadata(Seal(epoch_t(3), NodeID(0, 1))),
                           LocalLogStore::WriteOptions());
    // local log store already contains a soft seal record with epoch 2
    store.writeLogMetadata(logid_t(2),
                           SoftSealMetadata(Seal(epoch_t(2), NodeID(5, 1))),
                           LocalLogStore::WriteOptions());
    TestSealStorageTask task =
        create_task(logid_t(2), epoch_t(1), EPOCH_INVALID);
    EXPECT_EQ(E::PREEMPTED, task.executeImpl(store, map));
    // preempted by normal seal
    EXPECT_EQ(Seal(epoch_t(3), NodeID(0, 1)), task.getSeal());

    folly::Optional<Seal> soft_seal =
        map.get(logid_t(2), SHARD_IDX).getSeal(LogStorageState::SealType::SOFT);
    // soft seal should have value but stay the same
    ASSERT_TRUE(soft_seal.hasValue());
    EXPECT_EQ(Seal(epoch_t(2), NodeID(5, 1)), soft_seal.value());

    folly::Optional<Seal> normal_seal =
        map.get(logid_t(2), SHARD_IDX)
            .getSeal(LogStorageState::SealType::NORMAL);
    // soft seal should have value but stay the same
    ASSERT_TRUE(normal_seal.hasValue());
    EXPECT_EQ(Seal(epoch_t(3), NodeID(0, 1)), normal_seal.value());
  }
}

TEST(SealStorageTaskTest, EpochInfoWithByteOffset) {
  TemporaryRocksDBStore store;
  LogStorageStateMap map(1);

  std::vector<TestRecord> records = {
      TestRecord(logid_t(1), lsn(2, 1), ESN_INVALID)
          .offsetsWithinEpoch(OffsetMap({{BYTE_OFFSET, 10}})),
      TestRecord(logid_t(1), lsn(2, 2), esn_t(1)),
  };

  store_fill(store, records);
  // get epoch info for all epochs in <0, 120]
  TestSealStorageTask task1 =
      TestSealStorageTask(logid_t(1),
                          epoch_t(0),
                          Seal(epoch_t(120), NodeID(0, 1)),
                          Address(NodeID()),
                          false);
  ASSERT_EQ(E::OK, task1.executeImpl(store, map));
  ASSERT_NE(nullptr, task1.epoch_info_);
}

TEST(SealStorageTaskTest, EpochInfo) {
  TemporaryRocksDBStore store;
  LogStorageStateMap map(1);

  std::vector<TestRecord> records = {
      TestRecord(logid_t(1), lsn(2, 1), ESN_INVALID),
      TestRecord(logid_t(1), lsn(2, 2), esn_t(1)),
      TestRecord(logid_t(1), lsn(4, 1), ESN_INVALID),
      TestRecord(logid_t(1), lsn(4, 4), esn_t(3)),
      TestRecord(logid_t(1), lsn(7, 21), ESN_INVALID),
      TestRecord(logid_t(1), lsn(6000, 35542), esn_t(200)),
      TestRecord(logid_t(1), lsn(6000, 77213), esn_t(35353)),
      TestRecord(logid_t(1), lsn(EPOCH_MAX.val_, 1), ESN_INVALID),
      TestRecord(logid_t(1), lsn(EPOCH_MAX.val_, 2), esn_t(1)),
  };

  store_fill(store, records);
  // get epoch info for all epochs in <0, 120]
  TestSealStorageTask task1 = create_task(logid_t(1), epoch_t(120), epoch_t(0));
  ASSERT_EQ(E::OK, task1.executeImpl(store, map));
  ASSERT_NE(nullptr, task1.epoch_info_);
  EXPECT_EQ(EpochInfoMap({{2, EpochInfo{esn_t(1), esn_t(2), OffsetMap()}},
                          {4, EpochInfo{esn_t(3), esn_t(4), OffsetMap()}},
                          {7, EpochInfo{esn_t(0), esn_t(21), OffsetMap()}}}),
            *(task1.epoch_info_));
  std::vector<lsn_t> epoch_lng;
  std::vector<OffsetMap> epoch_offset_map;
  std::vector<uint64_t> last_timestamp;
  std::vector<lsn_t> max_seen_lsn;
  task1.getAllEpochInfo(
      epoch_lng, epoch_offset_map, last_timestamp, max_seen_lsn);
  ASSERT_EQ(120, epoch_lng.size());
  ASSERT_EQ(120, epoch_offset_map.size());
  ASSERT_EQ(120, last_timestamp.size());
  ASSERT_EQ(120, max_seen_lsn.size());

  // tail records: [(2, 1), (4, 1)]
  ASSERT_EQ(2, task1.tail_records_.size());
  ASSERT_EQ(lsn(2, 1), task1.tail_records_[0].header.lsn);
  ASSERT_EQ(lsn(4, 1), task1.tail_records_[1].header.lsn);

  // get epoch info for all epochs in <3, 6000]
  TestSealStorageTask task2 =
      create_task_purging(logid_t(1), epoch_t(6000), epoch_t(3));
  ASSERT_EQ(E::OK, task2.executeImpl(store, map));
  ASSERT_NE(nullptr, task2.epoch_info_);
  EXPECT_EQ(EpochInfoMap(
                {{4, EpochInfo{esn_t(3), esn_t(4), OffsetMap()}},
                 {7, EpochInfo{esn_t(0), esn_t(21), OffsetMap()}},
                 {6000, EpochInfo{esn_t(35353), esn_t(77213), OffsetMap()}}}),
            *(task2.epoch_info_));
  task2.getAllEpochInfo(
      epoch_lng, epoch_offset_map, last_timestamp, max_seen_lsn);
  ASSERT_EQ(5997, epoch_lng.size());
  ASSERT_EQ(5997, epoch_offset_map.size());
  ASSERT_EQ(5997, last_timestamp.size());
  ASSERT_EQ(5997, max_seen_lsn.size());

  // tail records should be empty for purging
  ASSERT_TRUE(task2.tail_records_.empty());
}

TEST(SealStorageTaskTest, PurgeStillGetEpochInfoOnPreemption) {
  TemporaryRocksDBStore store;
  LogStorageStateMap map(1);

  std::vector<TestRecord> records = {
      TestRecord(logid_t(1), lsn(7, 21), esn_t(5))};

  store_fill(store, records);
  Seal seal(epoch_t(98), NodeID(0, 1));
  // local log store already contains a normal seal record with epoch 98
  store.writeLogMetadata(
      logid_t(1), SealMetadata(seal), LocalLogStore::WriteOptions());
  map.insertOrGet(logid_t(1), SHARD_IDX)
      ->updateSeal(seal, LogStorageState::SealType::NORMAL);

  // try to seal epoch 10 for recovery
  TestSealStorageTask task =
      create_task(logid_t(1), epoch_t(10), EPOCH_INVALID);
  EXPECT_EQ(E::PREEMPTED, task.executeImpl(store, map));
  std::vector<lsn_t> epoch_lng;
  std::vector<OffsetMap> epoch_offset_map;
  std::vector<uint64_t> last_timestamp;
  std::vector<lsn_t> max_seen_lsn;
  task.getAllEpochInfo(
      epoch_lng, epoch_offset_map, last_timestamp, max_seen_lsn);
  ASSERT_TRUE(epoch_lng.empty());
  ASSERT_TRUE(epoch_offset_map.empty());
  ASSERT_TRUE(max_seen_lsn.empty());
  EXPECT_EQ(Seal(epoch_t(98), NodeID(0, 1)), task.getSeal());

  // try to seal epoch 9 for purging
  TestSealStorageTask task2 =
      create_task_purging(logid_t(1), epoch_t(9), epoch_t(3));
  EXPECT_EQ(E::PREEMPTED, task2.executeImpl(store, map));
  ASSERT_NE(nullptr, task2.epoch_info_);
  EXPECT_EQ(EpochInfoMap({
                {7, EpochInfo{esn_t(5), esn_t(21), OffsetMap()}},
            }),
            *(task2.epoch_info_));
  task2.getAllEpochInfo(
      epoch_lng, epoch_offset_map, last_timestamp, max_seen_lsn);
  ASSERT_EQ(6, epoch_lng.size());
  ASSERT_EQ(6, epoch_offset_map.size());
  ASSERT_EQ(6, last_timestamp.size());
  ASSERT_EQ(6, max_seen_lsn.size());
  EXPECT_EQ(Seal(epoch_t(98), NodeID(0, 1)), task2.getSeal());
}

// test that SealStorageTask can make use of the MutablePerEpochLogMetadata
// to get a more accurate tail record
TEST(SealStorageTaskTest, TailRecordWithMutablePerEpochLogMetadata) {
  TemporaryRocksDBStore store;
  LogStorageStateMap map(1);

  std::vector<TestRecord> records = {
      TestRecord(logid_t(1), lsn(2, 1), ESN_INVALID),
      TestRecord(logid_t(1), lsn(2, 2), esn_t(1)),
      TestRecord(logid_t(1), lsn(4, 1), ESN_INVALID),
      TestRecord(logid_t(1), lsn(4, 4), esn_t(2)),
      TestRecord(logid_t(1), lsn(7, 21), ESN_INVALID),
      TestRecord(logid_t(1), lsn(6000, 35542), esn_t(200)),
      TestRecord(logid_t(1), lsn(6000, 77213), esn_t(35353)),
      TestRecord(logid_t(1), lsn(EPOCH_MAX.val_, 1), ESN_INVALID),
      TestRecord(logid_t(1), lsn(EPOCH_MAX.val_, 2), esn_t(1)),
  };

  store_fill(store, records);
  // write mutable per-epoch log metadata for some epochs
  auto write_per_epoch_release = [&](epoch_t epoch, esn_t lng) {
    OffsetMap epoch_size_map;
    epoch_size_map.setCounter(BYTE_OFFSET, 0);
    MutablePerEpochLogMetadata metadata(1, lng, epoch_size_map);
    int rv =
        store.updatePerEpochLogMetadata(logid_t(1),
                                        epoch,
                                        metadata,
                                        LocalLogStore::SealPreemption::DISABLE,
                                        LocalLogStore::WriteOptions());
    ASSERT_EQ(0, rv);
  };

  write_per_epoch_release(epoch_t(4), esn_t(10));
  write_per_epoch_release(epoch_t(7), esn_t(355));
  write_per_epoch_release(epoch_t(6000), esn_t(700));
  write_per_epoch_release(EPOCH_MAX, esn_t(3));

  // get epoch info for all epochs in <0, epoch_max]
  TestSealStorageTask task1 = create_task(logid_t(1), EPOCH_MAX, epoch_t(0));
  ASSERT_EQ(E::OK, task1.executeImpl(store, map));
  ASSERT_NE(nullptr, task1.epoch_info_);
  // tail records: [(2, 1), (4, 4), (7, 21), (EPOCH_MAX, 2)]
  ASSERT_EQ(4, task1.tail_records_.size());
  ASSERT_EQ(lsn(2, 1), task1.tail_records_[0].header.lsn);
  ASSERT_EQ(lsn(4, 4), task1.tail_records_[1].header.lsn);
  ASSERT_EQ(lsn(7, 21), task1.tail_records_[2].header.lsn);
  ASSERT_EQ(lsn(EPOCH_MAX.val_, 2), task1.tail_records_[3].header.lsn);
}

TEST(SealStorageTaskTest, TailRecordWithEpochOffset) {
  TemporaryRocksDBStore store;
  LogStorageStateMap map(1);

  bool tail_optimized = true;

  char buf[TAIL_RECORD_INLINE_LIMIT + 1];
  memset(buf, 'n', sizeof(buf));

  // epoch 2: tail record has byte offset
  // epoch 4: tail record does not have epoch offset, but one previous epoch has
  // epoch 7: tail record does not have epoch offset, but mutable per-epoch log
  //          metadata has the epoch size
  std::vector<TestRecord> records = {
      TestRecord(logid_t(1), lsn(2, 1), ESN_INVALID)
          .offsetsWithinEpoch(OffsetMap({{BYTE_OFFSET, 32}})),
      TestRecord(logid_t(1), lsn(2, 2), esn_t(1)),
      TestRecord(logid_t(1), lsn(4, 1), ESN_INVALID)
          .offsetsWithinEpoch(OffsetMap({{BYTE_OFFSET, 54210}})),
      TestRecord(logid_t(1), lsn(4, 4), esn_t(2))
          .payload(Payload("test", 4))
          .offsetsWithinEpoch(OffsetMap({{BYTE_OFFSET, 54214}})),
      TestRecord(logid_t(1), lsn(4, 7), esn_t(4)),
      TestRecord(logid_t(1), lsn(7, 21), ESN_INVALID)
          .payload(Payload(buf, sizeof(buf))),
  };

  store_fill(store, records);
  // write mutable per-epoch log metadata for some epochs
  auto write_per_epoch_release = [&](epoch_t epoch, esn_t lng, uint64_t es) {
    OffsetMap epoch_size_map;
    epoch_size_map.setCounter(BYTE_OFFSET, es);
    MutablePerEpochLogMetadata metadata(1, lng, epoch_size_map);
    int rv =
        store.updatePerEpochLogMetadata(logid_t(1),
                                        epoch,
                                        metadata,
                                        LocalLogStore::SealPreemption::DISABLE,
                                        LocalLogStore::WriteOptions());
    ASSERT_EQ(0, rv);
  };

  write_per_epoch_release(epoch_t(7), esn_t(355), 672);
  write_per_epoch_release(epoch_t(2), esn_t(1), 32);

  // get epoch info for all epochs in <0, epoch_max]
  TestSealStorageTask task1 =
      create_task(logid_t(1), EPOCH_MAX, epoch_t(0), tail_optimized);
  ASSERT_EQ(E::OK, task1.executeImpl(store, map));
  ASSERT_NE(nullptr, task1.epoch_info_);
  // tail records: [(2, 1), (4, 4), (7, 21)]
  // expected epoch offset: 32, 54210, 672
  ASSERT_EQ(3, task1.tail_records_.size());
  ASSERT_EQ(lsn(2, 1), task1.tail_records_[0].header.lsn);
  ASSERT_EQ(lsn(4, 4), task1.tail_records_[1].header.lsn);
  ASSERT_TRUE(task1.tail_records_[1].hasPayload());
  auto slice = task1.tail_records_[1].getPayloadSlice();
  ASSERT_EQ(4, slice.size);
  ASSERT_EQ("test", std::string((char*)slice.data, slice.size));
  ASSERT_EQ(lsn(7, 21), task1.tail_records_[2].header.lsn);
  // payload too large exceeded the inline limit
  ASSERT_FALSE(task1.tail_records_[2].hasPayload());
  for (const auto& r : task1.tail_records_) {
    ASSERT_TRUE(r.header.flags & TailRecordHeader::OFFSET_WITHIN_EPOCH);
    if (!r.hasPayload()) {
      ASSERT_FALSE(r.header.flags & TailRecordHeader::CHECKSUM);
      ASSERT_FALSE(r.header.flags & TailRecordHeader::CHECKSUM_64BIT);
      ASSERT_TRUE(r.header.flags & TailRecordHeader::CHECKSUM_PARITY);
    }
  }
  ASSERT_EQ(32, task1.tail_records_[0].offsets_map_.getCounter(BYTE_OFFSET));
  ASSERT_EQ(
      32, (*task1.epoch_info_)[2].epoch_offset_map.getCounter(BYTE_OFFSET));
  ASSERT_EQ(54214, task1.tail_records_[1].offsets_map_.getCounter(BYTE_OFFSET));
  ASSERT_EQ(672, task1.tail_records_[2].offsets_map_.getCounter(BYTE_OFFSET));
}

// test Seal should succeed on retries with the same Seal ID, but only
// the first attempt requires sync
TEST(SealStorageTaskTest, RetrySealingAndDurability) {
  TemporaryRocksDBStore store;
  LogStorageStateMap map(1);

  std::vector<TestRecord> records = {
      TestRecord(logid_t(1), lsn(1, 2), esn_t(1)),
  };
  store_fill(store, records);

  // seal 5 times
  for (int attempt = 1; attempt <= 5; ++attempt) {
    // get epoch info for all epochs in <0, 2]
    TestSealStorageTask task1 = create_task(logid_t(1), epoch_t(2), epoch_t(0));
    ASSERT_EQ(E::OK, task1.executeImpl(store, map));
    std::vector<lsn_t> epoch_lng;
    std::vector<OffsetMap> epoch_offset_map;
    std::vector<uint64_t> last_timestamp;
    std::vector<lsn_t> max_seen_lsn;
    task1.getAllEpochInfo(
        epoch_lng, epoch_offset_map, last_timestamp, max_seen_lsn);
    EXPECT_EQ(
        std::vector<lsn_t>({lsn(1, 1), lsn(2, ESN_INVALID.val_)}), epoch_lng);

    EXPECT_EQ(std::vector<lsn_t>({
                  lsn(1, 2),
                  lsn(2, ESN_INVALID.val_),
              }),
              max_seen_lsn);

    if (attempt == 1) {
      // needs sync only on the first attempt
      ASSERT_EQ(Durability::SYNC_WRITE, task1.durability());
    } else {
      ASSERT_EQ(Durability::INVALID, task1.durability());
    }
  }

  {
    // preempted seal task should not require sync either
    TestSealStorageTask task1 = create_task(logid_t(1), epoch_t(1), epoch_t(0));
    ASSERT_EQ(E::PREEMPTED, task1.executeImpl(store, map));
    ASSERT_EQ(Durability::INVALID, task1.durability());
  }
  {
    // updated seal again, sync is required
    TestSealStorageTask task1 = create_task(logid_t(1), epoch_t(4), epoch_t(0));
    ASSERT_EQ(E::OK, task1.executeImpl(store, map));
    ASSERT_EQ(Durability::SYNC_WRITE, task1.durability());
  }
}

}} // namespace facebook::logdevice
