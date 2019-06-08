/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/server/StoreStorageTask.h"

#include <cstdint>
#include <iterator>
#include <map>
#include <queue>
#include <vector>

#include <folly/Memory.h>
#include <gtest/gtest.h>

#include "logdevice/common/protocol/STORE_Message.h"
#include "logdevice/common/stats/Stats.h"
#include "logdevice/server/RecordCacheDisposal.h"
#include "logdevice/server/read_path/LogStorageStateMap.h"
#include "logdevice/server/storage_tasks/WriteBatchStorageTask.h"

namespace facebook { namespace logdevice {

const shard_index_t THIS_SHARD = 0;

static StoreChainLink chain_link{ShardID(0, THIS_SHARD), ClientID()};

static STORE_Header create_header(logid_t log_id,
                                  lsn_t lsn,
                                  STORE_flags_t flags,
                                  uint32_t timeout_ms) {
  STORE_Header header;
  header.rid = RecordID{lsn_to_esn(lsn), lsn_to_epoch(lsn), log_id};
  header.timestamp = 0;
  header.last_known_good = esn_t(0);
  header.wave = 1;
  header.flags = flags;
  header.copyset_size = 1;
  header.timeout_ms = timeout_ms;

  return header;
}

static STORE_Extra create_extra(recovery_id_t recovery_id,
                                epoch_t recovery_epoch) {
  STORE_Extra extra;
  extra.recovery_id = recovery_id;
  extra.recovery_epoch = recovery_epoch;
  return extra;
}

class MockStoreStorageTask : public StoreStorageTask {
 public:
  MockStoreStorageTask(size_t idx,
                       logid_t log_id,
                       lsn_t lsn,
                       epoch_t recovery_epoch,
                       LogStorageStateMap* state_map,
                       bool drain = false,
                       uint32_t timeout_ms = 0,
                       std::chrono::steady_clock::time_point start_time =
                           std::chrono::steady_clock::now())
      : StoreStorageTask(create_header(log_id,
                                       lsn,
                                       getFlags(recovery_epoch, drain),
                                       timeout_ms),
                         &chain_link,
                         LSN_INVALID,
                         std::map<KeyType, std::string>(),
                         std::make_shared<PayloadHolder>(nullptr, 0),
                         create_extra(recovery_id_t(1), recovery_epoch),
                         ClientID(),
                         start_time,
                         Durability::ASYNC_WRITE,
                         false /* write_find_time_index */,
                         false /* merge_mutable_per_epoch_log_metadata */,
                         false /* write_shard_id_in_copyset */),
        idx_(idx),
        state_map_(state_map),
        stats_(StatsParams().setIsServer(true)) {}

  size_t idx_;
  LogStorageStateMap* state_map_;

 protected:
  LogStorageStateMap* getLogStateMap() const override {
    return state_map_;
  }

  StatsHolder* stats() override {
    return &stats_;
  }

  shard_index_t getShardIdx() const override {
    return THIS_SHARD;
  }

 private:
  static STORE_flags_t getFlags(epoch_t recovery_epoch, bool drain) {
    STORE_flags_t flags = 0;
    if (recovery_epoch != EPOCH_INVALID) {
      flags |= STORE_Header::RECOVERY;
    }
    if (drain) {
      flags |= STORE_Header::DRAINING;
    }
    return flags;
  }

  StatsHolder stats_;
};

class MockWriteBatchStorageTask : public WriteBatchStorageTask {
 public:
  MockWriteBatchStorageTask()
      : WriteBatchStorageTask(ThreadType::FAST_TIME_SENSITIVE),
        stats_(StatsParams().setIsServer(true)) {}

  void add(std::unique_ptr<WriteStorageTask> task) {
    queue_.push(std::move(task));
  }

  std::vector<std::unique_ptr<WriteStorageTask>> completed_;

 protected:
  size_t getWriteBatchSize() const override {
    return 32;
  }

  size_t getWriteBatchBytes() const override {
    return 32768;
  }

  StatsHolder* stats() override {
    return &stats_;
  }

  void sendBackToWorker(std::unique_ptr<WriteStorageTask> task) override {
    completed_.push_back(std::move(task));
  }
  void
  sendDroppedToWorker(std::unique_ptr<WriteStorageTask> /*task*/) override {}

  int writeMulti(const std::vector<const WriteOp*>& /*unused*/) override {
    return 0;
  }

  bool throttleIfNeeded() override {
    return false;
  }

  std::unique_ptr<WriteStorageTask> tryGetWrite() override {
    if (queue_.empty()) {
      return nullptr;
    }

    std::unique_ptr<WriteStorageTask> task = std::move(queue_.front());
    queue_.pop();
    return task;
  }

  folly::small_vector<std::unique_ptr<WriteStorageTask>, 4>
  tryGetWriteBatch(size_t max_size, size_t max_bytes) override {
    folly::small_vector<std::unique_ptr<WriteStorageTask>, 4> res;
    size_t res_bytes = 0;
    while (res.size() < max_size && res_bytes < max_bytes) {
      auto ptr = tryGetWrite();
      if (!ptr) {
        break;
      }
      res_bytes += ptr->getPayloadSize();
      res.push_back(std::move(ptr));
    }
    return res;
  }

 private:
  std::queue<std::unique_ptr<WriteStorageTask>> queue_;
  StatsHolder stats_;
};

size_t get_task_idx(WriteStorageTask* task) {
  return dynamic_cast<MockStoreStorageTask*>(task)->idx_;
}

// Simulate processing a few STORE message. Make sure that stores for sealed
// epochs fail.
TEST(StoreStorageTaskTest, StoreRecords) {
  LogStorageStateMap map(1);
  MockWriteBatchStorageTask write_task;

  write_task.add(std::make_unique<MockStoreStorageTask>(
      1, logid_t(1), compose_lsn(epoch_t(1), esn_t(1)), EPOCH_INVALID, &map));
  write_task.add(std::make_unique<MockStoreStorageTask>(
      2, logid_t(1), compose_lsn(epoch_t(2), esn_t(1)), EPOCH_INVALID, &map));
  write_task.add(std::make_unique<MockStoreStorageTask>(
      3, logid_t(2), compose_lsn(epoch_t(1), esn_t(1)), EPOCH_INVALID, &map));
  write_task.add(std::make_unique<MockStoreStorageTask>(
      4,
      logid_t(2),
      compose_lsn(epoch_t(1), esn_t(1)),
      EPOCH_INVALID,
      &map,
      false,
      1,
      std::chrono::steady_clock::now() - std::chrono::seconds(1)));

  // epoch 1 of log 1 was sealed
  map.insertOrGet(logid_t(1), THIS_SHARD)
      ->updateSeal(
          Seal(epoch_t(1), NodeID(0, 1)), LogStorageState::SealType::NORMAL);
  // soft seals must also be populated at the time of store
  map.insertOrGet(logid_t(1), THIS_SHARD)
      ->updateSeal(Seal(), LogStorageState::SealType::SOFT);
  map.insertOrGet(logid_t(2), THIS_SHARD)
      ->updateSeal(Seal(), LogStorageState::SealType::NORMAL);
  map.insertOrGet(logid_t(2), THIS_SHARD)
      ->updateSeal(Seal(), LogStorageState::SealType::SOFT);

  write_task.execute();
  ASSERT_EQ(4, write_task.completed_.size());

  std::vector<size_t> preempted;
  std::vector<size_t> successful;
  std::vector<size_t> timedout;

  for (auto& it : write_task.completed_) {
    if (it->status_ == E::OK) {
      successful.push_back(get_task_idx(it.get()));
    } else if (it->status_ == E::PREEMPTED) {
      ASSERT_TRUE(it->seal_.seq_node.isNodeID());
      preempted.push_back(get_task_idx(it.get()));
    } else if (it->status_ == E::TIMEDOUT) {
      timedout.push_back(get_task_idx(it.get()));
    }
  }

  EXPECT_EQ(std::vector<size_t>({1}), preempted);
  EXPECT_EQ(std::vector<size_t>({2, 3}), successful);
  EXPECT_EQ(std::vector<size_t>({4}), timedout);
}

// Mutations from recovery should fail if another sequencer starts recovery for
// the same log and is finished sealing.
TEST(StoreStorageTaskTest, RecoveryMutation) {
  LogStorageStateMap map(1);
  MockWriteBatchStorageTask write_task;

  // A sequencer seals epoch 1, then performs a mutation in it
  map.insertOrGet(logid_t(1), THIS_SHARD)
      ->updateSeal(
          Seal(epoch_t(1), NodeID(0, 1)), LogStorageState::SealType::NORMAL);
  // populate a soft seal
  map.insertOrGet(logid_t(1), THIS_SHARD)
      ->updateSeal(Seal(), LogStorageState::SealType::SOFT);
  write_task.add(std::make_unique<MockStoreStorageTask>(
      1, logid_t(1), compose_lsn(epoch_t(1), esn_t(1)), epoch_t(1), &map));

  write_task.execute();
  ASSERT_EQ(1, write_task.completed_.size());
  EXPECT_EQ(E::OK, write_task.completed_[0]->status_);

  // Another sequencer seals epoch 2. Mutation from the first sequencer in
  // epoch 1 fails.
  map.insertOrGet(logid_t(1), THIS_SHARD)
      ->updateSeal(
          Seal(epoch_t(2), NodeID(1, 1)), LogStorageState::SealType::NORMAL);
  write_task.add(std::make_unique<MockStoreStorageTask>(
      2, logid_t(1), compose_lsn(epoch_t(1), esn_t(2)), epoch_t(1), &map));

  write_task.completed_.clear();
  write_task.execute();
  ASSERT_EQ(1, write_task.completed_.size());
  EXPECT_EQ(E::PREEMPTED, write_task.completed_[0]->status_);
  EXPECT_EQ(NodeID(1, 1), write_task.completed_[0]->seal_.seq_node);
}

TEST(StoreStorageTaskTest, SoftSeals) {
  LogStorageStateMap map(1);
  MockWriteBatchStorageTask write_task;

  // log 1 has soft seal to epoch 3 from N1, normal seal to epoch 2 from N2
  // write 1 has lsn (1, 1)                    ->   preempted  N1
  // write 2 has lsn (2, 1) w/ DRAINING flag   ->   preempted  N2
  // write 3 has lsn (3, 1)                    ->   preempted soft only N1
  // write 4 has lsn (3, 1) w/ DRAINING flag   -> not preempted
  // write 5 has lsn (4, 1)                    -> not preempted
  // write 6 has lsn (4, 1) w/ DRAINING flag   -> not preempted

  write_task.add(std::make_unique<MockStoreStorageTask>(
      1, logid_t(1), compose_lsn(epoch_t(1), esn_t(1)), EPOCH_INVALID, &map));
  write_task.add(
      std::make_unique<MockStoreStorageTask>(2,
                                             logid_t(1),
                                             compose_lsn(epoch_t(2), esn_t(1)),
                                             EPOCH_INVALID,
                                             &map,
                                             /* drain= */ true));
  write_task.add(std::make_unique<MockStoreStorageTask>(
      3, logid_t(1), compose_lsn(epoch_t(3), esn_t(1)), EPOCH_INVALID, &map));
  write_task.add(
      std::make_unique<MockStoreStorageTask>(4,
                                             logid_t(1),
                                             compose_lsn(epoch_t(3), esn_t(1)),
                                             EPOCH_INVALID,
                                             &map,
                                             /* drain= */ true));
  write_task.add(std::make_unique<MockStoreStorageTask>(
      5, logid_t(1), compose_lsn(epoch_t(4), esn_t(1)), EPOCH_INVALID, &map));
  write_task.add(
      std::make_unique<MockStoreStorageTask>(6,
                                             logid_t(1),
                                             compose_lsn(epoch_t(4), esn_t(1)),
                                             EPOCH_INVALID,
                                             &map,
                                             /* drain= */ true));

  // epoch 1 of log 1 was sealed
  map.insertOrGet(logid_t(1), THIS_SHARD)
      ->updateSeal(
          Seal(epoch_t(2), NodeID(2, 1)), LogStorageState::SealType::NORMAL);
  map.insertOrGet(logid_t(1), THIS_SHARD)
      ->updateSeal(
          Seal(epoch_t(3), NodeID(1, 1)), LogStorageState::SealType::SOFT);
  write_task.execute();
  ASSERT_EQ(6, write_task.completed_.size());

  std::vector<size_t> preempted_normal;
  std::vector<size_t> preempted_soft_only;
  std::vector<size_t> successful;
  std::map<int, std::vector<size_t>> redirect_to;

  for (auto& it : write_task.completed_) {
    if (it->status_ == E::OK) {
      successful.push_back(get_task_idx(it.get()));
    } else if (it->status_ == E::PREEMPTED) {
      ASSERT_TRUE(it->seal_.seq_node.isNodeID());
      redirect_to[it->seal_.seq_node.index()].push_back(get_task_idx(it.get()));
      if (static_cast<StoreStorageTask*>(it.get())
              ->isPreemptedBySoftSealOnly()) {
        preempted_soft_only.push_back(get_task_idx(it.get()));
      } else {
        preempted_normal.push_back(get_task_idx(it.get()));
      }
    } else {
      FAIL();
    }
  }

  EXPECT_EQ(std::vector<size_t>({1, 2}), preempted_normal);
  EXPECT_EQ(std::vector<size_t>({3}), preempted_soft_only);
  EXPECT_EQ(std::vector<size_t>({4, 5, 6}), successful);
  EXPECT_EQ(std::vector<size_t>({1, 3}), redirect_to[1]);
  EXPECT_EQ(std::vector<size_t>({2}), redirect_to[2]);
}

TEST(StoreStorageTaskTest, ValidBuffers) {
  LogStorageStateMap map(1);
  MockStoreStorageTask task(
      0, logid_t(1), compose_lsn(epoch_t(1), esn_t(1)), epoch_t(555), &map);

  const std::string& record_buf = task.getRecordHeaderBuf();
  Slice record_blob(record_buf.data(), record_buf.size());
  std::chrono::milliseconds ts{100};
  esn_t lng{100};
  LocalLogStoreRecordFormat::flags_t flags{100};
  uint32_t wave{100};
  copyset_size_t copyset_size{100};
  std::array<ShardID, 100> copyset_arr;
  Payload payload{reinterpret_cast<void*>(0xff), 5};

  int rv = LocalLogStoreRecordFormat::parse(record_blob,
                                            &ts,
                                            &lng,
                                            &flags,
                                            &wave,
                                            &copyset_size,
                                            copyset_arr.data(),
                                            copyset_arr.size(),
                                            /*offsets_within_epoch*/ nullptr,
                                            /*optional_keys*/ nullptr,
                                            &payload,
                                            THIS_SHARD);
  ASSERT_EQ(rv, 0);
  ASSERT_EQ(ts, std::chrono::milliseconds(0));
  ASSERT_EQ(lng, esn_t(0));
  ASSERT_EQ(flags, LocalLogStoreRecordFormat::FLAG_WRITTEN_BY_RECOVERY);
  ASSERT_EQ(wave, 555);
  ASSERT_EQ(copyset_size, 1);
  ASSERT_EQ(chain_link.destination, copyset_arr[0]);
  ASSERT_EQ(payload.data(), nullptr);
  ASSERT_EQ(payload.size(), 0);

  const std::string& csi_buf = task.getCopySetIndexEntryBuf();
  Slice csi_blob(csi_buf.data(), csi_buf.size());

  std::vector<ShardID> csi_copyset = {ShardID(1, 15), ShardID(2, 3)};
  uint32_t csi_wave{100};
  LocalLogStoreRecordFormat::csi_flags_t csi_flags{0xff};

  bool bool_rv = LocalLogStoreRecordFormat::parseCopySetIndexSingleEntry(
      csi_blob, &csi_copyset, &csi_wave, &csi_flags, THIS_SHARD);
  ASSERT_TRUE(bool_rv);
  ASSERT_EQ(csi_wave, 555);
  ASSERT_EQ(csi_flags, LocalLogStoreRecordFormat::CSI_FLAG_WRITTEN_BY_RECOVERY);
  ASSERT_EQ(csi_copyset, std::vector<ShardID>{chain_link.destination});
}

}} // namespace facebook::logdevice
