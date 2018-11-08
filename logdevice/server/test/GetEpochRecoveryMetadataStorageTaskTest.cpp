/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/server/GetEpochRecoveryMetadataStorageTask.h"

#include <gtest/gtest.h>

#include "logdevice/common/Metadata.h"
#include "logdevice/common/protocol/GET_EPOCH_RECOVERY_METADATA_Message.h"
#include "logdevice/common/protocol/GET_EPOCH_RECOVERY_METADATA_REPLY_Message.h"
#include "logdevice/server/locallogstore/test/StoreUtil.h"
#include "logdevice/server/locallogstore/test/TemporaryLogStore.h"
#include "logdevice/server/read_path/LogStorageStateMap.h"

using namespace facebook::logdevice;

namespace {

const logid_t LOG_ID(222);
const epoch_t PURGE_TO(1677777);
const epoch_t START_EPOCH(19);
const epoch_t END_EPOCH(19);

GetEpochRecoveryMetadataStorageTask create_task(logid_t log_id,
                                                epoch_t start_epoch,
                                                epoch_t end_epoch) {
  return GetEpochRecoveryMetadataStorageTask(
      GET_EPOCH_RECOVERY_METADATA_Header{
          log_id, PURGE_TO, start_epoch, 0, 0, 0, end_epoch, request_id_t(1)},
      Address(NodeID()),
      0);
}

GetEpochRecoveryMetadataStorageTask create_task() {
  return create_task(LOG_ID, START_EPOCH, END_EPOCH);
}

TEST(GetEpochRecoveryMetadataStorageTask, EmptyLogButUnclean) {
  TemporaryRocksDBStore store;
  LogStorageStateMap map(1);

  auto task = create_task();
  EXPECT_EQ(E::NOTREADY, task.executeImpl(store, map));
  EXPECT_TRUE(task.getEpochRecoveryStateMap().empty());
}

TEST(GetEpochRecoveryMetadataStorageTask, EmptyLogWhileClean) {
  TemporaryRocksDBStore store;
  LogStorageStateMap map(1);

  store.writeLogMetadata(
      LOG_ID, LastCleanMetadata(START_EPOCH), LocalLogStore::WriteOptions());
  auto task = create_task();
  EXPECT_EQ(E::OK, task.executeImpl(store, map));
  EXPECT_TRUE(!task.getEpochRecoveryStateMap().empty());
  EXPECT_EQ(1, task.getEpochRecoveryStateMap().count(START_EPOCH.val_));
  EXPECT_EQ(
      E::EMPTY, task.getEpochRecoveryStateMap().at(START_EPOCH.val_).first);
  EXPECT_TRUE(
      !task.getEpochRecoveryStateMap().at(START_EPOCH.val_).second.valid());

  auto lce = map.get(LOG_ID, /*shard_idx=*/0).getLastCleanEpoch();
  // map should also be populated
  EXPECT_TRUE(lce.hasValue());
  EXPECT_EQ(START_EPOCH, lce.value());
}

TEST(GetEpochRecoveryMetadataStorageTask, RangeBelowTrimPoint) {
  TemporaryRocksDBStore store;
  LogStorageStateMap map(1);
  store.writeLogMetadata(
      LOG_ID, LastCleanMetadata(epoch_t(70)), LocalLogStore::WriteOptions());
  store.writeLogMetadata(LOG_ID,
                         TrimMetadata(compose_lsn(epoch_t(50), ESN_INVALID)),
                         LocalLogStore::WriteOptions());
  auto task = create_task(LOG_ID, epoch_t(5), epoch_t(7));
  EXPECT_EQ(E::EMPTY, task.executeImpl(store, map));
  EXPECT_TRUE(task.getEpochRecoveryStateMap().empty());
}

TEST(GetEpochRecoveryMetadataStorageTask, RangeAbovLastCleanEpoch) {
  TemporaryRocksDBStore store;
  LogStorageStateMap map(1);
  store.writeLogMetadata(
      LOG_ID, LastCleanMetadata(epoch_t(2)), LocalLogStore::WriteOptions());
  auto task = create_task(LOG_ID, epoch_t(5), epoch_t(7));
  EXPECT_EQ(E::NOTREADY, task.executeImpl(store, map));
  EXPECT_TRUE(task.getEpochRecoveryStateMap().empty());
}

TEST(GetEpochRecoveryMetadataStorageTask, GetMetadata) {
  TemporaryRocksDBStore store;
  LogStorageStateMap map(1);

  // lce = epoch 10
  store.writeLogMetadata(
      LOG_ID, LastCleanMetadata(epoch_t(10)), LocalLogStore::WriteOptions());

  // trimmed epoch - 1
  store.writeLogMetadata(LOG_ID,
                         TrimMetadata(compose_lsn(epoch_t(1), ESN_INVALID)),
                         LocalLogStore::WriteOptions());

  // empty epochs 1(trimmed),3,5,7,9
  // non empty epochs  2, 4, 6, 8, 10
  std::vector<EpochRecoveryMetadata> non_empty_epoch;
  epoch_t start = epoch_t(1);
  epoch_t end = epoch_t(20);
  OffsetMap epoch_size_map;
  epoch_size_map.setCounter(BYTE_OFFSET, 200);
  TailRecord tail_record;
  tail_record.offsets_map_.setCounter(BYTE_OFFSET, 100);
  OffsetMap epoch_end_offsets;
  epoch_end_offsets.setCounter(BYTE_OFFSET, 100);
  for (auto epoch = 2; epoch <= 10; epoch++) {
    if (epoch % 2 == 0) {
      EpochRecoveryMetadata erm(epoch_t(epoch),
                                esn_t(2),
                                esn_t(4),
                                0,
                                tail_record,
                                epoch_size_map,
                                epoch_end_offsets);
      non_empty_epoch.push_back(erm);
      int rv =
          store.updatePerEpochLogMetadata(LOG_ID,
                                          epoch_t(epoch),
                                          erm,
                                          LocalLogStore::SealPreemption::ENABLE,
                                          LocalLogStore::WriteOptions());
      EXPECT_EQ(0, rv);
    }
  }

  auto task = create_task(LOG_ID, start, end);
  EXPECT_EQ(E::OK, task.executeImpl(store, map));
  EXPECT_TRUE(!task.getEpochRecoveryStateMap().empty());
  EXPECT_EQ(20, task.getEpochRecoveryStateMap().size());
  auto it = non_empty_epoch.begin();
  for (auto epoch = start.val(); epoch <= end.val(); epoch++) {
    if (epoch <= 10 && epoch % 2 == 0) {
      // Non empty, clean epochs
      EXPECT_EQ(E::OK, task.getEpochRecoveryStateMap().at(epoch).first);
      EXPECT_TRUE(task.getEpochRecoveryStateMap().at(epoch).second.valid());
      EXPECT_EQ(*it, task.getEpochRecoveryStateMap().at(epoch).second);
      it = non_empty_epoch.erase(it);
      continue;
    }
    if (epoch <= 10 && epoch % 2 != 0) {
      // Empty clean epochs
      EXPECT_EQ(E::EMPTY, task.getEpochRecoveryStateMap().at(epoch).first);
      EXPECT_TRUE(!task.getEpochRecoveryStateMap().at(epoch).second.valid());
      continue;
    }

    EXPECT_EQ(E::NOTREADY, task.getEpochRecoveryStateMap().at(epoch).first);
    EXPECT_TRUE(!task.getEpochRecoveryStateMap().at(epoch).second.valid());
  }
  EXPECT_TRUE(non_empty_epoch.empty());
}
} // namespace
