/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/server/read_path/LocalLogStoreReader.h"

#include <folly/Memory.h>
#include <folly/Random.h>
#include <gtest/gtest.h>

#include "logdevice/common/LocalLogStoreRecordFormat.h"
#include "logdevice/common/MetaDataLog.h"
#include "logdevice/common/debug.h"
#include "logdevice/common/protocol/RECORD_Message.h"
#include "logdevice/common/protocol/STORE_Message.h"
#include "logdevice/common/test/TestUtil.h"
#include "logdevice/common/util.h"
#include "logdevice/server/locallogstore/LocalLogStore.h"
#include "logdevice/server/locallogstore/WriteOps.h"
#include "logdevice/server/locallogstore/test/LocalLogStoreTestReader.h"
#include "logdevice/server/locallogstore/test/StoreUtil.h"
#include "logdevice/server/locallogstore/test/TemporaryLogStore.h"

using namespace facebook::logdevice;
using LocalLogStoreReader::getTailRecord;
using LocalLogStoreReader::ReadPointer;

#define LOGID \
  logid_t {   \
    1         \
  }

// Useful shortcuts for writing ShardIDs.
#define N0 ShardID(0, 0)
#define N1 ShardID(1, 0)
#define N2 ShardID(2, 0)
#define N3 ShardID(3, 0)
#define N4 ShardID(4, 0)
#define N5 ShardID(5, 0)

#define CONFIG_PATH \
  verifyFileExists("logdevice/server/test/configs/local_scd_test.conf").c_str()

namespace {

struct RecordDescriptor {
  lsn_t lsn;
  uint32_t wave;
  std::vector<ShardID> copyset;
  STORE_flags_t flags = 0;
  size_t extra_payload_size = 0;
  DataKeyFormat key_format = DataKeyFormat::DEFAULT;
};

} // namespace

enum Options { WAVE_IN_VALUE = 0, WRITE_CSI = 1, SHARD_ID_IN_COPYSET = 2 };

using LLSFilter = LocalLogStoreReadFilter;

class MockLocalLogStoreReadFilter : public LocalLogStoreReadFilter {
 public:
  explicit MockLocalLogStoreReadFilter(std::shared_ptr<ServerConfig> config)
      : LocalLogStoreReadFilter(), config_(config) {}

  std::shared_ptr<const NodesConfiguration>
  getNodesConfiguration() const override {
    return config_->getNodesConfigurationFromServerConfigSource();
  }

 private:
  std::shared_ptr<ServerConfig> config_;
};

class LocalLogStoreReaderTest : public ::testing::TestWithParam<Options> {
 public:
  LocalLogStoreReaderTest() {
    dbg::currentLevel = getLogLevelFromEnv().value_or(dbg::Level::INFO);
    dbg::assertOnData = true;
    Options type = GetParam();
    csi_enabled_ = type >= WRITE_CSI;
    shard_id_in_copyset_ = type >= SHARD_ID_IN_COPYSET;
  }

 protected:
  bool shardIDInCopyset() const {
    return shard_id_in_copyset_;
  }

  bool useCSI() const {
    return csi_enabled_;
  }

  bool csi_enabled_{false};
  bool shard_id_in_copyset_{false};

  Slice formRecordHeader(const RecordDescriptor& rec, std::string* buf) {
    STORE_Header hdr;
    hdr.rid = RecordID(rec.lsn, LOGID_INVALID);
    hdr.timestamp = 0;
    hdr.last_known_good = esn_t(0);
    hdr.wave = rec.wave;
    hdr.flags = rec.flags | STORE_Header::CHECKSUM_PARITY;
    hdr.copyset_size = rec.copyset.size();
    std::vector<StoreChainLink> chain;
    for (ShardID shard : rec.copyset) {
      chain.push_back(StoreChainLink{shard, ClientID::INVALID});
    }
    return LocalLogStoreRecordFormat::formRecordHeader(
        hdr,
        chain.data(),
        buf,
        shardIDInCopyset(),
        std::map<KeyType, std::string>());
  }

  Slice formCopySetIndexEntry(const RecordDescriptor& rec, std::string* buf) {
    auto flags = LocalLogStoreRecordFormat::formCopySetIndexFlags(rec.flags);
    if (shardIDInCopyset()) {
      flags |= LocalLogStoreRecordFormat::CSI_FLAG_SHARD_ID;
    }
    return LocalLogStoreRecordFormat::formCopySetIndexEntry(rec.wave,
                                                            rec.copyset.data(),
                                                            rec.copyset.size(),
                                                            LSN_INVALID,
                                                            flags,
                                                            buf);
  }

  // Create a temporary store with initial data on it for one log.
  std::unique_ptr<TemporaryRocksDBStore>
  createStore(std::vector<RecordDescriptor> data) {
    std::unique_ptr<TemporaryRocksDBStore> store =
        std::make_unique<TemporaryRocksDBStore>();

    std::vector<std::string> payloads(data.size());
    std::vector<std::string> copyset_index_entries(data.size());
    std::vector<PutWriteOp> put_ops;
    for (int i = 0; i < data.size(); ++i) {
      Slice s = formRecordHeader(data[i], &payloads[i]);
      if (data[i].extra_payload_size > 0) {
        payloads[i].append(data[i].extra_payload_size, 'x');
        s = Slice(payloads[i].data(), payloads[i].size());
      }
      put_ops.emplace_back(LOGID, data[i].lsn, s);
      Slice csi = formCopySetIndexEntry(data[i], &copyset_index_entries[i]);
      put_ops.back().copyset_index_entry = csi;
      put_ops.back().copyset_index_lsn = LSN_INVALID;
      put_ops.back().TEST_data_key_format = data[i].key_format;
    }

    std::vector<const WriteOp*> ops;
    for (auto& x : put_ops) {
      ops.push_back(&x);
    }

    EXPECT_EQ(0, store->writeMulti(ops));

    return store;
  }
};

#define ASSERT_SHIPPED(records, ...)           \
  {                                            \
    std::vector<lsn_t> expected{__VA_ARGS__};  \
    std::vector<lsn_t> got;                    \
    for (int i = 0; i < records.size(); ++i) { \
      got.push_back(records[i].lsn);           \
    }                                          \
    ASSERT_EQ(expected, got);                  \
  }

using ReadOperation = test::LocalLogStoreTestReader;

/******************************************************************************
 ******************************************************************************
 ******************************************************************************/

TEST_P(LocalLogStoreReaderTest, UntilLsnReached) {
  auto store = createStore({{1, 1, {N1, N2, N3, N4}},
                            {2, 1, {N1, N2, N3, N4}},
                            {3, 1, {N1, N2, N3, N4}},
                            {4, 1, {N1, N2, N3, N4}}});

  std::vector<RawRecord> records;
  const Status st = ReadOperation()
                        .use_csi(useCSI())
                        .until_lsn(3)
                        .window_high(3)
                        .last_released(4)
                        .process(store.get(), records);

  ASSERT_EQ(E::UNTIL_LSN_REACHED, st);
  ASSERT_SHIPPED(records, 1, 2, 3);
}

TEST_P(LocalLogStoreReaderTest, CaughtUp) {
  auto store = createStore({{1, 1, {N1, N2, N3, N4}},
                            {2, 1, {N1, N2, N3, N4}},
                            {3, 1, {N1, N2, N3, N4}},
                            {4, 1, {N1, N2, N3, N4}}});

  std::vector<RawRecord> records;
  const Status st = ReadOperation()
                        .use_csi(useCSI())
                        .until_lsn(3)
                        .window_high(3)
                        .last_released(2)
                        .process(store.get(), records);

  ASSERT_EQ(E::CAUGHT_UP, st);
  ASSERT_SHIPPED(records, 1, 2);
}

TEST_P(LocalLogStoreReaderTest, WindowEndReached) {
  auto store = createStore({{1, 1, {N1, N2, N3, N4}},
                            {2, 1, {N1, N2, N3, N4}},
                            {3, 1, {N1, N2, N3, N4}},
                            {4, 1, {N1, N2, N3, N4}}});

  std::vector<RawRecord> records;
  const Status st = ReadOperation()
                        .use_csi(useCSI())
                        .until_lsn(3)
                        .window_high(2)
                        .last_released(4)
                        .process(store.get(), records);

  ASSERT_EQ(E::WINDOW_END_REACHED, st);
  ASSERT_SHIPPED(records, 1, 2);
}

// Verify that if last_released_lsn < until_lsn and we are at the end, we get
// CAUGHT_UP.
TEST_P(LocalLogStoreReaderTest, AtEndCaughtUp) {
  auto store = createStore({{1, 1, {N1, N2, N3, N4}},
                            {2, 1, {N1, N2, N3, N4}},
                            {3, 1, {N1, N2, N3, N4}},
                            {4, 1, {N1, N2, N3, N4}}});

  std::vector<RawRecord> records;
  const Status st = ReadOperation()
                        .use_csi(useCSI())
                        .until_lsn(42)
                        .window_high(4)
                        .last_released(4)
                        .process(store.get(), records);

  ASSERT_EQ(E::CAUGHT_UP, st);
  ASSERT_SHIPPED(records, 1, 2, 3, 4);
}

// Verify that if last_released_lsn >= until_lsn and we are at the end, we get
// UNTIL_LSN_REACHED.
TEST_P(LocalLogStoreReaderTest, AtEndUntilLsnReached) {
  auto store = createStore({{1, 1, {N1, N2, N3, N4}},
                            {2, 1, {N1, N2, N3, N4}},
                            {3, 1, {N1, N2, N3, N4}},
                            {4, 1, {N1, N2, N3, N4}}});

  std::vector<RawRecord> records;
  const Status st = ReadOperation()
                        .use_csi(useCSI())
                        .until_lsn(4)
                        .window_high(4)
                        .last_released(4)
                        .process(store.get(), records);

  ASSERT_EQ(E::UNTIL_LSN_REACHED, st);
  ASSERT_SHIPPED(records, 1, 2, 3, 4);
}

// Verify we do get one recod byte no matter its size if first_record_any_size
// is set to true.
TEST_P(LocalLogStoreReaderTest, SmallByteLimitFirstRecordAnySize) {
  auto store = createStore({{1, 1, {N1, N2, N3, N4}},
                            {2, 1, {N1, N2, N3, N4}},
                            {3, 1, {N1, N2, N3, N4}},
                            {4, 1, {N1, N2, N3, N4}}});

  std::vector<RawRecord> records;
  const Status st = ReadOperation()
                        .use_csi(useCSI())
                        .until_lsn(4)
                        .window_high(4)
                        .last_released(4)
                        .max_bytes_all_records(1)
                        .first_record_any_size(true)
                        .process(store.get(), records);

  ASSERT_EQ(E::BYTE_LIMIT_REACHED, st);
  ASSERT_SHIPPED(records, 1);
}

// Same as the test before but this time we also have a limit on the number of
// bytes to read that's smaller than the size of the first record.
TEST_P(LocalLogStoreReaderTest, MaxBytesToReadAndFirstRecordAnySize) {
  auto store = createStore({{1, 1, {N1, N2, N3, N4}},
                            {2, 1, {N1, N2, N3, N4}},
                            {3, 1, {N1, N2, N3, N4}},
                            {4, 1, {N1, N2, N3, N4}}});

  std::vector<RawRecord> records;
  const Status st = ReadOperation()
                        .use_csi(useCSI())
                        .until_lsn(4)
                        .window_high(4)
                        .last_released(4)
                        .max_bytes_all_records(1)
                        .max_bytes_to_read(1)
                        .first_record_any_size(true)
                        .process(store.get(), records);

  EXPECT_EQ(E::PARTIAL, st);
  ASSERT_SHIPPED(records, 1);
}

// Verify that we get E::BYTE_LIMIT_REACHED if max_bytes_all_records is zero and
// first_record_any_size is false.
TEST_P(LocalLogStoreReaderTest, ZeroByteLimit_) {
  auto store = createStore({{1, 1, {N1, N2, N3, N4}},
                            {2, 1, {N1, N2, N3, N4}},
                            {3, 1, {N1, N2, N3, N4}},
                            {4, 1, {N1, N2, N3, N4}}});

  std::vector<RawRecord> records;
  const Status st = ReadOperation()
                        .use_csi(useCSI())
                        .until_lsn(4)
                        .window_high(4)
                        .last_released(4)
                        .max_bytes_all_records(0)
                        .first_record_any_size(false)
                        .process(store.get(), records);

  ASSERT_EQ(E::BYTE_LIMIT_REACHED, st);
  ASSERT_EQ(0, records.size());
}

// Verify that we get E::TIME_LIMIT_REACHED if max_record_read_execution_time is
// zero.
TEST_P(LocalLogStoreReaderTest, ZeroExecutionTime_) {
  auto store = createStore({{1, 1, {N1, N2, N3, N4}},
                            {2, 1, {N1, N2, N3, N4}},
                            {3, 1, {N1, N2, N3, N4}},
                            {4, 1, {N1, N2, N3, N4}}});

  std::vector<RawRecord> records;
  const Status st = ReadOperation()
                        .use_csi(useCSI())
                        .until_lsn(4)
                        .window_high(4)
                        .last_released(4)
                        .max_execution_time(std::chrono::milliseconds::zero())
                        .process(store.get(), records);

  ASSERT_EQ(E::PARTIAL, st);
  ASSERT_EQ(1, records.size());
}
/**
 * If the local log store contains an old-format and a new-format DataKey with
 * the same log ID and LSN, LocalLogStoreReader only delivers the copy with the
 * new-format key.
 */
TEST_P(LocalLogStoreReaderTest, TwoRecordsWithSameLSN) {
  auto store = createStore({{1, 1, {N1, N2, N3, N4}},
                            {2, 1, {N1, N2, N3, N4}, 0, 0, DataKeyFormat::OLD},
                            {2, 2, {N1, N2, N3, N4}, 0, 0, DataKeyFormat::NEW},
                            {3, 1, {N1, N2, N3, N4}}});

  std::vector<RawRecord> records;
  const Status st = ReadOperation()
                        .use_csi(useCSI())
                        .until_lsn(4)
                        .window_high(4)
                        .last_released(3)
                        .process(store.get(), records);

  ASSERT_EQ(E::CAUGHT_UP, st);
  ASSERT_SHIPPED(records, 1, 2, 3);
}

// Check we correctly filter out records that do not have
// required_node_in_copyset_ in the copyset.
TEST_P(LocalLogStoreReaderTest, CopysetFiltering) {
  // Suppose we have two records, one with a copyset {1, 3} and another with
  // {1, 2}.  Only the second one should be sent over the wire because we are
  // looking for records that have node index 2 in the copyset.
  auto store = createStore({{1, 1, {N1, N3}}, {2, 1, {N1, N2}}});

  auto filter = std::make_shared<LLSFilter>();
  filter->required_in_copyset_.push_back(N2);
  std::vector<RawRecord> records;
  ReadPointer read_ptr;
  const Status st = ReadOperation()
                        .use_csi(useCSI())
                        .until_lsn(4)
                        .window_high(4)
                        .last_released(3)
                        .filter(filter)
                        .process(store.get(), records, &read_ptr);

  ASSERT_EQ(E::CAUGHT_UP, st);
  ASSERT_SHIPPED(records, 2);
  // Since there is no record with lsn 3 in the db and last_released is 3, the
  // read ptr should be advanced to lsn 4.
  ASSERT_EQ(4, read_ptr.lsn);
}

// A basic test where we check that records are filtered properly when scd is in
// use.
TEST_P(LocalLogStoreReaderTest, SingleCopyDeliverySimple) {
  auto store = createStore({{1, 1, {N1, N2, N3, N4, N5}},
                            {2, 1, {N2, N5, N0, N4, N3}},
                            {3, 1, {N3, N4, N1, N5, N2}},
                            {4, 1, {N0, N3, N1, N5, N2}},
                            {5, 1, {N5, N0, N4, N2, N3}},
                            {6, 1, {N0, N3, N2, N4, N1}}});

  auto filter = std::make_shared<LLSFilter>();
  filter->scd_my_shard_id_ = N0;

  ReadPointer read_ptr;
  std::vector<RawRecord> records;
  const Status st = ReadOperation()
                        .use_csi(useCSI())
                        .until_lsn(7)
                        .window_high(7)
                        .last_released(6)
                        .filter(filter)
                        .process(store.get(), records, &read_ptr);

  ASSERT_EQ(E::CAUGHT_UP, st);
  // Only 4 and 6 should be sent back.
  ASSERT_SHIPPED(records, 4, 6);
  ASSERT_EQ(7, read_ptr.lsn);
}

// Verify we ship nothing if none of the records in the batch have our node id
// as primary recpient.
TEST_P(LocalLogStoreReaderTest, SingleCopyDeliveryEmptyBatch) {
  auto store = createStore({{1, 1, {N1, N2, N3, N4, N5}},
                            {2, 1, {N2, N5, N0, N4, N3}},
                            {3, 1, {N3, N4, N1, N5, N2}},
                            {4, 1, {N1, N3, N1, N5, N2}},
                            {5, 1, {N5, N0, N4, N2, N3}},
                            {6, 1, {N1, N3, N2, N4, N1}}});

  auto filter = std::make_shared<LLSFilter>();
  filter->scd_my_shard_id_ = N0;

  ReadPointer read_ptr;
  std::vector<RawRecord> records;
  const Status st = ReadOperation()
                        .use_csi(useCSI())
                        .until_lsn(7)
                        .window_high(7)
                        .last_released(6)
                        .filter(filter)
                        .process(store.get(), records, &read_ptr);

  ASSERT_EQ(E::CAUGHT_UP, st);
  // Nothing should be sent.
  ASSERT_EQ(0, records.size());
  ASSERT_EQ(7, read_ptr.lsn);
}

TEST_P(LocalLogStoreReaderTest, SingleCopyDeliverySimpleOneKnownDown) {
  auto store = createStore({{1, 1, {N1, N0, N3, N4, N5}},
                            {2, 1, {N2, N5, N0, N4, N3}},
                            {3, 1, {N3, N4, N1, N5, N2}},
                            {4, 1, {N0, N3, N1, N5, N2}},
                            {5, 1, {N5, N0, N4, N2, N3}},
                            {6, 1, {N0, N3, N2, N4, N1}}});

  // SCD is enabled and 1 is down.
  auto filter = std::make_shared<LLSFilter>();
  filter->scd_my_shard_id_ = N0;
  filter->scd_known_down_ = {N1};

  ReadPointer read_ptr;
  std::vector<RawRecord> records;
  const Status st = ReadOperation()
                        .use_csi(useCSI())
                        .until_lsn(7)
                        .window_high(7)
                        .last_released(6)
                        .filter(filter)
                        .process(store.get(), records, &read_ptr);

  ASSERT_EQ(E::CAUGHT_UP, st);
  // lsns 1, 4, 6 should be sent back
  // 4 and 6 because we are the primary recipient for these records. 1 because
  // its primary recipient is in the known down list.
  ASSERT_SHIPPED(records, 1, 4, 6);
  ASSERT_EQ(7, read_ptr.lsn);
}

TEST_P(LocalLogStoreReaderTest, SingleCopyDeliveryOurselvesInKnownDown) {
  auto store = createStore({{1, 1, {N1, N0, N3, N4, N5}},
                            {2, 1, {N2, N5, N0, N4, N3}},
                            {3, 1, {N3, N4, N1, N5, N2}},
                            {4, 1, {N0, N3, N1, N5, N2}},
                            {5, 1, {N5, N0, N4, N2, N3}},
                            {6, 1, {N0, N3, N2, N4, N1}}});

  // Enable single copy delivery. We are ourselves in the known down list, which
  // can happen if the client thought we are down and we are back up.
  auto filter = std::make_shared<LLSFilter>();
  filter->scd_my_shard_id_ = N0;
  filter->scd_known_down_ = {N0, N5};

  ReadPointer read_ptr;
  std::vector<RawRecord> records;
  const Status st = ReadOperation()
                        .use_csi(useCSI())
                        .until_lsn(7)
                        .window_high(7)
                        .last_released(6)
                        .filter(filter)
                        .process(store.get(), records, &read_ptr);

  ASSERT_EQ(E::CAUGHT_UP, st);
  // 4, 5, 6 should be shipped. 4 and 6 because we are the primary recipient of
  // these records and 5 because its primary is in the known down list.
  ASSERT_SHIPPED(records, 4, 5, 6);
  ASSERT_EQ(7, read_ptr.lsn);
}

TEST_P(LocalLogStoreReaderTest, SingleCopyDeliveryRebuilding) {
  auto store = createStore({{1, 1, {N1, N0, N3, N4, N5}},
                            {2, 1, {N2, N5, N0, N1, N3}},
                            {3, 1, {N3, N4, N1, N5, N2}},
                            {4, 1, {N0, N3, N1, N5, N2}},
                            {5, 1, {N5, N0, N4, N2, N3}},
                            {6, 1, {N0, N3, N2, N5, N1}},
                            {7, 1, {N4, N0, N2, N5, N1}}});

  // Node 5 is in the known down list and we only records that have 4 in the
  // copyset. Note that 4 is also in the known down list because
  // required_in_copyset_ should always be in the known down list by
  // ServerReadStream.
  auto filter = std::make_shared<LLSFilter>();
  filter->scd_my_shard_id_ = N0;
  filter->required_in_copyset_.push_back(N4);
  filter->scd_known_down_ = {N5, N4};

  ReadPointer read_ptr;
  std::vector<RawRecord> records;
  const Status st = ReadOperation()
                        .use_csi(useCSI())
                        .until_lsn(8)
                        .window_high(8)
                        .last_released(7)
                        .filter(filter)
                        .process(store.get(), records, &read_ptr);

  ASSERT_EQ(E::CAUGHT_UP, st);
  // 5, 7 should be sent.
  ASSERT_SHIPPED(records, 5, 7);
  ASSERT_EQ(8, read_ptr.lsn);
}

// This tests an edge case where our own node index is not in the copyset and
// all nodes in the copyset are in the known down list. Here we just want to
// check that we don't crash and the record is delivered.
TEST_P(LocalLogStoreReaderTest, SingleCopyDeliveryFailCopyset) {
  auto store = createStore({{1, 1, {N1, N2, N3, N4, N5}}});

  auto filter = std::make_shared<LLSFilter>();
  filter->scd_my_shard_id_ = N0;
  filter->scd_known_down_ = {N1, N2, N3, N4, N5};

  ReadPointer read_ptr;
  std::vector<RawRecord> records;
  const Status st = ReadOperation()
                        .use_csi(useCSI())
                        .until_lsn(7)
                        .window_high(7)
                        .last_released(6)
                        .filter(filter)
                        .process(store.get(), records, &read_ptr);

  ASSERT_EQ(E::CAUGHT_UP, st);
  ASSERT_SHIPPED(records, 1);
  ASSERT_EQ(7, read_ptr.lsn);
}

TEST_P(LocalLogStoreReaderTest, SingleCopyDeliveryShuffleCopysets) {
  const int UNTIL_LSN = 1000;
  std::vector<RecordDescriptor> data;
  // Initialise the store with many records, all having N0 as the primary and
  // random shards as the other two in the copyset.
  // Because the first copy is fixed as N0 in this test, there need to be
  // enough other shards to generate enough unique copysets for randomness to
  // do its thing.
  std::vector<ShardID> shards(99);
  for (size_t i = 0; i < shards.size(); ++i) {
    shards[i] = ShardID(i, 1);
  }
  std::mt19937 rng{0xabcddcba};
  std::uniform_int_distribution<int> dist(0, shards.size() - 1);
  for (lsn_t lsn = 1; lsn <= UNTIL_LSN; ++lsn) {
    ShardID second = shards[dist(rng)];
    ShardID third;
    do
      third = shards[dist(rng)];
    while (second == third);
    RecordDescriptor d = {lsn, 1, {N0, second, third}};
    data.push_back(d);
  }
  auto store = createStore(data);

  auto filter = std::make_shared<LLSFilter>();
  filter->scd_my_shard_id_ = N0;
  filter->scd_copyset_reordering_ = SCDCopysetReordering::HASH_SHUFFLE;

  std::vector<RawRecord> records;
  ReadPointer read_ptr;
  const Status st = ReadOperation()
                        .use_csi(useCSI())
                        .until_lsn(UNTIL_LSN)
                        .window_high(UNTIL_LSN)
                        .last_released(UNTIL_LSN)
                        .filter(filter)
                        .process(store.get(), records, &read_ptr);

  double ratio = double(records.size()) / UNTIL_LSN;
  // Without copyset shuffling, ratio would be 1 because all copies would be
  // served by N0, the primary.  With copyset shuffling we expect only 1/3 of
  // the records to be served by N0.
  ld_info(
      "N0 shipped %.1f%% of records (should be around 33.3%%)", 100 * ratio);
  ASSERT_GE(ratio, 0.29);
  ASSERT_LE(ratio, 0.37);
}

TEST_P(LocalLogStoreReaderTest, SingleCopyDeliverySeededShuffleCopysets) {
  const int UNTIL_LSN = 1000;
  std::vector<RecordDescriptor> data;
  std::vector<ShardID> shards(99);
  for (size_t i = 0; i < shards.size(); ++i) {
    shards[i] = ShardID(i, 1);
  }
  std::mt19937 rng{0xabcddcba};
  std::uniform_int_distribution<int> dist(0, shards.size() - 1);
  for (lsn_t lsn = 1; lsn <= UNTIL_LSN; ++lsn) {
    ShardID second = shards[dist(rng)];
    ShardID third;
    do
      third = shards[dist(rng)];
    while (second == third);
    RecordDescriptor d = {lsn, 1, {N0, second, third}};
    data.push_back(d);
  }
  auto store = createStore(data);

  auto filter = std::make_shared<LLSFilter>();
  filter->scd_my_shard_id_ = N0;
  filter->scd_copyset_reordering_ =
      SCDCopysetReordering::HASH_SHUFFLE_CLIENT_SEED;

  // This test does not monitor the entire read path,
  // so no client session id has been provided yet.
  // Will generate the hash of one now
  std::string client_session_id_ = "mock_client_session_id";
  // Randomly generated seeds for SpookyHash
  // same as in LocalLogStoreReadFilter::applyCopysetReordering(
  uint64_t h1 = 0x59d2d101d78f02ad, h2 = 0x430bfb34b1cd41e1;

  folly::hash::SpookyHashV2::Hash128(client_session_id_.c_str(),
                                     client_session_id_.length() * sizeof(char),
                                     &h1,
                                     &h2);

  filter->csid_hash_pt1 = h1;
  filter->csid_hash_pt2 = h2;

  std::vector<RawRecord> records;
  ReadPointer read_ptr;
  const Status st = ReadOperation()
                        .use_csi(useCSI())
                        .until_lsn(UNTIL_LSN)
                        .window_high(UNTIL_LSN)
                        .last_released(UNTIL_LSN)
                        .filter(filter)
                        .process(store.get(), records, &read_ptr);

  double ratio = double(records.size()) / UNTIL_LSN;
  // Without copyset shuffling, ratio would be 1 because all copies would be
  // served by N0, the primary.  With copyset shuffling we expect only 1/3 of
  // the records to be served by N0.
  ld_info(
      "N0 shipped %.1f%% of records (should be around 33.3%%)", 100 * ratio);
  ASSERT_GE(ratio, 0.29);
  ASSERT_LE(ratio, 0.37);
}

// In this example, we verify that the read_ptr is not advanced beyond the lsn
// of the last record in the batch if this record gets filtered and the batch
// completed because we reached the byte limit.
TEST_P(LocalLogStoreReaderTest, FilteredAtEndOfBatch) {
  auto store = createStore({{1, 1, {N1, N2, N3, N4, N5}},
                            {1, 2, {N0, N5, N0, N4, N3}},
                            {2, 1, {N0, N3, N2, N5, N4}},
                            {3, 1, {N3, N4, N1, N5, N2}}});

  auto filter = std::make_shared<LLSFilter>();
  filter->scd_my_shard_id_ = N0;

  ReadPointer read_ptr;
  std::vector<RawRecord> records;
  const Status st = ReadOperation()
                        .use_csi(useCSI())
                        .until_lsn(4)
                        .window_high(4)
                        .last_released(3)
                        .filter(filter)
                        .max_bytes_all_records(1)
                        .process(store.get(), records, &read_ptr);

  ASSERT_EQ(E::BYTE_LIMIT_REACHED, st);
  // LocalLogStoreReader::read() only considered the first two records in the
  // db. However, the first one was filtered because its primary recipient is
  // not us, and sending the second one would've exceeded the byte limit. So
  // the number of messages that come out is zero.
  ASSERT_EQ(0, records.size());
  // However, it is not safe to advance the read_ptr's lsn because there might
  // be another record with the same lsn in the next batch. So the read_ptr's
  // lsn should still be 1, and the wave should have increased.
  ASSERT_EQ(1, read_ptr.lsn);
}

// Same test as FilteredAtEndOfBatch, but this time the last record to be
// filtered in the batch is also until_lsn.
TEST_P(LocalLogStoreReaderTest, UntilLSNFiltered) {
  auto store = createStore({{1, 1, {N1, N2, N3, N4, N5}},
                            {1, 2, {N0, N5, N0, N4, N3}},
                            {2, 1, {N0, N3, N2, N5, N4}},
                            {3, 1, {N3, N4, N1, N5, N2}}});

  auto filter = std::make_shared<LLSFilter>();
  filter->scd_my_shard_id_ = N0;

  ReadPointer read_ptr;
  std::vector<RawRecord> records;
  const Status st = ReadOperation()
                        .use_csi(useCSI())
                        .until_lsn(1)
                        .window_high(1)
                        .last_released(3)
                        .filter(filter)
                        .max_bytes_all_records(1)
                        .process(store.get(), records, &read_ptr);

  ASSERT_EQ(E::BYTE_LIMIT_REACHED, st);
  ASSERT_EQ(0, records.size());
  ASSERT_EQ(1, read_ptr.lsn);
}

// Same test as FilteredAtEndOfBatch, but this time the last record to be
// filtered in the batch is also window_high.
TEST_P(LocalLogStoreReaderTest, WindowEndFiltered) {
  auto store = createStore({{1, 1, {N1, N2, N3, N4, N5}},
                            {1, 2, {N0, N5, N0, N4, N3}},
                            {2, 1, {N0, N3, N2, N5, N4}},
                            {3, 1, {N3, N4, N1, N5, N2}}});

  auto filter = std::make_shared<LLSFilter>();
  filter->scd_my_shard_id_ = N0;

  ReadPointer read_ptr;
  std::vector<RawRecord> records;
  const Status st = ReadOperation()
                        .use_csi(useCSI())
                        .until_lsn(4)
                        .window_high(1)
                        .last_released(3)
                        .filter(filter)
                        .max_bytes_all_records(1)
                        .process(store.get(), records, &read_ptr);

  ASSERT_EQ(E::BYTE_LIMIT_REACHED, st);
  ASSERT_EQ(0, records.size());
  ASSERT_EQ(1, read_ptr.lsn);
}

// Same test as FilteredAtEndOfBatch, but this time the last record to be
// filtered in the batch is also the last_released lsn.
TEST_P(LocalLogStoreReaderTest, LastReleasedFiltered) {
  auto store = createStore({{1, 1, {N1, N2, N3, N4, N5}},
                            {1, 2, {N0, N5, N0, N4, N3}},
                            {2, 1, {N0, N3, N2, N5, N4}},
                            {3, 1, {N3, N4, N1, N5, N2}}});

  auto filter = std::make_shared<LLSFilter>();
  filter->scd_my_shard_id_ = N0;

  ReadPointer read_ptr;
  std::vector<RawRecord> records;
  const Status st = ReadOperation()
                        .use_csi(useCSI())
                        .until_lsn(4)
                        .window_high(3)
                        .last_released(1)
                        .filter(filter)
                        .max_bytes_all_records(1)
                        .process(store.get(), records, &read_ptr);

  ASSERT_EQ(E::BYTE_LIMIT_REACHED, st);
  ASSERT_EQ(0, records.size());
  ASSERT_EQ(1, read_ptr.lsn);
}

// Verify the behaviour when the callback fails. The batch should be aborted and
// read_ptr should ponit to the lsn at which this happened.
TEST_P(LocalLogStoreReaderTest, BatchAborted) {
  auto store = createStore(
      {{1, 1, {N1, N2, N3, N4, N5}},
       {2, 1, {N0, N3, N2, N5, N4}}, // This one gets sent
       {3, 1, {N4, N2, N5, N3, N0}},
       {3, 2, {N0, N2, N5, N3, N4}}, // This one should be sent but the cb fails
       {4, 1, {N3, N4, N1, N5, N2}},
       {5, 1, {N2, N4, N1, N5, N3}}});

  auto filter = std::make_shared<LLSFilter>();
  filter->scd_my_shard_id_ = N0;

  ReadPointer read_ptr;
  std::vector<RawRecord> records;
  const Status st =
      ReadOperation()
          .use_csi(useCSI())
          .until_lsn(7)
          .window_high(4)
          .last_released(3)
          .filter(filter)
          .fail_after(1) // the callback will abort after one record
          .process(store.get(), records, &read_ptr);

  ASSERT_EQ(E::ABORTED, st);
  // Only lsn 2 could be sent, lsn 3 could not be sent beause the callback
  // failed.
  ASSERT_SHIPPED(records, 2);
  // Read ptr should be set to lsn 3 (where the callback failed).
  ASSERT_EQ(3, read_ptr.lsn);
}

// There are many filtered records starting from read_ptr. We reach the limit on
// the number of bytes read so LocalLogStoreReader should complete the batch
// early and return E::PARTIAL.
TEST_P(LocalLogStoreReaderTest, ReadLimitNoRecordsShipped) {
  auto store = createStore({{1, 1, {N1, N2, N3, N4}},
                            {2, 1, {N1, N2, N3, N4}},
                            {3, 1, {N1, N2, N3, N4}},
                            {4, 1, {N1, N2, N3, N4}},
                            {5, 1, {N1, N2, N3, N4}},
                            {6, 1, {N1, N5, N3, N4}},
                            {7, 1, {N1, N5, N3, N4}}});

  // We only want to ship records that have 5 in the copyset.
  // Lsns 1-5 will be skipped.
  auto filter = std::make_shared<LLSFilter>();
  filter->required_in_copyset_.push_back(N5);

  std::string tmp_payload;
  formRecordHeader({1, 1, {N1, N2, N3, N4}}, &tmp_payload);

  std::vector<RawRecord> records;
  ReadPointer read_ptr;
  size_t max_bytes_to_read = tmp_payload.size() * 4;
  const Status st = ReadOperation()
                        .use_csi(useCSI())
                        .until_lsn(7)
                        .window_high(7)
                        .last_released(7)
                        // In non-CSI mode, we should give up the batch after
                        // filtering out the first 4 records.
                        .max_bytes_to_read(max_bytes_to_read)
                        .first_record_any_size(true)
                        .filter(filter)
                        .process(store.get(), records, &read_ptr);

  EXPECT_EQ(E::PARTIAL, st);
  if (!useCSI()) {
    // We could not ship anything.
    ASSERT_EQ(0, records.size());
    // read_ptr should be past the last filtered record which was lsn 4.
    ASSERT_EQ(5, read_ptr.lsn);
  } else {
    // We ship a record with CSI, since CSI does less reading
    {
      // Estimating csi entry size - fix this test if it changes
      std::string csi_buf;
      std::vector<ShardID> cs{N1, N2, N3, N4};
      Slice csi = LocalLogStoreRecordFormat::formCopySetIndexEntry(
          1, cs.data(), cs.size(), LSN_INVALID, false, &csi_buf);
      ASSERT_NE(nullptr, csi.data);
      // If the assert below fails, the results of this test might change,
      // re-check
      ASSERT_EQ(14, csi.size);
    }

    ASSERT_SHIPPED(records, 6)
    // read_ptr should be past the last filtered record which was lsn 6.
    ASSERT_EQ(7, read_ptr.lsn);
  }
}

TEST_P(LocalLogStoreReaderTest, ScdShipRecordIfExtra) {
  // Replication factor is 4, there is one extra.
  auto store = createStore({{1, 1, {N1, N2, N3, N4, N5}},
                            {2, 1, {N2, N5, N0, N4, N3}},
                            {3, 1, {N3, N4, N1, N5, N2}},
                            {4, 1, {N0, N3, N1, N5, N2}},
                            {5, 1, {N5, N0, N4, N2, N3}},
                            {6, 1, {N0, N3, N2, N4, N1}}});

  auto filter = std::make_shared<LLSFilter>();
  filter->scd_my_shard_id_ = N3;
  filter->scd_replication_ = 4;

  ReadPointer read_ptr;
  std::vector<RawRecord> records;
  const Status st = ReadOperation()
                        .use_csi(useCSI())
                        .until_lsn(7)
                        .window_high(7)
                        .last_released(6)
                        .filter(filter)
                        .process(store.get(), records, &read_ptr);

  ASSERT_EQ(E::CAUGHT_UP, st);
  // Record with lsn 3 should be sent because we are the leader recipient for
  // it. Records with lsn 2, 5 should be sent because we are the extra
  // recipient for them.
  ASSERT_SHIPPED(records, 2, 3, 5);
  ASSERT_EQ(7, read_ptr.lsn);
}

TEST_P(LocalLogStoreReaderTest, ScdShipRecordIfExtraWhileKnownDown) {
  // Replication factor is 3, there are two extras.
  auto store = createStore({{1, 1, {N1, N2, N3, N4, N5}}, // We ship it
                            {2, 1, {N2, N5, N0, N4, N3}}, // We ship it
                            {3, 1, {N3, N4, N1, N5, N2}}, // We ship it
                            {4, 1, {N0, N3, N1, N4, N2}},
                            {5, 1, {N5, N0, N3, N2, N1}}, // We ship it
                            {6, 1, {N0, N3, N2, N4, N1}}});

  auto filter = std::make_shared<LLSFilter>();
  filter->scd_my_shard_id_ = N3;
  filter->scd_replication_ = 3;
  filter->scd_known_down_ = {N5, N2};

  ReadPointer read_ptr;
  std::vector<RawRecord> records;
  const Status st = ReadOperation()
                        .use_csi(useCSI())
                        .until_lsn(7)
                        .window_high(7)
                        .last_released(6)
                        .filter(filter)
                        .process(store.get(), records, &read_ptr);

  ASSERT_EQ(E::CAUGHT_UP, st);
  ASSERT_SHIPPED(records, 1, 2, 3, 5);
  ASSERT_EQ(7, read_ptr.lsn);
}

// Verify that when we reach the byte limit by skipping records due to
// filtering them out from the original seek position, and we are past the
// window end, we get E::WINDOW_END_REACHED
TEST_P(LocalLogStoreReaderTest, SeekFilteredWindowEndReached) {
  auto store =
      createStore({{1, 1, {N1, N2, N3, N4}}, // Filtered by ReadIterator
                   {2, 1, {N1, N2, N3, N4}}, // Filtered by ReadIterator
                   {3, 1, {N1, N2, N3, N4}}, // Filtered and reaching read limit
                   {4, 1, {N1, N2, N3, N4}}});

  std::string tmp_payload;
  if (useCSI()) {
    formCopySetIndexEntry({1, 1, {N1, N2, N3, N4}}, &tmp_payload);
  } else {
    formRecordHeader({1, 1, {N1, N2, N3, N4}}, &tmp_payload);
  }

  auto filter = std::make_shared<LLSFilter>();
  filter->scd_my_shard_id_ = N3;
  filter->scd_replication_ = 4;
  std::vector<RawRecord> records;
  ReadPointer read_ptr;
  const Status st = ReadOperation()
                        .use_csi(useCSI())
                        .until_lsn(1337)
                        .window_high(2)
                        .last_released(4)
                        .filter(filter)
                        .max_bytes_to_read(tmp_payload.size() * 2)
                        .process(store.get(), records, &read_ptr);

  ASSERT_EQ(E::WINDOW_END_REACHED, st);
  ASSERT_EQ(0, records.size());
  ASSERT_EQ(3, read_ptr.lsn); // Next time we start reading LSN 3...
}

// Verify that when we reach the byte limit by skipping records due to
// filtering them out from after the original seek position (i.e. 2nd record
// onwards), and we are past the window end, we get E::WINDOW_END_REACHED
TEST_P(LocalLogStoreReaderTest, NextFilteredWindowEndReached) {
  auto store = createStore(
      {{1, 1, {N3, N2, N1, N4}}, // Shipped
       {2, 1, {N1, N2, N3, N4}}, // Filtered
       {3, 1, {N1, N2, N3, N4}}, // Filetered and reaching read limit
       {4, 1, {N1, N2, N3, N4}}});

  std::string tmp_payload;
  std::string tmp_csi;
  formRecordHeader({1, 1, {N1, N2, N3, N4}}, &tmp_payload);
  if (useCSI()) {
    formCopySetIndexEntry({1, 1, {N1, N2, N3, N4}}, &tmp_csi);
  }

  auto filter = std::make_shared<LLSFilter>();
  filter->scd_my_shard_id_ = N3;
  filter->scd_replication_ = 4;
  std::vector<RawRecord> records;
  ReadPointer read_ptr;
  const Status st = ReadOperation()
                        .use_csi(useCSI())
                        .until_lsn(1337)
                        .window_high(2)
                        .last_released(4)
                        .filter(filter)
                        .max_bytes_to_read(useCSI() ? tmp_payload.size() +
                                                   tmp_csi.size() * 2 + 1
                                                    : tmp_payload.size() * 3)
                        .process(store.get(), records, &read_ptr);

  ASSERT_EQ(E::WINDOW_END_REACHED, st);
  ASSERT_SHIPPED(records, 1);
  ASSERT_EQ(3, read_ptr.lsn); // Next time we start reading LSN 3...
}

TEST_P(LocalLogStoreReaderTest, SeekFilteredCaughtUp) {
  auto store = createStore(
      {{1, 1, {N1, N2, N3, N4}}, // Filtered
                                 // Record with LSN 2 still not written (last
                                 // released is 1...)
       {3, 1, {N1, N2, N3, N4}}, // Filtered, and here we reaching read limit
       {4, 1, {N1, N2, N3, N4}}});

  std::string tmp_payload;
  formRecordHeader({1, 1, {N1, N2, N3, N4}}, &tmp_payload);

  auto filter = std::make_shared<LLSFilter>();
  filter->scd_my_shard_id_ = N3;
  filter->scd_replication_ = 4;
  std::vector<RawRecord> records;
  ReadPointer read_ptr;
  const Status st = ReadOperation()
                        .use_csi(useCSI())
                        .until_lsn(1337)
                        .window_high(1337)
                        .last_released(1)
                        .filter(filter)
                        .max_bytes_to_read(tmp_payload.size() * 3)
                        .process(store.get(), records, &read_ptr);

  ASSERT_EQ(E::CAUGHT_UP, st);
  ASSERT_EQ(0, records.size());
  // Even though ReaderIterator::seek() read record with LSN 3, last
  // released lsn is 1 so we will read starting from LSN 2 next time.
  ASSERT_EQ(2, read_ptr.lsn);
}

TEST_P(LocalLogStoreReaderTest, NextFilteredCaughtUp) {
  auto store = createStore(
      {{1, 1, {N3, N2, N1, N4}}, // Shipped
                                 // Record with LSN 2 still not written (last
                                 // released is 1...)
       {3, 1, {N1, N2, N3, N4}}, // Filtered, and here we reach read limit
       {4, 1, {N1, N2, N3, N4}}});

  std::string tmp_payload;
  formRecordHeader({1, 1, {N1, N2, N3, N4}}, &tmp_payload);

  auto filter = std::make_shared<LLSFilter>();
  filter->scd_my_shard_id_ = N3;
  filter->scd_replication_ = 4;
  std::vector<RawRecord> records;
  ReadPointer read_ptr;
  const Status st = ReadOperation()
                        .use_csi(useCSI())
                        .until_lsn(1337)
                        .window_high(1337)
                        .last_released(1)
                        .filter(filter)
                        .max_bytes_to_read(tmp_payload.size() * 3)
                        .process(store.get(), records, &read_ptr);

  ASSERT_EQ(E::CAUGHT_UP, st);
  ASSERT_SHIPPED(records, 1);

  // Even though ReaderIterator::seek() read record with LSN 3, last
  // released lsn is 1 so we will read starting from LSN 2 next time.
  ASSERT_EQ(2, read_ptr.lsn);
}

// Verify that the read ptr that we get after an operation that ends in
// E::BYTE_LIMIT_REACHED (when first_record_any_size==true) will give us records
// without unexpected gaps when used on a subsequent operation.
TEST_P(LocalLogStoreReaderTest, ReadPtrContinuityFirstRecordAnySize) {
  auto store = createStore({{1, 1, {N1, N2, N3, N4}},
                            {2, 1, {N1, N2, N3, N4}},
                            {3, 1, {N1, N2, N3, N4}},
                            {4, 1, {N1, N2, N3, N4}}});

  ReadPointer read_ptr{LSN_INVALID};

  std::string tmp_payload;
  formRecordHeader({1, 1, {N1, N2, N3, N4}}, &tmp_payload);
  size_t msg_size = RECORD_Message::expectedSize(tmp_payload.size());

  std::vector<RawRecord> records;
  const Status st = ReadOperation()
                        .use_csi(useCSI())
                        .until_lsn(4)
                        .window_high(4)
                        .last_released(4)
                        .max_bytes_all_records(1)
                        .first_record_any_size(true)
                        .process(store.get(), records, &read_ptr);

  ASSERT_EQ(E::BYTE_LIMIT_REACHED, st);
  ASSERT_SHIPPED(records, 1);
  ASSERT_EQ(2, read_ptr.lsn);

  records.clear();
  const Status st2 = ReadOperation()
                         .use_csi(useCSI())
                         .from(read_ptr)
                         .until_lsn(4)
                         .window_high(4)
                         .last_released(4)
                         .max_bytes_all_records(msg_size)
                         .process(store.get(), records, &read_ptr);

  ASSERT_EQ(E::BYTE_LIMIT_REACHED, st2);
  ASSERT_SHIPPED(records, 2);
  ASSERT_EQ(3, read_ptr.lsn);
}

// Verify that the read ptr that we get after an operation that ends in
// E::BYTE_LIMIT_REACHED (when first_record_any_size==false) will give us
// records without unexpected gaps when used on a subsequent operation.
TEST_P(LocalLogStoreReaderTest, ReadPtrContinuityByteLimitReached) {
  auto store = createStore({{1, 1, {N1, N2, N3, N4}},
                            {2, 1, {N1, N2, N3, N4}},
                            {3, 1, {N1, N2, N3, N4}},
                            {4, 1, {N1, N2, N3, N4}}});

  ReadPointer read_ptr{LSN_INVALID};

  std::string tmp_payload;
  formRecordHeader({1, 1, {N1, N2, N3, N4}}, &tmp_payload);
  size_t msg_size = RECORD_Message::expectedSize(tmp_payload.size());

  std::vector<RawRecord> records;
  const Status st = ReadOperation()
                        .use_csi(useCSI())
                        .until_lsn(4)
                        .window_high(4)
                        .last_released(4)
                        .max_bytes_all_records(msg_size)
                        .process(store.get(), records, &read_ptr);

  ASSERT_EQ(E::BYTE_LIMIT_REACHED, st);
  ASSERT_SHIPPED(records, 1);
  ASSERT_EQ(2, read_ptr.lsn);

  records.clear();
  const Status st2 = ReadOperation()
                         .use_csi(useCSI())
                         .from(read_ptr)
                         .until_lsn(4)
                         .window_high(4)
                         .last_released(4)
                         .max_bytes_all_records(msg_size)
                         .process(store.get(), records, &read_ptr);

  ASSERT_EQ(E::BYTE_LIMIT_REACHED, st);
  ASSERT_SHIPPED(records, 2);
  ASSERT_EQ(3, read_ptr.lsn);
}

// Verify that the read ptr that we get after an operation that ends in
// E::PARTIAL will give us records without unexpected gaps when used on a
// subsequent operation.
TEST_P(LocalLogStoreReaderTest, ReadPtrContinuityPartial) {
  auto store = createStore({{1, 1, {N1, N2, N3, N4}},
                            {2, 1, {N1, N2, N3, N4}},
                            {3, 1, {N1, N2, N3, N4}},
                            {4, 1, {N1, N2, N3, N4}}});

  ReadPointer read_ptr{LSN_INVALID};

  std::string tmp_payload;
  formRecordHeader({1, 1, {N1, N2, N3, N4}}, &tmp_payload);
  size_t msg_size = RECORD_Message::expectedSize(tmp_payload.size());

  std::vector<RawRecord> records;
  const Status st = ReadOperation()
                        .use_csi(useCSI())
                        .until_lsn(4)
                        .window_high(4)
                        .last_released(4)
                        .max_bytes_to_read(tmp_payload.size())
                        .process(store.get(), records, &read_ptr);

  ASSERT_EQ(E::PARTIAL, st);
  ASSERT_SHIPPED(records, 1);
  ASSERT_EQ(2, read_ptr.lsn);

  records.clear();
  const Status st2 = ReadOperation()
                         .use_csi(useCSI())
                         .from(read_ptr)
                         .until_lsn(2)
                         .window_high(4)
                         .last_released(4)
                         .process(store.get(), records);

  ASSERT_EQ(E::UNTIL_LSN_REACHED, st2);
  ASSERT_SHIPPED(records, 2);
}

INSTANTIATE_TEST_CASE_P(LocalLogStoreReaderTest,
                        LocalLogStoreReaderTest,
                        ::testing::Values(WAVE_IN_VALUE,
                                          WRITE_CSI,
                                          SHARD_ID_IN_COPYSET));

// SCDCopysetReordering algorithms must not change once deployed.  Because
// their permanence may depend on details like random number generation, a
// test with fixed inputs and outputs may be valuable to prevent inadvertent
// changes.
TEST(LocalLogStoreReaderTest, HashShuffleFixed) {
  auto shuf_v1 = [](std::vector<ShardID> v) {
    LLSFilter filter;
    filter.scd_copyset_reordering_ = SCDCopysetReordering::HASH_SHUFFLE;
    filter.applyCopysetReordering(v.data(), v.size());
    return v;
  };

  auto c = [&](const std::vector<node_index_t>& in) {
    std::vector<ShardID> out;
    out.resize(in.size());
    for (int i = 0; i < in.size(); ++i) {
      out[i] = ShardID(in[i], 0);
    }
    return out;
  };

  EXPECT_EQ(c({1, 3, 2}), shuf_v1(c({1, 2, 3})));
  EXPECT_EQ(c({5, 8, 1, 7, 6, 2, 0, 4, 9, 3}),
            shuf_v1(c({8, 7, 0, 9, 5, 4, 2, 3, 6, 1})));
  EXPECT_EQ(c({94, 54, 75, 30, 73, 84, 81, 61, 41, 69}),
            shuf_v1(c({54, 84, 61, 75, 41, 30, 73, 94, 81, 69})));
  EXPECT_EQ(c({76, 68, 47, 79, 23, 16, 71, 28, 13, 26}),
            shuf_v1(c({79, 76, 13, 71, 68, 47, 26, 23, 16, 28})));
}

// A basic test where we check that records are filtered properly when local scd
// is in use.
TEST_P(LocalLogStoreReaderTest, LocalScdSimple) {
  auto store = createStore({{1, 1, {N1, N2, N3, N4, N5}, 0},
                            {2, 1, {N2, N5, N0, N4, N3}, 0},
                            {3, 1, {N3, N4, N1, N5, N2}, 0},
                            {4, 1, {N0, N3, N1, N5, N2}, 0},
                            {5, 1, {N5, N0, N4, N2, N3}, 0},
                            {6, 1, {N0, N3, N2, N4, N1}, 0}});

  auto config = Configuration::fromJsonFile(CONFIG_PATH);

  auto filter =
      std::make_shared<MockLocalLogStoreReadFilter>(config->serverConfig());
  filter->scd_my_shard_id_ = N0;
  filter->client_location_ = std::make_unique<NodeLocation>();
  filter->client_location_->fromDomainString("rgn1.dc1.cl1.row1.rck1");

  ReadPointer read_ptr;
  std::vector<RawRecord> records;
  const Status st = ReadOperation()
                        .use_csi(useCSI())
                        .until_lsn(7)
                        .window_high(7)
                        .last_released(6)
                        .filter(filter)
                        .process(store.get(), records, &read_ptr);

  ASSERT_EQ(E::CAUGHT_UP, st);
  // Only 4, 5, and 6 should be sent back.
  ASSERT_SHIPPED(records, 4, 5, 6);
  ASSERT_EQ(7, read_ptr.lsn);
}

// Verify we ship nothing if our node is not a local node and all of the records
// in the batch has a local node id.
TEST_P(LocalLogStoreReaderTest, LocalScdNotLocalNode) {
  auto store = createStore({{1, 1, {N1, N2, N3, N4, N5}, 0},
                            {2, 1, {N2, N5, N0, N4, N3}, 0},
                            {3, 1, {N3, N4, N1, N5, N2}, 0},
                            {4, 1, {N0, N3, N1, N5, N2}, 0},
                            {5, 1, {N5, N0, N4, N2, N3}, 0},
                            {6, 1, {N0, N3, N2, N4, N1}, 0}});

  auto config = Configuration::fromJsonFile(CONFIG_PATH);

  auto filter =
      std::make_shared<MockLocalLogStoreReadFilter>(config->serverConfig());
  filter->scd_my_shard_id_ = N3;
  filter->client_location_ = std::make_unique<NodeLocation>();
  filter->client_location_->fromDomainString("rgn1.dc1.cl1.row1.rck1");

  ReadPointer read_ptr;
  std::vector<RawRecord> records;
  const Status st = ReadOperation()
                        .use_csi(useCSI())
                        .until_lsn(7)
                        .window_high(7)
                        .last_released(6)
                        .filter(filter)
                        .process(store.get(), records, &read_ptr);

  ASSERT_EQ(E::CAUGHT_UP, st);
  // Nothing should be sent.
  ASSERT_EQ(0, records.size());
  ASSERT_EQ(7, read_ptr.lsn);
}

// Local SCD is enabled and N1 is down.
TEST_P(LocalLogStoreReaderTest, LocalScdSimpleOneKnownDown) {
  auto store = createStore({{1, 1, {N1, N0, N3, N4, N5}, 0},
                            {2, 1, {N2, N5, N0, N4, N3}, 0},
                            {3, 1, {N3, N4, N1, N5, N2}, 0},
                            {4, 1, {N0, N3, N1, N5, N2}, 0},
                            {5, 1, {N5, N0, N4, N2, N3}, 0},
                            {6, 1, {N0, N3, N2, N4, N1}, 0}});

  auto config = Configuration::fromJsonFile(CONFIG_PATH);

  auto filter =
      std::make_shared<MockLocalLogStoreReadFilter>(config->serverConfig());
  filter->scd_my_shard_id_ = N0;
  filter->client_location_ = std::make_unique<NodeLocation>();
  filter->client_location_->fromDomainString("rgn1.dc1.cl1.row1.rck1");
  filter->scd_known_down_ = {N1};

  ReadPointer read_ptr;
  std::vector<RawRecord> records;
  const Status st = ReadOperation()
                        .use_csi(useCSI())
                        .until_lsn(7)
                        .window_high(7)
                        .last_released(6)
                        .filter(filter)
                        .process(store.get(), records, &read_ptr);

  ASSERT_EQ(E::CAUGHT_UP, st);
  // lsns 1, 4, 5, 6 should be sent back
  // 4, 5, and 6 because we are the primary recipient for these records.
  // 1 because its primary recipient is in the known down list.
  ASSERT_SHIPPED(records, 1, 4, 5, 6);
  ASSERT_EQ(7, read_ptr.lsn);
}

// We are ourselves in the known down list, which can happen if the client
// thought we are down, but this node came back up.
TEST_P(LocalLogStoreReaderTest, LocalScdOurselvesInKnownDown) {
  auto store = createStore({{1, 1, {N1, N0, N3, N4, N5}, 0},
                            {2, 1, {N2, N5, N0, N4, N3}, 0},
                            {3, 1, {N3, N4, N1, N5, N2}, 0},
                            {4, 1, {N0, N3, N1, N5, N2}, 0},
                            {5, 1, {N5, N0, N4, N2, N3}, 0},
                            {6, 1, {N0, N3, N2, N4, N1}, 0}});

  auto config = Configuration::fromJsonFile(CONFIG_PATH);

  auto filter =
      std::make_shared<MockLocalLogStoreReadFilter>(config->serverConfig());
  filter->scd_my_shard_id_ = N0;
  filter->client_location_ = std::make_unique<NodeLocation>();
  filter->client_location_->fromDomainString("rgn1.dc1.cl1.row1.rck1");
  filter->scd_known_down_ = {N0, N1};

  ReadPointer read_ptr;
  std::vector<RawRecord> records;
  const Status st = ReadOperation()
                        .use_csi(useCSI())
                        .until_lsn(7)
                        .window_high(7)
                        .last_released(6)
                        .filter(filter)
                        .process(store.get(), records, &read_ptr);

  ASSERT_EQ(E::CAUGHT_UP, st);
  // lsns 1, 4, 5, 6 should be sent back
  // 4, 5, and 6 because we are the primary recipient for these records.
  // 1 because its primary recipient is in the known down list.
  ASSERT_SHIPPED(records, 1, 4, 5, 6);
  ASSERT_EQ(7, read_ptr.lsn);
}

// Local SCD is enabled, but a record has no local nodes.
// Verify that logic reverts to normal SCD and another node ships the record.
TEST_P(LocalLogStoreReaderTest, LocalScdNoLocalNodes) {
  auto store = createStore({{1, 1, {N1, N2, N3, N4, N5}, 0},
                            {2, 1, {N2, N5, N0, N4, N3}, 0},
                            {3, 1, {N3, N4, N1, N5, N2}, 0},
                            {4, 1, {N0, N3, N1, N5, N2}, 0},
                            {5, 1, {N5, N0, N4, N2, N3}, 0},
                            {6, 1, {N0, N3, N2, N4, N1}, 0}});

  auto config = Configuration::fromJsonFile(CONFIG_PATH);

  auto filter =
      std::make_shared<MockLocalLogStoreReadFilter>(config->serverConfig());
  filter->scd_my_shard_id_ = N0;
  filter->client_location_ = std::make_unique<NodeLocation>();
  filter->client_location_->fromDomainString("rgn3.dc3.cl3.row3.rck3");

  ReadPointer read_ptr;
  std::vector<RawRecord> records;
  const Status st = ReadOperation()
                        .use_csi(useCSI())
                        .until_lsn(7)
                        .window_high(7)
                        .last_released(6)
                        .filter(filter)
                        .process(store.get(), records, &read_ptr);

  ASSERT_EQ(E::CAUGHT_UP, st);
  // Only 6 should be sent back.
  ASSERT_SHIPPED(records, 6);
  ASSERT_EQ(7, read_ptr.lsn);
}

// Local SCD is enabled, but the only node in the client region is down.
// Verify that logic reverts to normal SCD and ships records.
TEST_P(LocalLogStoreReaderTest, LocalScdLocalNodesAreDown) {
  auto store = createStore({{1, 1, {N1, N2, N3, N4, N5}, 0},
                            {2, 1, {N2, N5, N0, N4, N3}, 0},
                            {3, 1, {N3, N4, N1, N5, N2}, 0},
                            {4, 1, {N0, N3, N1, N5, N2}, 0},
                            {5, 1, {N5, N0, N4, N2, N3}, 0},
                            {6, 1, {N0, N3, N2, N4, N1}, 0}});

  auto config = Configuration::fromJsonFile(CONFIG_PATH);

  auto filter =
      std::make_shared<MockLocalLogStoreReadFilter>(config->serverConfig());
  filter->scd_my_shard_id_ = N1;
  filter->client_location_ = std::make_unique<NodeLocation>();
  filter->client_location_->fromDomainString("rgn3.dc3.cl3.row3.rck3");
  filter->scd_known_down_ = {N5};

  ReadPointer read_ptr;
  std::vector<RawRecord> records;
  const Status st = ReadOperation()
                        .use_csi(useCSI())
                        .until_lsn(7)
                        .window_high(7)
                        .last_released(6)
                        .filter(filter)
                        .process(store.get(), records, &read_ptr);

  ASSERT_EQ(E::CAUGHT_UP, st);
  // Only 1 should be sent back.
  ASSERT_SHIPPED(records, 1);
  ASSERT_EQ(7, read_ptr.lsn);
}

// Regardless of known down or copyset, drained records should not be
// returned.
TEST_P(LocalLogStoreReaderTest, DrainedRecords) {
  auto store = createStore(
      {{1, 1, {N1, N2, N3, N4, N5}, LocalLogStoreRecordFormat::FLAG_DRAINED},
       {2, 1, {N2, N5, N1, N4, N3}, LocalLogStoreRecordFormat::FLAG_DRAINED},
       {3, 1, {N3, N4, N1, N5, N2}, LocalLogStoreRecordFormat::FLAG_DRAINED},
       {4, 1, {N4, N3, N1, N5, N2}, LocalLogStoreRecordFormat::FLAG_DRAINED},
       {5, 1, {N5, N1, N4, N2, N3}, LocalLogStoreRecordFormat::FLAG_DRAINED},
       {6, 1, {N5, N3, N2, N4, N1}, LocalLogStoreRecordFormat::FLAG_DRAINED}});

  auto config = Configuration::fromJsonFile(CONFIG_PATH);

  auto filter =
      std::make_shared<MockLocalLogStoreReadFilter>(config->serverConfig());
  filter->scd_my_shard_id_ = N0;
  filter->client_location_ = std::make_unique<NodeLocation>();
  filter->client_location_->fromDomainString("rgn1.dc1.cl1.row1.rck1");
  filter->scd_known_down_ = {N0, N1};

  ReadPointer read_ptr;
  std::vector<RawRecord> records;
  const Status st = ReadOperation()
                        .use_csi(useCSI())
                        .until_lsn(7)
                        .window_high(7)
                        .last_released(6)
                        .filter(filter)
                        .process(store.get(), records, &read_ptr);

  ASSERT_EQ(E::CAUGHT_UP, st);
  // lsns 1, 4, 5, 6 should be sent back
  // 4, 5, and 6 because we are the primary recipient for these records.
  // 1 because its primary recipient is in the known down list.
  ASSERT_SHIPPED(records);
  ASSERT_EQ(7, read_ptr.lsn);
}
namespace {
lsn_t lsn(int epoch, int esn) {
  return compose_lsn(epoch_t(epoch), esn_t(esn));
}
} // namespace

TEST_P(LocalLogStoreReaderTest, GetTailRecord) {
  TemporaryRocksDBStore store;
  std::string str_pl("dummypl");
  Payload pl(str_pl.data(), str_pl.size());

  std::vector<TestRecord> records = {
      TestRecord(logid_t(1), lsn(1, 1), ESN_INVALID),
      TestRecord(logid_t(1), lsn(1, 3), esn_t(1)).payload(pl),
      TestRecord(logid_t(1), lsn(1, 4), esn_t(3)),
      TestRecord(logid_t(1), lsn(2, 1), ESN_INVALID),
      TestRecord(logid_t(1), lsn(2, 2), esn_t(1)).payload(pl),
      TestRecord(logid_t(1), lsn(4, 1), ESN_INVALID).payload(pl),
      TestRecord(logid_t(1), lsn(4, 4), esn_t(1))
          .timestamp(std::chrono::milliseconds(2335))
          .offsetsWithinEpoch(OffsetMap({{BYTE_OFFSET, 329971}}))
          .writtenByRecovery(6)
          .bridge()
          .payload(pl),
      TestRecord(logid_t(2), lsn(1, 1), esn_t(1))};

  store_fill(store, records);

  LocalLogStore::ReadOptions read_options("GetTailRecordTest");
  read_options.allow_blocking_io = true;
  read_options.tailing = false;
  auto it = store.read(logid_t(1), read_options);
  ASSERT_NE(it, nullptr);
  int rv;
  TailRecord tail;

  // lng == 0, tail not found
  rv = getTailRecord(*it, logid_t(1), epoch_t(2), esn_t(0), &tail, false);
  ASSERT_EQ(-1, rv);
  ASSERT_EQ(err, E::NOTFOUND);

  // epoch 1 lng 2
  rv = getTailRecord(*it, logid_t(1), epoch_t(1), esn_t(2), &tail, false);
  ASSERT_EQ(0, rv);
  ASSERT_EQ(lsn(1, 1), tail.header.lsn);
  ASSERT_FALSE(tail.hasPayload());
  ASSERT_FALSE(tail.header.flags & TailRecordHeader::CHECKSUM);
  ASSERT_FALSE(tail.header.flags & TailRecordHeader::CHECKSUM_64BIT);
  ASSERT_TRUE(tail.header.flags & TailRecordHeader::CHECKSUM_PARITY);

  // byteoffset not available
  ASSERT_EQ(OffsetMap(), tail.offsets_map_);

  // epoch 1 lng 3
  rv = getTailRecord(*it, logid_t(1), epoch_t(1), esn_t(3), &tail, false);
  ASSERT_EQ(0, rv);
  ASSERT_EQ(lsn(1, 3), tail.header.lsn);
  ASSERT_FALSE(tail.hasPayload());
  ASSERT_FALSE(tail.header.flags & TailRecordHeader::CHECKSUM);
  ASSERT_FALSE(tail.header.flags & TailRecordHeader::CHECKSUM_64BIT);
  ASSERT_TRUE(tail.header.flags & TailRecordHeader::CHECKSUM_PARITY);

  // offsets not available
  ASSERT_EQ(OffsetMap(), tail.offsets_map_);

  // epoch 4 lng 10 w/ payload
  rv = getTailRecord(*it, logid_t(1), epoch_t(4), esn_t(10), &tail, true);
  ASSERT_EQ(0, rv);
  ASSERT_EQ(logid_t(1), tail.header.log_id);
  ASSERT_EQ(lsn(4, 4), tail.header.lsn);
  ASSERT_TRUE(tail.hasPayload());
  ASSERT_EQ(2335, tail.header.timestamp);
  ASSERT_EQ(329971, tail.offsets_map_.getCounter(BYTE_OFFSET));
  TailRecordHeader::flags_t expected_flags =
      (TailRecordHeader::HAS_PAYLOAD |
       TailRecordHeader::CHECKSUM_PARITY | // default in TestRecord
       TailRecordHeader::OFFSET_WITHIN_EPOCH | TailRecordHeader::OFFSET_MAP);

  ASSERT_EQ(expected_flags, tail.header.flags);
  Slice s = tail.getPayloadSlice();
  rv = strncmp((const char*)s.data, (const char*)pl.data(), pl.size());
  ASSERT_EQ(0, rv);

  // epoch 5 lng 10
  rv = getTailRecord(*it, logid_t(1), epoch_t(5), esn_t(10), &tail, false);
  ASSERT_EQ(-1, rv);
  ASSERT_EQ(err, E::NOTFOUND);
}

// Reproduces T23986847
TEST_P(LocalLogStoreReaderTest, ReadLimitAtEndOfWindow) {
  if (!useCSI()) {
    // The test requires CSI.
    return;
  }

  // A few 10KB records.
  auto store =
      createStore({{1, 1, {N1, N2}, 0, 10000},
                   {2, 1, {N1, N2}, 0, 10000},
                   {3, 1, {N1, N2}, STORE_Header::AMEND}, // dangling amend
                   {4, 1, {N2, N1}, 0, 10000},            // filtered out
                   //  -----  window end  -----
                   {5, 1, {N2, N1}, 0, 10000}});

  auto config = Configuration::fromJsonFile(CONFIG_PATH);
  auto filter =
      std::make_shared<MockLocalLogStoreReadFilter>(config->serverConfig());
  filter->scd_my_shard_id_ = N1;

  std::vector<RawRecord> records;
  const Status st = ReadOperation()
                        .use_csi(useCSI())
                        .window_high(4)
                        .max_bytes_to_read(25000) // read limit: 2.5 records
                        .filter(filter)
                        .process(store.get(), records);

  EXPECT_EQ(E::WINDOW_END_REACHED, st);
  ASSERT_SHIPPED(records, 1, 2);
}
