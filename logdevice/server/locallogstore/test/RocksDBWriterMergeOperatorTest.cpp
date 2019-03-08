/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include <memory>

#include <gtest/gtest.h>

#include "logdevice/server/locallogstore/test/LocalLogStoreTestReader.h"
#include "logdevice/server/locallogstore/test/StoreUtil.h"
#include "logdevice/server/locallogstore/test/TemporaryLogStore.h"

using namespace facebook::logdevice;

const shard_index_t THIS_SHARD = 0;

// Useful shortcuts for writing ShardIDs.
#define N0 ShardID(0, THIS_SHARD)
#define N1 ShardID(1, THIS_SHARD)
#define N2 ShardID(2, THIS_SHARD)
#define N3 ShardID(3, THIS_SHARD)
#define N4 ShardID(4, THIS_SHARD)
#define N5 ShardID(5, THIS_SHARD)
#define N6 ShardID(6, THIS_SHARD)
#define N7 ShardID(7, THIS_SHARD)
#define N8 ShardID(8, THIS_SHARD)
#define N9 ShardID(9, THIS_SHARD)

namespace {

const logid_t LOG_ID(21091985);

using LocalLogStoreRecordFormat::FLAG_AMEND;
using LocalLogStoreRecordFormat::FLAG_BRIDGE;
using LocalLogStoreRecordFormat::FLAG_HOLE;
using LocalLogStoreRecordFormat::FLAG_WRITTEN_BY_RECOVERY;
using LocalLogStoreRecordFormat::flags_t;

struct ParseResult {
  flags_t flags;
  uint32_t wave;
  std::string payload;
  std::vector<ShardID> copyset;
  OffsetMap offsets_within_epoch;
};

ParseResult parse(const RawRecord& rec) {
  ParseResult ret;
  Payload payload;

  copyset_size_t copyset_size;
  ret.copyset.resize(COPYSET_SIZE_MAX);
  int rv = LocalLogStoreRecordFormat::parse(rec.blob,
                                            nullptr,
                                            nullptr,
                                            &ret.flags,
                                            &ret.wave,
                                            &copyset_size,
                                            ret.copyset.data(),
                                            ret.copyset.size(),
                                            &ret.offsets_within_epoch,
                                            nullptr,
                                            &payload,
                                            THIS_SHARD);
  ld_check(rv == 0);
  ret.copyset.resize(copyset_size);
  if (payload.size() != 0) {
    ret.payload.assign((const char*)payload.data(), payload.size());
  }
  return ret;
}

class RocksDBWriterMergeOperatorTest : public ::testing::Test {
 public:
  void storeRecords(const std::vector<TestRecord>& records) {
    store_fill(store_, records);
  }
  void
  verify(std::function<void(const std::vector<RawRecord>&)> run_on_all,
         std::function<void(const std::vector<RawRecord>&)> run_on_records =
             [](const std::vector<RawRecord>&) {},
         std::function<void(const std::vector<RawRecord>&)> run_on_csi =
             [](const std::vector<RawRecord>&) {}) {
    for (int csi = 0; csi <= 1; ++csi) {
      auto reader = test::LocalLogStoreTestReader().logID(LOG_ID);
      if (csi) {
        reader.use_csi(true).csi_only();
      }
      std::vector<RawRecord> rec;
      Status st = reader.logID(LOG_ID).process(&store_, rec);
      ASSERT_EQ(E::UNTIL_LSN_REACHED, st);
      {
        SCOPED_TRACE("csi = " + std::to_string(csi));
        run_on_all(rec);
      }
      if (csi) {
        run_on_csi(rec);
      } else {
        run_on_records(rec);
      }
    }
  }

  LocalLogStore& getStore() {
    return store_;
  }

 private:
  TemporaryRocksDBStore store_;
};

TEST_F(RocksDBWriterMergeOperatorTest, SingleWave) {
  std::vector<TestRecord> test_data = {
      TestRecord(LOG_ID, lsn_t(1), esn_t(0))
          .wave(1)
          .copyset({N1, N2, N3})
          .payload(Payload("foo", 3)),
  };
  storeRecords(test_data);
  auto cb_all = [](const std::vector<RawRecord>& rec) {
    ASSERT_EQ(1, rec.size());
    ASSERT_FALSE(parse(rec[0]).flags & FLAG_AMEND);
    ASSERT_EQ(1, parse(rec[0]).wave);
    ASSERT_EQ(std::vector<ShardID>({N1, N2, N3}), parse(rec[0]).copyset);
  };
  auto cb_records_only = [](const std::vector<RawRecord>& rec) {
    ASSERT_EQ("foo", parse(rec[0]).payload);
  };
  verify(cb_all, cb_records_only);
}

// With multiple waves containing the payload, must take the later wave
TEST_F(RocksDBWriterMergeOperatorTest, TwoWavesWithPayload) {
  std::vector<TestRecord> test_data = {
      TestRecord(LOG_ID, lsn_t(1), esn_t(0))
          .wave(1)
          .copyset({N1, N2, N3})
          .payload(Payload("foo", 3)),
      TestRecord(LOG_ID, lsn_t(1), esn_t(0))
          .wave(2)
          .copyset({N4, N5, N6, N7})
          .payload(Payload("foo", 3)),
  };
  storeRecords(test_data);
  auto cb_all = [](const std::vector<RawRecord>& rec) {
    ASSERT_EQ(1, rec.size());
    ASSERT_FALSE(parse(rec[0]).flags & FLAG_AMEND);
    ASSERT_EQ(2, parse(rec[0]).wave);
    ASSERT_EQ(std::vector<ShardID>({N4, N5, N6, N7}), parse(rec[0]).copyset);
  };
  auto cb_records_only = [](const std::vector<RawRecord>& rec) {
    ASSERT_EQ("foo", parse(rec[0]).payload);
  };
  verify(cb_all, cb_records_only);
}

// Must take later wave+copyset even if STOREs get reordered
TEST_F(RocksDBWriterMergeOperatorTest, TwoWavesReordered) {
  std::vector<TestRecord> test_data = {
      TestRecord(LOG_ID, lsn_t(1), esn_t(0))
          .wave(2)
          .copyset({N4, N5, N6, N7})
          .payload(Payload("foo", 3)),
      TestRecord(LOG_ID, lsn_t(1), esn_t(0))
          .wave(1)
          .copyset({N1, N2, N3})
          .payload(Payload("foo", 3)),
  };

  storeRecords(test_data);
  auto cb_all = [](const std::vector<RawRecord>& rec) {
    ASSERT_EQ(1, rec.size());
    ASSERT_FALSE(parse(rec[0]).flags & FLAG_AMEND);
    ASSERT_EQ(2, parse(rec[0]).wave);
    ASSERT_EQ(std::vector<ShardID>({N4, N5, N6, N7}), parse(rec[0]).copyset);
  };
  auto cb_records_only = [](const std::vector<RawRecord>& rec) {
    ASSERT_EQ("foo", parse(rec[0]).payload);
  };
  verify(cb_all, cb_records_only);
}

// A simple case where a proper first wave is followed by a payloadless amend
TEST_F(RocksDBWriterMergeOperatorTest, TwoWavesAmend) {
  std::vector<TestRecord> test_data = {
      TestRecord(LOG_ID, lsn_t(1), esn_t(0))
          .wave(1)
          .copyset({N1, N2, N3})
          .payload(Payload("foo", 3)),
      TestRecord(LOG_ID, lsn_t(1), esn_t(0))
          .wave(2)
          .copyset({N4, N5, N6, N7})
          .flagAmend()
          .payload(Payload()),
  };

  storeRecords(test_data);
  auto cb_all = [](const std::vector<RawRecord>& rec) {
    ASSERT_EQ(1, rec.size());
    ASSERT_FALSE(parse(rec[0]).flags & FLAG_AMEND);
    ASSERT_EQ(2, parse(rec[0]).wave);
    ASSERT_EQ(std::vector<ShardID>({N4, N5, N6, N7}), parse(rec[0]).copyset);
  };
  auto cb_records_only = [](const std::vector<RawRecord>& rec) {
    ASSERT_EQ("foo", parse(rec[0]).payload);
  };
  verify(cb_all, cb_records_only);
}

// Record followed by amend with the same wave
TEST_F(RocksDBWriterMergeOperatorTest, OneWaveAmend) {
  std::vector<TestRecord> test_data = {
      TestRecord(LOG_ID, lsn_t(1), esn_t(0))
          .wave(1)
          .copyset({N1, N2, N3})
          .payload(Payload("foo", 3)),
      TestRecord(LOG_ID, lsn_t(1), esn_t(0))
          .wave(1)
          .copyset({N4, N5, N6, N7})
          .flagAmend()
          .payload(Payload()),
  };
  storeRecords(test_data);
  auto cb_all = [](const std::vector<RawRecord>& rec) {
    ASSERT_EQ(1, rec.size());
    ASSERT_FALSE(parse(rec[0]).flags & FLAG_AMEND);
    ASSERT_EQ(1, parse(rec[0]).wave);
    ASSERT_EQ(std::vector<ShardID>({N4, N5, N6, N7}), parse(rec[0]).copyset);
  };
  auto cb_records_only = [](const std::vector<RawRecord>& rec) {
    ASSERT_EQ("foo", parse(rec[0]).payload);
  };
  verify(cb_all, cb_records_only);
}

// A payloadless amend followed by a record with the same wave
TEST_F(RocksDBWriterMergeOperatorTest, OneWaveAmendReversed) {
  std::vector<TestRecord> test_data = {
      TestRecord(LOG_ID, lsn_t(1), esn_t(0))
          .wave(2)
          .copyset({N4, N5, N6, N7})
          .flagAmend()
          .payload(Payload()),
      TestRecord(LOG_ID, lsn_t(1), esn_t(0))
          .wave(2)
          .copyset({N1, N2, N3})
          .payload(Payload("foo", 3)),
  };
  storeRecords(test_data);
  auto cb_all = [](const std::vector<RawRecord>& rec) {
    ASSERT_EQ(1, rec.size());
    ASSERT_FALSE(parse(rec[0]).flags & FLAG_AMEND);
    ASSERT_EQ(2, parse(rec[0]).wave);
    ASSERT_EQ(std::vector<ShardID>({N1, N2, N3}), parse(rec[0]).copyset);
  };
  auto cb_records_only = [](const std::vector<RawRecord>& rec) {
    ASSERT_EQ("foo", parse(rec[0]).payload);
  };
  verify(cb_all, cb_records_only);
}

// Should be able to partially merge copyset amends
TEST_F(RocksDBWriterMergeOperatorTest, PartialMergeAmends) {
  std::vector<TestRecord> test_data = {
      TestRecord(LOG_ID, lsn_t(1), esn_t(0))
          .wave(3)
          .copyset({N3, N4, N5})
          .flagAmend()
          .payload(Payload()),
      TestRecord(LOG_ID, lsn_t(1), esn_t(0))
          .wave(2)
          .copyset({N2, N1})
          .flagAmend()
          .payload(Payload()),
  };
  storeRecords(test_data);

  // Iterator should skip the dangling amends for data records, however CSI
  // should register fine.
  auto cb_records_only = [](const std::vector<RawRecord>& rec) {
    ASSERT_EQ(0, rec.size());
  };
  auto cb_csi_only = [](const std::vector<RawRecord>& rec) {
    ASSERT_EQ(1, rec.size());
  };
  verify([](const std::vector<RawRecord>&) {}, cb_records_only, cb_csi_only);
}

TEST_F(RocksDBWriterMergeOperatorTest, SkipDanglingAmends) {
  std::vector<TestRecord> test_data = {
      TestRecord(LOG_ID, lsn_t(1), esn_t(0))
          .wave(3)
          .copyset({N3, N4, N5})
          .payload(Payload()),
      TestRecord(LOG_ID, lsn_t(2), esn_t(0))
          .wave(3)
          .copyset({N3, N4, N5})
          .flagAmend()
          .payload(Payload()),
      TestRecord(LOG_ID, lsn_t(2), esn_t(0))
          .wave(2)
          .copyset({N2, N1})
          .flagAmend()
          .payload(Payload()),
      TestRecord(LOG_ID, lsn_t(3), esn_t(0))
          .wave(3)
          .copyset({N3, N4, N5})
          .payload(Payload()),
      TestRecord(LOG_ID, lsn_t(4), esn_t(0))
          .wave(3)
          .copyset({N3, N4, N5})
          .flagAmend()
          .payload(Payload()),
  };
  storeRecords(test_data);

  for (int csi = 0; csi < 1; ++csi) {
    auto options = LocalLogStore::ReadOptions(
        "RocksDBWriterMergeOperatorTest.SkipDanglingAmends");
    if (csi) {
      options.allow_copyset_index = true;
      options.csi_data_only = true;
    };
    auto it = getStore().read(LOG_ID, options);
    it->seek(2);
    ASSERT_EQ(IteratorState::AT_RECORD, it->state());
    ASSERT_EQ(3, it->getLSN());

    it->prev();
    ASSERT_EQ(IteratorState::AT_RECORD, it->state());
    ASSERT_EQ(1, it->getLSN());

    it->prev();
    ASSERT_EQ(IteratorState::AT_END, it->state());

    it->seekForPrev(42);
    ASSERT_EQ(IteratorState::AT_RECORD, it->state());
    ASSERT_EQ(3, it->getLSN());

    it->next();
    ASSERT_EQ(IteratorState::AT_END, it->state());
  }
}

// epoch recovery should be able to amend an existing record with the seal_epoch
// and the FLAG_WRITTEN_BY_RECOVERY
TEST_F(RocksDBWriterMergeOperatorTest, AmendByEpochRecovery) {
  std::vector<TestRecord> test_data = {
      TestRecord(LOG_ID, lsn_t(1), esn_t(0))
          .wave(9999)
          .copyset({N4, N5, N6, N7})
          .payload(Payload("foo", 3)),
      TestRecord(LOG_ID, lsn_t(1), esn_t(0))
          .writtenByRecovery(epoch_t(2))
          .copyset({N1, N2, N3})
          .flagAmend()
          .payload(Payload()),
  };
  storeRecords(test_data);

  auto cb_all = [](const std::vector<RawRecord>& rec) {
    ASSERT_EQ(1, rec.size());
    ASSERT_FALSE(parse(rec[0]).flags & FLAG_AMEND);
    // the new record must be marked as written by recovery
    ASSERT_TRUE(parse(rec[0]).flags & FLAG_WRITTEN_BY_RECOVERY);
    // despite that the previous record has wave of 9999, the recovery store
    // must take the precedence
    ASSERT_EQ(2, parse(rec[0]).wave);
    // copyset should also be amended
    ASSERT_EQ(std::vector<ShardID>({N1, N2, N3}), parse(rec[0]).copyset);
  };
  auto cb_records_only = [](const std::vector<RawRecord>& rec) {
    ASSERT_EQ("foo", parse(rec[0]).payload);
  };
  verify(cb_all, cb_records_only);
}

// epoch recovery should be able to overwrite a hole written by a previous epoch
// recovery instance with a record
TEST_F(RocksDBWriterMergeOperatorTest, EpochRecoveryRecordOverwriteHole) {
  std::vector<TestRecord> test_data = {
      TestRecord(LOG_ID, lsn_t(1), esn_t(0))
          .writtenByRecovery(epoch_t(2))
          .hole()
          .copyset({N4, N5, N6, N7})
          .payload(Payload()),
      TestRecord(LOG_ID, lsn_t(1), esn_t(0))
          .writtenByRecovery(epoch_t(5))
          .copyset({N1, N2, N3})
          .payload(Payload("foo", 3)),
  };
  storeRecords(test_data);

  auto cb_all = [](const std::vector<RawRecord>& rec) {
    ASSERT_EQ(1, rec.size());
    ASSERT_FALSE(parse(rec[0]).flags & FLAG_AMEND);
    // the new record must be marked as written by recovery
    ASSERT_TRUE(parse(rec[0]).flags & FLAG_WRITTEN_BY_RECOVERY);
    // the new record must not be a hole
    ASSERT_FALSE(parse(rec[0]).flags & FLAG_HOLE);
    // the new record must be written by recovery with seal epoch == 5
    ASSERT_EQ(5, parse(rec[0]).wave);
    ASSERT_EQ(std::vector<ShardID>({N1, N2, N3}), parse(rec[0]).copyset);
  };
  auto cb_records_only = [](const std::vector<RawRecord>& rec) {
    ASSERT_EQ("foo", parse(rec[0]).payload);
  };
  verify(cb_all, cb_records_only);
}

// epoch recovery should be able to overwrite a bridge record  written by
// a previous epoch recovery instance with a normal hole record
TEST_F(RocksDBWriterMergeOperatorTest, EpochRecoveryRecordOverwriteBridge) {
  std::vector<TestRecord> test_data = {
      TestRecord(LOG_ID, lsn_t(1), esn_t(3))
          .writtenByRecovery(epoch_t(2))
          .bridge()
          .copyset({N4, N5, N6, N7})
          .payload(Payload()),
      TestRecord(LOG_ID, lsn_t(1), esn_t(3))
          .writtenByRecovery(epoch_t(5))
          .copyset({N1, N2, N3})
          .hole(),
  };
  storeRecords(test_data);

  auto cb_all = [](const std::vector<RawRecord>& rec) {
    ASSERT_EQ(1, rec.size());
    ASSERT_FALSE(parse(rec[0]).flags & FLAG_AMEND);
    // the new record must be written by recovery with seal epoch == 5
    ASSERT_TRUE(parse(rec[0]).flags & FLAG_WRITTEN_BY_RECOVERY);
    ASSERT_TRUE(parse(rec[0]).flags & FLAG_HOLE);
    ASSERT_FALSE(parse(rec[0]).flags & FLAG_BRIDGE);
    ASSERT_EQ(5, parse(rec[0]).wave);
    ASSERT_EQ(std::vector<ShardID>({N1, N2, N3}), parse(rec[0]).copyset);
  };
  auto cb_records_only = [](const std::vector<RawRecord>&) {};
  verify(cb_all, cb_records_only);
}

// epoch recovery should be not be able to amend an existing record written by
// recovery with higher seal epoch with amends or stores
TEST_F(RocksDBWriterMergeOperatorTest, DeniedAmendAndStore) {
  std::vector<TestRecord> test_data = {
      TestRecord(LOG_ID, lsn_t(1), esn_t(0))
          .writtenByRecovery(epoch_t(25))
          .copyset({N1, N2, N3, N5})
          .payload(Payload("foo", 3)),
      TestRecord(LOG_ID, lsn_t(1), esn_t(0)) // regular store
          .wave(9999)
          .copyset({N5, N6, N7})
          .payload(Payload("bar", 3)),
      TestRecord(LOG_ID, lsn_t(1), esn_t(0)) // regular amend
          .wave(9999)
          .flagAmend()
          .copyset({N5, N6, N7, N8, N9})
          .payload(Payload()),
      TestRecord(LOG_ID, lsn_t(1), esn_t(0)) // recovery amend of smaller seal
          .writtenByRecovery(epoch_t(9))
          .copyset({N4, N5, N6})
          .flagAmend()
          .payload(Payload()),
  };
  storeRecords(test_data);

  auto cb_all = [](const std::vector<RawRecord>& rec) {
    ASSERT_EQ(1, rec.size());
    ASSERT_FALSE(parse(rec[0]).flags & FLAG_AMEND);
    ASSERT_TRUE(parse(rec[0]).flags & FLAG_WRITTEN_BY_RECOVERY);
    ASSERT_EQ(25, parse(rec[0]).wave);
    // copyset should also be amended
    ASSERT_EQ(std::vector<ShardID>({N1, N2, N3, N5}), parse(rec[0]).copyset);
  };
  auto cb_records_only = [](const std::vector<RawRecord>& rec) {
    ASSERT_EQ("foo", parse(rec[0]).payload);
  };
  verify(cb_all, cb_records_only);
}

// epoch recovery should be able to amend byteoffset
TEST_F(RocksDBWriterMergeOperatorTest, EpochRecoveryOverrideByteOffset) {
  std::vector<TestRecord> test_data = {
      TestRecord(LOG_ID, lsn_t(1), esn_t(0))
          .writtenByRecovery(epoch_t(2))
          .copyset({N2, N3})
          .payload(Payload("foo", 3))
          .offsetsWithinEpoch(OffsetMap({{BYTE_OFFSET, 10}})),
      TestRecord(LOG_ID, lsn_t(1), esn_t(0))
          .writtenByRecovery(epoch_t(5))
          .copyset({N1, N2, N3})
          .payload(Payload())
          .flagAmend()
          .offsetsWithinEpoch(OffsetMap({{BYTE_OFFSET, 15}}))};
  storeRecords(test_data);

  auto cb_all = [](const std::vector<RawRecord>& rec) {
    ASSERT_EQ(1, rec.size());
    ASSERT_FALSE(parse(rec[0]).flags & FLAG_AMEND);
    // the new record must be marked as written by recovery
    ASSERT_TRUE(parse(rec[0]).flags & FLAG_WRITTEN_BY_RECOVERY);
    // the new record must be written by recovery with seal epoch == 5
    ASSERT_EQ(5, parse(rec[0]).wave);
  };
  auto cb_records_only = [](const std::vector<RawRecord>& rec) {
    ASSERT_EQ("foo", parse(rec[0]).payload);
    ASSERT_EQ(
        OffsetMap({{BYTE_OFFSET, 15}}), parse(rec[0]).offsets_within_epoch);
  };
  verify(cb_all, cb_records_only);
}

// if FLAG_OFFSET_WITHIN_EPOCH not set, recovery shouldn't amend byteoffset
TEST_F(RocksDBWriterMergeOperatorTest, EpochRecoveryNotOverrideByteOffset) {
  std::vector<TestRecord> test_data = {
      TestRecord(LOG_ID, lsn_t(1), esn_t(0))
          .writtenByRecovery(epoch_t(2))
          .copyset({N2, N3})
          .payload(Payload("foo", 3))
          .offsetsWithinEpoch(OffsetMap({{BYTE_OFFSET, 10}})),
      TestRecord(LOG_ID, lsn_t(1), esn_t(0))
          .writtenByRecovery(epoch_t(5))
          .copyset({N1, N2, N3})
          .payload(Payload())
          .flagAmend(),
  };
  storeRecords(test_data);

  auto cb_all = [](const std::vector<RawRecord>& rec) {
    ASSERT_EQ(1, rec.size());
    ASSERT_FALSE(parse(rec[0]).flags & FLAG_AMEND);
    // the new record must be marked as written by recovery
    ASSERT_TRUE(parse(rec[0]).flags & FLAG_WRITTEN_BY_RECOVERY);
    // the new record must be written by recovery with seal epoch == 5
    ASSERT_EQ(5, parse(rec[0]).wave);
  };
  auto cb_records_only = [](const std::vector<RawRecord>& rec) {
    ASSERT_EQ("foo", parse(rec[0]).payload);
    ASSERT_EQ(
        OffsetMap({{BYTE_OFFSET, 10}}), parse(rec[0]).offsets_within_epoch);
  };
  verify(cb_all, cb_records_only);
}

TEST_F(RocksDBWriterMergeOperatorTest, CumulativeFlags) {
  // Write a few merge operands, flushing after each one to make sure rocksdb
  // merges them all in one call.
  // The sequence of operations is probably not realistic, but why not.

  // Full record from rebuilding+recovery.
  // The only record having rebuilding flag; the flag should make it into
  // the final merge result.
  // Payload will be overridden by the full record below.
  storeRecords({TestRecord(LOG_ID, lsn_t(1), esn_t(0))
                    .writtenByRebuilding()
                    .flagWrittenByRecovery()
                    .copyset({N2, N3})
                    .payload(Payload("foo", 3))});
  getStore().sync(Durability::MEMORY);
  // Amend from recovery. Will be overriddem by the record and amend below.
  storeRecords({TestRecord(LOG_ID, lsn_t(1), esn_t(0))
                    .flagWrittenByRecovery()
                    .copyset({N2, N3})
                    .flagAmend()
                    .payload(Payload())});
  getStore().sync(Durability::MEMORY);
  // Full record from recovery. Overrides the payload.
  storeRecords({TestRecord(LOG_ID, lsn_t(1), esn_t(0))
                    .flagWrittenByRecovery()
                    .copyset({N2, N3})
                    .payload(Payload("pikachu", 7))});
  getStore().sync(Durability::MEMORY);
  // Amend from recovery. Overrides copyset.
  storeRecords({TestRecord(LOG_ID, lsn_t(1), esn_t(0))
                    .flagWrittenByRecovery()
                    .copyset({N1})
                    .flagAmend()
                    .payload(Payload())});
  getStore().sync(Durability::MEMORY);
  // Amend *not* from recovery. Overridden by the amend from recovery above.
  storeRecords({TestRecord(LOG_ID, lsn_t(1), esn_t(0))
                    .copyset({N2, N3})
                    .flagAmend()
                    .payload(Payload())});

  verify(
      [](const std::vector<RawRecord>& recs) {
        ASSERT_EQ(1, recs.size());
        ParseResult p = parse(recs[0]);
        auto expected_flags =
            LocalLogStoreRecordFormat::FLAG_WRITTEN_BY_RECOVERY |
            LocalLogStoreRecordFormat::FLAG_WRITTEN_BY_REBUILDING;
        EXPECT_EQ(
            LocalLogStoreRecordFormat::flagsToString(expected_flags),
            LocalLogStoreRecordFormat::flagsToString(p.flags & expected_flags));
        EXPECT_EQ(1, p.wave);

        EXPECT_EQ(std::vector<ShardID>({N1}), p.copyset);
      },
      [](const std::vector<RawRecord>& recs) {
        EXPECT_EQ("pikachu", parse(recs.at(0)).payload);
      },
      [](const std::vector<RawRecord>& recs) {
        EXPECT_EQ("", parse(recs.at(0)).payload);
      });
}

} // namespace
