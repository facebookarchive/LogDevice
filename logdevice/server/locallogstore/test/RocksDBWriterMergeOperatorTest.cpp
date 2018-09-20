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
  uint64_t offset_within_epoch;
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
                                            &ret.offset_within_epoch,
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

TEST(RocksDBWriterMergeOperatorTest, SingleWave) {
  TemporaryRocksDBStore store;

  std::vector<TestRecord> test_data = {
      TestRecord(LOG_ID, lsn_t(1), esn_t(0))
          .wave(1)
          .copyset({N1, N2, N3})
          .payload(Payload("foo", 3)),
  };
  store_fill(store, test_data);

  std::vector<RawRecord> rec;
  Status st =
      test::LocalLogStoreTestReader().logID(LOG_ID).process(&store, rec);
  ASSERT_EQ(E::UNTIL_LSN_REACHED, st);

  ASSERT_EQ(1, rec.size());
  ASSERT_FALSE(parse(rec[0]).flags & FLAG_AMEND);
  ASSERT_EQ(1, parse(rec[0]).wave);
  ASSERT_EQ("foo", parse(rec[0]).payload);
  ASSERT_EQ(std::vector<ShardID>({N1, N2, N3}), parse(rec[0]).copyset);
}

// With multiple waves containing the payload, must take the later wave
TEST(RocksDBWriterMergeOperatorTest, TwoWavesWithPayload) {
  TemporaryRocksDBStore store;

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
  store_fill(store, test_data);

  std::vector<RawRecord> rec;
  Status st =
      test::LocalLogStoreTestReader().logID(LOG_ID).process(&store, rec);
  ASSERT_EQ(E::UNTIL_LSN_REACHED, st);

  ASSERT_EQ(1, rec.size());
  ASSERT_FALSE(parse(rec[0]).flags & FLAG_AMEND);
  ASSERT_EQ(2, parse(rec[0]).wave);
  ASSERT_EQ("foo", parse(rec[0]).payload);
  ASSERT_EQ(std::vector<ShardID>({N4, N5, N6, N7}), parse(rec[0]).copyset);
}

// Must take later wave+copyset even if STOREs get reordered
TEST(RocksDBWriterMergeOperatorTest, TwoWavesReordered) {
  TemporaryRocksDBStore store;

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
  store_fill(store, test_data);

  std::vector<RawRecord> rec;
  Status st =
      test::LocalLogStoreTestReader().logID(LOG_ID).process(&store, rec);
  ASSERT_EQ(E::UNTIL_LSN_REACHED, st);

  ASSERT_EQ(1, rec.size());
  ASSERT_FALSE(parse(rec[0]).flags & FLAG_AMEND);
  ASSERT_EQ(2, parse(rec[0]).wave);
  ASSERT_EQ("foo", parse(rec[0]).payload);
  ASSERT_EQ(std::vector<ShardID>({N4, N5, N6, N7}), parse(rec[0]).copyset);
}

// A simple case where a proper first wave is followed by a payloadless amend
TEST(RocksDBWriterMergeOperatorTest, TwoWavesAmend) {
  TemporaryRocksDBStore store;

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
  store_fill(store, test_data);

  std::vector<RawRecord> rec;
  Status st =
      test::LocalLogStoreTestReader().logID(LOG_ID).process(&store, rec);
  ASSERT_EQ(E::UNTIL_LSN_REACHED, st);

  ASSERT_EQ(1, rec.size());
  ASSERT_FALSE(parse(rec[0]).flags & FLAG_AMEND);
  ASSERT_EQ(2, parse(rec[0]).wave);
  ASSERT_EQ("foo", parse(rec[0]).payload);
  ASSERT_EQ(std::vector<ShardID>({N4, N5, N6, N7}), parse(rec[0]).copyset);
}

// Record followed by amend with the same wave
TEST(RocksDBWriterMergeOperatorTest, OneWaveAmend) {
  TemporaryRocksDBStore store;

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
  store_fill(store, test_data);

  std::vector<RawRecord> rec;
  Status st =
      test::LocalLogStoreTestReader().logID(LOG_ID).process(&store, rec);
  ASSERT_EQ(E::UNTIL_LSN_REACHED, st);

  ASSERT_EQ(1, rec.size());
  ASSERT_FALSE(parse(rec[0]).flags & FLAG_AMEND);
  ASSERT_EQ(1, parse(rec[0]).wave);
  ASSERT_EQ("foo", parse(rec[0]).payload);
  ASSERT_EQ(std::vector<ShardID>({N4, N5, N6, N7}), parse(rec[0]).copyset);
}

// A payloadless amend followed by a record with the same wave
TEST(RocksDBWriterMergeOperatorTest, OneWaveAmendReversed) {
  TemporaryRocksDBStore store;

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
  store_fill(store, test_data);

  std::vector<RawRecord> rec;
  Status st =
      test::LocalLogStoreTestReader().logID(LOG_ID).process(&store, rec);
  ASSERT_EQ(E::UNTIL_LSN_REACHED, st);

  ASSERT_EQ(1, rec.size());
  ASSERT_FALSE(parse(rec[0]).flags & FLAG_AMEND);
  ASSERT_EQ(2, parse(rec[0]).wave);
  ASSERT_EQ("foo", parse(rec[0]).payload);
  ASSERT_EQ(std::vector<ShardID>({N1, N2, N3}), parse(rec[0]).copyset);
}

// Should be able to partially merge copyset amends
TEST(RocksDBWriterMergeOperatorTest, PartialMergeAmends) {
  TemporaryRocksDBStore store;

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
  store_fill(store, test_data);

  // Iterator should skip the dangling amends.
  std::vector<RawRecord> rec;
  Status st =
      test::LocalLogStoreTestReader().logID(LOG_ID).process(&store, rec);
  ASSERT_EQ(E::UNTIL_LSN_REACHED, st);

  ASSERT_EQ(0, rec.size());
}

TEST(RocksDBWriterMergeOperatorTest, SkipDanglingAmends) {
  TemporaryRocksDBStore store;

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
  store_fill(store, test_data);

  auto it =
      store.read(LOG_ID,
                 LocalLogStore::ReadOptions(
                     "RocksDBWriterMergeOperatorTest.SkipDanglingAmends"));
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

// epoch recovery should be able to amend an existing record with the seal_epoch
// and the FLAG_WRITTEN_BY_RECOVERY
TEST(RocksDBWriterMergeOperatorTest, AmendByEpochRecovery) {
  TemporaryRocksDBStore store;

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
  store_fill(store, test_data);

  std::vector<RawRecord> rec;
  Status st =
      test::LocalLogStoreTestReader().logID(LOG_ID).process(&store, rec);
  ASSERT_EQ(E::UNTIL_LSN_REACHED, st);

  ASSERT_EQ(1, rec.size());
  ASSERT_FALSE(parse(rec[0]).flags & FLAG_AMEND);
  // the new record must be marked as written by recovery
  ASSERT_TRUE(parse(rec[0]).flags & FLAG_WRITTEN_BY_RECOVERY);
  // despite that the previous record has wave of 9999, the recovery store must
  // take the precedence
  ASSERT_EQ(2, parse(rec[0]).wave);
  ASSERT_EQ("foo", parse(rec[0]).payload);
  // copyset should also be amended
  ASSERT_EQ(std::vector<ShardID>({N1, N2, N3}), parse(rec[0]).copyset);
}

// epoch recovery should be able to overwrite a hole written by a previous epoch
// recovery instance with a record
TEST(RocksDBWriterMergeOperatorTest, EpochRecoveryRecordOverwriteHole) {
  TemporaryRocksDBStore store;

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
  store_fill(store, test_data);

  std::vector<RawRecord> rec;
  Status st =
      test::LocalLogStoreTestReader().logID(LOG_ID).process(&store, rec);
  ASSERT_EQ(E::UNTIL_LSN_REACHED, st);

  ASSERT_EQ(1, rec.size());
  ASSERT_FALSE(parse(rec[0]).flags & FLAG_AMEND);
  // the new record must be marked as written by recovery
  ASSERT_TRUE(parse(rec[0]).flags & FLAG_WRITTEN_BY_RECOVERY);
  // the new record must not be a hole
  ASSERT_FALSE(parse(rec[0]).flags & FLAG_HOLE);
  // the new record must be written by recovery with seal epoch == 5
  ASSERT_EQ(5, parse(rec[0]).wave);
  ASSERT_EQ("foo", parse(rec[0]).payload);
  ASSERT_EQ(std::vector<ShardID>({N1, N2, N3}), parse(rec[0]).copyset);
}

// epoch recovery should be able to overwrite a bridge record  written by
// a previous epoch recovery instance with a normal hole record
TEST(RocksDBWriterMergeOperatorTest, EpochRecoveryRecordOverwriteBridge) {
  TemporaryRocksDBStore store;

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
  store_fill(store, test_data);

  std::vector<RawRecord> rec;
  Status st =
      test::LocalLogStoreTestReader().logID(LOG_ID).process(&store, rec);
  ASSERT_EQ(E::UNTIL_LSN_REACHED, st);

  ASSERT_EQ(1, rec.size());
  ASSERT_FALSE(parse(rec[0]).flags & FLAG_AMEND);
  // the new record must be written by recovery with seal epoch == 5
  ASSERT_TRUE(parse(rec[0]).flags & FLAG_WRITTEN_BY_RECOVERY);
  ASSERT_TRUE(parse(rec[0]).flags & FLAG_HOLE);
  ASSERT_FALSE(parse(rec[0]).flags & FLAG_BRIDGE);
  ASSERT_EQ(5, parse(rec[0]).wave);
  ASSERT_EQ(std::vector<ShardID>({N1, N2, N3}), parse(rec[0]).copyset);
}

// epoch recovery should be not be able to amend an existing record written by
// recovery with higher seal epoch with amends or stores
TEST(RocksDBWriterMergeOperatorTest, DeniedAmendAndStore) {
  TemporaryRocksDBStore store;

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
  store_fill(store, test_data);

  std::vector<RawRecord> rec;
  Status st =
      test::LocalLogStoreTestReader().logID(LOG_ID).process(&store, rec);
  ASSERT_EQ(E::UNTIL_LSN_REACHED, st);

  ASSERT_EQ(1, rec.size());
  ASSERT_FALSE(parse(rec[0]).flags & FLAG_AMEND);
  ASSERT_TRUE(parse(rec[0]).flags & FLAG_WRITTEN_BY_RECOVERY);
  ASSERT_EQ(25, parse(rec[0]).wave);
  ASSERT_EQ("foo", parse(rec[0]).payload);
  // copyset should also be amended
  ASSERT_EQ(std::vector<ShardID>({N1, N2, N3, N5}), parse(rec[0]).copyset);
}

// epoch recovery should be able to amend byteoffset
TEST(RocksDBWriterMergeOperatorTest, EpochRecoveryOverrideByteOffset) {
  TemporaryRocksDBStore store;

  std::vector<TestRecord> test_data = {TestRecord(LOG_ID, lsn_t(1), esn_t(0))
                                           .writtenByRecovery(epoch_t(2))
                                           .copyset({N2, N3})
                                           .payload(Payload("foo", 3))
                                           .offsetWithinEpoch(10),
                                       TestRecord(LOG_ID, lsn_t(1), esn_t(0))
                                           .writtenByRecovery(epoch_t(5))
                                           .copyset({N1, N2, N3})
                                           .payload(Payload())
                                           .flagAmend()
                                           .offsetWithinEpoch(15)};
  store_fill(store, test_data);

  std::vector<RawRecord> rec;
  Status st =
      test::LocalLogStoreTestReader().logID(LOG_ID).process(&store, rec);
  ASSERT_EQ(E::UNTIL_LSN_REACHED, st);

  ASSERT_EQ(1, rec.size());
  ASSERT_FALSE(parse(rec[0]).flags & FLAG_AMEND);
  // the new record must be marked as written by recovery
  ASSERT_TRUE(parse(rec[0]).flags & FLAG_WRITTEN_BY_RECOVERY);
  // the new record must be written by recovery with seal epoch == 5
  ASSERT_EQ(5, parse(rec[0]).wave);
  ASSERT_EQ("foo", parse(rec[0]).payload);
  ASSERT_EQ(15, parse(rec[0]).offset_within_epoch);
}

// if FLAG_OFFSET_WITHIN_EPOCH not set, recovery shouldn't amend byteoffset
TEST(RocksDBWriterMergeOperatorTest, EpochRecoveryNotOverrideByteOffset) {
  TemporaryRocksDBStore store;

  std::vector<TestRecord> test_data = {
      TestRecord(LOG_ID, lsn_t(1), esn_t(0))
          .writtenByRecovery(epoch_t(2))
          .copyset({N2, N3})
          .payload(Payload("foo", 3))
          .offsetWithinEpoch(10),
      TestRecord(LOG_ID, lsn_t(1), esn_t(0))
          .writtenByRecovery(epoch_t(5))
          .copyset({N1, N2, N3})
          .payload(Payload())
          .flagAmend(),
  };
  store_fill(store, test_data);

  std::vector<RawRecord> rec;
  Status st =
      test::LocalLogStoreTestReader().logID(LOG_ID).process(&store, rec);
  ASSERT_EQ(E::UNTIL_LSN_REACHED, st);

  ASSERT_EQ(1, rec.size());
  ASSERT_FALSE(parse(rec[0]).flags & FLAG_AMEND);
  // the new record must be marked as written by recovery
  ASSERT_TRUE(parse(rec[0]).flags & FLAG_WRITTEN_BY_RECOVERY);
  // the new record must be written by recovery with seal epoch == 5
  ASSERT_EQ(5, parse(rec[0]).wave);
  ASSERT_EQ("foo", parse(rec[0]).payload);
  ASSERT_EQ(10, parse(rec[0]).offset_within_epoch);
}

} // namespace
