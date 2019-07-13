/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/server/locallogstore/RocksDBLocalLogStore.h"

#include <memory>

#include <folly/Memory.h>
#include <folly/ScopeGuard.h>
#include <gtest/gtest.h>

#include "logdevice/common/LocalLogStoreRecordFormat.h"
#include "logdevice/common/Metadata.h"
#include "logdevice/common/configuration/InternalLogs.h"
#include "logdevice/common/debug.h"
#include "logdevice/common/types_internal.h"
#include "logdevice/include/Err.h"
#include "logdevice/include/types.h"
#include "logdevice/server/locallogstore/LocalLogStore.h"
#include "logdevice/server/locallogstore/PartitionedRocksDBStore.h"
#include "logdevice/server/locallogstore/WriteOps.h"
#include "logdevice/server/locallogstore/test/TemporaryLogStore.h"
#include "rocksdb/db.h"
#include "rocksdb/version.h"

using namespace facebook::logdevice;

namespace {

class CustomEnv : public rocksdb::EnvWrapper {
 public:
  class WritableFile : public rocksdb::WritableFileWrapper {
   public:
    WritableFile(CustomEnv* owner,
                 std::unique_ptr<rocksdb::WritableFile> target)
        : rocksdb::WritableFileWrapper(target.get()),
          owner_(owner),
          target_(std::move(target)) {}

    rocksdb::Status Append(const rocksdb::Slice& data) override {
      if (owner_->fail_writes_.load()) {
        return rocksdb::Status::IOError("test write I/O error");
      }
      return target_->Append(data);
    }

    rocksdb::Status Sync() override {
      if (owner_->fail_syncs_.load()) {
        return rocksdb::Status::IOError("test sync I/O error");
      }
      return target_->Sync();
    }

    rocksdb::Status Fsync() override {
      if (owner_->fail_syncs_.load()) {
        return rocksdb::Status::IOError("test fsync I/O error");
      }
      return target_->Fsync();
    }

   protected:
    rocksdb::Status Allocate(uint64_t /*offset*/, uint64_t /*len*/) override {
      return rocksdb::Status::OK();
    }
    rocksdb::Status RangeSync(uint64_t /*offset*/,
                              uint64_t /*nbytes*/) override {
      return rocksdb::Status::OK();
    }

   private:
    CustomEnv* owner_;
    std::unique_ptr<rocksdb::WritableFile> target_;
  };

  CustomEnv() : EnvWrapper(rocksdb::Env::Default()) {}

  rocksdb::Status NewWritableFile(const std::string& f,
                                  std::unique_ptr<rocksdb::WritableFile>* r,
                                  const rocksdb::EnvOptions& options) override {
    rocksdb::Status s = EnvWrapper::NewWritableFile(f, r, options);
    if (s.ok()) {
      r->reset(new WritableFile(this, std::move(*r)));
    }
    return s;
  }

  std::atomic<bool> fail_writes_{false};
  std::atomic<bool> fail_syncs_{false};
};

class RocksDBLocalLogStoreTest : public ::testing::Test {
 public:
  RocksDBLocalLogStoreTest()
      : stats_(StatsParams().setIsServer(true)),
        rocksdb_config_(UpdateableSettings<RocksDBSettings>(
                            RocksDBSettings::defaultTestSettings()),
                        UpdateableSettings<RebuildingSettings>(),
                        &env_,
                        nullptr,
                        &stats_) {
    rocksdb_config_.createMergeOperator(0);
    dbg::assertOnData = true;
    dbg::currentLevel = getLogLevelFromEnv().value_or(dbg::Level::INFO);
  }

  std::unique_ptr<LocalLogStore> createRocksDBLocalLogStore() {
    return std::make_unique<TemporaryLogStore>([&](std::string path) {
      return std::make_unique<RocksDBLocalLogStore>(
          0, 1, path, rocksdb_config_, &stats_, /* io_tracing */ nullptr);
    });
  }

  std::unique_ptr<LocalLogStore> createPartitionedRocksDBStore() {
    return std::make_unique<TemporaryLogStore>([&](std::string path) {
      return std::make_unique<PartitionedRocksDBStore>(
          0,
          1,
          path,
          rocksdb_config_,
          nullptr,
          &stats_,
          /* io_tracing */ nullptr);
    });
  }

  CustomEnv env_;
  StatsHolder stats_;
  RocksDBLogStoreConfig rocksdb_config_;
};

// A helper macro that generates tests for all supported log store types
#define STORE_TEST(test_case, name, store)                     \
  class test_case##_##name : public test_case {                \
   public:                                                     \
    void test(LocalLogStore& store);                           \
  };                                                           \
  TEST_F(test_case##_##name, name##_RocksDBLocalLogStore) {    \
    auto store = createRocksDBLocalLogStore();                 \
    test(*store);                                              \
  }                                                            \
  TEST_F(test_case##_##name, name##_PartitionedRocksDBStore) { \
    auto store = createPartitionedRocksDBStore();              \
    test(*store);                                              \
  }                                                            \
  void test_case##_##name ::test(LocalLogStore& store)

std::string makeRecordHeader() {
  std::string s;
  ShardID cs[2] = {ShardID(2, 0), ShardID(3, 0)};
  Slice r = LocalLogStoreRecordFormat::formRecordHeader(
      123456789012345l,
      esn_t(42),
      LocalLogStoreRecordFormat::FLAG_CHECKSUM_PARITY,
      0,
      folly::Range<const ShardID*>(cs, cs + 2),
      OffsetMap(),
      std::map<KeyType, std::string>(),
      &s);
  ld_check(r.size > 0);
  ld_check(r.size == s.size());
  return s;
}

// Reurns some record header acceptable by PartitionedRocksDBStore and
// RocksDBCompactionFilter.
Slice getHeader() {
  static std::string h = makeRecordHeader();
  return Slice(h.data(), h.size());
}

/**
 * Write a record and check rocksdb contents directly.
 */
TEST_F(RocksDBLocalLogStoreTest, WriteTest) {
  TemporaryRocksDBStore store;

  // Write a record to the store and close it
  PutWriteOp op{logid_t(123),
                511,
                getHeader(),
                Slice("abc1", 4),
                /*coordinator*/ folly::none,
                folly::none,
                Slice(nullptr, 0),
                {},
                Durability::ASYNC_WRITE,
                false};
  ASSERT_EQ(0, store.writeMulti(std::vector<const WriteOp*>{&op}));
  store.close();

  // Open rocksdb database directly and check contents
  rocksdb::DB* db;
  rocksdb::Status status =
      rocksdb::DB::Open(rocksdb_config_.options_, store.getPath(), &db);
  ASSERT_TRUE(status.ok()) << status.ToString();

  SCOPE_EXIT {
    delete db;
  };

  int keys_seen = 0;
  std::unique_ptr<rocksdb::Iterator> it(
      db->NewIterator(RocksDBLogStoreBase::getDefaultReadOptions()));
  for (it->SeekToFirst(); it->Valid(); it->Next()) {
    if (it->key().size() == strlen("schema_version") &&
        memcmp(it->key().data(), "schema_version", strlen("schema_version")) ==
            0) {
      continue;
    }

    // clang-format off
    unsigned char expected_key[] = {
        'd',                                  // header
        0,    0,    0,    0,    0, 0, 0, 123, // log id
        0,    0,    0,    0,    0, 0, 1, 255, // lsn
    };
    // clang-format on
    static_assert(sizeof expected_key == 17, "must be 17 bytes");
    EXPECT_EQ(17, it->key().size());
    EXPECT_EQ(0, memcmp(it->key().data(), expected_key, sizeof expected_key));
    EXPECT_EQ(getHeader().size + strlen("abc1"), it->value().size());
    EXPECT_EQ(
        0, memcmp(it->value().data(), getHeader().data, getHeader().size));
    ++keys_seen;
  }
  EXPECT_TRUE(it->status().ok());
  EXPECT_EQ(1, keys_seen);
}

static void
verifyRecord(const char* expected,
             const std::unique_ptr<LocalLogStore::ReadIterator>& it) {
  // Uses std::string for better test output
  const char* ptr = reinterpret_cast<const char*>(it->getRecord().data);
  const size_t header_size = getHeader().size;
  ASSERT_GE(it->getRecord().size, header_size);
  EXPECT_EQ(std::string(expected),
            std::string(ptr + header_size, ptr + it->getRecord().size));
}

/**
 * Test iterators over all logs.
 */
STORE_TEST(RocksDBLocalLogStoreTest, AllLogsIterators, store) {
  std::vector<PutWriteOp> put_ops{
      PutWriteOp{configuration::InternalLogs::CONFIG_LOG_SNAPSHOTS,
                 1,
                 getHeader(),
                 Slice("abccls1", 5),
                 folly::none,
                 folly::none,
                 Slice(nullptr, 0),
                 {},
                 Durability::ASYNC_WRITE,
                 false},
      PutWriteOp{MetaDataLog::metaDataLogID(logid_t(1)),
                 1,
                 getHeader(),
                 Slice("abcm11", 5),
                 folly::none,
                 folly::none,
                 Slice(nullptr, 0),
                 {},
                 Durability::ASYNC_WRITE,
                 false},
      PutWriteOp{logid_t(2),
                 1,
                 getHeader(),
                 Slice("abc21", 5),
                 folly::none,
                 folly::none,
                 Slice(nullptr, 0),
                 {},
                 Durability::ASYNC_WRITE,
                 false},
      PutWriteOp{logid_t(2),
                 5,
                 getHeader(),
                 Slice("abc25", 5),
                 folly::none,
                 folly::none,
                 Slice(nullptr, 0),
                 {},
                 Durability::ASYNC_WRITE,
                 false},
      PutWriteOp{logid_t(1),
                 6,
                 getHeader(),
                 Slice("abc16", 5),
                 folly::none,
                 folly::none,
                 Slice(nullptr, 0),
                 {},
                 Durability::ASYNC_WRITE,
                 false},
      PutWriteOp{logid_t(2),
                 4,
                 getHeader(),
                 Slice("abc24", 5),
                 folly::none,
                 folly::none,
                 Slice(nullptr, 0),
                 {},
                 Durability::ASYNC_WRITE,
                 false},
  };

  // Write some records, including an internal log record and a metadata log
  // record.

  std::vector<const WriteOp*> ops1{&put_ops[0], &put_ops[1], &put_ops[2]};
  std::vector<const WriteOp*> ops2{&put_ops[3], &put_ops[4], &put_ops[5]};

  ASSERT_EQ(0, store.writeMulti(ops1));
  ASSERT_EQ(0, store.writeMulti(ops2));

  std::map<std::pair<logid_t::raw_type, lsn_t>, std::string> written_records;
  for (const auto& op : put_ops) {
    written_records[std::make_pair(op.log_id.val(), op.lsn)] =
        std::string(static_cast<const char*>(op.data.data), op.data.size);
  }

  // Read everything using an AllLogsIterator.

  auto iterator = store.readAllLogs(
      LocalLogStore::ReadOptions("AllLogsIterators"), folly::none);
  std::map<std::pair<logid_t::raw_type, lsn_t>, std::string> read_records;
  const size_t header_size = getHeader().size;
  for (iterator->seek(*iterator->minLocation());
       iterator->state() == IteratorState::AT_RECORD;
       iterator->next()) {
    auto p = std::make_pair(iterator->getLogID().val_, iterator->getLSN());
    EXPECT_EQ(0, read_records.count(p));
    ASSERT_GE(iterator->getRecord().size, header_size);
    read_records[p] = std::string(iterator->getRecord().ptr() + header_size,
                                  iterator->getRecord().size - header_size);
  }
  EXPECT_EQ(written_records, read_records);
  EXPECT_EQ(IteratorState::AT_END, iterator->state());

  // Read only the metadata log record.

  iterator->seek(*iterator->metadataLogsBegin());
  ASSERT_EQ(IteratorState::AT_RECORD, iterator->state());
  EXPECT_EQ(MetaDataLog::metaDataLogID(logid_t(1)), iterator->getLogID());
  EXPECT_EQ(1, iterator->getLSN());
  ASSERT_GE(iterator->getRecord().size, header_size);
  EXPECT_EQ(written_records.at(
                std::make_pair(MetaDataLog::metaDataLogID(logid_t(1)).val_, 1)),
            std::string(iterator->getRecord().ptr() + header_size,
                        iterator->getRecord().size - header_size));
  iterator->next();
  EXPECT_TRUE(iterator->state() == IteratorState::AT_END ||
              !MetaDataLog::isMetaDataLog(iterator->getLogID()));
}

/**
 * Write a few records and read them back.  This verifies that ReadIterator
 * works as advertised and that records come back in the right order.
 */
STORE_TEST(RocksDBLocalLogStoreTest, WriteReadBackTest, store) {
  std::vector<PutWriteOp> put_ops{
      PutWriteOp{logid_t(2),
                 1,
                 getHeader(),
                 Slice("abc21", 5),
                 folly::none,
                 folly::none,
                 Slice(nullptr, 0),
                 {},
                 Durability::ASYNC_WRITE,
                 false},
      PutWriteOp{logid_t(2),
                 5,
                 getHeader(),
                 Slice("abc25", 5),
                 folly::none,
                 folly::none,
                 Slice(nullptr, 0),
                 {},
                 Durability::ASYNC_WRITE,
                 false},
      PutWriteOp{logid_t(1),
                 6,
                 getHeader(),
                 Slice("abc16", 5),
                 folly::none,
                 folly::none,
                 Slice(nullptr, 0),
                 {},
                 Durability::ASYNC_WRITE,
                 false},
      PutWriteOp{logid_t(2),
                 4,
                 getHeader(),
                 Slice("abc24", 5),
                 folly::none,
                 folly::none,
                 Slice(nullptr, 0),
                 {},
                 Durability::ASYNC_WRITE,
                 false},
  };

  std::vector<const WriteOp*> ops1{&put_ops[0], &put_ops[1]};
  std::vector<const WriteOp*> ops2{&put_ops[2], &put_ops[3]};

  ASSERT_EQ(0, store.writeMulti(ops1));
  ASSERT_EQ(0, store.writeMulti(ops2));

  {
    std::unique_ptr<LocalLogStore::ReadIterator> it =
        store.read(logid_t(1), LocalLogStore::ReadOptions("WriteReadBackTest"));
    it->seek(0);
    int nread = 0;
    for (nread = 0; it->state() == IteratorState::AT_RECORD;
         ++nread, it->next()) {
      if (nread == 0) {
        EXPECT_EQ(6, it->getLSN());
        verifyRecord("abc16", it);
      }
    }
    EXPECT_EQ(1, nread);
  }

  {
    // Test that we can get records for log 2, starting from LSN 1 (inclusive)
    std::unique_ptr<LocalLogStore::ReadIterator> it =
        store.read(logid_t(2), LocalLogStore::ReadOptions("WriteReadBackTest"));
    it->seek(1);
    int nread = 0;
    for (nread = 0; it->state() == IteratorState::AT_RECORD;
         ++nread, it->next()) {
      if (nread == 0) {
        EXPECT_EQ(1, it->getLSN());
        verifyRecord("abc21", it);
      } else if (nread == 1) {
        EXPECT_EQ(4, it->getLSN());
        verifyRecord("abc24", it);
      } else if (nread == 2) {
        EXPECT_EQ(5, it->getLSN());
        verifyRecord("abc25", it);
      }
    }
    EXPECT_EQ(3, nread);
  }

  {
    // Test that starting at LSN 2 skips the first record
    std::unique_ptr<LocalLogStore::ReadIterator> it =
        store.read(logid_t(2), LocalLogStore::ReadOptions("WriteReadBackTest"));
    it->seek(2);
    int nread = 0;
    for (nread = 0; it->state() == IteratorState::AT_RECORD;
         ++nread, it->next()) {
      if (nread == 0) {
        EXPECT_EQ(4, it->getLSN());
        verifyRecord("abc24", it);
      } else if (nread == 1) {
        EXPECT_EQ(5, it->getLSN());
        verifyRecord("abc25", it);
      }
    }
    EXPECT_EQ(2, nread);
  }
}

STORE_TEST(RocksDBLocalLogStoreTest, BatchWithDelete, store) {
  std::vector<std::unique_ptr<WriteOp>> ops;
  ops.push_back(
      std::make_unique<PutWriteOp>(logid_t(1),
                                   6,
                                   getHeader(),
                                   Slice("abc16", 5),
                                   folly::none,
                                   folly::none,
                                   Slice(nullptr, 0),
                                   std::vector<std::pair<char, std::string>>(),
                                   Durability::ASYNC_WRITE,
                                   false)),
      ops.push_back(std::make_unique<PutWriteOp>(
          logid_t(2),
          1,
          getHeader(),
          Slice("abc21", 5),
          folly::none,
          folly::none,
          Slice(nullptr, 0),
          std::vector<std::pair<char, std::string>>(),
          Durability::ASYNC_WRITE,
          false)),
      ops.push_back(std::make_unique<DeleteWriteOp>(logid_t(1), 6));

  std::vector<const WriteOp*> op_ptrs;
  for (auto& x : ops) {
    op_ptrs.push_back(x.get());
  }

  ASSERT_EQ(0, store.writeMulti(op_ptrs));

  {
    std::unique_ptr<LocalLogStore::ReadIterator> it =
        store.read(logid_t(1), LocalLogStore::ReadOptions("BatchWithDelete"));
    it->seek(0);
    int nread = 0;
    for (nread = 0; it->state() == IteratorState::AT_RECORD;
         ++nread, it->next()) {
    }
    EXPECT_EQ(0, nread);
  }

  {
    std::unique_ptr<LocalLogStore::ReadIterator> it =
        store.read(logid_t(2), LocalLogStore::ReadOptions("BatchWithDelete"));
    it->seek(0);
    int nread = 0;
    for (nread = 0; it->state() == IteratorState::AT_RECORD;
         ++nread, it->next()) {
      if (nread == 0) {
        EXPECT_EQ(1, it->getLSN());
        verifyRecord("abc21", it);
      }
    }
    EXPECT_EQ(1, nread);
  }
}

STORE_TEST(RocksDBLocalLogStoreTest, DumpReleaseStateTest, store) {
  DumpReleaseStateWriteOp op{{
      std::make_pair(logid_t(1), 1),
      std::make_pair(logid_t(2), 42),
  }};
  std::vector<const WriteOp*> ops{&op};

  ASSERT_EQ(0, store.writeMulti(ops));

  LastReleasedMetadata metadata;
  ASSERT_EQ(0, store.readLogMetadata(logid_t(1), &metadata));
  EXPECT_EQ(1, metadata.last_released_lsn_);

  ASSERT_EQ(0, store.readLogMetadata(logid_t(2), &metadata));
  EXPECT_EQ(42, metadata.last_released_lsn_);

  EXPECT_EQ(-1, store.readLogMetadata(logid_t(3), &metadata));
}

STORE_TEST(RocksDBLocalLogStoreTest, ReadMetadataTest, store) {
  ASSERT_EQ(0,
            store.writeLogMetadata(
                logid_t(1), TrimMetadata{42}, LocalLogStore::WriteOptions()));
  ASSERT_EQ(0,
            store.writeLogMetadata(logid_t(1),
                                   LastReleasedMetadata{100},
                                   LocalLogStore::WriteOptions()));

  TrimMetadata trim_metadata;
  LastReleasedMetadata release_metadata;

  ASSERT_EQ(0, store.readLogMetadata(logid_t(1), &trim_metadata));
  EXPECT_EQ(42, trim_metadata.trim_point_);

  ASSERT_EQ(0, store.readLogMetadata(logid_t(1), &release_metadata));
  EXPECT_EQ(100, release_metadata.last_released_lsn_);
}

STORE_TEST(RocksDBLocalLogStoreTest, EraseMetadataTest, store) {
  SealMetadata sm{Seal(epoch_t(7), NodeID(0, 1))};
  // the following choice ensures that no endianness issues are going on when we
  // delete a range
  logid_t todel1(0x001);
  logid_t todel2(0x003);
  logid_t todel3(0x004);
  logid_t nottodel1(0x101);
  logid_t nottodel2(0x102);

  for (auto id : {todel1, todel2, todel3, nottodel1, nottodel2}) {
    ASSERT_EQ(0, store.writeLogMetadata(id, sm, LocalLogStore::WriteOptions()));
    ASSERT_EQ(0, store.readLogMetadata(id, &sm));
  }

  ASSERT_EQ(0,
            store.deleteLogMetadata(todel1,
                                    todel3,
                                    LogMetadataType::SEAL,
                                    LocalLogStore::WriteOptions()));

  for (auto id : {todel1, todel2, todel3}) {
    ASSERT_EQ(-1, store.readLogMetadata(id, &sm));
    ASSERT_EQ(E::NOTFOUND, err);
  }

  ASSERT_EQ(0, store.readLogMetadata(nottodel1, &sm));
  ASSERT_EQ(0, store.readLogMetadata(nottodel2, &sm));
}

STORE_TEST(RocksDBLocalLogStoreTest, WriteMetadataBatch, store) {
  TrimMetadata t1{1};
  TrimMetadata t2{2};

  PutLogMetadataWriteOp op1{logid_t(1), &t1, Durability::SYNC_WRITE};
  PutLogMetadataWriteOp op2{logid_t(2), &t2, Durability::SYNC_WRITE};

  std::vector<const WriteOp*> ops{&op1, &op2};
  ASSERT_EQ(0, store.writeMulti(ops));

  for (int i = 1; i <= 2; ++i) {
    TrimMetadata metadata;
    ASSERT_EQ(0, store.readLogMetadata(logid_t(i), &metadata));
    EXPECT_EQ(lsn_t(i), metadata.trim_point_);
  }
}

STORE_TEST(RocksDBLocalLogStoreTest, UpdateMetadata, store) {
  TrimMetadata tm1{100};
  TrimMetadata tm2{200};
  ASSERT_EQ(
      0,
      store.writeLogMetadata(logid_t(1), tm1, LocalLogStore::WriteOptions()));
  ASSERT_EQ(
      0,
      store.updateLogMetadata(logid_t(1), tm2, LocalLogStore::WriteOptions()));

  TrimMetadata tm3{150};
  ASSERT_EQ(
      -1,
      store.updateLogMetadata(logid_t(1), tm3, LocalLogStore::WriteOptions()));
  ASSERT_EQ(E::UPTODATE, err);
  EXPECT_EQ(200, tm3.trim_point_);

  TrimMetadata tm4{10};
  ASSERT_EQ(
      0,
      store.updateLogMetadata(logid_t(2), tm4, LocalLogStore::WriteOptions()));
}

STORE_TEST(RocksDBLocalLogStoreTest, WriteInvalidMetadata, store) {
  // turn off dd_assert()
  dbg::assertOnData = false;
  std::vector<std::unique_ptr<LogMetadata>> invalids;
  const epoch_t max_invalid_epoch = epoch_t(EPOCH_MAX.val_ + 1);
  ASSERT_FALSE(epoch_valid_or_unset(max_invalid_epoch));
  invalids.push_back(std::make_unique<TrimMetadata>(LSN_MAX));
  invalids.push_back(
      std::make_unique<SealMetadata>(Seal(max_invalid_epoch, NodeID(1, 1))));
  invalids.push_back(
      std::make_unique<SealMetadata>(Seal(epoch_t(1), NodeID())));
  invalids.push_back(std::make_unique<LastReleasedMetadata>(LSN_MAX - 1));
  invalids.push_back(std::make_unique<LastCleanMetadata>(max_invalid_epoch));

  for (const auto& m : invalids) {
    ASSERT_EQ(
        -1,
        store.writeLogMetadata(logid_t(1), *m, LocalLogStore::WriteOptions()));
    ASSERT_EQ(E::LOCAL_LOG_STORE_WRITE, err);
  }

  // should also be rejected by update methods
  for (const auto& m : invalids) {
    ASSERT_EQ(
        -1,
        store.updateLogMetadata(logid_t(1),
                                *static_cast<ComparableLogMetadata*>(m.get()),
                                LocalLogStore::WriteOptions()));
    ASSERT_EQ(E::LOCAL_LOG_STORE_WRITE, err);
  }
}

STORE_TEST(RocksDBLocalLogStoreTest, Seek, store) {
  Slice data("foo", 3);
  lsn_t lsns[] = {
      compose_lsn(epoch_t(1), esn_t(1)),
      compose_lsn(epoch_t(1), esn_t(2)),
      compose_lsn(epoch_t(4), esn_t(1)),
  };

  std::vector<PutWriteOp> put_ops;
  for (lsn_t lsn : lsns) {
    put_ops.emplace_back(logid_t(1),
                         lsn,
                         getHeader(),
                         data,
                         folly::none,
                         folly::none,
                         Slice(nullptr, 0),
                         std::vector<std::pair<char, std::string>>(),
                         Durability::ASYNC_WRITE,
                         false);
    put_ops.emplace_back(LOGID_MAX_INTERNAL,
                         lsn,
                         getHeader(),
                         data,
                         folly::none,
                         folly::none,
                         Slice(nullptr, 0),
                         std::vector<std::pair<char, std::string>>(),
                         Durability::ASYNC_WRITE,
                         false);
  }

  std::vector<const WriteOp*> ops;
  for (auto& x : put_ops) {
    ops.push_back(&x);
  }

  ASSERT_EQ(0, store.writeMulti(ops));

  LocalLogStore::ReadOptions options("Seek");
  options.tailing = false;

  std::unique_ptr<LocalLogStore::ReadIterator> it =
      store.read(logid_t(1), options);

  it->seek(lsns[1]);
  ASSERT_EQ(IteratorState::AT_RECORD, it->state());
  EXPECT_EQ(lsns[1], it->getLSN());

  it->prev();
  ASSERT_EQ(IteratorState::AT_RECORD, it->state());
  EXPECT_EQ(lsns[0], it->getLSN());

  it->seekForPrev(LSN_MAX);
  ASSERT_EQ(IteratorState::AT_RECORD, it->state());
  EXPECT_EQ(lsns[2], it->getLSN());

  it = store.read(logid_t(2), options);
  it->seekForPrev(LSN_MAX);
  ASSERT_EQ(IteratorState::AT_END, it->state());

  it = store.read(LOGID_MAX_INTERNAL, options);
  it->seekForPrev(LSN_MAX);
  ASSERT_EQ(IteratorState::AT_RECORD, it->state());
  EXPECT_EQ(lsns[2], it->getLSN());
}

STORE_TEST(RocksDBLocalLogStoreTest, PersistentError, store) {
  EXPECT_EQ(E::OK, store.acceptingWrites());
  EXPECT_EQ(
      0, stats_.aggregate().totalPerShardStats().local_logstore_failed_writes);

  env_.fail_writes_.store(true);

  PutWriteOp op{logid_t(123),
                511,
                getHeader(),
                Slice("abc1", 4),
                folly::none,
                folly::none,
                Slice(nullptr, 0),
                {},
                Durability::ASYNC_WRITE,
                false};
  EXPECT_EQ(-1, store.writeMulti(std::vector<const WriteOp*>{&op}));
  EXPECT_EQ(E::DISABLED, store.acceptingWrites());
  EXPECT_EQ(
      1, stats_.aggregate().totalPerShardStats().local_logstore_failed_writes);
}

STORE_TEST(RocksDBLocalLogStoreTest, sync, store) {
  EXPECT_EQ(E::OK, store.acceptingWrites());

  PutWriteOp op{logid_t(123),
                511,
                getHeader(),
                Slice("abc1", 4),
                folly::none,
                folly::none,
                Slice(nullptr, 0),
                {},
                Durability::ASYNC_WRITE,
                false};
  EXPECT_EQ(0, store.writeMulti(std::vector<const WriteOp*>{&op}));
  EXPECT_EQ(0, store.sync(Durability::ASYNC_WRITE));
  EXPECT_EQ(E::OK, store.acceptingWrites());
}

STORE_TEST(RocksDBLocalLogStoreTest, failed_sync, store) {
  EXPECT_EQ(E::OK, store.acceptingWrites());
  // Flush memtables that can be in memory as part of recovery,
  // before failing fsyncs.
  store.sync(Durability::MEMORY);
  env_.fail_syncs_.store(true);

  PutWriteOp op{logid_t(123),
                511,
                getHeader(),
                Slice("abc1", 4),
                folly::none,
                folly::none,
                Slice(nullptr, 0),
                {},
                Durability::ASYNC_WRITE,
                false};
  EXPECT_EQ(0, store.writeMulti(std::vector<const WriteOp*>{&op}));
  EXPECT_EQ(-1, store.sync(Durability::ASYNC_WRITE));
  EXPECT_EQ(E::DISABLED, store.acceptingWrites());
}

TEST_F(RocksDBLocalLogStoreTest, StoreMetadata) {
  TemporaryRocksDBStore store;

  ClusterMarkerMetadata meta1_temp{"i'll be overwritten"};

  ASSERT_EQ(
      0, store.writeStoreMetadata(meta1_temp, LocalLogStore::WriteOptions()));

  ClusterMarkerMetadata meta1{"hello"};
  RebuildingCompleteMetadata meta2;

  ASSERT_EQ(0, store.writeStoreMetadata(meta1, LocalLogStore::WriteOptions()));
  ASSERT_EQ(0, store.writeStoreMetadata(meta2, LocalLogStore::WriteOptions()));

  for (int i = 0; i < 2; ++i) {
    if (i) {
      store.close();
      store.open();
    }

    ClusterMarkerMetadata meta1_read;

    ASSERT_EQ(0, store.readStoreMetadata(&meta1_read));
    ASSERT_EQ(0, store.readStoreMetadata(&meta2));

    ASSERT_EQ(meta1.marker_, meta1_read.marker_);
  }
}

TEST_F(RocksDBLocalLogStoreTest, StoreMetadataBatch) {
  TemporaryRocksDBStore store;

  ClusterMarkerMetadata meta1{"hi"};
  RebuildingCompleteMetadata meta2;

  PutStoreMetadataWriteOp op1{&meta1, Durability::SYNC_WRITE};
  PutStoreMetadataWriteOp op2{&meta2, Durability::SYNC_WRITE};

  std::vector<const WriteOp*> ops{&op1, &op2};
  ASSERT_EQ(0, store.writeMulti(ops));

  for (int i = 0; i < 2; ++i) {
    if (i) {
      store.close();
      store.open();
    }

    ClusterMarkerMetadata meta1_read;

    ASSERT_EQ(0, store.readStoreMetadata(&meta1_read));
    ASSERT_EQ(0, store.readStoreMetadata(&meta2));

    ASSERT_EQ(meta1.marker_, meta1_read.marker_);
  }
}

// test various PerEpochLogMetadata operations
TEST_F(RocksDBLocalLogStoreTest, PerEpochLogMetadata) {
  TailRecord tail_record;
  OffsetMap epoch_size_map;
  OffsetMap epoch_end_offsets;
  epoch_size_map.setCounter(BYTE_OFFSET, 200);
  epoch_end_offsets.setCounter(BYTE_OFFSET, 100);
  tail_record.offsets_map_.setCounter(BYTE_OFFSET, 100);
  tail_record.header.log_id = logid_t(1);
  EpochRecoveryMetadata erm_empty;
  ASSERT_FALSE(erm_empty.valid());
  // can be serialized despite invalid
  EpochRecoveryMetadata erm1, erm2;
  ASSERT_EQ(0, erm1.deserialize(erm_empty.serialize()));
  ASSERT_EQ(erm_empty, erm1);
  EpochRecoveryMetadata erm_valid(epoch_t(1),
                                  esn_t(2),
                                  esn_t(4),
                                  1,
                                  tail_record,
                                  epoch_size_map,
                                  epoch_end_offsets);
  ASSERT_TRUE(erm_valid.valid());
  Slice s = erm_valid.serialize();
  ASSERT_NE(nullptr, s.data);
  ASSERT_EQ(0, erm1.deserialize(s));
  ASSERT_EQ(erm_valid, erm1);
  ASSERT_EQ(E::UPTODATE, erm_valid.update(erm_empty));
  ASSERT_EQ(erm_valid, erm_empty);
  erm_empty.reset();
  // make erm1 invalid
  erm1.header_.sequencer_epoch = epoch_t(9);
  erm1.header_.last_digest_esn = esn_t(2);
  erm1.header_.last_known_good = esn_t(4);
  ASSERT_FALSE(erm1.valid());
  ASSERT_EQ(E::UPTODATE, erm_valid.update(erm1));
  ASSERT_EQ(erm_valid, erm1);
  epoch_size_map.setCounter(BYTE_OFFSET, 1230);
  tail_record.offsets_map_.setCounter(BYTE_OFFSET, 1020);
  epoch_end_offsets.setCounter(BYTE_OFFSET, 1020);
  EpochRecoveryMetadata erm_valid2(epoch_t(8),
                                   esn_t(1),
                                   esn_t(99),
                                   1,
                                   tail_record,
                                   epoch_size_map,
                                   epoch_end_offsets);
  ASSERT_EQ(0, erm1.deserialize(erm_valid2.serialize()));
  ASSERT_EQ(E::OK, erm_valid.update(erm_valid2));
  ASSERT_EQ(erm_valid2, erm_valid);
  ASSERT_EQ(erm1, erm_valid2);
}

STORE_TEST(RocksDBLocalLogStoreTest, PerEpochLogMetadata, store) {
  TailRecord tail_record;
  OffsetMap epoch_size_map;
  OffsetMap epoch_end_offsets;
  epoch_size_map.setCounter(BYTE_OFFSET, 200);
  epoch_end_offsets.setCounter(BYTE_OFFSET, 100);
  tail_record.offsets_map_.setCounter(BYTE_OFFSET, 100);
  const EpochRecoveryMetadata erm(epoch_t(7),
                                  esn_t(2),
                                  esn_t(4),
                                  0,
                                  tail_record,
                                  epoch_size_map,
                                  epoch_end_offsets);
  EpochRecoveryMetadata erm1;
  erm1.deserialize(erm.serialize());
  ASSERT_EQ(
      0,
      store.updatePerEpochLogMetadata(logid_t(1),
                                      epoch_t(1),
                                      erm1,
                                      LocalLogStore::SealPreemption::ENABLE,
                                      LocalLogStore::WriteOptions()));
  ASSERT_EQ(erm, erm1);
  EpochRecoveryMetadata erm2;
  ASSERT_EQ(0, store.readPerEpochLogMetadata(logid_t(1), epoch_t(1), &erm2));
  ASSERT_EQ(erm, erm2);
  ASSERT_EQ(-1, store.readPerEpochLogMetadata(logid_t(1), epoch_t(2), &erm2));
  ASSERT_EQ(E::NOTFOUND, err);
  ASSERT_EQ(-1, store.readPerEpochLogMetadata(logid_t(2), epoch_t(1), &erm2));
  ASSERT_EQ(E::NOTFOUND, err);
  epoch_size_map.setCounter(BYTE_OFFSET, 400);
  epoch_end_offsets.setCounter(BYTE_OFFSET, 200);
  tail_record.offsets_map_.setCounter(BYTE_OFFSET, 200);
  EpochRecoveryMetadata erm_old(epoch_t(6),
                                esn_t(9),
                                esn_t(11),
                                0,
                                tail_record,
                                epoch_size_map,
                                epoch_end_offsets);
  ASSERT_EQ(
      -1,
      store.updatePerEpochLogMetadata(logid_t(1),
                                      epoch_t(1),
                                      erm_old,
                                      LocalLogStore::SealPreemption::ENABLE,
                                      LocalLogStore::WriteOptions()));
  ASSERT_EQ(E::UPTODATE, err);
  ASSERT_EQ(erm, erm_old);
  epoch_size_map.setCounter(BYTE_OFFSET, 1200);
  epoch_end_offsets.setCounter(BYTE_OFFSET, 800);
  tail_record.offsets_map_.setCounter(BYTE_OFFSET, 800);
  const EpochRecoveryMetadata erm_new(epoch_t(12),
                                      esn_t(3),
                                      esn_t(99),
                                      0,
                                      tail_record,
                                      epoch_size_map,
                                      epoch_end_offsets);
  erm1.deserialize(erm_new.serialize());
  ASSERT_EQ(erm_new, erm1);
  ASSERT_EQ(
      0,
      store.updatePerEpochLogMetadata(logid_t(1),
                                      epoch_t(1),
                                      erm1,
                                      LocalLogStore::SealPreemption::ENABLE,
                                      LocalLogStore::WriteOptions()));
  ASSERT_EQ(erm_new, erm1);
  ASSERT_EQ(0, store.readPerEpochLogMetadata(logid_t(1), epoch_t(1), &erm2));
}

STORE_TEST(RocksDBLocalLogStoreTest, EmptyPerEpochLogMetadata, store) {
  TailRecord tail_record;
  OffsetMap epoch_size_map;
  OffsetMap epoch_end_offsets;
  epoch_size_map.setCounter(BYTE_OFFSET, 2);
  tail_record.offsets_map_.setCounter(BYTE_OFFSET, 1);
  epoch_end_offsets.setCounter(BYTE_OFFSET, 1);
  const EpochRecoveryMetadata erm_6e(epoch_t(6),
                                     esn_t(0),
                                     esn_t(0),
                                     0,
                                     tail_record,
                                     epoch_size_map,
                                     epoch_end_offsets);
  epoch_size_map.setCounter(BYTE_OFFSET, 5);
  tail_record.offsets_map_.setCounter(BYTE_OFFSET, 4);
  epoch_end_offsets.setCounter(BYTE_OFFSET, 4);
  const EpochRecoveryMetadata erm_7(epoch_t(7),
                                    esn_t(2),
                                    esn_t(4),
                                    0,
                                    tail_record,
                                    epoch_size_map,
                                    epoch_end_offsets);
  epoch_size_map.setCounter(BYTE_OFFSET, 7);
  tail_record.offsets_map_.setCounter(BYTE_OFFSET, 3);
  epoch_end_offsets.setCounter(BYTE_OFFSET, 3);
  const EpochRecoveryMetadata erm_8e(epoch_t(8),
                                     esn_t(0),
                                     esn_t(0),
                                     0,
                                     tail_record,
                                     epoch_size_map,
                                     epoch_end_offsets);
  ASSERT_TRUE(erm_6e.empty());
  ASSERT_FALSE(erm_7.empty());
  ASSERT_TRUE(erm_8e.empty());

  EpochRecoveryMetadata erm;
  erm.deserialize(erm_6e.serialize());
  ASSERT_EQ(
      0,
      store.updatePerEpochLogMetadata(logid_t(1),
                                      epoch_t(1),
                                      erm,
                                      LocalLogStore::SealPreemption::ENABLE,
                                      LocalLogStore::WriteOptions()));
  ASSERT_EQ(erm_6e, erm);
  EpochRecoveryMetadata erm2;
  // should not store since erm_6e is empty
  ASSERT_EQ(-1, store.readPerEpochLogMetadata(logid_t(1), epoch_t(1), &erm2));
  ASSERT_EQ(E::NOTFOUND, err);

  erm.deserialize(erm_7.serialize());
  ASSERT_EQ(
      0,
      store.updatePerEpochLogMetadata(logid_t(1),
                                      epoch_t(1),
                                      erm,
                                      LocalLogStore::SealPreemption::ENABLE,
                                      LocalLogStore::WriteOptions()));
  ASSERT_EQ(erm_7, erm);
  // non-empty metadata should be written
  ASSERT_EQ(0, store.readPerEpochLogMetadata(logid_t(1), epoch_t(1), &erm2));
  ASSERT_EQ(erm_7, erm2);

  erm.deserialize(erm_8e.serialize());
  ASSERT_EQ(
      0,
      store.updatePerEpochLogMetadata(logid_t(1),
                                      epoch_t(1),
                                      erm,
                                      LocalLogStore::SealPreemption::ENABLE,
                                      LocalLogStore::WriteOptions()));
  ASSERT_EQ(erm_8e, erm);
  // store should already erase the metadata by now
  ASSERT_EQ(-1, store.readPerEpochLogMetadata(logid_t(1), epoch_t(1), &erm2));
  ASSERT_EQ(E::NOTFOUND, err);

  // populate the seal for sequencer in e8
  SealMetadata sm{Seal(epoch_t(7), NodeID(0, 1))};
  ASSERT_EQ(0, store.writeLogMetadata(logid_t(1), sm));

  // metadata w/ seq_epoch 7 should be rejected since the metadata is prempted
  // by a seal
  erm.deserialize(erm_7.serialize());
  ASSERT_EQ(erm_7, erm);
  ASSERT_EQ(
      -1,
      store.updatePerEpochLogMetadata(logid_t(1),
                                      epoch_t(1),
                                      erm,
                                      LocalLogStore::SealPreemption::ENABLE,
                                      LocalLogStore::WriteOptions()));
  ASSERT_EQ(-1, store.readPerEpochLogMetadata(logid_t(1), epoch_t(1), &erm2));
  ASSERT_EQ(E::NOTFOUND, err);
}

STORE_TEST(RocksDBLocalLogStoreTest, PerEpochLogMetadataFindLast, store) {
  TailRecord tail_record;
  OffsetMap epoch_size_map;
  OffsetMap epoch_end_offsets;
  epoch_end_offsets.setCounter(BYTE_OFFSET, 100);
  epoch_size_map.setCounter(BYTE_OFFSET, 50);
  tail_record.offsets_map_.setCounter(BYTE_OFFSET, 100);
  EpochRecoveryMetadata erm(epoch_t(7),
                            esn_t(3),
                            esn_t(5),
                            0,
                            tail_record,
                            epoch_size_map,
                            epoch_end_offsets);
  tail_record.offsets_map_.setCounter(BYTE_OFFSET, 200);
  epoch_size_map.setCounter(BYTE_OFFSET, 50);
  epoch_end_offsets.setCounter(BYTE_OFFSET, 200);
  EpochRecoveryMetadata erm1(epoch_t(8),
                             esn_t(2),
                             esn_t(4),
                             0,
                             tail_record,
                             epoch_size_map,
                             epoch_end_offsets);
  epoch_size_map.setCounter(BYTE_OFFSET, 0);
  tail_record.offsets_map_.setCounter(BYTE_OFFSET, 0);
  epoch_end_offsets.setCounter(BYTE_OFFSET, 0);
  EpochRecoveryMetadata erm2(epoch_t(9),
                             esn_t(3),
                             esn_t(4),
                             0,
                             tail_record,
                             epoch_size_map,
                             epoch_end_offsets);

  ASSERT_EQ(
      0,
      store.updatePerEpochLogMetadata(logid_t(1),
                                      epoch_t(5),
                                      erm,
                                      LocalLogStore::SealPreemption::ENABLE,
                                      LocalLogStore::WriteOptions()));
  ASSERT_EQ(
      0,
      store.updatePerEpochLogMetadata(logid_t(3),
                                      epoch_t(2),
                                      erm1,
                                      LocalLogStore::SealPreemption::ENABLE,
                                      LocalLogStore::WriteOptions()));
  ASSERT_EQ(
      0,
      store.updatePerEpochLogMetadata(logid_t(3),
                                      epoch_t(5),
                                      erm2,
                                      LocalLogStore::SealPreemption::ENABLE,
                                      LocalLogStore::WriteOptions()));

  EpochRecoveryMetadata erm_out;

  // true find_last_available option should not change behavior if exact record
  // found.
  ASSERT_EQ(
      0, store.readPerEpochLogMetadata(logid_t(3), epoch_t(5), &erm_out, true));
  ASSERT_EQ(erm2, erm_out);

  // target record is in between two records of the same log_id. should find
  // smaller one
  ASSERT_EQ(
      0, store.readPerEpochLogMetadata(logid_t(3), epoch_t(3), &erm_out, true));
  ASSERT_EQ(erm1, erm_out);

  // iterator should seek to last and succeed
  ASSERT_EQ(
      0,
      store.readPerEpochLogMetadata(logid_t(3), epoch_t(40), &erm_out, true));
  ASSERT_EQ(erm2, erm_out);

  // iterator should seek to next log_id and go back
  ASSERT_EQ(
      0,
      store.readPerEpochLogMetadata(logid_t(1), epoch_t(50), &erm_out, true));
  ASSERT_EQ(erm, erm_out);

  // iterator should reach beginning of log and fail
  ASSERT_EQ(
      -1,
      store.readPerEpochLogMetadata(logid_t(1), epoch_t(3), &erm_out, true));
  ASSERT_EQ(E::NOTFOUND, err);

  // iterator should reach next log_id and fail
  ASSERT_EQ(
      -1,
      store.readPerEpochLogMetadata(logid_t(2), epoch_t(1), &erm_out, true));
  ASSERT_EQ(E::NOTFOUND, err);
  ASSERT_EQ(
      -1,
      store.readPerEpochLogMetadata(logid_t(3), epoch_t(1), &erm_out, true));
  ASSERT_EQ(E::NOTFOUND, err);

  // iterator should seek to last and fail because last log_id found is bigger
  // than target one.
  ASSERT_EQ(
      -1,
      store.readPerEpochLogMetadata(logid_t(100), epoch_t(5), &erm_out, true));
  ASSERT_EQ(E::NOTFOUND, err);
}

STORE_TEST(RocksDBLocalLogStoreTest, SnapshotsPersistence, store) {
  auto snapshots_type = LocalLogStore::LogSnapshotBlobType::RECORD_CACHE;
  std::map<logid_t, std::string> snapshots_content{
      {logid_t(1), std::string(5000000, '1')},    // 5mb
      {logid_t(777), std::string(10000000, '2')}, // 10mb
      {logid_t(1337), std::string(1, '3')}        // 1b
  };
  std::vector<std::pair<logid_t, Slice>> snapshots;
  for (auto& kv : snapshots_content) {
    snapshots.emplace_back(
        kv.first, Slice(kv.second.c_str(), kv.second.size()));
  }

  // Assert initially empty
  std::map<logid_t, std::string> blob_map;
  LocalLogStore::LogSnapshotBlobCallback callback = [&blob_map](logid_t logid,
                                                                Slice blob) {
    blob_map[logid] =
        std::string(reinterpret_cast<const char*>(blob.data), blob.size);
    return 0;
  };
  int rv = store.readAllLogSnapshotBlobs(snapshots_type, callback);
  ASSERT_TRUE(blob_map.empty());

  // Verify we can add and snapshot blobs are stored correctly
  rv = store.writeLogSnapshotBlobs(snapshots_type, snapshots);
  ASSERT_EQ(0, rv);
  rv = store.readAllLogSnapshotBlobs(snapshots_type, callback);
  ASSERT_EQ(0, rv);
  ASSERT_EQ(blob_map, snapshots_content);

  // Add another snapshot twice, should be fine
  std::string final_example = "final example";
  auto final_example_list = std::vector<std::pair<logid_t, Slice>>{
      {logid_t(999), Slice(final_example.c_str(), final_example.size())}};

  rv = store.writeLogSnapshotBlobs(snapshots_type, final_example_list);
  ASSERT_EQ(0, rv);
  rv = store.writeLogSnapshotBlobs(snapshots_type, final_example_list);
  ASSERT_EQ(0, rv);

  // Verify they are all stored
  blob_map.clear();
  rv = store.readAllLogSnapshotBlobs(snapshots_type, callback);
  ASSERT_EQ(0, rv);
  snapshots_content[logid_t(999)] = final_example;
  ASSERT_EQ(blob_map, snapshots_content);
}

} // namespace
