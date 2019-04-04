/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/server/RecordCache.h"

#include <algorithm>
#include <chrono>
#include <cstdlib>
#include <thread>

#include <folly/Memory.h>
#include <folly/Random.h>
#include <gtest/gtest.h>

#include "logdevice/common/CopySet.h"
#include "logdevice/common/RandomAccessQueue.h"
#include "logdevice/common/Semaphore.h"
#include "logdevice/common/SlidingWindow.h"
#include "logdevice/common/debug.h"
#include "logdevice/common/protocol/STORE_Message.h"
#include "logdevice/common/util.h"
#include "logdevice/server/EpochRecordCache.h"
#include "logdevice/server/EpochRecordCacheEntry.h"
#include "logdevice/server/RecordCacheDependencies.h"

using namespace facebook::logdevice;
using StoredBefore = EpochRecordCache::StoredBefore;
using Entry = EpochRecordCacheEntry;
using Result = RecordCache::Result;
using EpochRecordCacheHeader = EpochRecordCacheSerializer::CacheHeader;

const logid_t LOG_ID(7);
const shard_index_t SHARD(0);
const epoch_t EPOCH(3);

// Handy macros for writing ShardIDs
#define N0 ShardID(0, SHARD)
#define N1 ShardID(1, SHARD)
#define N2 ShardID(2, SHARD)
#define N3 ShardID(3, SHARD)
#define N4 ShardID(4, SHARD)
#define N5 ShardID(5, SHARD)
#define N6 ShardID(6, SHARD)
#define N7 ShardID(7, SHARD)
#define N8 ShardID(8, SHARD)
#define N9 ShardID(9, SHARD)

namespace {

using KeysType = std::map<KeyType, std::string>;

// simple Sequencer for simulating LogDevice write path for
// multi-threaded tests
class DummySequencer {
 public:
  struct DummyAppender {
    lsn_t lsn_{LSN_INVALID};
  };

  using append_func_t = std::function<void(DummyAppender*)>;

  DummySequencer(epoch_t epoch, size_t capacity) : window_(capacity) {
    window_.advanceEpoch(epoch);
  }

  struct Reaper {
   public:
    explicit Reaper(DummySequencer* seq) : seq_(seq) {}
    void operator()(DummyAppender* a) {
      seq_->updateLNG(a->lsn_);
      delete a;
    }

   private:
    DummySequencer* seq_;
  };

  lsn_t append(append_func_t append_func) {
    DummyAppender* a = new DummyAppender;
    lsn_t lsn = window_.grow(a);
    if (lsn != LSN_INVALID) {
      a->lsn_ = lsn;
      append_func(a);
      return lsn;
    }
    delete a;
    return LSN_INVALID;
  }

  void retire(DummyAppender* a) {
    ld_check(a && a->lsn_ != LSN_INVALID);
    Reaper reaper(this);
    window_.retire(a->lsn_, reaper);
  }

  void updateLNG(lsn_t reaped) {
    ld_check(lsn_to_esn(reaped) != ESN_INVALID);
    esn_t::raw_type expected = lsn_to_esn(reaped).val_ - 1;
    lng_.compare_exchange_strong(expected, lsn_to_esn(reaped).val_);
  }

  esn_t getLNG() const {
    return esn_t(lng_.load());
  }

 private:
  std::atomic<esn_t::raw_type> lng_{ESN_INVALID.val_};
  SlidingWindow<DummyAppender, Reaper> window_;
};

// for simplicity, payload of the record is a fixed 8 bytes storing the
// lsn value of the record
std::shared_ptr<PayloadHolder> createPayload(lsn_t lsn) {
  lsn_t* payload_flat = (lsn_t*)malloc(sizeof(lsn_t));
  *payload_flat = lsn;
  return std::make_shared<PayloadHolder>(payload_flat, sizeof(lsn_t));
}

std::shared_ptr<PayloadHolder> createPayload(size_t size, char fill) {
  void* payload_flat = malloc(size);
  memset(payload_flat, fill, size);
  return std::make_shared<PayloadHolder>(payload_flat, size);
}

// convenient function for putting a record in the cache
// TODO (T33977412) : change to take OffsetMap
template <typename Cache>
int putRecord(Cache* c,
              lsn_t lsn,
              esn_t::raw_type lng,
              STORE_flags_t flags = STORE_flags_t(0),
              KeysType keys = {},
              uint32_t wave_or_recovery_epoch = uint32_t(1),
              copyset_t copyset = copyset_t({N0, N1, N2}),
              OffsetMap offsets_within_epoch = OffsetMap()) {
  std::shared_ptr<PayloadHolder> ph = nullptr;
  Payload pl;
  if ((flags & STORE_Header::HOLE) || (flags & STORE_Header::AMEND)) {
    // no payload for hole plug of amends
  } else {
    ph = createPayload(lsn);
    pl = ph->getPayload();
  }
  // use timestamp as the same as lsn
  uint64_t timestamp = lsn;

  return c->putRecord(RecordID(lsn, LOG_ID),
                      timestamp,
                      esn_t(lng),
                      wave_or_recovery_epoch,
                      copyset,
                      flags,
                      // putRecord() can destroy keys, so make copy.
                      KeysType(keys),
                      Slice(pl),
                      ph,
                      std::move(offsets_within_epoch));
}

lsn_t lsn(epoch_t::raw_type epoch, esn_t::raw_type esn) {
  return compose_lsn(epoch_t(epoch), esn_t(esn));
}

lsn_t lsn(epoch_t epoch, esn_t::raw_type esn) {
  return compose_lsn(epoch, esn_t(esn));
}

void verifyEntry(const Entry& entry,
                 lsn_t lsn,
                 STORE_flags_t flags = STORE_flags_t(0),
                 KeysType keys = {},
                 uint32_t wave_or_recovery_epoch = uint32_t(1),
                 copyset_t copyset = copyset_t({N0, N1, N2})) {
  ld_check(lsn == entry.timestamp);
  ASSERT_EQ(lsn, entry.timestamp);
  ASSERT_EQ(flags, entry.flags);
  if (entry.flags & STORE_Header::HOLE) {
    ASSERT_EQ(nullptr, entry.payload_raw.data);
    ASSERT_EQ(0, entry.payload_raw.size);
  } else {
    ASSERT_EQ(sizeof(lsn_t), entry.payload_raw.size);
    ASSERT_EQ(lsn, *((lsn_t*)entry.payload_raw.data));
  }

  ASSERT_EQ(entry.keys, keys);
  ASSERT_EQ(entry.wave_or_recovery_epoch, wave_or_recovery_epoch);
  ASSERT_EQ(entry.copyset, copyset);
}

} // anonymous namespace

#define ASSERT_CACHE_ENTRY_FLAG(c, esn, lsn, flags, keys, wave, copyset)       \
  do {                                                                         \
    auto _result = (c)->getEntry(esn_t((esn)));                                \
    ASSERT_TRUE(_result.first);                                                \
    verifyEntry(                                                               \
        (*_result.second), (lsn), (flags), KeysType(keys), (wave), (copyset)); \
  } while (0)

#define ASSERT_CACHE_ENTRY(c, esn, lsn)     \
  ASSERT_CACHE_ENTRY_FLAG(c,                \
                          esn,              \
                          lsn,              \
                          STORE_flags_t(0), \
                          {},               \
                          uint32_t(1),      \
                          copyset_t({N0, N1, N2}));

#define ASSERT_NO_CACHE_ENTRY(c, esn)           \
  do {                                          \
    auto result_ = (c)->getEntry(esn_t((esn))); \
    ASSERT_FALSE(result_.first);                \
  } while (0)

template <typename ERCPointer>
void ASSERT_TAIL_RECORD_OFFSET(const ERCPointer& c,
                               lsn_t _lsn,
                               OffsetMap _offset) {
  TailRecord r = (c)->getTailRecord();
  ASSERT_EQ(_lsn, r.header.lsn);
  if (_lsn != LSN_INVALID) {
    ASSERT_EQ(_lsn, r.header.timestamp);
    if (_offset.isValid()) {
      ASSERT_EQ(_offset, r.offsets_map_);
    }
    ASSERT_NE(0, r.header.flags & TailRecordHeader::OFFSET_WITHIN_EPOCH);
    if ((c)->isTailOptimized()) {
      ASSERT_TRUE(r.hasPayload());
      auto s = r.getPayloadSlice();
      ASSERT_EQ(sizeof(lsn_t), s.size);
      ASSERT_EQ(_lsn, *((lsn_t*)s.data));
    } else {
      ASSERT_FALSE(r.hasPayload());
      ASSERT_FALSE(r.header.flags & TailRecordHeader::CHECKSUM);
      ASSERT_FALSE(r.header.flags & TailRecordHeader::CHECKSUM_64BIT);
      ASSERT_TRUE(r.header.flags & TailRecordHeader::CHECKSUM_PARITY);
    }
  }
}

#define ASSERT_TAIL_RECORD(c, _lsn) \
  ASSERT_TAIL_RECORD_OFFSET(c, _lsn, OffsetMap())

#define ASSERT_NO_TAIL_RECORD(c) ASSERT_TAIL_RECORD(c, LSN_INVALID)

class MockEpochRecordCacheDependencies;

class EpochRecordCacheTest : public ::testing::Test {
 public:
  size_t capacity_ = 128;
  std::mutex mutex_;
  std::vector<std::unique_ptr<Entry>> dropped_;
  bool tail_optimized_ = false;
  StoredBefore stored_before_ = StoredBefore::MAYBE;
  std::unique_ptr<EpochRecordCacheDependencies> deps_;
  std::unique_ptr<EpochRecordCache> cache_;

  // for multithreaded tests:
  bool multi_threaded_{false};
  std::unique_ptr<DummySequencer> seq_;
  std::atomic<lsn_t> max_lsn_{LSN_INVALID};
  std::atomic<size_t> append_success_{0};
  std::atomic<size_t> append_failed_{0};
  std::atomic<size_t> append_cached_{0};
  std::atomic<size_t> append_cached_failed_{0};

 public:
  EpochRecordCacheTest() {}

  void create();

  bool testEntriesIdentical(const Entry& a, const Entry& b) {
    return EpochRecordCacheSerializer::EpochRecordCacheCompare ::
        testEntriesIdentical(a, b);
  }

  bool testSnapshotsIdentical(const EpochRecordCache::Snapshot& a,
                              const EpochRecordCache::Snapshot& b) {
    return EpochRecordCacheSerializer::EpochRecordCacheCompare ::
        testSnapshotsIdentical(a, b);
  }

  bool testEpochRecordCachesIdentical(const EpochRecordCache& a,
                                      const EpochRecordCache& b) {
    return EpochRecordCacheSerializer::EpochRecordCacheCompare ::
        testEpochRecordCachesIdentical(a, b);
  }

  lsn_t doAppend() {
    ld_check(seq_ != nullptr);
    auto append_cb = [this](DummySequencer::DummyAppender* a) {
      // sleep for a while, put it into cache and then retire
      std::normal_distribution<> d(10, 20);
      auto rng = folly::ThreadLocalPRNG();
      std::chrono::milliseconds duration(std::lround(d(rng)));
      /* sleep override */
      std::this_thread::sleep_for(duration);
      int rv = putRecord(cache_.get(), a->lsn_, seq_->getLNG().val_);
      if (rv == 0) {
        ++append_cached_;
      } else {
        ++append_cached_failed_;
      }
      seq_->retire(a);
    };

    lsn_t lsn = seq_->append(append_cb);
    if (lsn != LSN_INVALID) {
      atomic_fetch_max(max_lsn_, lsn);
      ++append_success_;
    } else {
      ++append_failed_;
    }
    return lsn;
  }
};

class MockEpochRecordCacheDependencies : public EpochRecordCacheDependencies {
 public:
  explicit MockEpochRecordCacheDependencies(EpochRecordCacheTest* test)
      : test_(test) {}

  void disposeOfCacheEntry(std::unique_ptr<EpochRecordCacheEntry> e) override {
    std::lock_guard<std::mutex> lock(test_->mutex_);
    test_->dropped_.push_back(std::move(e));
  }

  void onRecordsReleased(const EpochRecordCache&,
                         lsn_t /*begin*/,
                         lsn_t /*end*/,
                         const ReleasedVector& /*entries*/) override {}

 private:
  EpochRecordCacheTest* const test_;
};

void EpochRecordCacheTest::create() {
  if (multi_threaded_) {
    seq_ = std::make_unique<DummySequencer>(EPOCH, capacity_);
  }

  deps_ = std::make_unique<MockEpochRecordCacheDependencies>(this);
  cache_ = std::make_unique<EpochRecordCache>(
      LOG_ID,
      SHARD,
      EPOCH,
      deps_.get(),
      capacity_,
      (tail_optimized_ ? EpochRecordCache::TailOptimized::YES
                       : EpochRecordCache::TailOptimized::NO),
      stored_before_);
}

TEST_F(EpochRecordCacheTest, Basic) {
  capacity_ = 128;
  stored_before_ = StoredBefore::NEVER;
  create();

  // test the cache initialization and destruction
  ASSERT_EQ(128, cache_->capacity());
  ASSERT_EQ(ESN_INVALID, cache_->getLNG());
  ASSERT_TRUE(cache_->isConsistent());

  // insert one record of esn 4, lng 2
  int rv = putRecord(cache_.get(), lsn(EPOCH, 4), 2);
  ASSERT_EQ(0, rv);
  ASSERT_TRUE(dropped_.empty());
  ASSERT_TRUE(cache_->isConsistent());
  ASSERT_EQ(esn_t(2), cache_->getLNG());
  ASSERT_CACHE_ENTRY(cache_, 4, lsn(EPOCH, 4));
  ASSERT_NO_TAIL_RECORD(cache_);

  // destroy the cache_, check for dropped entries
  cache_.reset();
  ASSERT_EQ(1, dropped_.size());
  verifyEntry(*dropped_.back(), lsn(EPOCH, 4));
}

TEST_F(EpochRecordCacheTest, OffsetWithinEpoch) {
  capacity_ = 128;
  stored_before_ = StoredBefore::NEVER;
  tail_optimized_ = true;
  create();

  // insert one record of esn 4, offset 10
  int rv = putRecord(cache_.get(),
                     lsn(EPOCH, 4),
                     2,
                     0,
                     {},
                     1,
                     {N0},
                     OffsetMap({{BYTE_OFFSET, 10}}));
  ASSERT_EQ(0, rv);
  ASSERT_NO_TAIL_RECORD(cache_);

  // esn 5, offset INVALID
  rv = putRecord(cache_.get(), lsn(EPOCH, 5), 3, 0, {}, 1, {N0}, OffsetMap());
  ASSERT_EQ(0, rv);
  ASSERT_NO_TAIL_RECORD(cache_);

  // esn 6, offset 30
  rv = putRecord(cache_.get(),
                 lsn(EPOCH, 6),
                 4,
                 0,
                 {},
                 1,
                 {N0},
                 OffsetMap({{BYTE_OFFSET, 30}}));
  ASSERT_EQ(0, rv);
  // 4 evicted and became the tail
  ASSERT_TAIL_RECORD_OFFSET(
      cache_, lsn(EPOCH, 4), OffsetMap({{BYTE_OFFSET, 10}}));

  // esn 7, offset 25
  rv = putRecord(cache_.get(),
                 lsn(EPOCH, 7),
                 5,
                 0,
                 {},
                 1,
                 {N0},
                 OffsetMap({{BYTE_OFFSET, 25}}));
  ASSERT_EQ(0, rv);
  // tail became 5 but offset is still 10
  ASSERT_TAIL_RECORD_OFFSET(
      cache_, lsn(EPOCH, 5), OffsetMap({{BYTE_OFFSET, 10}}));

  // offsets within epoch should be 30
  ASSERT_EQ(30, cache_->getOffsetsWithinEpoch().getCounter(BYTE_OFFSET));
}

// test EpochRecordCache can handle AMENDs and STOREs well, considering
// store priorities
TEST_F(EpochRecordCacheTest, TestAMEND) {
  capacity_ = 128;
  stored_before_ = StoredBefore::NEVER;
  create();

  // insert one record of esn 4, lng 2
  int rv = putRecord(cache_.get(), lsn(EPOCH, 4), 2);
  ASSERT_EQ(0, rv);
  ASSERT_TRUE(dropped_.empty());
  ASSERT_TRUE(cache_->isConsistent());
  ASSERT_EQ(esn_t(2), cache_->getLNG());
  ASSERT_CACHE_ENTRY(cache_, 4, lsn(EPOCH, 4));

  // try storing a mutation amend over the same record
  STORE_flags_t amend_flags =
      (STORE_Header::AMEND | STORE_Header::WRITTEN_BY_RECOVERY);
  rv = putRecord(cache_.get(),
                 lsn(EPOCH, 4),
                 2,
                 amend_flags,
                 {{KeyType::FINDKEY, "hello"}},
                 /* recovery_epoch */ 6,
                 copyset_t({N2, N3, N4}));
  ASSERT_EQ(0, rv);
  ASSERT_TRUE(cache_->isConsistent());
  ASSERT_EQ(esn_t(2), cache_->getLNG());
  ASSERT_CACHE_ENTRY_FLAG(cache_,
                          4,
                          lsn(EPOCH, 4),
                          STORE_Header::WRITTEN_BY_RECOVERY,
                          {},
                          6,
                          copyset_t({N2, N3, N4}));

  // amend with a mutation store with a smaller recovery epoch
  rv = putRecord(cache_.get(),
                 lsn(EPOCH, 4),
                 2,
                 amend_flags,
                 {},
                 /* recovery_epoch */ 5,
                 copyset_t({N4, N5, N6}));
  ASSERT_EQ(0, rv);
  // amend should not be successful
  ASSERT_CACHE_ENTRY_FLAG(cache_,
                          4,
                          lsn(EPOCH, 4),
                          STORE_Header::WRITTEN_BY_RECOVERY,
                          {},
                          6,
                          copyset_t({N2, N3, N4}));

  // attempt to overwrite with a hole plug of smaller recovery epoch, again this
  // should not succeed
  rv = putRecord(cache_.get(),
                 lsn(EPOCH, 4),
                 2,
                 STORE_Header::HOLE | STORE_Header::WRITTEN_BY_RECOVERY,
                 {},
                 /* recovery_epoch */ 4,
                 copyset_t({N4, N5, N7}));
  ASSERT_EQ(0, rv);
  // amend should not be successful
  ASSERT_CACHE_ENTRY_FLAG(cache_,
                          4,
                          lsn(EPOCH, 4),
                          STORE_Header::WRITTEN_BY_RECOVERY,
                          {},
                          6,
                          copyset_t({N2, N3, N4}));

  // overwrite with a hole plug of higher recovery epoch, this time store should
  // succeed
  rv = putRecord(cache_.get(),
                 lsn(EPOCH, 4),
                 2,
                 STORE_Header::HOLE | STORE_Header::WRITTEN_BY_RECOVERY,
                 {},
                 /* recovery_epoch */ 7,
                 copyset_t({N7, N8, N9}));
  ASSERT_EQ(0, rv);
  // amend should not be successful
  ASSERT_CACHE_ENTRY_FLAG(
      cache_,
      4,
      lsn(EPOCH, 4),
      STORE_Header::HOLE | STORE_Header::WRITTEN_BY_RECOVERY,
      {},
      7,
      copyset_t({N7, N8, N9}));

  cache_.reset();
  // in total 5 entries are created and dropped
  ASSERT_EQ(5, dropped_.size());
}

TEST_F(EpochRecordCacheTest, OutOfOrderStore) {
  capacity_ = 128;
  stored_before_ = StoredBefore::NEVER;
  create();

  // esn 9, lng 3
  int rv = putRecord(cache_.get(), lsn(EPOCH, 9), 3);
  ASSERT_EQ(0, rv);
  ASSERT_EQ(esn_t(3), cache_->getLNG());
  ASSERT_CACHE_ENTRY(cache_, 9, lsn(EPOCH, 9));
  ASSERT_NO_TAIL_RECORD(cache_);
  // esn 4, lng 2 should be stored
  rv = putRecord(cache_.get(), lsn(EPOCH, 4), 2);
  ASSERT_EQ(0, rv);
  ASSERT_EQ(esn_t(3), cache_->getLNG());
  ASSERT_CACHE_ENTRY(cache_, 4, lsn(EPOCH, 4));
  ASSERT_NO_TAIL_RECORD(cache_);
  // esn 3, lng 2 should be rejected
  rv = putRecord(cache_.get(), lsn(EPOCH, 3), 2);
  ASSERT_EQ(-1, rv);
  ASSERT_NO_CACHE_ENTRY(cache_, 3);
}

TEST_F(EpochRecordCacheTest, BasicConsistency) {
  capacity_ = 10;
  stored_before_ = StoredBefore::MAYBE;
  create();

  ASSERT_EQ(ESN_INVALID, cache_->getLNG());
  // cache is not safe to use at this time since it does not know
  // whether there are records stored in the epoch before
  ASSERT_FALSE(cache_->isConsistent());

  // insert one record of esn 4, lng 2, cache should still be inconsistent
  int rv = putRecord(cache_.get(), lsn(EPOCH, 4), 2);
  ASSERT_EQ(0, rv);
  ASSERT_TRUE(dropped_.empty());
  ASSERT_EQ(esn_t(2), cache_->getLNG());
  ASSERT_CACHE_ENTRY(cache_, 4, lsn(EPOCH, 4));
  ASSERT_FALSE(cache_->isConsistent());
  ASSERT_NO_TAIL_RECORD(cache_);

  // insert esn 8, lng 6, the first record should be dropped, but
  // the cache is still inconsistent
  rv = putRecord(cache_.get(), lsn(EPOCH, 8), 6);
  ASSERT_EQ(0, rv);
  ASSERT_EQ(esn_t(6), cache_->getLNG());
  ASSERT_EQ(1, dropped_.size());
  verifyEntry(*dropped_.back(), lsn(EPOCH, 4));
  ASSERT_CACHE_ENTRY(cache_, 8, lsn(EPOCH, 8));
  ASSERT_NO_CACHE_ENTRY(cache_, 4);
  ASSERT_FALSE(cache_->isConsistent());

  // insert esn 13, lng 12, the cache should be consistent
  rv = putRecord(cache_.get(), lsn(EPOCH, 13), 12);
  ASSERT_EQ(0, rv);
  ASSERT_EQ(esn_t(12), cache_->getLNG());
  ASSERT_EQ(2, dropped_.size());
  verifyEntry(*dropped_.back(), lsn(EPOCH, 8));
  ASSERT_NO_CACHE_ENTRY(cache_, 8);
  ASSERT_CACHE_ENTRY(cache_, 13, lsn(EPOCH, 13));
  ASSERT_TRUE(cache_->isConsistent());
  ASSERT_TAIL_RECORD(cache_, lsn(EPOCH, 8));
}

// similar to the test above, but advance lng w/o storing records
// to evict entries and make cache consistent
TEST_F(EpochRecordCacheTest, AdvanceLNG) {
  capacity_ = 10;
  stored_before_ = StoredBefore::MAYBE;
  create();

  ASSERT_EQ(ESN_INVALID, cache_->getLNG());
  // cache is not safe to use at this time since it does not know
  // whether there are records stored in the epoch before
  ASSERT_FALSE(cache_->isConsistent());

  // first advance LNG to 2
  cache_->advanceLNG(esn_t(2));
  ASSERT_EQ(esn_t(2), cache_->getLNG());

  // insert one record of esn 4, lng 3, cache should still be inconsistent
  int rv = putRecord(cache_.get(), lsn(EPOCH, 4), 3);
  ASSERT_EQ(0, rv);
  ASSERT_TRUE(dropped_.empty());
  ASSERT_EQ(esn_t(3), cache_->getLNG());
  ASSERT_CACHE_ENTRY(cache_, 4, lsn(EPOCH, 4));
  ASSERT_FALSE(cache_->isConsistent());
  ASSERT_NO_TAIL_RECORD(cache_);

  // advance lng to 6, the first record should be dropped, but
  // the cache is still inconsistent
  cache_->advanceLNG(esn_t(6));
  ASSERT_EQ(esn_t(6), cache_->getLNG());
  ASSERT_EQ(1, dropped_.size());
  verifyEntry(*dropped_.back(), lsn(EPOCH, 4));
  ASSERT_NO_CACHE_ENTRY(cache_, 4);
  ASSERT_FALSE(cache_->isConsistent());
  ASSERT_TAIL_RECORD(cache_, lsn(EPOCH, 4));

  // cache is still not consistent w/ LNG 11
  cache_->advanceLNG(esn_t(11));
  ASSERT_EQ(esn_t(11), cache_->getLNG());
  ASSERT_FALSE(cache_->isConsistent());

  // advance lng to 12 will make the cache consistent
  cache_->advanceLNG(esn_t(12));
  ASSERT_TRUE(cache_->isConsistent());
  ASSERT_TAIL_RECORD(cache_, lsn(EPOCH, 4));
}

TEST_F(EpochRecordCacheTest, DisableCache) {
  capacity_ = 10;
  stored_before_ = StoredBefore::NEVER;
  create();
  ASSERT_TRUE(cache_->isConsistent());

  int rv = putRecord(cache_.get(), lsn(EPOCH, 4), 2);
  ASSERT_EQ(0, rv);
  ASSERT_TRUE(cache_->isConsistent());
  ASSERT_CACHE_ENTRY(cache_, 4, lsn(EPOCH, 4));

  // inserting a record of esn 14, lng 3
  // the record implies a sequencer sliding window size > 10, which should
  // cause the cache to be disabled with existing records disposed
  rv = putRecord(cache_.get(), lsn(EPOCH, 14), 3);
  // insertion should fail
  ASSERT_EQ(-1, rv);
  ASSERT_FALSE(cache_->isConsistent());
  ASSERT_EQ(1, dropped_.size());
  verifyEntry(*dropped_.back(), lsn(EPOCH, 4));
  ASSERT_NO_CACHE_ENTRY(cache_, 4);

  // further insertion should fail as well
  rv = putRecord(cache_.get(), lsn(EPOCH, 5), 3);
  ASSERT_EQ(-1, rv);
}

TEST_F(EpochRecordCacheTest, Snapshot) {
  capacity_ = 64;
  stored_before_ = StoredBefore::NEVER;
  create();

  ASSERT_TRUE(cache_->neverStored());
  ASSERT_EQ(ESN_INVALID, cache_->getLNG());
  ASSERT_EQ(ESN_INVALID, cache_->getMaxSeenESN());

  auto snapshot = cache_->createSnapshot();
  ASSERT_NE(nullptr, snapshot);
  ASSERT_TRUE(snapshot->empty());
  ASSERT_FALSE(snapshot->getRecord(ESN_MIN).first);
  auto it = snapshot->createIterator();
  ASSERT_TRUE(it->atEnd());

  // insert three records (7, /lng/2), (9, 3), (15, 4)
  int rv = putRecord(cache_.get(), lsn(EPOCH, 7), 2);
  ASSERT_EQ(0, rv);
  rv = putRecord(cache_.get(), lsn(EPOCH, 9), 3);
  ASSERT_EQ(0, rv);
  rv = putRecord(cache_.get(), lsn(EPOCH, 15), 4);
  ASSERT_EQ(0, rv);
  // snapshot in [2, 6] should be empty
  snapshot = cache_->createSnapshot(esn_t(2), esn_t(6));
  ASSERT_NE(nullptr, snapshot);
  ASSERT_TRUE(snapshot->empty());
  it = snapshot->createIterator();
  ASSERT_TRUE(it->atEnd());
  // snapshot in [7, 7] should have 1 record
  snapshot = cache_->createSnapshot(esn_t(7), esn_t(7));
  ASSERT_FALSE(snapshot->empty());
  ASSERT_TRUE(snapshot->getRecord(esn_t(7)).first);
  it = snapshot->createIterator();
  ASSERT_FALSE(it->atEnd());
  ASSERT_EQ(esn_t(7), it->getESN());
  it->next();
  ASSERT_TRUE(it->atEnd());
  // snapshot [16, ESN_MAX] should be empty
  snapshot = cache_->createSnapshot(esn_t(16), ESN_MAX);
  ASSERT_TRUE(snapshot->empty());
  // snapshot in [8, 15] should have 2 records
  snapshot = cache_->createSnapshot(esn_t(8), esn_t(15));
  ASSERT_FALSE(snapshot->empty());
  it = snapshot->createIterator();
  ASSERT_EQ(esn_t(9), it->getESN());
  it->next();
  ASSERT_EQ(esn_t(15), it->getESN());
  it->next();
  ASSERT_TRUE(it->atEnd());
  // snapshot should still be valid after the detruction of the cache
  cache_.reset();
  it = snapshot->createIterator();
  ASSERT_EQ(esn_t(9), it->getESN());
  auto record = it->getRecord();
  ASSERT_EQ(sizeof(lsn_t), record.payload_raw.size);
  ASSERT_EQ(lsn(EPOCH, 9), *((lsn_t*)record.payload_raw.data));
}

TEST_F(EpochRecordCacheTest, MultithreadedAppend) {
  multi_threaded_ = true;
  capacity_ = 64;
  stored_before_ = StoredBefore::NEVER;
  dropped_.reserve(5000);
  create();

  const size_t total_records_est = 2000;
  const size_t num_threads = 12;
  Semaphore sem;

  auto writer = [&]() {
    while (append_success_.load() < total_records_est) {
      lsn_t lsn = doAppend();
      if (lsn == LSN_INVALID) {
        /* sleep override */
        std::this_thread::sleep_for(
            std::chrono::milliseconds(folly::Random::rand32(1, 10)));
      } else {
        // must be in the same epoch where seq_ is created
        ASSERT_EQ(EPOCH, lsn_to_epoch(lsn));
      }
    }
    sem.post();
  };

  std::vector<std::thread> writer_threads;
  for (int i = 0; i < num_threads; ++i) {
    writer_threads.emplace_back(writer);
  }
  for (int i = 0; i < num_threads; ++i) {
    sem.wait();
  }

  for (std::thread& t : writer_threads) {
    t.join();
  }

  ld_info("Append success: %lu, failed: %lu",
          append_success_.load(),
          append_failed_.load());
  ld_info("Max LSN: %s, Sequencer lng: %u, Cache lng: %u",
          lsn_to_string(max_lsn_.load()).c_str(),
          seq_->getLNG().val_,
          cache_->getLNG().val_);
  ld_info("Writes cached: %lu, failed to cache: %lu, entries dropped: %lu",
          append_cached_.load(),
          append_cached_failed_.load(),
          dropped_.size());

  // check invariants
  ASSERT_EQ(append_success_.load(), append_cached_.load());
  ASSERT_EQ(0, append_cached_failed_.load());
  ASSERT_EQ(append_success_.load(),
            (lsn_to_esn(max_lsn_.load()).val_ - cache_->getLNG().val_ +
             dropped_.size()));
  for (esn_t::raw_type e = cache_->getLNG().val_ + 1;
       e <= lsn_to_esn(max_lsn_.load()).val_;
       ++e) {
    ASSERT_CACHE_ENTRY(cache_, e, lsn(EPOCH, e));
  }
}

TEST_F(EpochRecordCacheTest, EntrySequencing) {
  capacity_ = 64;
  stored_before_ = StoredBefore::NEVER;
  create();

  // Create two buffers
  size_t buflen = 1000000; // 1mb
  std::unique_ptr<char[]> buf1(new char[buflen]);
  std::unique_ptr<char[]> buf2(new char[buflen]);
  memset(buf1.get(), 0, buflen);
  memset(buf2.get(), 0, buflen);

  // Setup Slice, PayloadHolder objects
  size_t payload_len = 100;
  size_t payload_len_short = 10;
  auto payload1 = createPayload(payload_len, 0);
  auto payload2 = createPayload(payload_len, 0);
  auto payload_short = createPayload(payload_len_short, 1);
  Slice slice1 = Slice(payload1->getPayload());
  Slice slice2 = Slice(payload2->getPayload());
  Slice slice_short = Slice(payload_short->getPayload());

  // Serialize OffsetMap
  uint32_t flags = STORE_Header::OFFSET_MAP;

  // Serialize different Entry objects w/ equal values, expect same result
  copyset_t copyset({N0, N1, N2});
  auto entry1 = std::shared_ptr<Entry>(new Entry(0,
                                                 flags,
                                                 10,
                                                 esn_t(11),
                                                 12,
                                                 copyset,
                                                 OffsetMap({{BYTE_OFFSET, 15}}),
                                                 KeysType{},
                                                 slice1,
                                                 payload1),
                                       Entry::Disposer(deps_.get()));
  auto entry2 = std::shared_ptr<Entry>(new Entry(0,
                                                 flags,
                                                 10,
                                                 esn_t(11),
                                                 12,
                                                 copyset,
                                                 OffsetMap({{BYTE_OFFSET, 15}}),
                                                 KeysType{},
                                                 slice2,
                                                 payload2),
                                       Entry::Disposer(deps_.get()));
  int entry1_serial_len = entry1->toLinearBuffer(buf1.get(), buflen);
  ASSERT_NE(entry1_serial_len, -1);
  ASSERT_NE(memcmp(buf1.get(), buf2.get(), entry1_serial_len), 0);
  int entry2_serial_len = entry2->toLinearBuffer(buf2.get(), buflen);
  ASSERT_NE(entry2_serial_len, -1);

  ASSERT_EQ(entry1_serial_len, entry2_serial_len);
  ASSERT_EQ(memcmp(buf1.get(), buf2.get(), entry1_serial_len), 0);

  // Compare w/ Entry w/ different payloads, expect different result
  entry2 = std::shared_ptr<Entry>(new Entry(0,
                                            0,
                                            10,
                                            esn_t(11),
                                            12,
                                            copyset,
                                            OffsetMap({{BYTE_OFFSET, 15}}),
                                            KeysType{},
                                            slice_short,
                                            payload_short),
                                  Entry::Disposer(deps_.get()));
  entry2_serial_len = entry2->toLinearBuffer(buf2.get(), buflen);
  ASSERT_NE(entry2_serial_len, -1);
  ASSERT_NE(entry1_serial_len, entry2_serial_len);
  ASSERT_NE(memcmp(buf1.get(),
                   buf2.get(),
                   std::max(entry1_serial_len, entry2_serial_len)),
            0);

  // Construct entry2 from the sequenced data, expect equality
  auto reconstructed_entry = EpochRecordCacheEntry::createFromLinearBuffer(
      0, buf2.get(), buflen, Entry::Disposer(deps_.get()));
  ASSERT_NE(reconstructed_entry, nullptr);

  // Verify that header fields are written and read correcly
  ASSERT_EQ(entry2->flags, reconstructed_entry->flags);
  ASSERT_EQ(entry2->timestamp, reconstructed_entry->timestamp);
  ASSERT_EQ(entry2->last_known_good, reconstructed_entry->last_known_good);
  ASSERT_EQ(entry2->wave_or_recovery_epoch,
            reconstructed_entry->wave_or_recovery_epoch);
  ASSERT_EQ(entry2->copyset.size(), reconstructed_entry->copyset.size());
  ASSERT_TRUE(std::equal(entry2->copyset.begin(),
                         entry2->copyset.end(),
                         reconstructed_entry->copyset.begin()));
  ASSERT_EQ(entry2->keys, reconstructed_entry->keys);
  ASSERT_EQ(entry2->payload_raw.size, reconstructed_entry->payload_raw.size);
  ASSERT_EQ(memcmp(entry2->payload_raw.data,
                   reconstructed_entry->payload_raw.data,
                   entry2->payload_raw.size),
            0);

  // Compare the serializations, expect equality
  int reconstructed_len =
      reconstructed_entry->toLinearBuffer(buf1.get(), buflen);
  ASSERT_NE(reconstructed_len, -1);
  ASSERT_EQ(reconstructed_len, entry2_serial_len);
  ASSERT_EQ(memcmp(buf1.get(), buf2.get(), buflen), 0);
}

TEST_F(EpochRecordCacheTest, SnapshotSequencingWithOffsetMap) {
  capacity_ = 64;
  stored_before_ = StoredBefore::NEVER;
  bool enable_offset_map = true;
  create();

  size_t buflen = 1000000; // 1mb
  std::unique_ptr<char[]> buf(new char[buflen]);
  memset(buf.get(), 0, buflen);

  // Create snapshot, verify linearized size
  auto snapshot = cache_->createSerializableSnapshot(enable_offset_map);
  ASSERT_NE(nullptr, snapshot);
  auto linear_size = snapshot->toLinearBuffer(buf.get(), buflen);
  ASSERT_NE(linear_size, -1);
  ASSERT_EQ(linear_size, snapshot->sizeInLinearBuffer());
  auto snapshot_repopulated =
      EpochRecordCache::Snapshot::createFromLinearBuffer(
          buf.get(), buflen, deps_.get());
  ASSERT_NE(snapshot_repopulated, nullptr);
  ASSERT_TRUE(testSnapshotsIdentical(*snapshot, *snapshot_repopulated));
}

TEST_F(EpochRecordCacheTest, SnapshotSequencing) {
  capacity_ = 64;
  stored_before_ = StoredBefore::NEVER;
  create();

  size_t buflen = 1000000; // 1mb
  std::unique_ptr<char[]> buf(new char[buflen]);
  std::unique_ptr<char[]> buf_old(new char[buflen]);
  memset(buf.get(), 0, buflen);
  memset(buf_old.get(), 0, buflen);

  // Create partial snapshot, should not linearize
  auto snapshot = cache_->createSnapshot();
  ASSERT_NE(nullptr, snapshot);
  ASSERT_TRUE(snapshot->empty());
  auto linear_size = snapshot->toLinearBuffer(buf.get(), buflen);
  ASSERT_EQ(linear_size, -1);
  ASSERT_EQ(memcmp(buf.get(), buf_old.get(), buflen), 0);

  // Create snapshot, verify linearized size
  snapshot = cache_->createSerializableSnapshot();
  ASSERT_NE(nullptr, snapshot);
  ASSERT_TRUE(snapshot->empty());
  linear_size = snapshot->toLinearBuffer(buf.get(), buflen);
  ASSERT_NE(linear_size, -1);
  ASSERT_EQ(linear_size, sizeof(EpochRecordCacheHeader));
  ASSERT_NE(memcmp(buf.get(), buf_old.get(), buflen), 0);

  // Repopulate empty snapshot, check equality
  auto snapshot_repopulated =
      EpochRecordCache::Snapshot::createFromLinearBuffer(
          buf.get(), buflen, deps_.get());
  ASSERT_NE(snapshot_repopulated, nullptr);
  ASSERT_TRUE(testSnapshotsIdentical(*snapshot, *snapshot_repopulated));

  memcpy(buf_old.get(), buf.get(), buflen);
  auto linear_size_old = linear_size;
  linear_size = snapshot_repopulated->toLinearBuffer(buf.get(), buflen);
  ASSERT_EQ(linear_size, linear_size_old);
  ASSERT_EQ(memcmp(buf.get(), buf_old.get(), buflen), 0);

  // insert three records (7, /lng/2), (9, 3), (15, 4)
  int rv = putRecord(cache_.get(), lsn(EPOCH, 7), 2);
  ASSERT_EQ(0, rv);
  rv = putRecord(cache_.get(), lsn(EPOCH, 9), 3);
  ASSERT_EQ(0, rv);
  rv = putRecord(cache_.get(), lsn(EPOCH, 15), 4);
  ASSERT_EQ(0, rv);

  // Create partial snapshot - this time with content. Should not linearize
  snapshot = cache_->createSnapshot();
  ASSERT_NE(nullptr, snapshot);
  ASSERT_FALSE(snapshot->empty());
  linear_size = snapshot->toLinearBuffer(buf.get(), buflen);
  ASSERT_EQ(linear_size, -1);
  ASSERT_EQ(memcmp(buf.get(), buf_old.get(), buflen), 0);

  // Create new snapshot, linearize
  snapshot = cache_->createSerializableSnapshot();
  ASSERT_NE(nullptr, snapshot);
  ASSERT_FALSE(snapshot->empty());
  linear_size = snapshot->toLinearBuffer(buf.get(), buflen);
  ASSERT_NE(linear_size, -1);
  ASSERT_NE(linear_size, linear_size_old);
  ASSERT_NE(memcmp(buf.get(), buf_old.get(), buflen), 0);

  linear_size_old = linear_size;
  memcpy(buf_old.get(), buf.get(), buflen);

  // Repopulate a snapshot from linearized data, verify equality
  snapshot_repopulated = EpochRecordCache::Snapshot::createFromLinearBuffer(
      buf.get(), buflen, deps_.get());
  ASSERT_NE(snapshot_repopulated, nullptr);
  ASSERT_TRUE(testSnapshotsIdentical(*snapshot, *snapshot_repopulated));
  linear_size = snapshot_repopulated->toLinearBuffer(buf.get(), buflen);
  ASSERT_EQ(linear_size, linear_size_old);
  ASSERT_EQ(memcmp(buf.get(), buf_old.get(), buflen), 0);
}

TEST_F(EpochRecordCacheTest, BasicSequencing) {
  capacity_ = 64;
  stored_before_ = StoredBefore::NEVER;
  tail_optimized_ = true;
  create();

  size_t buflen = 1'000'000; // 1mb
  std::unique_ptr<char[]> buf(new char[buflen]);
  memset(buf.get(), 0, buflen);

  // Cache is currently empty. Linearize, repopulate, confirm equality.
  auto linear_size = cache_->toLinearBuffer(buf.get(), buflen);
  ASSERT_NE(linear_size, -1);
  ASSERT_EQ(linear_size, sizeof(EpochRecordCacheHeader));
  std::unique_ptr<EpochRecordCache> repopulated_cache =
      EpochRecordCache::fromLinearBuffer(
          LOG_ID, SHARD, buf.get(), buflen, deps_.get());
  ASSERT_NE(nullptr, repopulated_cache);
  ASSERT_TRUE(testEpochRecordCachesIdentical(*cache_, *repopulated_cache));
  ASSERT_NO_TAIL_RECORD(cache_);

  // Insert three records, expect caches to be different
  int rv = putRecord(cache_.get(), lsn(EPOCH, 2), 1);
  ASSERT_EQ(0, rv);
  rv = putRecord(cache_.get(),
                 lsn(EPOCH, 3),
                 2,
                 STORE_Header::CUSTOM_KEY,
                 {{KeyType::FINDKEY, "blah"}, {KeyType::FILTERABLE, "yummy"}});
  ASSERT_EQ(0, rv);
  rv = putRecord(cache_.get(), lsn(EPOCH, 15), 4);
  ASSERT_EQ(0, rv);
  ASSERT_TAIL_RECORD(cache_, lsn(EPOCH, 3));
  ASSERT_FALSE(testEpochRecordCachesIdentical(*cache_, *repopulated_cache));

  ASSERT_EQ(esn_t(4), cache_->getLNG());
  ASSERT_EQ(esn_t(15), cache_->getMaxSeenESN());
  // timestamp is the same as LSN in these tests
  const uint64_t expect_ts = uint64_t(lsn(EPOCH, 15));
  ASSERT_EQ(expect_ts, cache_->getMaxSeenTimestamp());

  // Linearize cache_, repopulate a cache and verify equality
  cache_->toLinearBuffer(buf.get(), buflen);
  repopulated_cache = EpochRecordCache::fromLinearBuffer(
      LOG_ID, SHARD, buf.get(), buflen, deps_.get());
  ASSERT_NE(nullptr, repopulated_cache);
  ASSERT_TRUE(testEpochRecordCachesIdentical(*cache_, *repopulated_cache));
  ASSERT_TRUE(repopulated_cache->isTailOptimized());
  ASSERT_TAIL_RECORD(cache_, lsn(EPOCH, 3));
  ASSERT_TAIL_RECORD(repopulated_cache, lsn(EPOCH, 3));

  ASSERT_EQ(esn_t(4), repopulated_cache->getLNG());
  ASSERT_EQ(esn_t(15), repopulated_cache->getMaxSeenESN());
  // timestamp is the same as LSN in these tests
  ASSERT_EQ(expect_ts, repopulated_cache->getMaxSeenTimestamp());

  // Change offsets within epoch, expect inequality
  cache_->setOffsetsWithinEpoch(OffsetMap({{BYTE_OFFSET, 1337}}));
  ASSERT_FALSE(testEpochRecordCachesIdentical(*cache_, *repopulated_cache));

  // Linearize cache_, repopulate a cache and verify equality
  cache_->toLinearBuffer(buf.get(), buflen);
  repopulated_cache = EpochRecordCache::fromLinearBuffer(
      LOG_ID, SHARD, buf.get(), buflen, deps_.get());
  ASSERT_NE(nullptr, repopulated_cache);
  ASSERT_TRUE(testEpochRecordCachesIdentical(*cache_, *repopulated_cache));
  ASSERT_TRUE(repopulated_cache->isTailOptimized());
  ASSERT_TAIL_RECORD(cache_, lsn(EPOCH, 3));
  ASSERT_TAIL_RECORD(repopulated_cache, lsn(EPOCH, 3));
}

TEST_F(EpochRecordCacheTest, SequencingProhibitedCases) {
  capacity_ = 64;
  stored_before_ = StoredBefore::NEVER;
  create();

  size_t buflen = 1000000; // 1mb
  std::unique_ptr<char[]> buf(new char[buflen]);
  memset(buf.get(), 0, buflen);

  // Verify we can't create EpochRecordCache from a partial snapshot
  auto snapshot = cache_->createSnapshot(esn_t(1), esn_t(5));
  ASSERT_NE(nullptr, snapshot);
  auto repopulated_cache = EpochRecordCache::createFromSnapshot(
      LOG_ID, SHARD, *snapshot, deps_.get());
  ASSERT_EQ(repopulated_cache, nullptr);
}

TEST_F(EpochRecordCacheTest, SequencingDisabledCache) {
  capacity_ = 10;
  stored_before_ = StoredBefore::NEVER;
  create();

  size_t buflen = 1000000; // 1mb
  std::unique_ptr<char[]> buf(new char[buflen]);
  std::unique_ptr<char[]> buf_backup(new char[buflen]);

  // Verify we can write a disabled cache to linearized buffer
  cache_->disableCache();
  ASSERT_FALSE(cache_->isConsistent());
  auto linear_size = cache_->toLinearBuffer(buf.get(), buflen);
  ASSERT_EQ(linear_size, sizeof(EpochRecordCacheHeader));
  std::unique_ptr<EpochRecordCache> repopulated_cache =
      EpochRecordCache::fromLinearBuffer(
          LOG_ID, SHARD, buf.get(), buflen, deps_.get());
  ASSERT_NE(nullptr, repopulated_cache);
  ASSERT_FALSE(repopulated_cache->isConsistent());
  ASSERT_EQ(repopulated_cache->capacity(), cache_->capacity());
  ASSERT_EQ(repopulated_cache->bufferedRecords(), 0);
  ASSERT_EQ(repopulated_cache->bufferedPayloadBytes(), 0);
  ASSERT_EQ(repopulated_cache->getMaxSeenESN(), ESN_INVALID);
  ASSERT_TRUE(repopulated_cache->neverStored());
  ASSERT_EQ(repopulated_cache->getOffsetsWithinEpoch(),
            cache_->getOffsetsWithinEpoch());
  repopulated_cache->disableCache();
  ASSERT_TRUE(testEpochRecordCachesIdentical(*cache_, *repopulated_cache));
  cache_.reset();
  repopulated_cache.reset();
  create();

  // Verify serialization of disabled cache ignores entries
  int rv = putRecord(cache_.get(), lsn(EPOCH, 4), 2);
  ASSERT_EQ(0, rv);
  ASSERT_CACHE_ENTRY(cache_, 4, lsn(EPOCH, 4));
  linear_size = cache_->toLinearBuffer(buf_backup.get(), buflen);
  ASSERT_NE(linear_size, sizeof(EpochRecordCacheHeader));
  ASSERT_NE(memcmp(buf.get(), buf_backup.get(), linear_size), 0);

  cache_->disableCache();
  linear_size = cache_->toLinearBuffer(buf_backup.get(), buflen);
  ASSERT_EQ(linear_size, sizeof(EpochRecordCacheHeader));
  ASSERT_EQ(memcmp(buf.get(), buf_backup.get(), linear_size), 0);
  ASSERT_FALSE(cache_->isConsistent());
  ASSERT_NO_CACHE_ENTRY(cache_, 4);

  repopulated_cache = EpochRecordCache::fromLinearBuffer(
      LOG_ID, SHARD, buf.get(), buflen, deps_.get());
  ASSERT_FALSE(repopulated_cache->isConsistent());

  // Verify cache can become consistent!
  // insert one record of esn 4, lng 2, cache should still be inconsistent
  rv = putRecord(repopulated_cache.get(), lsn(EPOCH, 4), 2);
  ASSERT_CACHE_ENTRY(repopulated_cache, 4, lsn(EPOCH, 4));
  ASSERT_FALSE(repopulated_cache->isConsistent());

  // insert esn 13, lng 12, the cache should be consistent
  rv = putRecord(repopulated_cache.get(), lsn(EPOCH, 13), 12);
  ASSERT_EQ(0, rv);
  ASSERT_EQ(esn_t(12), repopulated_cache->getLNG());
  ASSERT_CACHE_ENTRY(repopulated_cache, 13, lsn(EPOCH, 13));
  ASSERT_TRUE(repopulated_cache->isConsistent());

  // Go to linear buffer and back to a cache, verify that it is consistent
  linear_size = repopulated_cache->toLinearBuffer(buf.get(), buflen);
  ASSERT_NE(linear_size, -1);
  repopulated_cache = EpochRecordCache::fromLinearBuffer(
      LOG_ID, SHARD, buf.get(), buflen, deps_.get());
  ASSERT_NE(repopulated_cache, nullptr);
  ASSERT_TRUE(repopulated_cache->isConsistent());

  cache_.reset();
  repopulated_cache.reset();
  create();

  // Verify that having the LNG be ESN_INVALID does not make the cache
  // repopulate as if it had been disabled
  rv = putRecord(cache_.get(), lsn(EPOCH, 4), ESN_INVALID.val());
  ASSERT_CACHE_ENTRY(cache_, 4, lsn(EPOCH, 4));
  linear_size = cache_->toLinearBuffer(buf.get(), buflen);
  ASSERT_NE(linear_size, -1);
  repopulated_cache = EpochRecordCache::fromLinearBuffer(
      LOG_ID, SHARD, buf.get(), buflen, deps_.get());
  ASSERT_NE(repopulated_cache.get(), nullptr);
  ASSERT_TRUE(testEpochRecordCachesIdentical(*cache_, *repopulated_cache));
}

TEST_F(EpochRecordCacheTest, SequencingInconsistentCache) {
  capacity_ = 10;
  stored_before_ = StoredBefore::MAYBE;
  tail_optimized_ = true;
  std::vector<std::shared_ptr<EpochRecordCacheEntry>> dropped2;
  create();

  ASSERT_TRUE(cache_->isTailOptimized());
  size_t buflen = 1000000; // 1mb
  std::unique_ptr<char[]> buf(new char[buflen]);
  memset(buf.get(), 0, buflen);

  ASSERT_EQ(ESN_INVALID, cache_->getLNG());
  // cache is not safe to use at this time since it does not know
  // whether there are records stored in the epoch before
  ASSERT_FALSE(cache_->isConsistent());

  // insert one record of esn 4, lng 2, cache should still be inconsistent
  int rv = putRecord(cache_.get(), lsn(EPOCH, 4), 2);
  ASSERT_EQ(0, rv);
  ASSERT_TRUE(dropped_.empty());
  ASSERT_EQ(esn_t(2), cache_->getLNG());
  ASSERT_CACHE_ENTRY(cache_, 4, lsn(EPOCH, 4));
  ASSERT_FALSE(cache_->isConsistent());
  ASSERT_NO_TAIL_RECORD(cache_);

  // Go to linear buffer and back to a cache, verify inconsistency remains
  auto linear_size = cache_->toLinearBuffer(buf.get(), buflen);
  ASSERT_NE(linear_size, -1);
  ASSERT_EQ(0, dropped_.size());
  auto repopulated_cache = EpochRecordCache::fromLinearBuffer(
      LOG_ID, SHARD, buf.get(), buflen, deps_.get());
  auto dropped_offset = dropped_.size();
  ASSERT_NE(repopulated_cache, nullptr);
  ASSERT_FALSE(repopulated_cache->isConsistent());
  ASSERT_NO_TAIL_RECORD(repopulated_cache);
  ASSERT_TRUE(repopulated_cache->isTailOptimized());

  // insert esn 8, lng 6, the first record should be dropped, but
  // the cache is still inconsistent
  rv = putRecord(cache_.get(), lsn(EPOCH, 8), 6);
  ASSERT_EQ(0, rv);
  ASSERT_EQ(esn_t(6), cache_->getLNG());
  // tail entry are kept in record cache as tail record and not destroyed yet
  ASSERT_TRUE(dropped_.empty());
  ASSERT_CACHE_ENTRY(cache_, 8, lsn(EPOCH, 8));
  ASSERT_NO_CACHE_ENTRY(cache_, 4);
  ASSERT_FALSE(cache_->isConsistent());
  ASSERT_TAIL_RECORD(cache_, lsn(EPOCH, 4));

  // Go to linear buffer and back to a cache, verify inconsistency remains
  linear_size = cache_->toLinearBuffer(buf.get(), buflen);
  ASSERT_NE(linear_size, -1);
  auto dropped_before = dropped_.size();
  repopulated_cache = EpochRecordCache::fromLinearBuffer(
      LOG_ID, SHARD, buf.get(), buflen, deps_.get());
  ASSERT_NE(nullptr, repopulated_cache);
  ASSERT_TRUE(testEpochRecordCachesIdentical(*cache_, *repopulated_cache));
  dropped_offset += dropped_.size() - dropped_before;
  ASSERT_NE(repopulated_cache, nullptr);
  ASSERT_FALSE(repopulated_cache->isConsistent());
  ASSERT_TAIL_RECORD(repopulated_cache, lsn(EPOCH, 4));

  // insert esn 13, lng 12, the cache should be consistent
  rv = putRecord(cache_.get(), lsn(EPOCH, 13), 12);
  ASSERT_EQ(0, rv);
  ASSERT_EQ(esn_t(12), cache_->getLNG());
  ASSERT_EQ(2 + dropped_offset - 1, dropped_.size());
  // 8 evicted to become tail record, 4 (previous tail) will be dropped
  verifyEntry(*dropped_.back(), lsn(EPOCH, 4));
  ASSERT_NO_CACHE_ENTRY(cache_, 8);
  ASSERT_CACHE_ENTRY(cache_, 13, lsn(EPOCH, 13));
  ASSERT_TRUE(cache_->isConsistent());
  ASSERT_TAIL_RECORD(cache_, lsn(EPOCH, 8));

  // Go to linear buffer and back to a cache, verify that it is consistent
  linear_size = cache_->toLinearBuffer(buf.get(), buflen);
  ASSERT_NE(linear_size, -1);
  repopulated_cache = EpochRecordCache::fromLinearBuffer(
      LOG_ID, SHARD, buf.get(), buflen, deps_.get());
  ASSERT_NE(repopulated_cache, nullptr);
  ASSERT_TRUE(repopulated_cache->isConsistent());
  ASSERT_TAIL_RECORD(repopulated_cache, lsn(EPOCH, 8));
}

TEST_F(EpochRecordCacheTest, SequencingAdvanceLNG) {
  capacity_ = 64;
  stored_before_ = StoredBefore::MAYBE;
  create();

  size_t buflen = 1000000; // 1mb
  std::unique_ptr<char[]> buf(new char[buflen]);
  memset(buf.get(), 0, buflen);

  // Insert three records, expect caches to be different
  int rv = putRecord(cache_.get(), lsn(EPOCH, 7), 2);
  ASSERT_EQ(0, rv);
  rv = putRecord(cache_.get(), lsn(EPOCH, 9), 3);
  ASSERT_EQ(0, rv);
  rv = putRecord(cache_.get(), lsn(EPOCH, 15), 4);
  ASSERT_EQ(0, rv);

  // Linearize cache_, repopulate a cache and verify equality
  cache_->toLinearBuffer(buf.get(), buflen);
  auto repopulated_cache = EpochRecordCache::fromLinearBuffer(
      LOG_ID, SHARD, buf.get(), buflen, deps_.get());
  ASSERT_NE(nullptr, repopulated_cache);
  ASSERT_TRUE(testEpochRecordCachesIdentical(*cache_, *repopulated_cache));

  // Advance LNG to drop some records, verify inequality
  cache_->advanceLNG(esn_t(10));
  ASSERT_FALSE(testEpochRecordCachesIdentical(*cache_, *repopulated_cache));

  // Should be equal if we advance LNG in the repopulated cache as well
  repopulated_cache->advanceLNG(esn_t(10));
  ASSERT_TRUE(testEpochRecordCachesIdentical(*cache_, *repopulated_cache));

  // Linearize and repopulate. Should be equal.
  cache_->toLinearBuffer(buf.get(), buflen);
  repopulated_cache = EpochRecordCache::fromLinearBuffer(
      LOG_ID, SHARD, buf.get(), buflen, deps_.get());
  ASSERT_NE(nullptr, repopulated_cache);
  ASSERT_TRUE(testEpochRecordCachesIdentical(*cache_, *repopulated_cache));

  // Advance LNG to drop all records, verify inequality
  cache_->advanceLNG(esn_t(ESN_MAX));
  ASSERT_FALSE(testEpochRecordCachesIdentical(*cache_, *repopulated_cache));

  // Should be equal if we advance LNG in the repopulated cache as well
  repopulated_cache->advanceLNG(esn_t(ESN_MAX));
  ASSERT_TRUE(testEpochRecordCachesIdentical(*cache_, *repopulated_cache));

  // Again, linearize and repopulate, and verify equality
  cache_->toLinearBuffer(buf.get(), buflen);
  repopulated_cache = EpochRecordCache::fromLinearBuffer(
      LOG_ID, SHARD, buf.get(), buflen, deps_.get());
  ASSERT_NE(nullptr, repopulated_cache);
  ASSERT_TRUE(testEpochRecordCachesIdentical(*cache_, *repopulated_cache));
}

TEST_F(EpochRecordCacheTest, SequencingSizeCalculation) {
  capacity_ = 64;
  stored_before_ = StoredBefore::MAYBE;
  create();

  size_t buflen = 1000000; // 1mb
  std::unique_ptr<char[]> buf(new char[buflen]);
  memset(buf.get(), 0, buflen);

  // 1) Confirm sizes match for empty cache
  auto calculated_size = cache_->sizeInLinearBuffer();
  ASSERT_NE(calculated_size, -1);
  ASSERT_EQ(sizeof(EpochRecordCacheSerializer::CacheHeader), calculated_size);
  auto linear_size = cache_->toLinearBuffer(buf.get(), buflen);
  ASSERT_NE(linear_size, -1);
  ASSERT_EQ(calculated_size, linear_size);

  // 2) Add records, confirm sizes match
  int rv = putRecord(cache_.get(), lsn(EPOCH, 4), 2);
  ASSERT_EQ(0, rv);
  rv = putRecord(cache_.get(), lsn(EPOCH, 6), 2);
  ASSERT_EQ(0, rv);
  calculated_size = cache_->sizeInLinearBuffer();
  ASSERT_NE(calculated_size, -1);
  ASSERT_NE(sizeof(EpochRecordCacheSerializer::CacheHeader), calculated_size);
  linear_size = cache_->toLinearBuffer(buf.get(), buflen);
  ASSERT_NE(linear_size, -1);
  ASSERT_EQ(calculated_size, linear_size);

  // 3) Disable cache, verify size is that of an empty cache
  cache_->disableCache();
  calculated_size = cache_->sizeInLinearBuffer();
  ASSERT_EQ(calculated_size, sizeof(EpochRecordCacheHeader));
}

TEST_F(EpochRecordCacheTest, SequencingSizeCalculation2) {
  capacity_ = 64;
  tail_optimized_ = true;
  stored_before_ = StoredBefore::MAYBE;
  create();
  size_t buflen = 1000000; // 1mb
  std::unique_ptr<char[]> buf(new char[buflen]);
  memset(buf.get(), 0, buflen);
  size_t r0, r1, r1_tail, rd;

  r0 = cache_->sizeInLinearBuffer();
  ASSERT_EQ(sizeof(EpochRecordCacheSerializer::CacheHeader), r0);
  auto linear_size = cache_->toLinearBuffer(buf.get(), buflen);
  ASSERT_EQ(r0, linear_size);

  int rv = putRecord(cache_.get(), lsn(EPOCH, 4), 2);
  ASSERT_EQ(0, rv);
  r1 = cache_->sizeInLinearBuffer();
  ASSERT_NE(sizeof(EpochRecordCacheSerializer::CacheHeader), r1);
  ASSERT_NO_TAIL_RECORD(cache_);

  rv = putRecord(cache_.get(), lsn(EPOCH, 6), 4);
  ASSERT_EQ(0, rv);
  ASSERT_TAIL_RECORD(cache_, lsn(EPOCH, 4));
  r1_tail = cache_->sizeInLinearBuffer();
  ASSERT_NE(r1, r1_tail);
  TailRecord tr = cache_->getTailRecord();
  ASSERT_EQ(r1 + tr.sizeInLinearBuffer(), r1_tail);
  linear_size = cache_->toLinearBuffer(buf.get(), buflen);
  ASSERT_EQ(r1_tail, linear_size);

  cache_->disableCache();
  rd = cache_->sizeInLinearBuffer();
  ASSERT_NO_TAIL_RECORD(cache_);
  ASSERT_EQ(r0, rd);
  linear_size = cache_->toLinearBuffer(buf.get(), buflen);
  ASSERT_EQ(rd, linear_size);
}

// Are keys properly stored & retrieved?
TEST_F(EpochRecordCacheTest, Keys) {
  stored_before_ = StoredBefore::NEVER;
  create();

  // Just findkey.
  int rv = putRecord(cache_.get(),
                     lsn(EPOCH, 4),
                     2,
                     STORE_Header::CUSTOM_KEY,
                     {{KeyType::FINDKEY, "hello"}});
  ASSERT_EQ(0, rv);
  ASSERT_TRUE(dropped_.empty());
  ASSERT_TRUE(cache_->isConsistent());
  ASSERT_CACHE_ENTRY_FLAG(cache_,
                          4,
                          lsn(EPOCH, 4),
                          STORE_Header::CUSTOM_KEY,
                          (KeysType{{KeyType::FINDKEY, "hello"}}),
                          uint32_t(1),
                          copyset_t({N0, N1, N2}));

  // Just filterable.
  rv = putRecord(cache_.get(),
                 lsn(EPOCH, 5),
                 2,
                 STORE_Header::CUSTOM_KEY,
                 {{KeyType::FILTERABLE, "goodbye"}},
                 uint32_t(1),
                 copyset_t({N2, N3, N4}));
  ASSERT_EQ(0, rv);
  ASSERT_TRUE(cache_->isConsistent());
  ASSERT_EQ(esn_t(2), cache_->getLNG());
  ASSERT_CACHE_ENTRY_FLAG(cache_,
                          5,
                          lsn(EPOCH, 5),
                          STORE_Header::CUSTOM_KEY,
                          (KeysType{{KeyType::FILTERABLE, "goodbye"}}),
                          1,
                          copyset_t({N2, N3, N4}));

  // Both keys.
  rv = putRecord(cache_.get(),
                 lsn(EPOCH, 6),
                 2,
                 STORE_Header::CUSTOM_KEY,
                 {{KeyType::FINDKEY, "blah"}, {KeyType::FILTERABLE, "yummy"}},
                 uint32_t(1),
                 copyset_t({N2, N3, N4}));
  ASSERT_EQ(0, rv);
  ASSERT_TRUE(cache_->isConsistent());
  ASSERT_EQ(esn_t(2), cache_->getLNG());
  ASSERT_CACHE_ENTRY_FLAG(
      cache_,
      6,
      lsn(EPOCH, 6),
      STORE_Header::CUSTOM_KEY,
      (KeysType{{KeyType::FINDKEY, "blah"}, {KeyType::FILTERABLE, "yummy"}}),
      1,
      copyset_t({N2, N3, N4}));
}
// TODO:add tests for ESN_MAX corner cases

///////////////////// RecordCache Tests /////////////////////////

class MockRecordCacheDependencies;

class RecordCacheTest : public ::testing::Test {
 public:
  size_t epoch_cache_capacity_ = 128;
  std::mutex mutex_;
  std::vector<std::unique_ptr<Entry>> dropped_;
  std::unique_ptr<RecordCacheDependencies> deps_;
  std::unique_ptr<RecordCache> cache_;
  folly::Optional<epoch_t> initial_seal_epoch_{EPOCH_INVALID};
  folly::Optional<epoch_t> initial_soft_seal_epoch_{EPOCH_INVALID};
  folly::Optional<lsn_t> highest_lsn_;

  bool tail_optimized_{false};

  void create();

  bool persistAndRepopulateCache(char* buf, size_t buflen);

  bool testRecordCachesIdentical(const RecordCache& a, const RecordCache& b) {
    return RecordCacheSerializer::RecordCacheCompare ::
        testRecordCachesIdentical(a, b);
  }
};

class MockRecordCacheDependencies : public RecordCacheDependencies {
 public:
  explicit MockRecordCacheDependencies(RecordCacheTest* test) : test_(test) {}

  void disposeOfCacheEntry(std::unique_ptr<Entry> entry) override {
    std::lock_guard<std::mutex> guard(test_->mutex_);
    test_->dropped_.push_back(std::move(entry));
  }

  void onRecordsReleased(const EpochRecordCache&,
                         lsn_t /*begin*/,
                         lsn_t /*end*/,
                         const ReleasedVector& /*entries*/) override {}

  folly::Optional<Seal> getSeal(logid_t /*logid*/,
                                shard_index_t /*shard*/,
                                bool soft) const override {
    const NodeID nid(0, 1);
    if (soft) {
      return test_->initial_soft_seal_epoch_.hasValue()
          ? Seal(test_->initial_soft_seal_epoch_.value(), nid)
          : folly::Optional<Seal>();
    }
    return test_->initial_seal_epoch_.hasValue()
        ? Seal(test_->initial_seal_epoch_.value(), nid)
        : folly::Optional<Seal>();
  }

  int getHighestInsertedLSN(logid_t log_id,
                            shard_index_t /*shard*/,
                            lsn_t* highest_lsn) const override {
    EXPECT_EQ(LOG_ID, log_id);
    if (test_->highest_lsn_.hasValue()) {
      *highest_lsn = test_->highest_lsn_.value();
      return 0;
    }
    err = E::NOTSUPPORTED;
    return -1;
  }

  size_t getEpochRecordCacheSize(logid_t /*logid*/) const override {
    return test_->epoch_cache_capacity_;
  }

  bool tailOptimized(logid_t /*logid*/) const override {
    return test_->tail_optimized_;
  }

 private:
  RecordCacheTest* const test_;
};

void RecordCacheTest::create() {
  deps_ = std::make_unique<MockRecordCacheDependencies>(this);
  cache_ = std::make_unique<RecordCache>(LOG_ID, SHARD, deps_.get());
}

bool RecordCacheTest::persistAndRepopulateCache(char* buf, size_t buflen) {
  auto linear_size = cache_->toLinearBuffer(buf, buflen);
  if (linear_size == -1) {
    return false;
  }
  auto repopulated_cache =
      RecordCache::fromLinearBuffer(buf, buflen, deps_.get(), SHARD);
  if (!repopulated_cache) {
    return false;
  }
  if (!testRecordCachesIdentical(*cache_, *repopulated_cache)) {
    return false;
  }
  cache_ = std::move(repopulated_cache);
  return true;
}

TEST_F(RecordCacheTest, Basic) {
  epoch_cache_capacity_ = 10;
  create();

  std::pair<Result, std::shared_ptr<EpochRecordCache>> result =
      cache_->getEpochRecordCache(EPOCH);
  ASSERT_EQ(Result::MISS, result.first);
  ASSERT_EQ(nullptr, result.second);

  int rv = putRecord(cache_.get(), lsn(EPOCH, 4), 2);
  ASSERT_EQ(0, rv);

  result = cache_->getEpochRecordCache(EPOCH);
  ASSERT_EQ(Result::HIT, result.first);
  ASSERT_NE(nullptr, result.second);

  ASSERT_TRUE(result.second->isConsistent());
  ASSERT_CACHE_ENTRY(result.second, 4, lsn(EPOCH, 4));

  // try storing a record with epoch less than the first seen epoch,
  // should fail
  rv = putRecord(cache_.get(), lsn(EPOCH.val_ - 1, 4), 2);
  ASSERT_EQ(-1, rv);
}

TEST_F(RecordCacheTest, EmptyCacheWithNonAuthoritativeEpoch) {
  epoch_cache_capacity_ = 10;
  initial_seal_epoch_ = epoch_t(2);
  initial_soft_seal_epoch_ = epoch_t(4);
  create();
  // simulate the update in SealStorageTask
  cache_->updateLastNonAuthoritativeEpoch(LOG_ID);
  // should be NO_RECORD but NOT be a cache miss for epoch 6
  std::pair<Result, std::shared_ptr<EpochRecordCache>> result =
      cache_->getEpochRecordCache(epoch_t(6));
  ASSERT_EQ(Result::NO_RECORD, result.first);
  ASSERT_EQ(nullptr, result.second);
  // should be cache miss for epoch 5
  result = cache_->getEpochRecordCache(epoch_t(5));
  ASSERT_EQ(Result::MISS, result.first);
  ASSERT_EQ(nullptr, result.second);
}

TEST_F(RecordCacheTest, EmptyCacheWithNonAuthoritativeEpoch2) {
  epoch_cache_capacity_ = 10;
  highest_lsn_ = lsn(8, 7);
  // seal and soft seal not available at the time
  initial_seal_epoch_.clear();
  initial_soft_seal_epoch_.clear();
  create();
  cache_->updateLastNonAuthoritativeEpoch(LOG_ID);

  // should be NO_RECORD but NOT be a cache miss for epoch 9
  std::pair<Result, std::shared_ptr<EpochRecordCache>> result =
      cache_->getEpochRecordCache(epoch_t(9));
  ASSERT_EQ(Result::NO_RECORD, result.first);
  ASSERT_EQ(nullptr, result.second);
  // should be cache miss for epoch 1-8
  for (int e = 1; e <= 8; ++e) {
    ld_info("testing %d", e);
    result = cache_->getEpochRecordCache(epoch_t(e));
    ASSERT_EQ(Result::MISS, result.first);
    ASSERT_EQ(nullptr, result.second);
  }
}

TEST_F(RecordCacheTest, BasicConsistency) {
  epoch_cache_capacity_ = 10;
  initial_seal_epoch_ = epoch_t(5);
  initial_soft_seal_epoch_ = epoch_t(8);

  create();
  // seal epoch 5, soft seal epoch 8
  // receive the first store in epoch 5 (i.e., mutation)
  const STORE_flags_t recovery_flags = STORE_Header::RECOVERY;

  int rv = putRecord(cache_.get(), lsn(5, 4), 2, recovery_flags);
  ASSERT_EQ(0, rv);

  // should be a cache miss since there is no way to tell if there are records
  // stored in epoch 5 before
  std::pair<Result, std::shared_ptr<EpochRecordCache>> result =
      cache_->getEpochRecordCache(epoch_t(5));
  ASSERT_EQ(Result::MISS, result.first);
  ASSERT_EQ(nullptr, result.second);

  // same for epoch 9
  result = cache_->getEpochRecordCache(epoch_t(9));
  ASSERT_EQ(Result::MISS, result.first);
  ASSERT_EQ(nullptr, result.second);

  // on the other hand, we can be assued that there are no record previously
  // stored in epoch 10 at this time
  result = cache_->getEpochRecordCache(epoch_t(10));
  ASSERT_EQ(Result::NO_RECORD, result.first);
  ASSERT_EQ(nullptr, result.second);

  // store another record in epoch 10
  rv = putRecord(cache_.get(), lsn(10, 7), 3);
  ASSERT_EQ(0, rv);
  // since epoch 10 is larger than the initial soft seal epoch, it is safe
  // to use the cache
  result = cache_->getEpochRecordCache(epoch_t(10));
  ASSERT_EQ(Result::HIT, result.first);
  ASSERT_NE(nullptr, result.second);
  ASSERT_TRUE(result.second->isConsistent());
  ASSERT_CACHE_ENTRY(result.second, 7, lsn(10, 7));

  // receive the next record in epoch 8
  rv = putRecord(cache_.get(), lsn(8, 5), 2);
  ASSERT_EQ(0, rv);
  result = cache_->getEpochRecordCache(epoch_t(8));
  ASSERT_EQ(Result::MISS, result.first);
  ASSERT_EQ(nullptr, result.second);

  // despite the cache miss, epoch 8 should be already cached and can be
  // activated by storing more records
  rv = putRecord(cache_.get(), lsn(8, 16), 12);
  ASSERT_EQ(0, rv);
  result = cache_->getEpochRecordCache(epoch_t(8));
  ASSERT_EQ(Result::HIT, result.first);
  ASSERT_NE(nullptr, result.second);
  ASSERT_TRUE(result.second->isConsistent());
  ASSERT_CACHE_ENTRY(result.second, 16, lsn(8, 16));
  ASSERT_EQ(1, dropped_.size());
  verifyEntry(*dropped_.back(), lsn(8, 5));
}

// test the condition that an already consitent cache can be evicted and
// reset to an inconsistent state, but later can become consistent again
TEST_F(RecordCacheTest, ResetEvictEpoch) {
  epoch_cache_capacity_ = 10;

  // softseal epoch 1, epoch caches with EPOCH == 3 should be
  // consistent initially
  initial_seal_epoch_ = epoch_t(1);
  initial_soft_seal_epoch_ = epoch_t(1);
  create();
  cache_->updateLastNonAuthoritativeEpoch(LOG_ID);

  int rv;
  auto result = cache_->getEpochRecordCache(EPOCH);
  ASSERT_EQ(Result::NO_RECORD, result.first);
  ASSERT_EQ(nullptr, result.second);

  // insert 5 records to the cache
  for (int i = 2; i < 7; ++i) {
    rv = putRecord(cache_.get(), lsn(EPOCH, i), /*lng=*/1);
    ASSERT_EQ(0, rv);
    result = cache_->getEpochRecordCache(EPOCH);
    ASSERT_EQ(Result::HIT, result.first);
    ASSERT_NE(nullptr, result.second);
    ASSERT_TRUE(result.second->isConsistent());
    ASSERT_CACHE_ENTRY(result.second, i, lsn(EPOCH, i));
  }

  result = cache_->getEpochRecordCache(EPOCH);
  ASSERT_EQ(Result::HIT, result.first);
  ASSERT_NE(nullptr, result.second);
  // obtain a reference of the epoch cache before eviction
  std::shared_ptr<EpochRecordCache> epoch_cache_snapshot =
      std::move(result.second);

  // evict the epoch
  cache_->evictResetEpoch(EPOCH);
  // further access the cache will result a miss
  result = cache_->getEpochRecordCache(EPOCH);
  ASSERT_EQ(Result::MISS, result.first);
  ASSERT_EQ(nullptr, result.second);

  // however, the orginal reference to the epoch cache should stay intact
  ASSERT_TRUE(epoch_cache_snapshot->isConsistent());
  for (int i = 2; i < 7; ++i) {
    ASSERT_CACHE_ENTRY(epoch_cache_snapshot, i, lsn(EPOCH, i));
  }

  // keep inserting new records to the epoch, the epoch cache should
  // eventually be consitent again
  for (int i = 7; i < 25; ++i) {
    // starting from esn 7, keep inserting records with lng == esn - 1
    rv = putRecord(cache_.get(), lsn(EPOCH, i), /*lng=*/i - 1);
    ASSERT_EQ(0, rv);
    // new first_seen_lng == 6, cache should be consistent as it sees a record
    // with lng >= first_seen_lng + capacity() == 16
    result = cache_->getEpochRecordCache(EPOCH);
    if (i - 1 < 6 + epoch_cache_capacity_) {
      ASSERT_EQ(Result::MISS, result.first);
      ASSERT_EQ(nullptr, result.second);
    } else {
      ASSERT_EQ(Result::HIT, result.first);
      ASSERT_NE(nullptr, result.second);
      ASSERT_TRUE(result.second->isConsistent());
      ASSERT_CACHE_ENTRY(result.second, i, lsn(EPOCH, i));
    }
  }

  // these records should not appear in the snapshot
  for (int i = 7; i < 25; ++i) {
    ASSERT_NO_CACHE_ENTRY(epoch_cache_snapshot, i);
  }

  // destroy the snapshot, only the orginal 5 records should be evicted
  dropped_.clear();
  epoch_cache_snapshot.reset();
  ASSERT_EQ(5, dropped_.size());
}

// test the condition that eviction leaves an empty epoch consistent
TEST_F(RecordCacheTest, EvictEmptyEpoch) {
  epoch_cache_capacity_ = 10;

  // softseal epoch 1, epoch caches with EPOCH == 3 should be
  // consistent initially
  initial_seal_epoch_ = epoch_t(1);
  initial_soft_seal_epoch_ = epoch_t(1);
  create();
  cache_->updateLastNonAuthoritativeEpoch(LOG_ID);

  int rv;
  auto result = cache_->getEpochRecordCache(EPOCH);
  ASSERT_EQ(Result::NO_RECORD, result.first);
  ASSERT_EQ(nullptr, result.second);

  // insert 1 record to the cache, and release the record to evict it
  rv = putRecord(cache_.get(), lsn(EPOCH, 2), /*lng=*/1);
  ASSERT_EQ(0, rv);
  cache_->onRelease(lsn(EPOCH, 2));

  result = cache_->getEpochRecordCache(EPOCH);
  ASSERT_EQ(Result::HIT, result.first);
  ASSERT_NE(nullptr, result.second);
  ASSERT_TRUE(result.second->isConsistent());
  ASSERT_NO_CACHE_ENTRY(result.second, 2);
  ASSERT_TRUE(result.second->empty());

  // evict the epoch
  cache_->evictResetEpoch(EPOCH);
  // the epoch should still be empty but consistent
  result = cache_->getEpochRecordCache(EPOCH);
  ASSERT_EQ(Result::HIT, result.first);
  ASSERT_NE(nullptr, result.second);
  ASSERT_TRUE(result.second->isConsistent());
  ASSERT_TRUE(result.second->empty());
}

// test that we should evict epoch in which there is no record cached
// but still have a tail record with non-empty payload
TEST_F(RecordCacheTest, EvictEmptyEpochTailOptimized) {
  tail_optimized_ = true;
  epoch_cache_capacity_ = 10;

  // softseal epoch 1, epoch caches with EPOCH == 3 should be
  // consistent initially
  initial_seal_epoch_ = epoch_t(1);
  initial_soft_seal_epoch_ = epoch_t(1);
  create();
  cache_->updateLastNonAuthoritativeEpoch(LOG_ID);

  int rv;
  auto result = cache_->getEpochRecordCache(EPOCH);
  ASSERT_EQ(Result::NO_RECORD, result.first);
  ASSERT_EQ(nullptr, result.second);

  // insert 1 record to the cache, and release the record to evict it
  rv = putRecord(cache_.get(), lsn(EPOCH, 2), /*lng=*/1);
  ASSERT_EQ(0, rv);
  cache_->onRelease(lsn(EPOCH, 2));

  result = cache_->getEpochRecordCache(EPOCH);
  ASSERT_EQ(Result::HIT, result.first);
  ASSERT_NE(nullptr, result.second);
  ASSERT_TRUE(result.second->isConsistent());
  ASSERT_NO_CACHE_ENTRY(result.second, 2);

  // the epoch cache is considered empty but it does have a
  // non-empty tail record
  ASSERT_TRUE(result.second->empty());
  ASSERT_TAIL_RECORD(result.second, lsn(EPOCH, 2));
  ASSERT_TRUE(result.second->getTailRecord().hasPayload());
  ASSERT_FALSE(result.second->emptyWithoutTailPayload());

  // evict the epoch
  cache_->evictResetEpoch(EPOCH);

  // the epoch will get evicted because of the non-empty
  // payload in tail record
  result = cache_->getEpochRecordCache(EPOCH);
  // cache miss because the epoch is no longer consistent
  ASSERT_EQ(Result::MISS, result.first);
  ASSERT_EQ(nullptr, result.second);
}

TEST_F(RecordCacheTest, AdvanceLCE) {
  epoch_cache_capacity_ = 10;
  initial_seal_epoch_ = epoch_t(5);
  initial_soft_seal_epoch_ = epoch_t(8);
  create();
  int rv = putRecord(cache_.get(), lsn(8, 5), 2);
  ASSERT_EQ(0, rv);
  rv = putRecord(cache_.get(), lsn(10, 7), 3);
  ASSERT_EQ(0, rv);
  auto result = cache_->getEpochRecordCache(epoch_t(8));
  ASSERT_EQ(Result::MISS, result.first);
  result = cache_->getEpochRecordCache(epoch_t(10));
  ASSERT_EQ(Result::HIT, result.first);
  cache_->onLastCleanEpochAdvanced(epoch_t(7));
  ASSERT_TRUE(dropped_.empty());
  cache_->onLastCleanEpochAdvanced(epoch_t(8));
  ASSERT_EQ(1, dropped_.size());
  verifyEntry(*dropped_.back(), lsn(8, 5));
  cache_->onLastCleanEpochAdvanced(epoch_t(10));
  // EpochRecordCache for epoch 10 is dropped, however, we
  // still hold a reference to the cache so it is not destroyed yet
  ASSERT_EQ(1, dropped_.size());
  // destroy the reference, cache for epoch 10 should be destroyed
  result.second.reset();
  ASSERT_EQ(2, dropped_.size());
  verifyEntry(*dropped_.back(), lsn(10, 7));

  // attempt to access epoch record cache for epoch 10 again, this time
  // it should be a cache miss since the epoch is evicted
  result = cache_->getEpochRecordCache(epoch_t(10));
  ASSERT_EQ(Result::MISS, result.first);
  ASSERT_EQ(nullptr, result.second);
}

TEST_F(RecordCacheTest, BasicSequencing) {
  epoch_cache_capacity_ = 10;
  create();

  size_t buflen = 10000000; // 1mb
  std::unique_ptr<char[]> buf(new char[buflen]);
  memset(buf.get(), 0, buflen);

  // Linearize empty cache, repopulate and verify equality
  auto linear_size = cache_->toLinearBuffer(buf.get(), buflen);
  ASSERT_NE(linear_size, -1);
  ASSERT_EQ(sizeof(RecordCacheSerializer::CacheHeader), linear_size);
  auto repopulated_cache =
      RecordCache::fromLinearBuffer(buf.get(), buflen, deps_.get(), SHARD);
  ASSERT_NE(repopulated_cache.get(), nullptr);
  ASSERT_TRUE(testRecordCachesIdentical(*cache_, *repopulated_cache));

  // Add record, verify inequality, then repopulate and verify equality
  int rv = putRecord(cache_.get(), lsn(EPOCH, 4), 2);
  ASSERT_EQ(0, rv);
  ASSERT_FALSE(testRecordCachesIdentical(*cache_, *repopulated_cache));
  ASSERT_TRUE(persistAndRepopulateCache(buf.get(), buflen));
}

TEST_F(RecordCacheTest, VersionMismatch) {
  epoch_cache_capacity_ = 10;
  create();

  size_t buflen = 200 * 8; // 1mb
  std::unique_ptr<char[]> buf((char*)new uint64_t[buflen / sizeof(uint64_t)]);
  memset(buf.get(), 0, buflen);

  // Linearize empty cache, repopulate and verify equality
  auto linear_size = cache_->toLinearBuffer(buf.get(), buflen);
  ASSERT_NE(linear_size, -1);
  ASSERT_EQ(sizeof(RecordCacheSerializer::CacheHeader), linear_size);

  auto* header =
      reinterpret_cast<RecordCacheSerializer::CacheHeader*>(buf.get());
  header->version++;

  auto repopulated_cache =
      RecordCache::fromLinearBuffer(buf.get(), buflen, deps_.get(), SHARD);

  // repopulation should fail
  ASSERT_EQ(nullptr, repopulated_cache.get());
}

TEST_F(RecordCacheTest, SequencingEmptyCacheWithNonAuthoritativeEpoch) {
  epoch_cache_capacity_ = 10;
  initial_seal_epoch_ = epoch_t(2);
  initial_soft_seal_epoch_ = epoch_t(4);
  create();

  size_t buflen = 10000000; // 1mb
  std::unique_ptr<char[]> buf(new char[buflen]);
  memset(buf.get(), 0, buflen);

  // simulate the update in SealStorageTask
  cache_->updateLastNonAuthoritativeEpoch(LOG_ID);
  ASSERT_TRUE(persistAndRepopulateCache(buf.get(), buflen));

  // should be NO_RECORD but NOT be a cache miss for epoch 6
  std::pair<Result, std::shared_ptr<EpochRecordCache>> result =
      cache_->getEpochRecordCache(epoch_t(6));
  ASSERT_EQ(Result::NO_RECORD, result.first);
  ASSERT_EQ(nullptr, result.second);
  // should be cache miss for epoch 5
  result = cache_->getEpochRecordCache(epoch_t(5));
  ASSERT_EQ(Result::MISS, result.first);
  ASSERT_EQ(nullptr, result.second);
  ASSERT_TRUE(persistAndRepopulateCache(buf.get(), buflen));
}

TEST_F(RecordCacheTest, SequencingInconsistentCache) {
  epoch_cache_capacity_ = 10;
  initial_seal_epoch_ = epoch_t(5);
  initial_soft_seal_epoch_ = epoch_t(8);
  size_t buflen = 10000000; // 1mb
  std::unique_ptr<char[]> buf(new char[buflen]);
  memset(buf.get(), 0, buflen);

  create();
  // seal epoch 5, soft seal epoch 8
  // receive the first store in epoch 5 (i.e., mutation)
  const STORE_flags_t recovery_flags = STORE_Header::RECOVERY;

  int rv = putRecord(cache_.get(), lsn(5, 4), 2, recovery_flags);
  ASSERT_EQ(0, rv);

  auto dropped_before = dropped_.size();
  ASSERT_TRUE(persistAndRepopulateCache(buf.get(), buflen));
  auto dropped_offset = dropped_.size() - dropped_before;

  // should be a cache miss since there is no way to tell if there are records
  // stored in epoch 5 before
  std::pair<Result, std::shared_ptr<EpochRecordCache>> result =
      cache_->getEpochRecordCache(epoch_t(5));
  ASSERT_EQ(Result::MISS, result.first);
  ASSERT_EQ(nullptr, result.second);

  // same for epoch 9
  result = cache_->getEpochRecordCache(epoch_t(9));
  ASSERT_EQ(Result::MISS, result.first);
  ASSERT_EQ(nullptr, result.second);

  // on the other hand, we can be assued that there are no record previously
  // stored in epoch 10 at this time
  result = cache_->getEpochRecordCache(epoch_t(10));
  ASSERT_EQ(Result::NO_RECORD, result.first);
  ASSERT_EQ(nullptr, result.second);

  // store another record in epoch 10
  rv = putRecord(cache_.get(), lsn(10, 7), 3);
  ASSERT_EQ(0, rv);

  ASSERT_EQ(dropped_offset, dropped_.size());
  dropped_before = dropped_.size();
  ASSERT_TRUE(persistAndRepopulateCache(buf.get(), buflen));
  result.second.reset();
  dropped_offset += dropped_.size() - dropped_before;
  ASSERT_EQ(dropped_offset, dropped_.size());

  // since epoch 10 is larger than the initial soft seal epoch, it is safe
  // to use the cache
  result = cache_->getEpochRecordCache(epoch_t(10));
  ASSERT_EQ(Result::HIT, result.first);
  ASSERT_NE(nullptr, result.second);
  ASSERT_TRUE(result.second->isConsistent());
  ASSERT_CACHE_ENTRY(result.second, 7, lsn(10, 7));

  // receive the next record in epoch 8
  ASSERT_EQ(dropped_offset, dropped_.size());
  rv = putRecord(cache_.get(), lsn(8, 5), 2);
  ASSERT_EQ(dropped_offset, dropped_.size());

  dropped_before = dropped_.size();
  ASSERT_TRUE(persistAndRepopulateCache(buf.get(), buflen));
  result.second.reset();
  dropped_offset += dropped_.size() - dropped_before;
  ASSERT_EQ(dropped_offset, dropped_.size());

  ASSERT_EQ(0, rv);
  ASSERT_EQ(dropped_offset, dropped_.size());
  result = cache_->getEpochRecordCache(epoch_t(8));
  ASSERT_EQ(dropped_offset, dropped_.size());
  ASSERT_EQ(Result::MISS, result.first);
  ASSERT_EQ(nullptr, result.second);

  // despite the cache miss, epoch 8 should be already cached and can be
  // activated by storing more records
  ASSERT_EQ(dropped_offset, dropped_.size());
  rv = putRecord(cache_.get(), lsn(8, 16), 12);
  ASSERT_EQ(dropped_offset + 1, dropped_.size());
  ASSERT_EQ(0, rv);

  dropped_before = dropped_.size();
  ASSERT_TRUE(persistAndRepopulateCache(buf.get(), buflen));
  result.second.reset();
  dropped_offset += dropped_.size() - dropped_before;

  result = cache_->getEpochRecordCache(epoch_t(8));
  ASSERT_EQ(Result::HIT, result.first);
  ASSERT_NE(nullptr, result.second);
  ASSERT_TRUE(result.second->isConsistent());
  ASSERT_CACHE_ENTRY(result.second, 16, lsn(8, 16));
  ASSERT_EQ(dropped_offset + 1, dropped_.size());
  lsn_t target_dropped_lsn = lsn(8, 5);
  bool found = false;
  for (auto& entry : dropped_) {
    if (entry->timestamp == target_dropped_lsn) {
      found = true;
    }
  }
  ASSERT_TRUE(found);

  // Expect to find epoch 8, esn 16 but not 5, and also epoch 10 esn 7.
  std::unordered_map<int, int> targets = {{8, 16}, {10, 7}};
  std::unordered_map<int, int> expect_absent = {{8, 5}, {10, LSN_INVALID}};
  for (auto kv : targets) {
    int epoch = kv.first;
    int esn = kv.second;
    int absent_esn = expect_absent[epoch];
    auto epoch_record_cache_pair = cache_->getEpochRecordCache(epoch_t(epoch));
    ASSERT_NE(epoch_record_cache_pair.second, nullptr);
    ASSERT_CACHE_ENTRY(epoch_record_cache_pair.second, esn, lsn(epoch, esn));
    ASSERT_NO_CACHE_ENTRY(epoch_record_cache_pair.second, absent_esn);
  }
}

TEST_F(RecordCacheTest, RecordCacheInitializeRandomAccessQueue) {
  epoch_cache_capacity_ = 10;
  initial_seal_epoch_ = epoch_t(5);
  initial_soft_seal_epoch_ = epoch_t(8);
  create();

  // Create an epoch, advance LCE past it to make cache empty
  int rv = putRecord(cache_.get(), lsn(4, 7), 3);
  ASSERT_EQ(0, rv);
  cache_->onLastCleanEpochAdvanced(epoch_t(4));
  verifyEntry(*dropped_.back(), lsn(4, 7));

  // Verify that we can reconstruct this using the below constructor
  RecordCache record_cache_e5(LOG_ID, SHARD, deps_.get(), 5, 5, 9);
  ASSERT_TRUE(testRecordCachesIdentical(*cache_, record_cache_e5));
}

TEST_F(RecordCacheTest, SequencingAdvanceLCE) {
  epoch_cache_capacity_ = 10;
  initial_seal_epoch_ = epoch_t(5);
  initial_soft_seal_epoch_ = epoch_t(8);
  create();

  size_t buflen = 10000000; // 1mb
  std::unique_ptr<char[]> buf(new char[buflen]);
  memset(buf.get(), 0, buflen);

  int rv = putRecord(cache_.get(), lsn(8, 5), 2);
  ASSERT_EQ(0, rv);
  rv = putRecord(cache_.get(), lsn(10, 7), 3);
  ASSERT_EQ(0, rv);
  ASSERT_TRUE(persistAndRepopulateCache(buf.get(), buflen));
  auto dropped_offset = dropped_.size();
  auto result = cache_->getEpochRecordCache(epoch_t(8));
  ASSERT_EQ(Result::MISS, result.first);
  result = cache_->getEpochRecordCache(epoch_t(10));
  ASSERT_EQ(Result::HIT, result.first);
  cache_->onLastCleanEpochAdvanced(epoch_t(7));
  ASSERT_EQ(dropped_offset, dropped_.size());
  cache_->onLastCleanEpochAdvanced(epoch_t(8));
  ASSERT_EQ(dropped_offset + 1, dropped_.size());
  verifyEntry(*dropped_.back(), lsn(8, 5));
  cache_->onLastCleanEpochAdvanced(epoch_t(10));
  // EpochRecordCache for epoch 10 is dropped, however, we
  // still hold a reference to the cache so it is not destroyed yet
  ASSERT_EQ(dropped_offset + 1, dropped_.size());
  // destroy the reference, cache for epoch 10 should be destroyed
  result.second.reset();
  ASSERT_EQ(dropped_offset + 2, dropped_.size());
  verifyEntry(*dropped_.back(), lsn(10, 7));
}

TEST_F(RecordCacheTest, RepopulateFromIncompleteBuffer) {
  epoch_cache_capacity_ = 10;
  create();

  size_t buflen = 10000000; // 1mb
  std::unique_ptr<char[]> buf(new char[buflen]);
  memset(buf.get(), 0, buflen);
  int num_iterations = 100;

  // 1) Linearize empty cache, repeatedly repopulate with random buffer sizes,
  // all being too small. Verify that repopulation halts and does not crash.
  // Then, repopulate from large enough buffer and verify equality
  auto linear_size = cache_->toLinearBuffer(buf.get(), buflen);
  ASSERT_NE(linear_size, -1);
  ASSERT_EQ(sizeof(RecordCacheSerializer::CacheHeader), linear_size);
  for (int i = 0; i < num_iterations; i++) {
    size_t random_buflen = (folly::Random::rand64() % (linear_size - 1)) + 1;
    auto repopulated_cache_from_incomplete_buffer =
        RecordCache::fromLinearBuffer(
            buf.get(), random_buflen, deps_.get(), SHARD);
    ASSERT_EQ(nullptr, repopulated_cache_from_incomplete_buffer);
  }
  auto repopulated_cache =
      RecordCache::fromLinearBuffer(buf.get(), buflen, deps_.get(), SHARD);
  ASSERT_NE(repopulated_cache.get(), nullptr);
  ASSERT_TRUE(testRecordCachesIdentical(*cache_, *repopulated_cache));

  // 2) Add record, verify inequality
  int rv = putRecord(cache_.get(), lsn(EPOCH, 4), 2);
  ASSERT_EQ(0, rv);
  ASSERT_FALSE(testRecordCachesIdentical(*cache_, *repopulated_cache));

  // 3) Do the same as in 1), with the same expectations
  linear_size = cache_->toLinearBuffer(buf.get(), buflen);
  ASSERT_NE(linear_size, -1);
  for (int i = 0; i < num_iterations; i++) {
    size_t random_buflen = (folly::Random::rand64() % (linear_size - 1)) + 1;
    auto repopulated_cache_from_incomplete_buffer =
        RecordCache::fromLinearBuffer(
            buf.get(), random_buflen, deps_.get(), SHARD);
    ASSERT_EQ(nullptr, repopulated_cache_from_incomplete_buffer);
  }
  repopulated_cache =
      RecordCache::fromLinearBuffer(buf.get(), buflen, deps_.get(), SHARD);
  ASSERT_NE(repopulated_cache.get(), nullptr);
  ASSERT_TRUE(testRecordCachesIdentical(*cache_, *repopulated_cache));
}

TEST_F(RecordCacheTest, SequencingSizeCalculation) {
  epoch_cache_capacity_ = 10;
  create();

  size_t buflen = 10000000; // 1mb
  std::unique_ptr<char[]> buf(new char[buflen]);
  memset(buf.get(), 0, buflen);

  // 1) Confirm sizes match for empty cache
  auto calculated_size = cache_->sizeInLinearBuffer();
  ASSERT_NE(calculated_size, -1);
  ASSERT_EQ(sizeof(RecordCacheSerializer::CacheHeader), calculated_size);
  auto linear_size = cache_->toLinearBuffer(buf.get(), buflen);
  ASSERT_NE(linear_size, -1);
  ASSERT_EQ(calculated_size, linear_size);

  // 2) Add records, confirm sizes match
  int rv = putRecord(cache_.get(), lsn(epoch_t(5), 7), 2);
  ASSERT_EQ(0, rv);
  rv = putRecord(cache_.get(), lsn(epoch_t(8), 5), 2);
  ASSERT_EQ(0, rv);
  calculated_size = cache_->sizeInLinearBuffer();
  ASSERT_NE(calculated_size, -1);
  ASSERT_NE(sizeof(RecordCacheSerializer::CacheHeader), calculated_size);
  linear_size = cache_->toLinearBuffer(buf.get(), buflen);
  ASSERT_NE(linear_size, -1);
  ASSERT_EQ(calculated_size, linear_size);
}
