/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/server/storage/CachedDigest.h"

#include <chrono>
#include <memory>
#include <random>
#include <utility>
#include <vector>

#include <folly/Memory.h>
#include <folly/Random.h>
#include <gtest/gtest.h>

#include "logdevice/common/CopySet.h"
#include "logdevice/common/DataRecordOwnsPayload.h"
#include "logdevice/common/LocalLogStoreRecordFormat.h"
#include "logdevice/common/Timer.h"
#include "logdevice/common/debug.h"
#include "logdevice/common/protocol/GAP_Message.h"
#include "logdevice/common/protocol/Message.h"
#include "logdevice/common/protocol/RECORD_Message.h"
#include "logdevice/common/protocol/STARTED_Message.h"
#include "logdevice/common/test/MockBackoffTimer.h"
#include "logdevice/common/test/MockTimer.h"
#include "logdevice/server/EpochRecordCache.h"
#include "logdevice/server/EpochRecordCacheEntry.h"
#include "logdevice/server/RecordCacheDependencies.h"
#include "logdevice/server/storage/AllCachedDigests.h"

namespace facebook { namespace logdevice {

namespace {

// TODO: This isn't specific to this test, so move it somewhere more general.
// Putting it in RECORD_Header makes sense, although it's only needed for
// tests. So maybe put it in a test utilities files?
bool operator==(const RECORD_Header& h1, const RECORD_Header& h2) {
  return h1.log_id == h2.log_id && h1.read_stream_id == h2.read_stream_id &&
      h1.lsn == h2.lsn && h1.timestamp == h2.timestamp &&
      h1.flags == h2.flags && h1.shard == h2.shard;
}
} // namespace

using Snapshot = EpochRecordCache::Snapshot;

const logid_t LOG_ID(777);
const shard_index_t SHARD{0};
const epoch_t EPOCH(3);

// Handy macros for writing ShardIDs
#define N0 ShardID(0, 0)
#define N1 ShardID(1, 0)
#define N2 ShardID(2, 0)

class CachedDigestTest : public ::testing::Test {
 public:
  //////// may be set by each tests
  // esn to start digest
  esn_t start_esn_ = ESN_MIN;

  // tail record of the epoch, must <= start_esn_
  esn_t tail_esn_ = ESN_INVALID;
  // for simplicity all records in the cache will have the same lng
  // must >= tail_esn_
  esn_t lng_ = ESN_MIN;
  // number of real records in the record cache whose esn >= start_esn_,
  // excluding the tail record
  size_t num_record_deliverable_ = 10;

  bool worker_shutdown_ = false;
  size_t bytes_queued_ = 0;
  size_t max_bytes_queued_ = 9999999;
  size_t messages_credit_ = 9999999;
  // used to generate the expected digest
  double record_probability_ = 0.5;
  bool epoch_empty_ = false;

  ////////////////
  bool push_timer_active_ = false;
  bool completed_ = false;

  std::unique_ptr<EpochRecordCacheDependencies> cache_deps_;
  std::shared_ptr<EpochRecordCache> cache_;
  std::unique_ptr<CachedDigest> digest_;

  // container of messages intercepted
  std::vector<std::unique_ptr<Message>> messages_;

  // container of all records, set by setUp() function;
  std::map<esn_t, Snapshot::Record> records_;
  esn_t last_record_ = ESN_INVALID;

  // computed by setUp()
  size_t record_message_size_;

  CachedDigestTest() {}

  void setUp();
  void verifyResult();
  void firePushTimer();
  bool isDelayTimerActive() const;
};

class MockedCachedDigest : public CachedDigest {
  using MockSender = SenderTestProxy<MockedCachedDigest>;

 public:
  explicit MockedCachedDigest(CachedDigestTest* test)
      : CachedDigest(LOG_ID,
                     shard_index_t{0},
                     read_stream_id_t(1),
                     ClientID(1),
                     compose_lsn(EPOCH, test->start_esn_),
                     (test->cache_ != nullptr
                          ? test->cache_->createSerializableSnapshot()
                          : nullptr),
                     nullptr,
                     nullptr),
        test_(test) {
    sender_ = std::make_unique<MockSender>(this);
  }

  std::unique_ptr<Timer> createPushTimer(std::function<void()>) override {
    return nullptr;
  }
  void activatePushTimer() override {
    test_->push_timer_active_ = true;
  }
  void cancelPushTimer() override {
    test_->push_timer_active_ = false;
  }

  std::unique_ptr<BackoffTimer>
  createDelayTimer(std::function<void()> callback) override {
    auto timer = std::make_unique<MockBackoffTimer>();
    timer->setCallback(callback);
    return std::move(timer);
  }

  bool canPushRecords() const override {
    return test_->bytes_queued_ < test_->max_bytes_queued_;
  }

  void onBytesEnqueued(size_t msg_size) override {
    test_->bytes_queued_ += msg_size;
  }

  void onDigestComplete() override {
    test_->completed_ = true;
  }

  bool includeExtraMetadata() const override {
    return false;
  }

  bool canSendToImpl(const Address&, TrafficClass, BWAvailableCallback&) {
    return true;
  }

  int sendMessageImpl(std::unique_ptr<Message>&& msg,
                      const Address& addr,
                      BWAvailableCallback*,
                      SocketCallback*) {
    ld_check(addr.isClientAddress());
    if (test_->worker_shutdown_) {
      err = E::SHUTDOWN;
      return -1;
    }

    if (test_->messages_credit_ > 0) {
      --test_->messages_credit_;
    } else {
      err = E::NOBUFS;
      return -1;
    }

    test_->messages_.push_back(std::move(msg));
    return 0;
  }

 private:
  CachedDigestTest* const test_;
};

class MockEpochRecordCacheDependencies : public EpochRecordCacheDependencies {
 public:
  void disposeOfCacheEntry(std::unique_ptr<EpochRecordCacheEntry> e) override {
    e.reset();
  }

  void onRecordsReleased(const EpochRecordCache&,
                         lsn_t /* begin */,
                         lsn_t /* end */,
                         const ReleasedVector& /* entries */) override {}
};

void CachedDigestTest::setUp() {
  ASSERT_LE(tail_esn_, start_esn_);
  ASSERT_LE(tail_esn_, lng_);
  if (epoch_empty_) {
    ld_check(cache_ == nullptr);
    ld_info("epoch is empty.");
    digest_ = std::make_unique<MockedCachedDigest>(this);
    return;
  }

  size_t total_records = 0;
  const copyset_t dummyCopyset({N0, N1, N2});

  ld_check(records_.empty());
  auto genSnapshotRecord = [&](esn_t esn) {
    return Snapshot::Record{0,
                            /*timestamp*/ uint64_t(esn.val_),
                            /*lng*/ std::min(lng_, esn_t(esn.val_ - 1)),
                            /*wave*/ 0,
                            dummyCopyset,
                            OffsetMap(),
                            Slice()};
  };

  // step 1: generate tail record (if any)
  size_t tail = 0;
  if (tail_esn_ != ESN_INVALID) {
    records_[tail_esn_] = genSnapshotRecord(tail_esn_);
    if (tail_esn_ >= start_esn_) {
      tail = 1;
    }
    ++total_records;
  }

  // step 2: generate records in the cache
  size_t num_record_deliverable = 0;
  std::bernoulli_distribution d(record_probability_);
  auto rng = folly::ThreadLocalPRNG();
  uint64_t esn = 0;

  for (esn = lng_.val_ + 1; esn < start_esn_.val_ ||
       num_record_deliverable < num_record_deliverable_;
       ++esn) {
    if (d(rng)) {
      // populate a record
      records_[esn_t((uint32_t)esn)] = genSnapshotRecord(esn_t((uint32_t)esn));
      // within the deliverable range
      if (esn >= start_esn_.val_) {
        ++num_record_deliverable;
      }
      ++total_records;
    }
  }

  esn_t max_seen = records_.empty() ? ESN_INVALID : records_.rbegin()->first;
  last_record_ = (records_.empty() || records_.rbegin()->first < start_esn_)
      ? ESN_INVALID
      : records_.rbegin()->first;

  ASSERT_EQ(num_record_deliverable_, num_record_deliverable);
  ld_info("Digest: tail %u, lng %u, [head %u, max_seen %u], last record "
          "to deliver: %u. "
          "digest start: %u, records to send: %lu, total records %lu.",
          tail_esn_.val_,
          lng_.val_,
          lng_.val_ + 1,
          max_seen.val_,
          last_record_.val_,
          start_esn_.val_,
          num_record_deliverable + tail,
          total_records);

  cache_deps_ = std::make_unique<MockEpochRecordCacheDependencies>();
  cache_ =
      std::make_unique<EpochRecordCache>(LOG_ID,
                                         SHARD,
                                         EPOCH,
                                         cache_deps_.get(),
                                         esn > lng_.val_ ? esn - lng_.val_ : 1u,
                                         EpochRecordCache::TailOptimized::YES,
                                         EpochRecordCache::StoredBefore::NEVER);

  auto gen_payload = [](lsn_t lsn) {
    lsn_t* payload_flat = (lsn_t*)malloc(sizeof(lsn_t));
    *payload_flat = lsn;
    return std::make_shared<PayloadHolder>(payload_flat, sizeof(lsn_t));
  };

  auto put_record = [&](lsn_t lsn, const Snapshot::Record& r) {
    auto ph = gen_payload(lsn);
    Payload pl = ph->getPayload();
    cache_->putRecord(RecordID(lsn, LOG_ID),
                      r.timestamp,
                      r.last_known_good,
                      0,
                      dummyCopyset,
                      r.flags,
                      std::map<KeyType, std::string>{},
                      Slice(pl),
                      std::move(ph));
  };

  // now write all records to cache
  for (const auto& kv : records_) {
    put_record(compose_lsn(EPOCH, kv.first), kv.second);
  }

  cache_->advanceLNG(lng_);

  if (last_record_ == ESN_INVALID) {
    ASSERT_LE(cache_->getMaxSeenESN(), start_esn_);
  } else {
    ASSERT_EQ(last_record_, cache_->getMaxSeenESN());
  }

  record_message_size_ = RECORD_Message::expectedSize(sizeof(lsn_t));

  // create digest
  digest_ = std::make_unique<MockedCachedDigest>(this);
}

void CachedDigestTest::verifyResult() {
  // digest should be completed
  ASSERT_TRUE(completed_);
  ASSERT_FALSE(push_timer_active_);
  ASSERT_FALSE(messages_.empty());

  size_t m_gap = 0;
  size_t m_started = 0;

  if (last_record_ != ESN_MAX) {
    // last message must be GAP: [last_record_ + 1, ESN_MAX]
    GAP_Message* gap = dynamic_cast<GAP_Message*>(messages_.back().get());
    ASSERT_NE(nullptr, gap);
    GAP_Header expected_gap = {
        LOG_ID,
        read_stream_id_t(1),
        compose_lsn(EPOCH, std::max(esn_t(last_record_.val_ + 1), start_esn_)),
        compose_lsn(EPOCH, ESN_MAX),
        GapReason::NO_RECORDS,
        GAP_Header::DIGEST,
        SHARD};
    ASSERT_EQ(0,
              std::memcmp((void*)&expected_gap,
                          (void*)&gap->getHeader(),
                          sizeof(GAP_Header)));
    if (epoch_empty_) {
      ASSERT_EQ(compose_lsn(EPOCH, start_esn_), gap->getHeader().start_lsn);
      ASSERT_EQ(/*GAP*/ 1 + /*STARTED*/ 1, messages_.size());
    }

    ++m_gap;
    messages_.pop_back();
  } else {
    ASSERT_FALSE(epoch_empty_);
  }

  auto itr = records_.begin();
  auto itm = messages_.begin();
  // first message must be STARTED
  auto* started = dynamic_cast<STARTED_Message*>(itm->get());
  ASSERT_NE(nullptr, started);
  ++itm;
  ++m_started;

  size_t record_delivered = 0;
  while (itr != records_.end()) {
    if (itr->first < start_esn_) {
      // these records should get skipped but never delivered
      itr++;
      continue;
    }

    ASSERT_NE(messages_.end(), itm);
    RECORD_Message* rm = dynamic_cast<RECORD_Message*>(itm->get());
    ASSERT_NE(nullptr, rm);
    Snapshot::Record r = itr->second;
    ASSERT_EQ(LOG_ID, rm->header_.log_id);
    ASSERT_EQ(read_stream_id_t(1), rm->header_.read_stream_id);
    ASSERT_EQ(compose_lsn(EPOCH, itr->first), rm->header_.lsn);
    ASSERT_EQ(r.timestamp, rm->header_.timestamp);
    ASSERT_EQ(
        CachedDigest::StoreFlagsToRecordFlags(r.flags), rm->header_.flags);
    ASSERT_EQ(SHARD, rm->header_.shard);
    ASSERT_EQ(sizeof(lsn_t), rm->payload_.size());
    ASSERT_EQ(rm->header_.lsn, *((lsn_t*)rm->payload_.data()));
    itr++, itm++;
    ++record_delivered;
  }

  ASSERT_EQ(record_delivered + /*STARTED*/ 1, messages_.size());
  ASSERT_EQ(messages_.end(), itm);
  ld_info("Message summary: STARTED %lu, GAP %lu, RECORD %lu, total %lu.",
          m_started,
          m_gap,
          record_delivered,
          messages_.size() + m_gap);
}

void CachedDigestTest::firePushTimer() {
  ld_check(digest_ != nullptr);
  ld_check(push_timer_active_);
  push_timer_active_ = false;
  digest_->pushRecords();
}

bool CachedDigestTest::isDelayTimerActive() const {
  ld_check(digest_ != nullptr);
  return (digest_->delay_timer_ == nullptr ? false
                                           : digest_->delay_timer_->isActive());
}

//////////////////////////////////////

TEST_F(CachedDigestTest, EmptyEpoch) {
  epoch_empty_ = true;
  start_esn_ = esn_t(348);
  setUp();
  digest_->start();
  verifyResult();
}

TEST_F(CachedDigestTest, EmptyCache) {
  start_esn_ = esn_t(348);
  num_record_deliverable_ = 0;
  setUp();
  digest_->start();
  verifyResult();
}

TEST_F(CachedDigestTest, EmptyCacheWithTail) {
  tail_esn_ = esn_t(200);
  lng_ = esn_t(220);
  start_esn_ = esn_t(348);
  num_record_deliverable_ = 0;
  setUp();
  digest_->start();
  verifyResult();
}

TEST_F(CachedDigestTest, StartAfterLNG) {
  tail_esn_ = esn_t(10);
  lng_ = esn_t(19);
  start_esn_ = esn_t(348);
  num_record_deliverable_ = 17;
  setUp();
  digest_->start();
  verifyResult();
}

TEST_F(CachedDigestTest, DeliverTailRecord) {
  tail_esn_ = esn_t(973);
  lng_ = esn_t(973);
  start_esn_ = esn_t(973);
  num_record_deliverable_ = 0;
  setUp();
  digest_->start();
  verifyResult();
  ASSERT_EQ(1, records_.size());
}

TEST_F(CachedDigestTest, DeliverTailRecord2) {
  tail_esn_ = esn_t(11322);
  lng_ = esn_t(11394);
  start_esn_ = esn_t(11322);
  num_record_deliverable_ = 70;
  setUp();
  digest_->start();
  verifyResult();
}

TEST_F(CachedDigestTest, SynchronousFinish) {
  start_esn_ = esn_t(761);
  num_record_deliverable_ = 1000;
  setUp();
  digest_->start();
  verifyResult();
}

TEST_F(CachedDigestTest, ClientSocketBufferFull) {
  const int cached_records = 280;
  const int queued_records = 17;
  auto expected_messages_sent = [=](int rounds) {
    return (rounds * queued_records) + /*STARTED*/ 1;
  };

  tail_esn_ = esn_t(348240);
  lng_ = esn_t(348240);
  start_esn_ = esn_t(348340);
  num_record_deliverable_ = cached_records;
  setUp();
  max_bytes_queued_ = queued_records * record_message_size_;

  digest_->start();
  // will send until it can no longer push records
  ASSERT_FALSE(completed_);
  ASSERT_TRUE(push_timer_active_);
  ASSERT_EQ(expected_messages_sent(1), messages_.size());
  bytes_queued_ = 0;
  firePushTimer();
  ASSERT_FALSE(completed_);
  ASSERT_TRUE(push_timer_active_);
  ASSERT_EQ(expected_messages_sent(2), messages_.size());
  bytes_queued_ = 0;
  max_bytes_queued_ =
      (cached_records + /*STARTED*/ 1 - expected_messages_sent(2)) *
      record_message_size_;
  // should send the rest of messages
  firePushTimer();
  verifyResult();
}

TEST_F(CachedDigestTest, MessageSendFailed) {
  const int cached_records = 1218;
  const int initial_sent_records = 71;
  const int initial_message_credit = initial_sent_records + /*STARTED*/ 1;

  start_esn_ = esn_t(256);
  num_record_deliverable_ = cached_records;
  setUp();
  messages_credit_ = initial_message_credit;
  digest_->start();
  ASSERT_FALSE(completed_);
  ASSERT_TRUE(push_timer_active_);
  ASSERT_EQ(initial_message_credit, messages_.size());
  messages_credit_ = cached_records - initial_sent_records;
  firePushTimer();
  // should send all record but failed to send the final gap
  ASSERT_FALSE(completed_);
  ASSERT_TRUE(push_timer_active_);
  ASSERT_EQ(cached_records + /*STARTED*/ 1, messages_.size());
  messages_credit_ = 1;
  firePushTimer();
  // send the final gap, should be done by now
  verifyResult();
}

TEST_F(CachedDigestTest, NoGapOnEsnMax) {
  tail_esn_ = esn_t(ESN_MAX.val_ - 9);
  start_esn_ = esn_t(ESN_MAX.val_ - 2);
  lng_ = esn_t(ESN_MAX.val_ - 3);
  num_record_deliverable_ = 3;
  // always generate record for every esn
  record_probability_ = 1;

  setUp();
  digest_->start();
  verifyResult();
  ASSERT_EQ(3 + /*STARTED*/ 1, messages_.size());
  GAP_Message* gap = dynamic_cast<GAP_Message*>(messages_.back().get());
  // should be 3 RECORD messages but no gap
  ASSERT_EQ(nullptr, gap);
}

TEST_F(CachedDigestTest, DelayTimer) {
  const int queued_records = 3;
  const int expected_messages_sent = queued_records + /*STARTED*/ 1;
  start_esn_ = esn_t(17);
  num_record_deliverable_ = 61;
  setUp();
  max_bytes_queued_ = queued_records * record_message_size_;
  digest_->start();
  ASSERT_FALSE(completed_);
  ASSERT_TRUE(push_timer_active_);
  ASSERT_EQ(expected_messages_sent, messages_.size());
  // fire push timer w/o draining records
  firePushTimer();
  // should not make any progress
  ASSERT_EQ(expected_messages_sent, messages_.size());
  // push timer should not activate but delay timer should instead
  ASSERT_FALSE(push_timer_active_);
  ASSERT_TRUE(isDelayTimerActive());
}

TEST_F(CachedDigestTest, WorkerShutDown) {
  start_esn_ = esn_t(5);
  num_record_deliverable_ = 23;
  setUp();
  worker_shutdown_ = true;
  digest_->start();
  ASSERT_FALSE(completed_);
  // neither push timer or delay timer should be active on shutdown
  ASSERT_FALSE(push_timer_active_);
  ASSERT_FALSE(isDelayTimerActive());
}

////////////   AllCachedDigestTest ////////////

class AllCachedDigestsTest : public ::testing::Test {
 public:
  std::unique_ptr<AllCachedDigests> digests_;
  size_t max_active_digests_{10};
  size_t max_bytes_queued_per_client_kb_{800};
  size_t max_streams_per_batch_{200};
  bool can_push_ = false;

  void setUp();
};

// a cached digest that can be synchronously finished
class DummyCachedDigest : public CachedDigest {
  using MockSender = SenderTestProxy<DummyCachedDigest>;

 public:
  explicit DummyCachedDigest(AllCachedDigestsTest* test,
                             ClientID client_id,
                             read_stream_id_t rid,
                             ClientDigests* client_digests,
                             AllCachedDigests* all_digests)
      : CachedDigest(LOG_ID,
                     shard_index_t{0},
                     rid,
                     client_id,
                     compose_lsn(EPOCH, esn_t(1)),
                     nullptr,
                     client_digests,
                     all_digests),
        test_(test) {
    sender_ = std::make_unique<MockSender>(this);
  }

  std::unique_ptr<Timer> createPushTimer(std::function<void()>) override {
    return nullptr;
  }
  void activatePushTimer() override {}
  void cancelPushTimer() override {}

  std::unique_ptr<BackoffTimer>
  createDelayTimer(std::function<void()> callback) override {
    auto timer = std::make_unique<MockBackoffTimer>();
    timer->setCallback(callback);
    return std::move(timer);
  }

  bool canPushRecords() const override {
    return test_->can_push_;
  }

  void onBytesEnqueued(size_t msg_size) override {}

  bool includeExtraMetadata() const override {
    return false;
  }

  bool canSendToImpl(const Address&, TrafficClass, BWAvailableCallback&) {
    return true;
  }

  int sendMessageImpl(std::unique_ptr<Message>&& msg,
                      const Address& addr,
                      BWAvailableCallback*,
                      SocketCallback*) {
    return test_->can_push_ ? 0 : -1;
  }

 private:
  AllCachedDigestsTest* const test_;
};

class MockAllCachedDigests : public AllCachedDigests {
 public:
  explicit MockAllCachedDigests(AllCachedDigestsTest* test)
      : AllCachedDigests(test->max_active_digests_,
                         test->max_bytes_queued_per_client_kb_),
        test_(test) {}

  std::unique_ptr<CachedDigest> createCachedDigest(
      logid_t log_id,
      shard_index_t shard,
      read_stream_id_t rid,
      ClientID client_id,
      lsn_t start_lsn,
      std::unique_ptr<const EpochRecordCache::Snapshot> epoch_snapshot,
      ClientDigests* client_digests) override {
    return std::make_unique<DummyCachedDigest>(
        test_, client_id, rid, client_digests, this);
  }

  std::unique_ptr<Timer>
  createRescheduleTimer(std::function<void()> callback) override {
    auto timer = std::make_unique<MockTimer>(std::move(callback));
    return std::move(timer);
  }

  void activateRescheduleTimer() override {
    auto& timer = getRescheduleTimer();
    ASSERT_NE(nullptr, timer);
    timer->activate(std::chrono::microseconds(0));
  }

  size_t getMaxStreamsStartedBatch() const override {
    return test_->max_streams_per_batch_;
  }

 private:
  AllCachedDigestsTest* const test_;
};

void AllCachedDigestsTest::setUp() {
  digests_ = std::make_unique<MockAllCachedDigests>(this);
}

TEST_F(AllCachedDigestsTest, MaximumDigestsPerIteration) {
  max_active_digests_ = 10;
  max_streams_per_batch_ = 233;
  can_push_ = false;
  setUp();
  // start 8000 cached digests
  for (int i = 1; i <= 8000; ++i) {
    auto st = digests_->startDigest(LOG_ID,
                                    shard_index_t(0),
                                    read_stream_id_t(i),
                                    i % 2 ? ClientID(1) : ClientID(2),
                                    compose_lsn(EPOCH, ESN_MIN),
                                    nullptr);
    ASSERT_EQ(E::OK, st);
  }

  const size_t init_queue_size = digests_->queueSize();
  // all digests other than the 10 active ones are enqueued
  ASSERT_EQ(8000 - 10, init_queue_size);

  // make digests can sychonously finish
  can_push_ = true;

  for (int i = 0; digests_->queueSize() > 0; ++i) {
    ASSERT_EQ(
        init_queue_size - max_streams_per_batch_ * i, digests_->queueSize());
    if (i == 0) {
      CachedDigest* first_digest =
          digests_->getDigest(ClientID(1), read_stream_id_t(1));
      ASSERT_NE(nullptr, first_digest);
      first_digest->pushRecords();
    } else {
      const auto& timer = digests_->getRescheduleTimer();
      ASSERT_NE(nullptr, timer);
      if (digests_->queueSize() > 0) {
        // timer must be active for the next batch
        ASSERT_TRUE(timer->isActive());
      }
      checked_downcast<MockTimer*>(timer.get())->trigger();
    }
  }
  ASSERT_EQ(0, digests_->queueSize());
}

}} // namespace facebook::logdevice
