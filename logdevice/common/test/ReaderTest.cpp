/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/include/Reader.h"

#include <chrono>
#include <condition_variable>
#include <cstdlib>
#include <memory>
#include <mutex>
#include <set>
#include <thread>

#include <folly/Memory.h>
#include <gtest/gtest.h>

#include "logdevice/common/DataRecordOwnsPayload.h"
#include "logdevice/common/ReaderImpl.h"
#include "logdevice/common/Timer.h"
#include "logdevice/common/test/TestUtil.h"
#include "logdevice/common/types_internal.h"
#include "logdevice/include/Client.h"

namespace facebook { namespace logdevice {

class TestReader : public ReaderImpl {
 public:
  TestReader(size_t max_logs, size_t client_read_buffer_size)
      : ReaderImpl(max_logs,
                   nullptr,
                   nullptr,
                   nullptr,
                   "",
                   client_read_buffer_size) {
    destructor_stops_reading_ = false;
  }

  // Convenience - creates a Testreader that masquerades as a ReaderImpl
  static std::unique_ptr<ReaderImpl> create(size_t max_logs,
                                            size_t client_read_buffer_size) {
    return std::unique_ptr<ReaderImpl>(
        new TestReader(max_logs, client_read_buffer_size));
  }

  read_stream_id_t getLastReadStreamID() const {
    ld_check(last_issued_rsid_.val_ != 0);
    return last_issued_rsid_;
  }

  ReaderBridge* getBridge() {
    return bridge_.get();
  }

 protected:
  int startReadingImpl(
      logid_t /*log_id*/,
      lsn_t /*from*/,
      lsn_t /*until*/,
      ReadingHandle* handle_out,
      const ReadStreamAttributes* /*attrs = nullptr*/) override {
    static std::atomic<read_stream_id_t::raw_type> next_rsid(1);
    last_issued_rsid_.val_ = next_rsid.fetch_add(1);
    handle_out->read_stream_id = last_issued_rsid_;
    handle_out->worker_id.val_ = -2; // should never be used
    return 0;                        // pretend it always suceeds
  }
  int postStopReadingRequest(ReadingHandle /*handle*/,
                             std::function<void()> cb) override {
    // pretend it always succeeds
    if (cb) {
      cb();
    }
    return 0;
  }

 private:
  read_stream_id_t last_issued_rsid_{READ_STREAM_ID_INVALID};
};

/**
 * Fixture that sets up a reader for a single log
 */
class ReaderTestSingleLog : public ::testing::Test {
 protected:
  void SetUp() override {
    dbg::assertOnData = true;
    init();
  }

  void init() {
    reader_ = TestReader::create(1, 100);
    bridge_ = dynamic_cast<TestReader*>(reader_.get())->getBridge();
    EXPECT_EQ(0, reader_->startReading(LOG_ID, lsn_t(1), until_lsn_));
    rsid_ = dynamic_cast<TestReader*>(reader_.get())->getLastReadStreamID();
  }

  lsn_t until_lsn_ = LSN_MAX; // default, can be changed and init() called

  const logid_t LOG_ID{12321};
  std::unique_ptr<ReaderImpl> reader_;
  ReaderBridge* bridge_;
  read_stream_id_t rsid_;
};

static std::unique_ptr<DataRecordOwnsPayload> make_record(logid_t log_id,
                                                          lsn_t lsn) {
  static std::atomic<int64_t> timestamp{19850921};
  size_t nbytes = 100;
  char* buf = (char*)malloc(nbytes);
  snprintf(buf, nbytes, "record %ld", lsn);
  Payload payload(buf, nbytes);
  return std::make_unique<DataRecordOwnsPayload>(
      log_id,
      Payload(buf, 100),
      lsn,
      std::chrono::milliseconds(timestamp++),
      (RECORD_flags_t)0);
}

/**
 * Simple test where we simulate ClientReadStream putting a few records on the
 * Reader's queue and then read() them
 */
TEST_F(ReaderTestSingleLog, Basic) {
  std::vector<std::unique_ptr<DataRecord>> records_out;
  GapRecord gap_out;

  bridge_->onDataRecord(rsid_, make_record(LOG_ID, lsn_t(1)), false);
  bridge_->onDataRecord(rsid_, make_record(LOG_ID, lsn_t(2)), false);
  bridge_->onDataRecord(rsid_, make_record(LOG_ID, lsn_t(3)), false);

  ssize_t nread;
  nread = reader_->read(1, &records_out, &gap_out);
  ASSERT_EQ(1, nread);
  ASSERT_EQ(1, records_out.size());
  ASSERT_STREQ("record 1", (const char*)records_out[0]->payload.data());
  nread = reader_->read(2, &records_out, &gap_out);
  ASSERT_EQ(2, nread);
  ASSERT_EQ(3, records_out.size());
  ASSERT_STREQ("record 2", (const char*)records_out[1]->payload.data());
  ASSERT_STREQ("record 3", (const char*)records_out[2]->payload.data());
}

/**
 * A non-blocking read() should pick off whatever is in the queue and return
 * immediately
 */
TEST_F(ReaderTestSingleLog, NonBlocking) {
  std::vector<std::unique_ptr<DataRecord>> records_out;
  GapRecord gap_out;

  bridge_->onDataRecord(rsid_, make_record(LOG_ID, lsn_t(1)), false);
  bridge_->onDataRecord(rsid_, make_record(LOG_ID, lsn_t(2)), false);

  reader_->setTimeout(std::chrono::milliseconds::zero());

  ssize_t nread;
  // Request a lot of records, should immediately return
  nread = reader_->read(99999, &records_out, &gap_out);
  ASSERT_EQ(2, nread);
  ASSERT_STREQ("record 1", (const char*)records_out[0]->payload.data());
  ASSERT_STREQ("record 2", (const char*)records_out[1]->payload.data());
}

/**
 * A blocking read() should wait until the requested number of records is
 * available.
 */
TEST_F(ReaderTestSingleLog, Blocking) {
  std::vector<std::unique_ptr<DataRecord>> records_out;
  GapRecord gap_out;

  bridge_->onDataRecord(rsid_, make_record(LOG_ID, lsn_t(1)), false);

  std::thread producer([&]() {
    /* sleep override */
    std::this_thread::sleep_for(std::chrono::seconds(1));
    bridge_->onDataRecord(rsid_, make_record(LOG_ID, lsn_t(2)), false);
  });

  Alarm alarm(std::chrono::seconds(10));
  ssize_t nread;
  // Request 2 records, should wait for producer thread
  nread = reader_->read(2, &records_out, &gap_out);
  ASSERT_EQ(2, nread);
  ASSERT_STREQ("record 1", (const char*)records_out[0]->payload.data());
  ASSERT_STREQ("record 2", (const char*)records_out[1]->payload.data());

  producer.join();
}

/**
 * If waitOnlyWhenNoData() is called, read() should not block whenever there
 * is some data available to return to the client.
 */
TEST_F(ReaderTestSingleLog, WaitOnlyWhenNoData) {
  std::vector<std::unique_ptr<DataRecord>> records_out;
  GapRecord gap_out;

  reader_->waitOnlyWhenNoData();

  bridge_->onDataRecord(rsid_, make_record(LOG_ID, lsn_t(1)), false);

  Alarm alarm(std::chrono::seconds(10));
  ssize_t nread;
  // Request 100 records when 1 is available, should not block.
  nread = reader_->read(100, &records_out, &gap_out);
  ASSERT_EQ(1, nread);
  ASSERT_STREQ("record 1", (const char*)records_out[0]->payload.data());

  // However, a subsequent call should block until the producer makes
  // something available.
  std::thread producer([&]() {
    /* sleep override */
    std::this_thread::sleep_for(std::chrono::seconds(1));
    bridge_->onDataRecord(rsid_, make_record(LOG_ID, lsn_t(2)), false);
  });
  nread = reader_->read(100, &records_out, &gap_out);
  ASSERT_EQ(1, nread);
  ASSERT_STREQ("record 2", (const char*)records_out[1]->payload.data());

  producer.join();
}

TEST_F(ReaderTestSingleLog, Timeout) {
  std::vector<std::unique_ptr<DataRecord>> records_out;
  GapRecord gap_out;

  bridge_->onDataRecord(rsid_, make_record(LOG_ID, lsn_t(1)), false);
  bridge_->onDataRecord(rsid_, make_record(LOG_ID, lsn_t(2)), false);

  using namespace std::chrono;
  milliseconds timeout(100);
  reader_->setTimeout(timeout);

  Alarm alarm(std::chrono::seconds(10));
  ssize_t nread;
  // Request a lot of records, should return after timing out
  auto tstart = steady_clock::now();
  nread = reader_->read(99999, &records_out, &gap_out);
  ASSERT_EQ(2, nread);
  auto elapsed = steady_clock::now() - tstart;
  ASSERT_STREQ("record 1", (const char*)records_out[0]->payload.data());
  ASSERT_STREQ("record 2", (const char*)records_out[1]->payload.data());
  ASSERT_GE(duration_cast<milliseconds>(elapsed).count(), timeout.count());
}

TEST_F(ReaderTestSingleLog, Gap) {
  std::vector<std::unique_ptr<DataRecord>> records_out;
  GapRecord gap_out;

  bridge_->onDataRecord(rsid_, make_record(LOG_ID, lsn_t(1)), false);
  bridge_->onDataRecord(rsid_, make_record(LOG_ID, lsn_t(2)), false);
  bridge_->onDataRecord(rsid_, make_record(LOG_ID, lsn_t(3)), false);
  bridge_->onGapRecord(
      rsid_, GapRecord(LOG_ID, GapType::DATALOSS, lsn_t(4), lsn_t(5)), false);
  bridge_->onDataRecord(rsid_, make_record(LOG_ID, lsn_t(6)), false);
  bridge_->onDataRecord(rsid_, make_record(LOG_ID, lsn_t(7)), false);
  bridge_->onDataRecord(rsid_, make_record(LOG_ID, lsn_t(8)), false);

  reader_->setTimeout(std::chrono::milliseconds::zero());

  ssize_t nread;
  // Ask for many records.  We should get the first three data records
  // immediately...
  nread = reader_->read(100, &records_out, &gap_out);
  ASSERT_EQ(3, nread);
  ASSERT_STREQ("record 1", (const char*)records_out[0]->payload.data());
  ASSERT_STREQ("record 2", (const char*)records_out[1]->payload.data());
  ASSERT_STREQ("record 3", (const char*)records_out[2]->payload.data());
  records_out.clear();

  // ... then a gap ...
  nread = reader_->read(100, &records_out, &gap_out);
  ASSERT_EQ(-1, nread);
  ASSERT_EQ(E::GAP, err);
  ASSERT_EQ(GapType::DATALOSS, gap_out.type);
  ASSERT_EQ(lsn_t(4), gap_out.lo);
  ASSERT_EQ(lsn_t(5), gap_out.hi);

  // ... then more data.
  nread = reader_->read(100, &records_out, &gap_out);
  ASSERT_EQ(3, nread);
  ASSERT_STREQ("record 6", (const char*)records_out[0]->payload.data());
  ASSERT_STREQ("record 7", (const char*)records_out[1]->payload.data());
  ASSERT_STREQ("record 8", (const char*)records_out[2]->payload.data());
}

/**
 * If a client stops reading a log, any buffered records should be discarded
 */
TEST_F(ReaderTestSingleLog, StopReading) {
  std::vector<std::unique_ptr<DataRecord>> records_out;
  GapRecord gap_out;

  bridge_->onDataRecord(rsid_, make_record(LOG_ID, lsn_t(1)), false);
  bridge_->onDataRecord(rsid_, make_record(LOG_ID, lsn_t(2)), false);
  bridge_->onDataRecord(rsid_, make_record(LOG_ID, lsn_t(3)), false);
  bridge_->onDataRecord(rsid_, make_record(LOG_ID, lsn_t(4)), false);

  reader_->setTimeout(std::chrono::milliseconds::zero());

  ssize_t nread;
  nread = reader_->read(2, &records_out, &gap_out);
  ASSERT_EQ(2, nread);
  ASSERT_STREQ("record 1", (const char*)records_out[0]->payload.data());
  ASSERT_STREQ("record 2", (const char*)records_out[1]->payload.data());

  int rv;
  rv = reader_->stopReading(LOG_ID);
  ASSERT_EQ(0, rv);
  nread = reader_->read(2, &records_out, &gap_out);
  ASSERT_EQ(0, nread);
}

/**
 * There was a bug where stopReading() could cause a read with a timeout to
 * hang.
 */
TEST(ReaderTest, StopReadingTimeout) {
  const logid_t LOG1 = logid_t(333), LOG2 = logid_t(444);

  std::unique_ptr<ReaderImpl> reader = TestReader::create(2, 100);
  ReaderBridge* bridge = dynamic_cast<TestReader*>(reader.get())->getBridge();

  EXPECT_EQ(0, reader->startReading(LOG1, lsn_t(1), LSN_MAX));
  read_stream_id_t rsid1 =
      dynamic_cast<TestReader*>(reader.get())->getLastReadStreamID();
  EXPECT_EQ(0, reader->startReading(LOG2, lsn_t(1), LSN_MAX));

  // Push a record with notify_when_consumed=true
  bridge->onDataRecord(rsid1, make_record(LOG1, lsn_t(1)), true);
  // Stop reading
  int rv;
  rv = reader->stopReading(LOG1);
  ASSERT_EQ(0, rv);

  // Now try a read with a short timeout.  The call should return quickly,
  // after the timeout expires.
  reader->setTimeout(std::chrono::milliseconds(1));
  Alarm alarm(std::chrono::seconds(1));
  std::vector<std::unique_ptr<DataRecord>> records_out;
  GapRecord gap_out;
  ssize_t nread = reader->read(1, &records_out, &gap_out);
  ASSERT_EQ(0, nread);
}

/**
 * If a client restarts reading a log (at a different LSN, say), any buffered
 * records should be discarded
 */
TEST_F(ReaderTestSingleLog, RestartReading) {
  std::vector<std::unique_ptr<DataRecord>> records_out;
  GapRecord gap_out;

  bridge_->onDataRecord(rsid_, make_record(LOG_ID, lsn_t(1)), false);
  bridge_->onDataRecord(rsid_, make_record(LOG_ID, lsn_t(2)), false);
  bridge_->onDataRecord(rsid_, make_record(LOG_ID, lsn_t(3)), false);
  bridge_->onDataRecord(rsid_, make_record(LOG_ID, lsn_t(4)), false);

  reader_->setTimeout(std::chrono::milliseconds::zero());

  ssize_t nread;
  nread = reader_->read(2, &records_out, &gap_out);
  ASSERT_EQ(2, nread);
  ASSERT_STREQ("record 1", (const char*)records_out[0]->payload.data());
  ASSERT_STREQ("record 2", (const char*)records_out[1]->payload.data());
  records_out.clear();

  // Now restart reading at another LSN
  int rv = reader_->startReading(LOG_ID, lsn_t(101), LSN_MAX);
  ASSERT_EQ(0, rv);
  rsid_ = dynamic_cast<TestReader*>(reader_.get())->getLastReadStreamID();

  bridge_->onDataRecord(rsid_, make_record(LOG_ID, lsn_t(101)), false);
  bridge_->onDataRecord(rsid_, make_record(LOG_ID, lsn_t(102)), false);
  nread = reader_->read(100, &records_out, &gap_out);
  ASSERT_EQ(2, nread);
  // Records 3 and 4 should have been discarded from the buffer
  ASSERT_STREQ("record 101", (const char*)records_out[0]->payload.data());
  ASSERT_STREQ("record 102", (const char*)records_out[1]->payload.data());
}

/**
 * Test that a blocking call yields if we reach the end of a log.
 */
TEST_F(ReaderTestSingleLog, LogEndsWithDataRecord) {
  this->until_lsn_ = 3;
  this->init();

  std::vector<std::unique_ptr<DataRecord>> records_out;
  GapRecord gap_out;

  bridge_->onDataRecord(rsid_, make_record(LOG_ID, lsn_t(1)), false);
  bridge_->onDataRecord(rsid_, make_record(LOG_ID, lsn_t(2)), false);
  bridge_->onDataRecord(rsid_, make_record(LOG_ID, lsn_t(3)), false);

  // Despite asking for 100 records with an infinite timeout, this read() call
  // should return early because it reached the end of the log.
  Alarm alarm(std::chrono::seconds(10));
  ssize_t nread;
  nread = reader_->read(100, &records_out, &gap_out);
  ASSERT_EQ(3, nread);

  // Subsequent read() calls should just quickly return since there are no more
  // logs to read.
  nread = reader_->read(100, &records_out, &gap_out);
  ASSERT_EQ(0, nread);
}

/**
 * Test the skipGaps() facility, which makes Reader not report most gaps.
 */
TEST_F(ReaderTestSingleLog, SkipGaps) {
  this->until_lsn_ = 9;
  this->init();

  std::vector<std::unique_ptr<DataRecord>> records_out;
  GapRecord gap_out;

  bridge_->onDataRecord(rsid_, make_record(LOG_ID, lsn_t(1)), false);
  bridge_->onGapRecord(
      rsid_, GapRecord(LOG_ID, GapType::DATALOSS, lsn_t(2), lsn_t(2)), false);
  bridge_->onDataRecord(rsid_, make_record(LOG_ID, lsn_t(3)), false);

  reader_->skipGaps();

  // Normally, the first read() call would return only 1 data record.  With
  // skipGaps() however, the gap at LSN 2 should get swallowed.
  ssize_t nread;
  nread = reader_->read(2, &records_out, &gap_out);
  ASSERT_EQ(2, nread);

  // A gap at the end of the log should still be reported, though.
  GapRecord actual_gap(LOG_ID, GapType::DATALOSS, lsn_t(4), lsn_t(9));
  bridge_->onGapRecord(rsid_, actual_gap, false);
  nread = reader_->read(2, &records_out, &gap_out);
  ASSERT_EQ(-1, nread);
  ASSERT_EQ(actual_gap.lo, gap_out.lo);
  ASSERT_EQ(actual_gap.hi, gap_out.hi);
}

// There used to be a bug where a blocking read() call would not return when
// there is a gap to deliver (even if that gap is at `until_lsn').
TEST_F(ReaderTestSingleLog, BlockingGap) {
  this->until_lsn_ = 1;
  this->init();

  // To trigger the bug, the reader needs to enter the blocking read() call
  // and wait on the condition variable before the gap arrives.  Post the gap
  // in a background thread after a short while.  Timing issues here are
  // unlikely as the reader will go to sleep shortly.  Even then, timing
  // issues wouldn't make the test spuriously fail but pass as the bug won't
  // repro.
  std::thread writer_thread([&] {
    /* sleep override */
    std::this_thread::sleep_for(std::chrono::seconds(1));
    bridge_->onGapRecord(
        rsid_,
        GapRecord(LOG_ID, GapType::DATALOSS, lsn_t(1), lsn_t(1)),
        /* notify */ false);
  });

  std::vector<std::unique_ptr<DataRecord>> records_out;
  GapRecord gap_out;
  ssize_t nread;
  Alarm alarm(std::chrono::seconds(10));
  nread = reader_->read(100, &records_out, &gap_out);
  ASSERT_EQ(-1, nread);
  writer_thread.join();
}

class ReaderTestBlockingStress : public ::testing::TestWithParam<int> {};

/**
 * Test that blocking calls behave with multiple threads.
 */
TEST_P(ReaderTestBlockingStress, Test) {
  const int nproducers = GetParam();
  const lsn_t UPTO_LSN = 10000 / nproducers;

  // Each of `nproducers` producers will push records for one log into a
  // single Reader.  These mimic workers running ClientReadStream instances.
  //
  // There will be one thread calling read(), like an application thread might.
  //
  // The main thread will, for every LSN, wait for all other threads to be
  // ready, then wake them all with a cv.notify_all() and let them race.

  std::unique_ptr<ReaderImpl> reader = TestReader::create(nproducers, 100);
  ReaderBridge* bridge = dynamic_cast<TestReader*>(reader.get())->getBridge();
  std::vector<logid_t> log_ids(nproducers);
  std::vector<read_stream_id_t> rsids(nproducers);

  for (int i = 0; i < nproducers; ++i) {
    log_ids[i] = logid_t(1 + i);
    // NOTE: reading until LSN_MAX (not UPTO_LSN) so that read() never returns
    // early because it reached the end of a log.
    ASSERT_EQ(0, reader->startReading(log_ids[i], 1, LSN_MAX));
    rsids[i] = dynamic_cast<TestReader*>(reader.get())->getLastReadStreamID();
  }

  Alarm alarm(std::chrono::seconds(10));

  std::atomic<lsn_t> current_lsn(0);
  std::mutex cv_mutex;
  std::condition_variable cv;
  Semaphore sem_ready;

  std::vector<std::thread> producers;
  auto one_producer = [&](const int idx) {
    for (lsn_t lsn = 1; lsn <= UPTO_LSN; ++lsn) {
      sem_ready.post();
      {
        std::unique_lock<std::mutex> lock(cv_mutex);
        cv.wait(lock, [&]() { return current_lsn.load() == lsn; });
      }
      bridge->onDataRecord(rsids[idx], make_record(log_ids[idx], lsn), false);
    }
  };
  for (int i = 0; i < nproducers; ++i) {
    producers.emplace_back(one_producer, i);
  }

  std::thread consumer([&]() {
    std::vector<std::unique_ptr<DataRecord>> records_out;
    for (lsn_t lsn = 1; lsn <= UPTO_LSN; ++lsn) {
      sem_ready.post();
      {
        std::unique_lock<std::mutex> lock(cv_mutex);
        cv.wait(lock, [&]() { return current_lsn.load() == lsn; });
      }
      records_out.clear();
      GapRecord gap_out;
      ssize_t nread = reader->read(nproducers, &records_out, &gap_out);
      ASSERT_EQ(nproducers, nread);

      std::set<logid_t> logs_seen;
      for (ssize_t i = 0; i < nread; ++i) {
        ASSERT_EQ(lsn, records_out[i]->attrs.lsn);
        logs_seen.insert(records_out[i]->logid);
      }
      ASSERT_EQ(nproducers, logs_seen.size());
    }
  });

  for (lsn_t lsn = 1; lsn <= UPTO_LSN; ++lsn) {
    for (int i = 0; i < nproducers + 1; ++i) {
      sem_ready.wait();
    }

    {
      std::unique_lock<std::mutex> lock(cv_mutex);
      current_lsn.store(lsn);
    }
    cv.notify_all();
  }

  for (auto& t : producers) {
    t.join();
  }
  consumer.join();
}

INSTANTIATE_TEST_CASE_P(SingleProducer,
                        ReaderTestBlockingStress,
                        ::testing::Values(1));
INSTANTIATE_TEST_CASE_P(MultipleProducers,
                        ReaderTestBlockingStress,
                        ::testing::Values(4, 16));

}} // namespace facebook::logdevice
