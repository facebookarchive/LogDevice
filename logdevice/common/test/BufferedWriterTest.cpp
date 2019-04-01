/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/include/BufferedWriter.h"

#include <random>

#include <folly/Memory.h>
#include <gtest/gtest.h>

#include "logdevice/common/DataRecordOwnsPayload.h"
#include "logdevice/common/Processor.h"
#include "logdevice/common/Request.h"
#include "logdevice/common/buffered_writer/BufferedWriteDecoderImpl.h"
#include "logdevice/common/buffered_writer/BufferedWriterImpl.h"
#include "logdevice/common/buffered_writer/BufferedWriterSingleLog.h"
#include "logdevice/common/debug.h"
#include "logdevice/common/protocol/RECORD_Message.h"
#include "logdevice/common/settings/Settings.h"
#include "logdevice/common/settings/util.h"
#include "logdevice/common/test/TestUtil.h"
#include "logdevice/include/Err.h"

/**
 * @file Unit tests for BufferedWriter.  Outgoing (batched) appends are
 * intercepted and recorded in-memory.
 */

using namespace facebook::logdevice;

// Null context for appenders that don't need one
constexpr BufferedWriter::AppendCallback::Context NULL_CONTEXT(nullptr);
using Compression = BufferedWriter::Options::Compression;

/**
 * This sub-class of ProcessorProxy intercepts calls to postWithRetrying(),
 * saves them until we have all of them, then issues them in a deterministically
 * shuffled order.
 */
class ProcessorTestProxy : public ProcessorProxy {
  std::mutex mutex_;
  std::vector<std::unique_ptr<Request>> requests_;
  const size_t numAppendsBeforePosting_;
  size_t numAppendsSoFar_ = 0;

 public:
  ProcessorTestProxy(Processor* p, size_t numAppendsBeforePosting)
      : ProcessorProxy(p), numAppendsBeforePosting_(numAppendsBeforePosting) {}

  int postWithRetrying(std::unique_ptr<Request>& rq) override {
    auto cbsr = dynamic_cast<BufferedWriterSingleLog::ContinueBlobSendRequest*>(
        rq.get());

    std::lock_guard<std::mutex> guard(mutex_);
    numAppendsSoFar_ += cbsr->getBatch().appends.size();
    EXPECT_LE(numAppendsSoFar_, numAppendsBeforePosting_);

    requests_.emplace_back(std::move(rq));

    if (numAppendsSoFar_ >= numAppendsBeforePosting_) {
      std::shuffle(
          requests_.begin(), requests_.end(), std::mt19937_64(1234567890));

      for (auto& request : requests_) {
        int rv = processor_->postWithRetrying(request);
        EXPECT_EQ(0, rv);
      }
    }
    return 0;
  }
};

namespace {

// Simulates APPENDED message for an append that supposedly went out to the
// cluster
class AppendedMessageReceivedRequest : public Request {
 public:
  AppendedMessageReceivedRequest(worker_id_t target_worker,
                                 std::function<void()> cb)
      : Request(RequestType::TEST_REQUEST),
        target_worker_(target_worker),
        cb_(std::move(cb)) {}

  int getThreadAffinity(int /*nthreads*/) override {
    return target_worker_.val();
  }
  Execution execute() override {
    cb_();
    return Execution::COMPLETE;
  }

 private:
  worker_id_t target_worker_;
  std::function<void()> cb_;
};

// This intercepts outgoing (batched) writes from BufferedWriter, allows
// failure injection and introspection
class TestAppendSink : public BufferedWriterAppendSink {
 public:
  explicit TestAppendSink(Processor* processor) : processor_(processor) {}

  // If this returns false, be sure to set logdevice::err as appropriate.
  bool checkAppend(logid_t, size_t payload_size, bool allow_extra) override {
    size_t max_size = processor_->settings()->max_payload_size;
    if (allow_extra) {
      max_size += MAX_PAYLOAD_EXTRA_SIZE;
    }
    if (payload_size > max_size) {
      err = E::TOOBIG;
      return false;
    }

    return true;
  }

  Status canSendToWorker() override {
    return check_worker_result_;
  }

  std::pair<Status, NodeID>
  appendBuffered(logid_t logid,
                 const BufferedWriter::AppendCallback::ContextSet&,
                 AppendAttributes /*attrs*/,
                 const Payload& payload,
                 AppendRequestCallback writer_callback,
                 worker_id_t target_worker,
                 int /*checksum_bits*/) override {
    // See if a failure was injected
    if (pre_append_callback_ && !pre_append_callback_()) {
      ld_info("injected failure");
      return std::make_pair(E::FAILED, NodeID());
    }

    std::lock_guard<std::mutex> guard(mutex_);
    flushed_appends_[logid].emplace_back(payload.toString());
    // Simulate a server round trip with a Request on the Processor so that we
    // don't invoke BufferedWriter's append callback synchronously
    lsn_t lsn = lsn_t(flushed_appends_[logid].size());
    auto wrapper_cb = [writer_callback, logid, payload, lsn]() {
      DataRecord record(logid, payload, lsn);
      writer_callback(E::OK, record, NodeID());
    };
    std::unique_ptr<Request> req =
        std::make_unique<AppendedMessageReceivedRequest>(
            target_worker, wrapper_cb);
    processor_->postWithRetrying(req);
    return std::make_pair(E::OK, NodeID());
  }

  // Returns the blobs flushed by BufferedWriter in its format
  std::vector<std::string> getFlushedBlobs(logid_t log) {
    std::lock_guard<std::mutex> guard(mutex_);
    return flushed_appends_[log];
  }

  // Passes the result of getFlushedBlobs() through BufferedWriteDecoder
  std::vector<std::string> getFlushedOriginalPayloads(logid_t log) {
    // BufferedWriterDecoder needs DataRecordOwnsPayload instances like those
    // that would arrive in RECORD messages
    std::vector<std::unique_ptr<DataRecord>> blob_payloads;
    lsn_t lsn = 1;
    for (const std::string& blob : getFlushedBlobs(log)) {
      void* buf = malloc(blob.size());
      memcpy(buf, blob.data(), blob.size());
      blob_payloads.push_back(std::make_unique<DataRecordOwnsPayload>(
          log,
          Payload(buf, blob.size()),
          lsn++,
          std::chrono::milliseconds(0),
          RECORD_flags_t(RECORD_Header::BUFFERED_WRITER_BLOB)));
    }
    const size_t nread = blob_payloads.size();

    BufferedWriteDecoderImpl decoder;
    std::vector<Payload> payloads;
    EXPECT_EQ(0, decoder.decode(std::move(blob_payloads), payloads));
    // At this point `decoder' owns the memory that `payloads' point into.
    // The pointers in `data' are now empty.
    EXPECT_EQ(std::vector<std::unique_ptr<DataRecord>>(nread), blob_payloads);

    ld_info("read %zd records from LogDevice, %zu after decoding batch",
            nread,
            payloads.size());

    // It is fine to access `payloads' because `decoder' is still in scope.
    std::vector<std::string> rv;
    for (Payload p : payloads) {
      rv.emplace_back(p.toString());
    }
    return rv;
  }

  // Return true if append should succeed
  std::function<bool()> pre_append_callback_;

  Status check_worker_result_{E::OK};

  void onBytesSentToWorker(ssize_t bytes) override {
    bytes_sent_to_shard_ += bytes;
  }
  void onBytesFreedByWorker(size_t bytes) override {
    bytes_freed_by_shard_ += bytes;
  }
  size_t bytes_sent_to_shard_{0};
  size_t bytes_freed_by_shard_{0};

 private:
  std::mutex mutex_;
  std::unordered_map<logid_t, std::vector<std::string>, logid_t::Hash>
      flushed_appends_;
  Processor* processor_;
};

// Universal client callback for use in different tests
class TestCallback : public BufferedWriter::AppendCallback {
 public:
  void onSuccess(logid_t,
                 ContextSet contexts,
                 const DataRecordAttributes& attrs) override {
    std::lock_guard<std::mutex> guard(mutex_);
    if (lsn_range.first == LSN_INVALID) {
      lsn_range.first = attrs.lsn;
    }
    lsn_range.first = std::min(lsn_range.first, attrs.lsn);
    lsn_range.second = std::max(lsn_range.second, attrs.lsn);
    last_time = std::chrono::steady_clock::now();
    for (auto& ctx : contexts) {
      payloads_succeeded.push_back(std::move(ctx.second));
      sem.post();
    }
  }

  void onFailure(logid_t /*log_id*/,
                 ContextSet contexts,
                 Status status) override {
    std::lock_guard<std::mutex> guard(mutex_);
    last_time = std::chrono::steady_clock::now();
    for (size_t i = 0; i < contexts.size(); ++i) {
      failures.insert(status);
      sem.post();
    }
  }

  RetryDecision onRetry(logid_t /*log_id*/,
                        const ContextSet& contexts,
                        Status /*status*/) override {
    std::lock_guard<std::mutex> guard(mutex_);
    last_time = std::chrono::steady_clock::now();
    nretries += contexts.size();
    // If a custom retry function was supplied, invoke it to decide whether to
    // allow or deny the retry.  Otherwise just allow.
    return retry_fn_ ? retry_fn_() : RetryDecision::ALLOW;
  }

  // Each complete append (success or failure) posts to this semaphore
  Semaphore sem;
  // Collects payloads for all successful writes
  std::vector<std::string> payloads_succeeded;

  size_t getNumSucceeded() const {
    std::lock_guard<std::mutex> guard(mutex_);
    return payloads_succeeded.size();
  }

  // Don't care about order?
  std::multiset<std::string> payloadsSucceededAsMultiset() const {
    return std::multiset<std::string>(
        payloads_succeeded.begin(), payloads_succeeded.end());
  }
  // Don't care about duplicates?
  std::set<std::string> payloadsSucceededAsSet() const {
    return std::set<std::string>(
        payloads_succeeded.begin(), payloads_succeeded.end());
  }
  // Failure statuses
  std::multiset<Status> failures;
  // How many appends onRetry() was called for
  int nretries = 0;
  // LSN range that was written
  std::pair<lsn_t, lsn_t> lsn_range{LSN_INVALID, LSN_INVALID};
  // When was the callback last invoked
  std::chrono::steady_clock::time_point last_time{
      std::chrono::steady_clock::now()};
  std::function<BufferedWriter::AppendCallback::RetryDecision()> retry_fn_;

 private:
  mutable std::mutex mutex_;
} callback;

} // anonymous namespace

class BufferedWriterTest : public ::testing::Test {
 public:
  void SetUp() override {
    initProcessor(create_default_settings<Settings>());
  }

  // This can be called again by tests to supply settings
  void initProcessor(Settings settings) {
    sink_.reset();
    processor_ = make_test_processor(settings);
    sink_ = std::make_unique<TestAppendSink>(processor_.get());
  }

  std::unique_ptr<BufferedWriterImpl>
  createWriter(BufferedWriter::AppendCallback* cb,
               BufferedWriter::Options opts = BufferedWriter::Options(),
               size_t numAppendsBeforePosting = 0) {
    return std::make_unique<BufferedWriterImpl>(
        numAppendsBeforePosting
            ? new ProcessorTestProxy(processor_.get(), numAppendsBeforePosting)
            : new ProcessorProxy(processor_.get()),
        cb,
        [opts](logid_t) -> BufferedWriter::LogOptions { return opts; },
        opts.memory_limit_mb,
        sink_.get());
  }

  int64_t getMemoryAvailable(BufferedWriterImpl& writer) const {
    // This test class gets access to this thanks to a friend declaration in
    // BufferedWriterImpl.h
    return writer.memory_available_.load();
  }

  void explicitFlushTest(BufferedWriter::Options::Mode,
                         size_t numAppendsBeforePosting);
  void roundTripTest(Compression, bool payloads_compressible);
  void bigPayloadFlushesTest(size_t);

 protected:
  std::shared_ptr<Processor> processor_;
  std::unique_ptr<TestAppendSink> sink_;

 private:
  Alarm alarm_{std::chrono::seconds(30)};
};

// Simple round trip test that uses a BufferedWriter to write a few records,
// explicitly flushes using flushAll(), then reads them.
void BufferedWriterTest::explicitFlushTest(BufferedWriter::Options::Mode mode,
                                           size_t numAppendsBeforePosting) {
  TestCallback cb;
  BufferedWriter::Options opts;
  opts.mode = mode;
  auto writer = this->createWriter(&cb, opts, numAppendsBeforePosting);
  const logid_t LOG_ID(1);

  std::vector<std::string> orig_payloads;

  int counter = 1; // goes in payload
  for (int nbatch = 0; nbatch < 10; ++nbatch) {
    for (int i = 0; i < 100; ++i) {
      std::string str = std::to_string(counter++);
      orig_payloads.push_back(str);
      int rv = writer->append(LOG_ID, std::move(str), NULL_CONTEXT);
      ASSERT_EQ(0, rv);
    }
    writer->flushAll();
  }

  std::vector<std::string> read_payloads;
  wait_until("BufferedWriter has flushed everything", [&]() {
    read_payloads = sink_->getFlushedOriginalPayloads(LOG_ID);
    return read_payloads.size() == orig_payloads.size() &&
        orig_payloads.size() == cb.getNumSucceeded();
  });

  ASSERT_EQ(orig_payloads, read_payloads);
  ASSERT_EQ(orig_payloads, cb.payloads_succeeded);
}

TEST_F(BufferedWriterTest, ExplicitFlushIndependent) {
  this->explicitFlushTest(BufferedWriter::Options::Mode::INDEPENDENT, 0);
}

TEST_F(BufferedWriterTest, ExplicitFlushOneAtATime) {
  this->explicitFlushTest(BufferedWriter::Options::Mode::ONE_AT_A_TIME, 0);
}

TEST_F(BufferedWriterTest, ExplicitFlushIndependentUseBackgroundThread) {
  Settings settings = create_default_settings<Settings>();
  settings.buffered_writer_bg_thread_bytes_threshold = 1;
  initProcessor(settings);

  this->explicitFlushTest(BufferedWriter::Options::Mode::INDEPENDENT, 1000);
}

TEST_F(BufferedWriterTest, ExplicitFlushOneAtATimeUseBackgroundThread) {
  Settings settings = create_default_settings<Settings>();
  settings.buffered_writer_bg_thread_bytes_threshold = 1;
  initProcessor(settings);

  // Since only one batch at a time is in flight, (a) the background threads
  // can't re-order them, and (b) if they hold up any batches, the test blocks.
  this->explicitFlushTest(BufferedWriter::Options::Mode::ONE_AT_A_TIME, 0);
}

// Round-trip test for compression with manual decoding to track compression
// ratio.  Parametrized by compression mode.
void BufferedWriterTest::roundTripTest(Compression compression,
                                       bool payloads_compressible) {
  TestCallback cb;
  BufferedWriter::Options opts;
  opts.compression = compression;
  auto writer = this->createWriter(&cb, opts);
  const logid_t LOG_ID(1);

  std::set<std::string> orig_payloads;

  std::mt19937_64 rnd(0xbabadeda);
  int counter = 1; // goes in payload
  auto gen_payload = [&]() {
    std::string ret = std::to_string(counter++);
    while (ret.size() < 20) {
      ret += payloads_compressible ? 'a' : rnd();
    }
    return ret;
  };

  for (int nbatch = 0; nbatch < 10; ++nbatch) {
    if (nbatch % 2 == 0) {
      // We can call the single-write append() many times ...
      for (int i = 0; i < 100; ++i) {
        std::string str = gen_payload();
        orig_payloads.insert(str);
        int rv = writer->append(LOG_ID, std::move(str), NULL_CONTEXT);
        ASSERT_EQ(0, rv);
      }
    } else {
      // ... or use the more efficient multi-write
      std::vector<BufferedWriter::Append> v;
      for (int i = 0; i < 100; ++i) {
        std::string str = gen_payload();
        orig_payloads.insert(str);
        AppendAttributes attrs;
        attrs.optional_keys[KeyType::FINDKEY] = std::string("12345678");

        v.push_back(BufferedWriter::Append(
            LOG_ID, std::move(str), NULL_CONTEXT, std::move(attrs)));
      }
      std::vector<Status> rv = writer->append(std::move(v));
      ASSERT_EQ(std::vector<Status>(v.size(), E::OK), rv);
    }
    writer->flushAll();
  }

  std::vector<std::string> read_payloads;
  wait_until("BufferedWriter has flushed everything", [&]() {
    read_payloads = sink_->getFlushedOriginalPayloads(LOG_ID);
    return read_payloads.size() == orig_payloads.size() &&
        orig_payloads.size() == cb.getNumSucceeded();
  });

  size_t bytes_read = 0;
  for (const std::string& str : sink_->getFlushedBlobs(LOG_ID)) {
    bytes_read += str.size();
  }
  size_t bytes_after_decoding = 0;
  for (const std::string& str : read_payloads) {
    bytes_after_decoding += str.size();
  }

  ASSERT_EQ(orig_payloads,
            std::set<std::string>(read_payloads.begin(), read_payloads.end()));
  ASSERT_EQ(orig_payloads, cb.payloadsSucceededAsSet());
  double compression_ratio = double(bytes_read) / bytes_after_decoding;
  ld_info("overall compression ratio was %.2f", compression_ratio);
  if (compression != Compression::NONE && payloads_compressible) {
    // Check that compression was effective
    ASSERT_LT(compression_ratio, 0.5);
  }
}

TEST_F(BufferedWriterTest, RoundTripNoCompression) {
  this->roundTripTest(Compression::NONE, true);
}

TEST_F(BufferedWriterTest, RoundTripZstd) {
  this->roundTripTest(Compression::ZSTD, true);
}

TEST_F(BufferedWriterTest, RoundTripLZ4) {
  this->roundTripTest(Compression::LZ4, true);
}

TEST_F(BufferedWriterTest, RoundTripLZ4_HC) {
  this->roundTripTest(Compression::LZ4_HC, true);
}

TEST_F(BufferedWriterTest, RoundTripLZ4NotCompressible) {
  this->roundTripTest(Compression::LZ4, false);
}

// Test Options::size_trigger.
TEST_F(BufferedWriterTest, SizeTrigger) {
  TestCallback cb;
  BufferedWriter::Options opts;
  opts.size_trigger = 10 * 1024;
  auto writer = this->createWriter(&cb, opts);
  const logid_t LOG_ID(1);

  // Buffer 1/2 of the size trigger, should not trigger a flush
  const std::string payload(opts.size_trigger / 2, 'a');
  ASSERT_EQ(0, writer->append(LOG_ID, std::string(payload), NULL_CONTEXT));
  /* sleep override */
  std::this_thread::sleep_for(std::chrono::milliseconds(200));
  ASSERT_EQ(0, cb.sem.value());

  // A second append of the same size should flush both writes
  ASSERT_EQ(0, writer->append(LOG_ID, std::string(payload), NULL_CONTEXT));
  cb.sem.wait();
  cb.sem.wait();
  ASSERT_EQ(2, cb.payloadsSucceededAsMultiset().size());
  ASSERT_EQ(0, cb.failures.size());
}

// Test writing a payload of size MAX_PAYLOAD_SIZE_PUBLIC.  This should fail
// because the default soft limit should be lower than
// MAX_PAYLOAD_SIZE_PUBLIC.
TEST_F(BufferedWriterTest, MaxSizePayloadFail) {
  TestCallback cb;
  BufferedWriter::Options opts;
  opts.compression = BufferedWriter::Options::Compression::NONE;
  auto writer = this->createWriter(&cb, opts);
  const logid_t LOG_ID(1);
  const std::string payload(MAX_PAYLOAD_SIZE_PUBLIC, 'a');
  ASSERT_NE(0, writer->append(LOG_ID, std::string(payload), NULL_CONTEXT));
  ASSERT_EQ(err, E::TOOBIG);
  // Callback should not get invoked
  ASSERT_EQ(0, cb.sem.value());
}

// Test writing a payload of size MAX_PAYLOAD_SIZE_PUBLIC.  This should
// succeed despite BufferedWriter adding a few bytes of overhead, if we set the
// max-payload-size setting appropriately
TEST_F(BufferedWriterTest, MaxSizePayloadSuccess) {
  Settings settings = create_default_settings<Settings>();
  settings.max_payload_size = MAX_PAYLOAD_SIZE_PUBLIC;
  initProcessor(settings);

  TestCallback cb;
  BufferedWriter::Options opts;
  opts.compression = BufferedWriter::Options::Compression::NONE;
  auto writer = this->createWriter(&cb, opts);
  const logid_t LOG_ID(1);
  const std::string payload(MAX_PAYLOAD_SIZE_PUBLIC, 'a');
  ASSERT_EQ(0, writer->append(LOG_ID, std::string(payload), NULL_CONTEXT));
  cb.sem.wait();
  ASSERT_EQ(1, cb.payloadsSucceededAsMultiset().size());
  ASSERT_EQ(0, cb.failures.size());
}

// Even without any triggers configured, big payloads should flush buffers
void BufferedWriterTest::bigPayloadFlushesTest(size_t max_payload_size) {
  Settings settings = create_default_settings<Settings>();
  settings.max_payload_size = max_payload_size;
  initProcessor(settings);

  TestCallback cb;
  auto writer = this->createWriter(&cb);
  const logid_t LOG_ID(1);

  // Send two appends at 75% of max_payload.  The first should get
  // buffered ...
  const std::string payload(0.75 * max_payload_size, 'a');
  ASSERT_EQ(0, writer->append(LOG_ID, std::string(payload), NULL_CONTEXT));
  /* sleep override */
  std::this_thread::sleep_for(std::chrono::milliseconds(200));
  ASSERT_EQ(0, cb.sem.value());

  // The second should flush the first and get buffered.
  ASSERT_EQ(0, writer->append(LOG_ID, std::string(payload), NULL_CONTEXT));
  cb.sem.wait();
  ASSERT_EQ(0, cb.sem.value());

  // An explicit flush should send out the second append
  ASSERT_EQ(0, writer->flushAll());
  cb.sem.wait();
  ASSERT_EQ(2, cb.payloadsSucceededAsMultiset().size());
  ASSERT_EQ(0, cb.failures.size());
}
TEST_F(BufferedWriterTest, BigPayloadFlushes) {
  this->bigPayloadFlushesTest(MAX_PAYLOAD_SIZE_PUBLIC);
}

TEST_F(BufferedWriterTest, BigPayloadFlushesSoftLimit) {
  this->bigPayloadFlushesTest(1024 * 1024);
}

// Test Options::time_trigger.
TEST_F(BufferedWriterTest, TimeTrigger) {
  using namespace std::chrono;
  BufferedWriter::Options opts;
  opts.time_trigger = milliseconds(100);
  TestCallback cb;
  auto writer = this->createWriter(&cb, opts);
  const logid_t LOG_ID(1);

  const std::string payload(100, 'a');
  auto tstart = steady_clock::now();
  ASSERT_EQ(0, writer->append(LOG_ID, std::string(payload), NULL_CONTEXT));
  // Wait ... Write should get flushed 100ms thanks to the time trigger
  cb.sem.wait();
  ASSERT_EQ(1, cb.payloadsSucceededAsMultiset().size());
  ASSERT_EQ(0, cb.failures.size());
  // Assert that 100ms elapsed
  auto elapsed_ms = duration_cast<milliseconds>(cb.last_time - tstart).count();
  ld_info("%ld ms elapsed before write completed", elapsed_ms);
  ASSERT_GE(elapsed_ms, opts.time_trigger.count());
}

TEST_F(BufferedWriterTest, Retry) {
  using namespace std::chrono;
  TestCallback cb;
  BufferedWriter::Options opts;
  opts.mode = BufferedWriter::Options::Mode::INDEPENDENT;
  opts.retry_count = 3;
  opts.retry_initial_delay = milliseconds(50);
  auto writer = this->createWriter(&cb, opts);

  const int FAIL_COUNT = 2;

  // Inject some failures
  int injected_failures_todo = FAIL_COUNT;
  sink_->pre_append_callback_ = [&]() { return injected_failures_todo-- <= 0; };

  const std::string payload(100, 'a');
  auto tstart = steady_clock::now();
  ASSERT_EQ(0, writer->append(logid_t(1), std::string(payload), NULL_CONTEXT));
  ASSERT_EQ(0, writer->flushAll());
  // Wait ... The first two attempts at sending out the write should fail,
  // scheduling retries in ~50 ms and ~100 ms but the third should succeed.
  cb.sem.wait();
  ASSERT_EQ(1, cb.payloadsSucceededAsMultiset().size());
  ASSERT_EQ(0, cb.failures.size());
  // Assert that >= 0.5*150ms elapsed (x0.5 because of the fuzz in scheduling
  // retries)
  auto elapsed_ms = duration_cast<milliseconds>(cb.last_time - tstart).count();
  ld_info("%ld ms elapsed before write succeeded", elapsed_ms);
  ASSERT_GE(elapsed_ms, 75);
  ASSERT_EQ(FAIL_COUNT, cb.nretries);
}

TEST_F(BufferedWriterTest, RetryAllowDeny) {
  using namespace std::chrono;
  TestCallback cb;
  cb.retry_fn_ = [&]() {
    // Allow one retry, block others
    using RetryDecision = BufferedWriter::AppendCallback::RetryDecision;
    return cb.nretries <= 1 ? RetryDecision::ALLOW : RetryDecision::DENY;
  };

  BufferedWriter::Options opts;
  opts.mode = BufferedWriter::Options::Mode::INDEPENDENT;
  opts.retry_count = 100;
  opts.retry_initial_delay = milliseconds(50);
  auto writer = this->createWriter(&cb, opts);
  // Fail all write attempts
  sink_->pre_append_callback_ = []() { return false; };

  ASSERT_EQ(0, writer->append(logid_t(1), std::string(100, 'a'), NULL_CONTEXT));
  ASSERT_EQ(0, writer->flushAll());
  cb.sem.wait();

  // Retry callback ought to have been called twice and then the write ought
  // to have failed.
  ASSERT_EQ(2, cb.nretries);
  ASSERT_EQ(1, cb.failures.size());
  ASSERT_EQ(0, cb.payloadsSucceededAsMultiset().size());
}

// Test Options::memory_limit_mb.
TEST_F(BufferedWriterTest, MemoryLimit) {
  // The test setup will write into a healthy number of logs to avoid hitting
  // the payload size limit and automatically flushing.  This will also test
  // that we return the correct amount of memory when batches for the same log
  // finish.
  const int NLOGS = 20;

  TestCallback cb;
  BufferedWriter::Options opts;
  opts.memory_limit_mb = 1;
  auto writer = this->createWriter(&cb, opts);
  ASSERT_EQ(1 << 20, this->getMemoryAvailable(*writer));

  std::mt19937_64 rnd(0xbabadeda);
  std::uniform_int_distribution<int> log_dist(1, NLOGS);
  auto select_log = [&]() { return logid_t(log_dist(rnd)); };

  // Use a 1 KB payload.
  const std::string pay(1024, 'a');

  int attempted = 0, buffered = 0;
  for (int i = 0; attempted < 2000; ++i) {
    if (i % 2 == 0) {
      // Use the single-write append() some of the time ...
      ++attempted;
      int rv = writer->append(select_log(), std::string(pay), NULL_CONTEXT);
      buffered += rv == 0;
    } else {
      // ... and multi-write some of the time.
      const int BATCH_SIZE = 2;
      attempted += BATCH_SIZE;
      std::vector<BufferedWriter::Append> v;
      while (v.size() < BATCH_SIZE) {
        AppendAttributes attrs;
        attrs.optional_keys[KeyType::FINDKEY] = std::string("12345678");
        v.push_back(BufferedWriter::Append(
            select_log(), std::string(pay), NULL_CONTEXT, std::move(attrs)));
      }
      std::vector<Status> rv = writer->append(std::move(v));
      if (rv == std::vector<Status>(v.size(), E::OK)) {
        buffered += BATCH_SIZE;
      } else {
        ASSERT_EQ(std::vector<Status>(v.size(), E::NOBUFS), rv);
      }
    }
  }

  // With a memory limit of 1 MB, we expect to fit around 500 writes into the
  // memory limit because BufferedWriterImpl::memoryForPayloadBytes() budgets
  // 2x.
  ld_info("BufferedWriter accepted %d out of %d attempted writes",
          buffered,
          attempted);
  ASSERT_GT(buffered, 480);
  ASSERT_LT(buffered, 520);
  ASSERT_LT(getMemoryAvailable(*writer), 2 * pay.size());

  // Now flush all writes, wait for them to complete, and verify that the
  // memory available counter goes back up to 1 MB.
  ASSERT_EQ(0, writer->flushAll());
  for (int i = 0; i < buffered; ++i) {
    cb.sem.wait();
  }
  wait_until("BufferedWriter has released all memory",
             [&]() { return this->getMemoryAvailable(*writer) == 1 << 20; });
}

// When BufferedWriter shuts down with records buffered and not dispatched, it
// should invoke the failure callback with E::SHUTDOWN.
TEST_F(BufferedWriterTest, Shutdown) {
  TestCallback cb;
  auto writer = this->createWriter(&cb);
  const logid_t LOG_ID(1);

  std::multiset<Status> expected_failures;
  for (int i = 0; i < 10; ++i) {
    int rv = writer->append(LOG_ID, std::to_string(i), NULL_CONTEXT);
    ASSERT_EQ(0, rv);
    expected_failures.insert(E::SHUTDOWN);
  }
  writer.reset();

  ASSERT_EQ(expected_failures, cb.failures);
}

// Test that the ONE_AT_A_TIME provides the ordering guarantee it promises.
// In particular, if we send writes 123 and 1 fails, 23 should not be written
// until 1 has succeeded.
TEST_F(BufferedWriterTest, OneAtATimeWithFailure) {
  using namespace std::chrono;
  TestCallback cb;
  BufferedWriter::Options opts;
  opts.mode = BufferedWriter::Options::Mode::ONE_AT_A_TIME;
  opts.retry_count = -1;
  opts.retry_initial_delay = milliseconds(50);
  auto writer = this->createWriter(&cb, opts);

  // Fail once then succeed
  std::atomic<int> injected_failures_todo(1);
  sink_->pre_append_callback_ = [&]() { return injected_failures_todo-- <= 0; };

  ASSERT_EQ(0, writer->append(logid_t(1), std::string("1"), NULL_CONTEXT));
  ASSERT_EQ(0, writer->flushAll());
  ASSERT_EQ(0, writer->append(logid_t(1), std::string("2"), NULL_CONTEXT));
  ASSERT_EQ(0, writer->append(logid_t(1), std::string("3"), NULL_CONTEXT));
  ASSERT_EQ(0, writer->flushAll());

  for (int i = 0; i < 3; ++i) {
    cb.sem.wait();
  }
  std::vector<std::string> expected{"1", "2", "3"};
  ASSERT_EQ(expected, cb.payloads_succeeded);
}

// Test that buffered writer fails with the error code that checkShard() returns
TEST_F(BufferedWriterTest, CheckShard) {
  TestCallback cb;
  BufferedWriter::Options opts;
  auto writer = this->createWriter(&cb, opts);
  sink_->check_worker_result_ = E::SEQNOBUFS;
  ASSERT_EQ(-1, writer->append(logid_t(1), std::string("1"), NULL_CONTEXT));
  ASSERT_EQ(E::SEQNOBUFS, err);

  std::vector<BufferedWriter::Append> v;
  for (int i = 0; i < 10; ++i) {
    AppendAttributes attrs;
    attrs.optional_keys[KeyType::FINDKEY] = std::string("12345678");
    v.push_back(BufferedWriter::Append(
        logid_t(1), std::string("abc"), NULL_CONTEXT, std::move(attrs)));
  }
  std::vector<Status> rv = writer->append(std::move(v));
  ASSERT_EQ(std::vector<Status>(v.size(), E::SEQNOBUFS), rv);
}

// Test that we delete payloads if instructed to do so
TEST_F(BufferedWriterTest, DestroyPayloads) {
  TestCallback cb;
  BufferedWriter::Options opts;
  opts.destroy_payloads = true;
  auto writer = this->createWriter(&cb, opts);
  ASSERT_EQ(0, writer->append(logid_t(1), std::string("1"), NULL_CONTEXT));
  ASSERT_EQ(0, writer->flushAll());
  cb.sem.wait();
  ASSERT_EQ(1, cb.payloadsSucceededAsMultiset().size());
  ASSERT_EQ(0, cb.failures.size());
  std::vector<std::string> expected{""};
  ASSERT_EQ(expected, cb.payloads_succeeded);
}

// Test that accounting in buffered writer works correctly
TEST_F(BufferedWriterTest, Accounting) {
  using namespace std::chrono;
  TestCallback cb;
  BufferedWriter::Options opts;
  opts.mode = BufferedWriter::Options::Mode::ONE_AT_A_TIME;
  opts.time_trigger = milliseconds(50);
  opts.retry_count = -1;
  opts.retry_initial_delay = milliseconds(50);
  opts.destroy_payloads = true;
  auto writer = this->createWriter(&cb, opts);

  ASSERT_EQ(0, writer->append(logid_t(1), std::string("1"), NULL_CONTEXT));
  ASSERT_EQ(0, writer->flushAll());
  ASSERT_EQ(0, writer->append(logid_t(1), std::string("2"), NULL_CONTEXT));
  ASSERT_EQ(0, writer->append(logid_t(1), std::string("3"), NULL_CONTEXT));
  ASSERT_EQ(0, writer->flushAll());

  size_t total_payload_size = 3;

  std::vector<BufferedWriter::Append> v;
  for (int i = 0; i < 10; ++i) {
    total_payload_size += 3;
    AppendAttributes attrs;
    attrs.optional_keys[KeyType::FINDKEY] = std::string("12345678");
    v.push_back(BufferedWriter::Append(
        logid_t(1), std::string("abc"), NULL_CONTEXT, std::move(attrs)));
  }
  std::vector<Status> rv = writer->append(std::move(v));
  ASSERT_EQ(std::vector<Status>(v.size(), E::OK), rv);

  for (int i = 0; i < 13; ++i) {
    cb.sem.wait();
  }
  ASSERT_EQ(total_payload_size, sink_->bytes_sent_to_shard_);
  ASSERT_EQ(total_payload_size, sink_->bytes_freed_by_shard_);
  std::vector<std::string> expected(13, "");

  ASSERT_EQ(expected, cb.payloads_succeeded);
}

TEST_F(BufferedWriterTest, InitialDelayGreaterThanMaxDelay) {
  TestCallback cb;
  BufferedWriter::Options opts;
  opts.retry_initial_delay = std::chrono::milliseconds(20);
  opts.retry_max_delay = std::chrono::milliseconds(10);
  opts.retry_count = 2;
  auto writer = this->createWriter(&cb, opts);

  int injected_failures_todo = 1;
  sink_->pre_append_callback_ = [&]() { return injected_failures_todo-- <= 0; };

  ASSERT_EQ(0, writer->append(logid_t(1), "a", NULL_CONTEXT));
  ASSERT_EQ(0, writer->flushAll());
  cb.sem.wait();

  ASSERT_EQ(1, cb.nretries);
  std::vector<std::string> expected = {"a"};
  ASSERT_EQ(expected, cb.payloads_succeeded);
}
