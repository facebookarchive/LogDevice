/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstring>
#include <iostream>
#include <iterator>
#include <memory>
#include <mutex>
#include <queue>
#include <thread>
#include <unordered_set>
#include <utility>
#include <vector>

#include <folly/Function.h>
#include <folly/TokenBucket.h>

#include "logdevice/common/debug.h"
#include "logdevice/include/Client.h"
#include "logdevice/include/Err.h"
#include "logdevice/include/Record.h"
#include "logdevice/include/types.h"
#include "logdevice/test/ldbench/worker/Options.h"
#include "logdevice/test/ldbench/worker/SteadyWriteRateWorker.h"
#include "logdevice/test/ldbench/worker/WorkerRegistry.h"

namespace facebook { namespace logdevice { namespace ldbench {
namespace {

static constexpr const char* BENCH_NAME = "backfill";

/**
 * Backfill benchmark worker.
 *
 * Performs async appends (i.e. writes) at a steady rate and periodically reads
 * back everything written, similar to Scribe's streaming copier. Every second,
 * records the number of pending reads for the current backfill iteration, and
 * prints a burn-chart at the end of the experiment.
 */
class BackfillWorker final : public SteadyWriteRateWorker {
 public:
  using SteadyWriteRateWorker::SteadyWriteRateWorker;
  ~BackfillWorker() override;
  int run() override;

 private:
  class PretendDataRecord;
  using RecordIdSet = std::unordered_set<std::pair<logid_t::raw_type, lsn_t>>;

  static constexpr auto PENDING_READS_MONITOR_INTERVAL =
      std::chrono::seconds(1);

  // Background thread that performs appends until end_time_.
  void appenderMain(append_callback_t append_cb, LogIdDist log_id_dist);

  // Background thread which periodically records the number of pending reads.
  void pendingReadsMonitorMain();

  void printResult();
  void appendCallback(Status status, const DataRecord& record);
  void recordCallback(const DataRecord& record);
  bool startReading(const LogToLsnMap& from_lsns,
                    RecordCallback record_cb,
                    GapCallback gap_cb = nullptr,
                    const LogToLsnMap* until_lsns = nullptr) override;
  bool stopReading() override;
  void handleAppendSuccess(Payload payload) override;
  void handleAppendError(Status st, Payload payload) override;
  size_t getNumPendingXacts() const noexcept override;
  bool error() const noexcept override;

  uint64_t npending_appends_ = 0;
  std::atomic<bool> error_{false};
  std::atomic<bool> reading_{false};
  RecordIdSet next_pending_reads_;
  RecordIdSet pending_reads_;
  LogToLsnMap from_lsns_;
  LogToLsnMap tail_lsns_;
  std::unique_ptr<std::thread> appender_thread_;
  std::unique_ptr<std::thread> pending_reads_monitor_thread_;
  std::vector<size_t> pending_reads_history_;
  std::unique_ptr<std::thread> pretend_reader_thread_;
  std::queue<std::unique_ptr<DataRecord>> next_pretend_read_queue_;
  std::queue<std::unique_ptr<DataRecord>> pretend_read_queue_;
};

/**
 * Data records for use in pretend mode. Just a thin wrapper class that
 * performs a deep copy of the payload in its constructor and frees that
 * payload in its destructor.
 */
class BackfillWorker::PretendDataRecord final : public DataRecord {
 public:
  explicit PretendDataRecord(const DataRecord& record);
  ~PretendDataRecord() override;
};

constexpr std::chrono::seconds BackfillWorker::PENDING_READS_MONITOR_INTERVAL;

void BackfillWorker::appenderMain(append_callback_t append_cb,
                                  LogIdDist log_id_dist) {
  std::unique_lock<std::recursive_mutex> lock(mutex_);
  while (!tryAppend(lock, log_id_dist, append_cb)) {
    /* keep going */
  }
}

void BackfillWorker::pendingReadsMonitorMain() {
  auto wakeup_time = Clock::now();
  while (wakeup_time < end_time_) {
    {
      std::unique_lock<std::recursive_mutex> lock(mutex_);
      pending_reads_history_.push_back(pending_reads_.size());
    }
    wakeup_time = wakeup_time + PENDING_READS_MONITOR_INTERVAL;
    if (wakeup_time < end_time_) {
      /* sleep override */ std::this_thread::sleep_until(wakeup_time);
    }
  }
};

BackfillWorker::~BackfillWorker() {
  if (appender_thread_ && appender_thread_->joinable()) {
    appender_thread_->join();
  }
  if (pending_reads_monitor_thread_ &&
      pending_reads_monitor_thread_->joinable()) {
    pending_reads_monitor_thread_->join();
  }
  if (pretend_reader_thread_ && pretend_reader_thread_->joinable()) {
    pretend_reader_thread_->join();
  }
  // Make sure no callbacks are called after this subclass is destroyed.
  destroyClient();
}

int BackfillWorker::run() {
  // Create random log id distribution function. Each worker is assigned a
  // partition of the log range. The partition is determined by the worker
  // index passed via the worker_id option. Within its partition, each worker
  // picks a log uniformly at random.
  std::vector<logid_t> all_logs;
  if (getLogs(all_logs)) {
    return 1;
  }
  auto logs = getLogsPartition(all_logs);
  auto log_id_dist = getLogIdDist(logs);

  // Keep per-log throughput independent of number of workers.
  write_rate_ = (log_id_dist != nullptr)
      ? double(options.write_rate) * logs.size() / all_logs.size()
      : 0.0;

  // Clear append token bucket.
  token_bucket_.reset(token_bucket_.defaultClockNow());

  // Callback for use with Client::append.
  append_callback_t append_cb([this](Status status, const DataRecord& record) {
    appendCallback(status, record);
  });

  std::unique_lock<std::recursive_mutex> lock(mutex_);

  // Warm up. Perform appends at steady write rate until
  // options.warmup_duration has passed. This is to make sure the servers are
  // in a steady state when we commence the actual benchmark.
  ld_info(
      "Performing warm-up for %" PRIu64 " seconds", options.warmup_duration);
  end_time_ = Clock::now() + std::chrono::seconds(options.warmup_duration);
  while (!tryAppend(lock, log_id_dist, append_cb)) {
    /* keep going */
  }
  ld_info("Warm-up complete");

  // Clear state after warmup.
  from_lsns_.clear();
  tail_lsns_.clear();
  next_pending_reads_.clear();
  decltype(next_pretend_read_queue_)().swap(next_pretend_read_queue_);

  // Actual benchmark.
  ld_info("Performing backfill benchmark for %" PRIu64 " seconds",
          options.duration);

  // Perform appends in background thread until options.duration has passed.
  end_time_ = Clock::now() + std::chrono::seconds(options.duration);
  appender_thread_ = std::make_unique<std::thread>(
      &BackfillWorker::appenderMain, this, append_cb, log_id_dist);

  // After every options.backfill_interval, for each log, read from lowest
  // appended LSN (from_lsns_) to highest appended LSN (tail_lsns_).
  auto wakeup_time = Clock::now();
  for (;;) {
    // Steady wait for options.backfill_interval.
    wakeup_time =
        std::min(end_time_,
                 wakeup_time + std::chrono::seconds(options.backfill_interval));
    cond_var_.wait_until(
        lock, wakeup_time, [this] { return errorOrStopped(); });

    // Stop backfill reads from previous iteration. They should be done
    // already, but it is possible that we are writing at a higher rate than
    // we can read if the cluster is overloaded.
    if (reading_) {
      lock.unlock();
      if (stopReading()) {
        return 1;
      }
      lock.lock();
    }
    ld_check(!reading_);

    // Check for end of benchmark.
    if (errorOrStopped() || Clock::now() > end_time_) {
      break;
    }

    // The next pending reads become the current pending reads.
    ld_check(!options.pretend ||
             next_pretend_read_queue_.size() == next_pending_reads_.size());
    pending_reads_ = std::move(next_pending_reads_);
    ld_check(next_pending_reads_.empty());
    pretend_read_queue_ = std::move(next_pretend_read_queue_);
    ld_check(next_pretend_read_queue_.empty());

    // Start another round of backfill reads.
    ld_info("Reading %" PRIu64 " records", pending_reads_.size());
    RecordCallback record_cb([this](std::unique_ptr<DataRecord>& record) {
      recordCallback(*record);
      record.reset();
      return true;
    });
    if (startReading(from_lsns_, std::move(record_cb), nullptr, &tail_lsns_)) {
      return 1;
    }
    ld_check(reading_);

    // Clear from LSNs (otherwise we would read records multiple times).
    from_lsns_.clear();

    // Lazily create pending read monitor thread.
    if (pending_reads_monitor_thread_ == nullptr) {
      pending_reads_monitor_thread_ = std::make_unique<std::thread>(
          &BackfillWorker::pendingReadsMonitorMain, this);
    }
  }

  // Join appender thread and pending read monitor thread. Results have been
  // recorded in pending_reads_history_.
  lock.unlock();
  appender_thread_->join();
  appender_thread_.reset();
  if (pending_reads_monitor_thread_ != nullptr) {
    pending_reads_monitor_thread_->join();
    pending_reads_monitor_thread_.reset();
  }
  lock.lock();
  ld_check(!reading_);
  ld_info("Benchmark complete");

  // Cool down. Perform appends at steady write rate until
  // options.cooldown_duration has passed. The purpose of this is to guarantee
  // a stable result if concurrent workers have started at slightly different
  // times.
  ld_info("Performing cool-down for %" PRIu64 " seconds",
          options.cooldown_duration);
  end_time_ = Clock::now() + std::chrono::seconds(options.cooldown_duration);
  while (!tryAppend(lock, log_id_dist, append_cb)) {
    /* keep going */
  }
  ld_info("Cool-down complete");

  // Wait for pending appends.
  ld_info("Waiting for pending appends: npending_appends=%" PRIu64,
          npending_appends_);
  cond_var_.wait(lock, [this] { return npending_appends_ == 0; });
  ld_info("All done");

  if (!error()) {
    // All is good.
    printResult();
    return 0;
  } else {
    return 1;
  }
}

void BackfillWorker::printResult() {
  for (auto npending_reads : pending_reads_history_) {
    std::cout << npending_reads << '\n';
  }
}

void BackfillWorker::appendCallback(Status status, const DataRecord& record) {
  std::unique_lock<std::recursive_mutex> lock(mutex_);

  // Decrement pending append counter.
  --npending_appends_;

  if (status == E::OK) {
    // Successful append. Add to next pending reads.
    auto log_id = record.logid.val();
    auto lsn = record.attrs.lsn;
    next_pending_reads_.emplace(log_id, lsn);

    // The min LSN we successfully append for a given log id points to the
    // record where we will start the backfill reads in the next benchmark
    // iteration.
    auto it = from_lsns_.find(log_id);
    if (it == from_lsns_.end()) {
      from_lsns_.emplace_hint(it, log_id, lsn);
    } else {
      it->second = std::min(it->second, lsn);
    }

    // Conversely, the max LSN we successfully append for a given log id points
    // to the record where we will end the backfill reads.
    it = tail_lsns_.find(log_id);
    if (it == tail_lsns_.end()) {
      tail_lsns_.emplace_hint(it, log_id, lsn);
    } else {
      it->second = std::max(it->second, lsn);
    }

    ld_check(from_lsns_[log_id] <= tail_lsns_[log_id]);

    if (options.pretend) {
      // Pretend mode. Make a deep copy of the record and put it into the
      // next_pretend_read_queue_ for the pretend reader thread during the
      // next backfill iteration.
      next_pretend_read_queue_.push(
          std::make_unique<PretendDataRecord>(record));
      ld_check(next_pretend_read_queue_.size() == next_pending_reads_.size());
    }
  } else {
    handleAppendError(status, record.payload);
  }

  lock.unlock();

  // Notify appender thread or main thread which may be waiting on
  // npending_appends_.
  cond_var_.notify_all();
}

void BackfillWorker::recordCallback(const DataRecord& record) {
  std::unique_lock<std::recursive_mutex> lock(mutex_);
  pending_reads_.erase(std::make_pair(record.logid.val(), record.attrs.lsn));
}

bool BackfillWorker::startReading(const LogToLsnMap& from_lsns,
                                  RecordCallback record_cb,
                                  GapCallback gap_cb,
                                  const LogToLsnMap* until_lsns) {
  ld_check(!reading_);
  ld_check(until_lsns != nullptr);

  reading_ = true;

  if (options.pretend) {
    // Start pretend reader thread. We read at double the write rate in order
    // to catch up in half the backfill interval.
    double read_rate = write_rate_ * 2;
    pretend_reader_thread_ = std::make_unique<std::thread>([this, read_rate] {
      if (read_rate <= 0) {
        return;
      }

      // Token bucket for performing reads at a steady rate.
      folly::TokenBucket read_bucket(
          read_rate, read_rate * 10, folly::TokenBucket::defaultClockNow());
      // Exit thread if reading_ becomes false (set by stopReading()), or if
      // out of records to read.
      std::unique_lock<std::recursive_mutex> lock(mutex_);
      while (reading_ && !pretend_read_queue_.empty()) {
        // Sleep to ensure steady read rate.
        const double missing_tokens = 1.0 - read_bucket.available();
        if (missing_tokens > 0) {
          const double missing_sec = missing_tokens / read_rate;
          auto now = Clock::now();
          auto wakeup_time = std::min(
              now +
                  std::chrono::nanoseconds(uint64_t(missing_sec * 1000000000)),
              end_time_);
          lock.unlock();
          /* sleep override */ std::this_thread::sleep_until(wakeup_time);
          lock.lock();
        }
        read_bucket.consumeOrDrain(1);

        if (!reading_ || pretend_read_queue_.empty()) {
          break;
        }

        // Call read callback with next record in pretend read queue.
        auto record = std::move(pretend_read_queue_.front());
        pretend_read_queue_.pop();
        recordCallback(*record);
      }
    });
    return false;
  } else {
    return SteadyWriteRateWorker::startReading(
        from_lsns, std::move(record_cb), std::move(gap_cb), until_lsns);
  }
}

bool BackfillWorker::stopReading() {
  ld_check(reading_);
  reading_ = false;
  if (options.pretend) {
    ld_check(pretend_reader_thread_ != nullptr);
    // Stop pretend reader thread.
    pretend_reader_thread_->join();
    pretend_reader_thread_.reset();
    return false;
  } else {
    return SteadyWriteRateWorker::stopReading();
  }
}

void BackfillWorker::handleAppendSuccess(Payload) {
  // mutex_ is held by caller.
  ++npending_appends_;
}

void BackfillWorker::handleAppendError(Status st, Payload) {
  // Log error. Set error_ flag in case of serious error.
  switch (st) {
    case E::AGAIN:
    case E::NOBUFS:
    case E::OVERLOADED:
    case E::PENDING_FULL:
    case E::SEQNOBUFS:
    case E::SEQSYSLIMIT:
    case E::SYSLIMIT:
    case E::TEMPLIMIT:
    case E::TIMEDOUT:
      RATELIMIT_WARNING(std::chrono::seconds(1),
                        1,
                        "Append failed, probably too much load: %s (%s)",
                        error_name(st),
                        error_description(st));
      break;
    default:
      error_ = true;
      RATELIMIT_ERROR(std::chrono::seconds(10),
                      1,
                      "Append failed, serious error: %s (%s)",
                      error_name(st),
                      error_description(st));
  }
}

size_t BackfillWorker::getNumPendingXacts() const noexcept {
  return npending_appends_;
}

bool BackfillWorker::error() const noexcept {
  return !options.ignore_errors && error_;
}

BackfillWorker::PretendDataRecord::PretendDataRecord(const DataRecord& record)
    : DataRecord(record.logid,
                 record.payload.dup(),
                 record.attrs.lsn,
                 record.attrs.timestamp,
                 record.attrs.batch_offset,
                 record.attrs.offsets) {}

BackfillWorker::PretendDataRecord::~PretendDataRecord() {
  std::free(const_cast<void*>(payload.data()));
}

} // namespace

void registerBackfillWorker() {
  registerWorkerImpl(BENCH_NAME,
                     []() -> std::unique_ptr<Worker> {
                       return std::make_unique<BackfillWorker>();
                     },
                     OptionsRestrictions({"pretend",
                                          "max-window",
                                          "warmup-duration",
                                          "duration",
                                          "cooldown-duration",
                                          "backfill-interval",
                                          "payload-size",
                                          "write-rate"},
                                         {PartitioningMode::LOG}));
}

}}} // namespace facebook::logdevice::ldbench
