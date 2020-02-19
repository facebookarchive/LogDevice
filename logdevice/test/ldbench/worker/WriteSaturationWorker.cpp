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
#include <cinttypes>
#include <condition_variable>
#include <cstdint>
#include <cstdlib>
#include <iostream>
#include <memory>
#include <mutex>
#include <random>
#include <thread>

#include <folly/Optional.h>
#include <folly/Random.h>

#include "logdevice/common/debug.h"
#include "logdevice/include/Err.h"
#include "logdevice/include/Record.h"
#include "logdevice/include/types.h"
#include "logdevice/test/ldbench/worker/Options.h"
#include "logdevice/test/ldbench/worker/Worker.h"
#include "logdevice/test/ldbench/worker/WorkerRegistry.h"

namespace facebook { namespace logdevice { namespace ldbench {
namespace {

static constexpr const char* BENCH_NAME = "write_saturation";

/**
 * Write saturation benchmark worker.
 *
 * Attempts to saturate a set of logs with async appends (i.e. writes).
 * Writes are uniformly randomly distributed across the set of logs. To
 * avoid thrashing, a dynamically sized window of pending writes is maintained,
 * using an additive increase multiplicative decrease (AIMD) policy. This
 * application-side window is in addition to any windows maintained by
 * LogDevice.
 */
class WriteSaturationWorker final : public Worker {
 public:
  using Worker::Worker;
  ~WriteSaturationWorker() override;
  int run() override;

 private:
  bool tryAppend(std::unique_lock<std::mutex>& lock,
                 const std::vector<logid_t>& logs,
                 const append_callback_t& append_cb);
  using Worker::tryAppend;
  void appendCallback(Status status, const DataRecord& record);
  void handleAppendError(Status status);
  inline bool error() const noexcept;
  inline bool errorOrStopped() const noexcept;

  std::mutex mutex_;
  std::condition_variable cond_var_;
  uint64_t nsuccess_ = 0;
  uint64_t npushbacks_ = 0;
  uint64_t nerrors_ = 0;
  uint64_t npending_ = 0;
  uint64_t window_ = 0;
  std::atomic<uint64_t> nwaiting_{0};
  std::chrono::steady_clock::time_point start_time_;
  folly::Optional<std::chrono::steady_clock::time_point> end_time_;
};

WriteSaturationWorker::~WriteSaturationWorker() {
  // Make sure no callbacks are called after this subclass is destroyed.
  destroyClient();
}

bool WriteSaturationWorker::tryAppend(std::unique_lock<std::mutex>& lock,
                                      const std::vector<logid_t>& logs,
                                      const append_callback_t& append_cb) {
  // Wait for fatal error, stop request, or window to open. We need to wake up
  // every second to poll for isStopped() in errorOrStopped(), because stop()
  // is an async-safe function. It cannot signal the condition variable.
  ld_check(window_ > 0);
  ld_debug("Waiting for open window: npending=%" PRIu64 ", window=%" PRIu64,
           npending_,
           window_);
  for (;;) {
    auto now_plus_one_sec =
        std::chrono::steady_clock::now() + std::chrono::seconds(1);
    auto wakeup_time = end_time_.hasValue()
        ? std::min(now_plus_one_sec, end_time_.value())
        : now_plus_one_sec;
    cond_var_.wait_until(lock, wakeup_time, [this] {
      return errorOrStopped() || (npending_ < window_);
    });
    if (errorOrStopped() ||
        (end_time_.hasValue() &&
         std::chrono::steady_clock::now() >= end_time_.value())) {
      // Fatal error, stop requested, or end of benchmark.
      return true;
    } else if (npending_ < window_) {
      // Window open. Proceed.
      break;
    }
  }

  // Pick random log id from uniform distribution over list of logs.
  ld_check(!logs.empty());
  logid_t log_id = logs[folly::Random::rand32() % logs.size()];

  // Try to append.
  ld_debug("Performing append: npending=%" PRIu64 ", window=%" PRIu64,
           npending_,
           window_);
  if (!tryAppend(log_id, generatePayload(), append_cb)) {
    // Success (so far). Increase pending counter.
    ++npending_;
  } else {
    // Some error occurred, handle it.
    handleAppendError(err);
  }

  if (options.pretend) {
    // Pretend mode. Sleep to throttle append rate. Release locks to give
    // callbacks a chance to execute.
    lock.unlock();
    /* sleep override */
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    lock.lock();
  } else if (nwaiting_ > 0) {
    // Yield to give waiting callbacks a chance to execute.
    lock.unlock();
    std::this_thread::yield();
    lock.lock();
  }

  // Come again.
  return false;
}

int WriteSaturationWorker::run() {
  std::vector<logid_t> logs;
  if (getLogs(logs)) {
    return 1;
  }
  if (logs.empty()) {
    ld_error("No logs.");
    return 1;
  }

  // Callback for use with Client::append. Created once for efficiency.
  // weak_ptr ensures the callback won't crash if worker is destroyed
  // concurrently.
  append_callback_t cb([this](Status status, const DataRecord& record) {
    appendCallback(status, record);
  });

  // Warm up. Perform appends until warmup_duration has passed. The purpose of
  // this is to make sure the servers are in a steady state when we commence
  // the actual benchmark.
  ld_info(
      "Performing warm-up for %" PRIu64 " seconds", options.warmup_duration);
  std::unique_lock<std::mutex> lock(mutex_);
  window_ = std::max<uint64_t>(options.init_window, 1);
  end_time_ = std::chrono::steady_clock::now() +
      std::chrono::seconds(options.warmup_duration);
  while (!tryAppend(lock, logs, cb)) {
    /* keep going */
  }
  ld_info("Warm-up complete: nsuccess=%" PRIu64 ", npushbacks=%" PRIu64,
          nsuccess_,
          npushbacks_);

  // Actual benchmark. Perform appends until duration has passed.
  ld_info("Performing write saturation benchmark for %" PRIi64 " seconds",
          options.duration);
  npushbacks_ = nsuccess_ = 0;
  start_time_ = std::chrono::steady_clock::now();
  if (options.duration >= 0) {
    end_time_ = start_time_ + std::chrono::seconds(options.duration);
  } else {
    end_time_.reset();
  }
  while (!tryAppend(lock, logs, cb)) {
    /* keep going */
  }
  auto actual_end_time = std::chrono::steady_clock::now();
  ld_info("Benchmark complete: nsuccess=%" PRIu64 ", npushbacks=%" PRIu64,
          nsuccess_,
          npushbacks_);

  if (!error()) {
    // Print result.
    auto actual_duration_ms =
        std::chrono::duration_cast<std::chrono::milliseconds>(actual_end_time -
                                                              start_time_)
            .count();
    std::cout << actual_duration_ms << ' ' << nsuccess_ << ' ' << npushbacks_
              << ' ' << nerrors_ << '\n';
  }

  // Cool down. Perform appends until cooldown_duration has passed. The purpose
  // of this is to guarantee a stable result if concurrent workers have started
  // at slightly different times.
  ld_info("Performing cool-down for %" PRIu64 " seconds",
          options.cooldown_duration);
  npushbacks_ = nsuccess_ = 0;
  end_time_ = std::chrono::steady_clock::now() +
      std::chrono::seconds(options.cooldown_duration);
  while (!tryAppend(lock, logs, cb)) {
    /* keep going */
  }
  ld_info("Cool-down complete: nsuccess=%" PRIu64 ", npushbacks=%" PRIu64,
          nsuccess_,
          npushbacks_);

  // Wait for pending appends (otherwise the callbacks may crash).
  ld_info("Waiting for pending appends: npending=%" PRIu64, npending_);
  cond_var_.wait(lock, [this] { return npending_ == 0; });
  ld_info("All done");

  // Return 1 if fatal error occurred, 0 otherwise.
  return error();
}

void WriteSaturationWorker::appendCallback(Status st, const DataRecord&) {
  ++nwaiting_;
  std::unique_lock<std::mutex> lock(mutex_);
  --nwaiting_;

  // Decrease pending counter.
  --npending_;

  if (st == E::OK) {
    // Success. Increase success counter and increase window size (up to max).
    ++nsuccess_;
    uint64_t new_window = std::min<uint64_t>(window_ + 1, options.max_window);
    ld_debug("Append success: nsuccess=%" PRIu64 ", old window=%" PRIu64
             ", new window=%" PRIu64,
             nsuccess_,
             window_,
             new_window);
    window_ = new_window;
  } else {
    // Some error occurred, handle it.
    handleAppendError(st);
  }
  lock.unlock();

  // Notify main thread in tryAppend() or run().
  cond_var_.notify_one();
}

void WriteSaturationWorker::handleAppendError(Status status) {
  switch (status) {
    case E::AGAIN:
    case E::NOBUFS:
    case E::OVERLOADED:
    case E::PENDING_FULL:
    case E::SEQNOBUFS:
    case E::SEQSYSLIMIT:
    case E::SYSLIMIT:
    case E::TEMPLIMIT:
    case E::TIMEDOUT:
      // Load-related error. Increase pushback counter.
      RATELIMIT_DEBUG(std::chrono::seconds(1),
                      1,
                      "Append failed: %s (%s), nsuccess=%" PRIu64 ", "
                      "npushbacks=%" PRIu64 " window=%" PRIu64,
                      error_name(status),
                      error_description(status),
                      nsuccess_,
                      npushbacks_,
                      window_);
      ++npushbacks_;
      if (npending_ < window_) {
        // Decrease window size multiplicatively, to avoid thrashing.
        uint64_t new_window = std::max<uint64_t>(window_ / 2, 1);
        ld_debug("Decreasing window size: npending=%" PRIu64
                 ", old window=%" PRIu64 ", new window=%" PRIu64,
                 npending_,
                 window_,
                 new_window);
        window_ = new_window;
      }
      break;
    default:
      // Serious error occurred. Increase error counter and log error.
      ++nerrors_;
      RATELIMIT_ERROR(std::chrono::seconds(10),
                      1,
                      "Unexpected append error: %s (%s), nerrors=%" PRIu64,
                      error_name(status),
                      error_description(status),
                      nerrors_);
  }
}

bool WriteSaturationWorker::error() const noexcept {
  return !options.ignore_errors && (nerrors_ > 0);
}

bool WriteSaturationWorker::errorOrStopped() const noexcept {
  return error() || isStopped();
}

} // namespace

void registerWriteSaturationWorker() {
  registerWorkerImpl(BENCH_NAME,
                     []() -> std::unique_ptr<Worker> {
                       return std::make_unique<WriteSaturationWorker>();
                     },
                     OptionsRestrictions({"pretend",
                                          "init-window",
                                          "max-window",
                                          "warmup-duration",
                                          "duration",
                                          "cooldown-duration",
                                          "payload-size"},
                                         {PartitioningMode::DEFAULT}));
}

}}} // namespace facebook::logdevice::ldbench
