/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "logdevice/test/ldbench/worker/SteadyWriteRateWorker.h"

#include <thread>

#include "logdevice/common/debug.h"

namespace facebook { namespace logdevice { namespace ldbench {

SteadyWriteRateWorker::~SteadyWriteRateWorker() = default;

bool SteadyWriteRateWorker::tryAppend(
    std::unique_lock<std::recursive_mutex>& lock,
    LogIdDist& log_id_dist,
    const append_callback_t& cb) {
  if (log_id_dist == nullptr || write_rate_ == 0) {
    // No log id distribution or write rate is zero. That means there are no
    // appends to be made by this worker. Immediate end of benchmark.
    return true;
  }

  // Wait for fatal error, stop request, or append token. We need to wake up
  // every second to poll for isStopped() in errorOrStopped(), because stop()
  // is an async-safe function. It cannot signal the condition variable.
  ld_debug("Waiting for append token: npending_xacts=%" PRIu64,
           getNumPendingXacts());
  for (;;) {
    const double missing_tokens = std::max(
        0.0, 1.0 - token_bucket_.available(write_rate_, options.max_window));
    const double missing_sec = missing_tokens / write_rate_;
    auto now = Clock::now();
    auto wakeup_time =
        std::min({now + std::chrono::seconds(1),
                  now + std::chrono::nanoseconds(uint64_t(missing_sec * 1e9)),
                  end_time_});
    bool token_consumed = cond_var_.wait_until(lock, wakeup_time, [this] {
      return errorOrStopped() ||
          ((getNumPendingXacts() < options.max_window) &&
           token_bucket_.consume(1, write_rate_, options.max_window));
    });
    if (errorOrStopped() || Clock::now() >= end_time_) {
      // Fatal error, stop requested, or end of benchmark.
      return true;
    } else if (token_consumed) {
      // Token consumed. Proceed.
      break;
    }
  }

  // Pick random log id from distribution.
  logid_t log_id(log_id_dist());

  // Try to append.
  ld_debug("Performing append: npending_xacts=%" PRIu64, getNumPendingXacts());
  std::string payload(generatePayload());
  if (!tryAppend(log_id, payload, cb)) {
    // Success (so far). Increase pending append counter and remember record id
    // as pending read.
    handleAppendSuccess(Payload{payload.data(), payload.size()});
  } else {
    // Some error occurred, handle it.
    handleAppendError(err, Payload{payload.data(), payload.size()});
  }

  if (nwaiting_ > 0) {
    // Yield to give waiting callbacks a chance to execute. It's possible that
    // a callback bumps up nwaiting_ right after we read nwaiting_ == 0. That
    // is okay, it will get a chance to run the next time.
    lock.unlock();
    std::this_thread::yield();
    lock.lock();
  }

  // Come again.
  return false;
}

bool SteadyWriteRateWorker::errorOrStopped() const noexcept {
  return error() || isStopped();
}

}}} // namespace facebook::logdevice::ldbench
