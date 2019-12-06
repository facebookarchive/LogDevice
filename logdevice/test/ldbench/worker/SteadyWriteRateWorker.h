/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <functional>
#include <mutex>
#include <string>

#include <folly/TokenBucket.h>

#include "logdevice/include/Client.h"
#include "logdevice/include/Err.h"
#include "logdevice/include/types.h"
#include "logdevice/test/ldbench/worker/Worker.h"

namespace facebook { namespace logdevice { namespace ldbench {

/**
 * Intermediate class for workers that need to perform writes at a steady rate.
 */
class SteadyWriteRateWorker : public Worker {
 public:
  using Worker::Worker;
  virtual ~SteadyWriteRateWorker() override;

 protected:
  using Clock = std::chrono::steady_clock;
  using TimePoint = Clock::time_point;
  using LogIdDist = std::function<logid_t()>;
  using TokenBucket = folly::DynamicTokenBucket;

  bool tryAppend(std::unique_lock<std::recursive_mutex>& lock,
                 LogIdDist& log_id_dist,
                 const append_callback_t& cb);
  using Worker::tryAppend;
  virtual void handleAppendSuccess(Payload payload) = 0;
  virtual void handleAppendError(Status st, Payload payload) = 0;
  virtual size_t getNumPendingXacts() const noexcept = 0;
  virtual bool error() const noexcept = 0;
  bool errorOrStopped() const noexcept;

  std::recursive_mutex mutex_;
  std::condition_variable_any cond_var_;
  std::atomic<uint64_t> nwaiting_{0};
  double write_rate_;
  TimePoint end_time_;
  TokenBucket token_bucket_;
};

}}} // namespace facebook::logdevice::ldbench
