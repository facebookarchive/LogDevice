/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <chrono>
#include <condition_variable>
#include <mutex>
#include <thread>
#include <vector>

#include "logdevice/common/RateLimiter.h"

namespace facebook { namespace logdevice {

class Processor;

class WatchDogThread {
 public:
  explicit WatchDogThread(Processor* p,
                          std::chrono::milliseconds poll_interval,
                          rate_limit_t bt_ratelimit);

  void shutdown();

 private:
  std::thread thread_;

  Processor* processor_;

  std::chrono::milliseconds poll_interval_ms_;

  // cached values of worker progress
  std::vector<size_t> events_called_;
  std::vector<size_t> events_completed_;

  bool shutdown_{false};

  RateLimiter bt_ratelimiter_;

  std::condition_variable cv_;

  std::mutex mutex_;

  std::vector<std::chrono::milliseconds> total_stalled_time_ms_;
  // Main thread loop.
  void run();

  void detectStalls();
};

}} // namespace facebook::logdevice
