/**
 * Copyright (c) 2017-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <chrono>
#include <condition_variable>
#include <memory>
#include <mutex>
#include <thread>

#include "logdevice/common/StatsPublisher.h"

namespace facebook { namespace logdevice {

/**
 * @file Background thread that periodically collects stats, calculates rates
 * and publishes to a provided StatsPublisher.
 */

struct Stats;
class StatsHolder;

class StatsCollectionThread {
 public:
  StatsCollectionThread(const StatsHolder* source,
                        std::chrono::seconds interval,
                        std::unique_ptr<StatsPublisher> publisher);

  /**
   * Blocks while the thread publishes stats one last time.
   */
  ~StatsCollectionThread();

  /**
   * Requests the thread to terminate its main loop soon.  The thread tries to
   * flush stats once more before terminating.  This does not need to be
   * called but can be beneficial ahead of destruction (when coordinating
   * shutdown of multiple threads).
   */
  void shutDown() {
    std::lock_guard<std::mutex> guard(mutex_);
    stop_ = true;
    cv_.notify_one();
  }

 private:
  void mainLoop();

  const StatsHolder* source_stats_;
  std::chrono::seconds interval_;
  std::unique_ptr<StatsPublisher> publisher_;
  bool stop_ = false;
  std::mutex mutex_;
  std::condition_variable cv_;
  std::thread thread_;
};

}} // namespace facebook::logdevice
