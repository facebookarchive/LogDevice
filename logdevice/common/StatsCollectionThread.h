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
#include <memory>
#include <mutex>
#include <thread>

#include "logdevice/common/StatsPublisher.h"

namespace facebook { namespace logdevice {

/**
 * @file Background thread that periodically collects stats, calculates rates
 * and publishes to a provided StatsPublisher.
 */

class PluginRegistry;
class ServerConfig;
struct Settings;
struct Stats;
class StatsHolder;
template <typename T>
class UpdateableSettings;

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

  void addStatsSource(const StatsHolder* source);

  static std::unique_ptr<StatsCollectionThread>
  maybeCreate(const UpdateableSettings<Settings>&,
              std::shared_ptr<ServerConfig>,
              std::shared_ptr<PluginRegistry>,
              int num_shards,
              const StatsHolder* source);

 private:
  void mainLoop();

  std::vector<const StatsHolder*> source_stats_sets_;
  std::chrono::seconds interval_;
  std::unique_ptr<StatsPublisher> publisher_;
  bool stop_ = false;
  std::mutex mutex_;
  std::condition_variable cv_;
  std::thread thread_;
};

}} // namespace facebook::logdevice
