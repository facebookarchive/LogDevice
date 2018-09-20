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
#include <queue>
#include <thread>
#include <unordered_set>

#include "logdevice/common/debug.h"
#include "logdevice/server/locallogstore/LocalLogStoreSettings.h"

/**
 * @file A background thread that peforms monitoring for logdevice.
 *
 *       Currently it only starts on storage nodes and perform free
 *       disk space monitoring (see checkFreeSpace()).
 */
namespace facebook { namespace logdevice {

class MonitorRequestQueue;
class ServerProcessor;

class LogStoreMonitor {
 public:
  LogStoreMonitor(ServerProcessor* processor,
                  UpdateableSettings<LocalLogStoreSettings> settings);
  ~LogStoreMonitor();

  void start();
  void stop();

 private:
  // Pointer to the Processor object for accessing other logdevice
  // components
  ServerProcessor* const processor_;

  // A copy of settings from commandline
  UpdateableSettings<LocalLogStoreSettings> settings_;

  // An instance of MonitorRequestQueue for posting request to Workers
  std::shared_ptr<MonitorRequestQueue> request_queue_;

  // we requested rebuilding for these shards
  std::unordered_set<uint32_t> shards_need_rebuilding_;

  // thread object for the monitoring thread
  std::thread thread_;

  // condition variable to stop the thread
  std::condition_variable cv_;
  std::mutex mutex_;

  // flag to stop the thread
  std::atomic<bool> should_stop_{false};

  /*
   * Check the disk space usage of local log stores
   *
   * Perform periodical checking for free disk space on partitions
   * that contain log storage. If the available disk space
   * drops before certain threshold (configurable through commandline),
   * logdevice will reject further writes to that local storage. When
   * the amount of free space goes back up above the threshold, monitor
   * will detect it and resumes accepting STOREs for the affected RocksDB
   * instances.
   */
  void checkFreeSpace();

  // Request rebuilding of some shards if needed.
  void checkNeedRebuilding();

  // mainLoop of the thread
  void threadMain();
};

}} // namespace facebook::logdevice
