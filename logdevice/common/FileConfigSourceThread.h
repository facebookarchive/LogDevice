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
#include <memory>
#include <mutex>
#include <pthread.h>
#include <unordered_set>

#include "logdevice/common/debug.h"
#include "logdevice/common/plugin/BuiltinConfigSourceFactory.h"

namespace facebook { namespace logdevice {

/**
 * @file A background thread that periodically pings all registered
 * FileConfigSources to check their files for updates..
 */

class FileConfigSource;

class FileConfigSourceThread {
 public:
  FileConfigSourceThread(
      FileConfigSource* parent,
      UpdateableSettings<BuiltinConfigSourceFactory::Settings> settings);
  ~FileConfigSourceThread();

  // Tells the thread to wake up and poll the config file as soon as possible.
  // Used for speeding up config updates tests.
  void advisePollingIteration();

  std::atomic<int64_t> main_loop_iterations_{0}; // for tests

 private:
  /**
   * Static wrapper around monitorLoop() that can be passed to
   * pthread_create().  Just calls mainLoop().
   *
   * @param arg Pointer to the singleton instance.
   */
  static void* threadEntryPoint(void* arg);

  /**
   * Main loop for the monitor.  Repeatedly sleeps for pollingInterval_ then
   * checks all registered configs for updates.
   */
  void mainLoop();

  FileConfigSource* parent_;
  pthread_t mainLoopThread_;

  // Used to stop mainLoop().
  std::atomic<bool> mainLoopStop_{false};
  std::condition_variable mainLoopWaitCondition_;
  std::mutex mainLoopWaitMutex_;

  std::atomic<std::chrono::milliseconds> pollingInterval_;

  UpdateableSettings<BuiltinConfigSourceFactory::Settings> settings_;
  UpdateableSettings<BuiltinConfigSourceFactory::Settings>::SubscriptionHandle
      settings_sub_handle_;
};

}} // namespace facebook::logdevice
