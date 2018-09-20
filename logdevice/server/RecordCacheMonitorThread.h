/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <cstddef>
#include <thread>
#include <utility>

#include "logdevice/common/SingleEvent.h"

namespace facebook { namespace logdevice {

class ServerProcessor;

/**
 * @file   A thread for monitoring the total size of record cache for all logs,
 *         and performing cache eviction if the total size exceeds the
 *         configurable limit.
 *
 *         The current eviction mechanism always evict logs with largest bytes
 *         in cached first, and for evicting a single log, it always evict all
 *         epochs currently cached. This could help to leave more logs in the
 *         cache, achieving better availability in terms of logs and less seeks
 *         durng epoch recovery.
 */

class RecordCacheMonitorThread {
 public:
  explicit RecordCacheMonitorThread(ServerProcessor* processor);
  ~RecordCacheMonitorThread();

 private:
  ServerProcessor* const processor_;

  // for controlling thread shut down
  SingleEvent shutdown_;

  // main thread handle
  std::thread thread_;

  // mainLoop of the thread
  void threadMain();

  // Check if the total size of all record caches exceeds the limit and
  // eviction is needed.
  //
  // @return  a pair of <eviction_needed, bytes_target>, where:
  //          eviction_needed is a boolean value indicating whether eviction
  //          is needed, and bytes_target is the total amount of bytes that
  //          should be evicted
  std::pair<bool, size_t> recordCacheNeedsEviction();

  // Perform eviction for all logs, attempting to evict @param target_bytes
  void evictCaches(size_t target_bytes);
};

}} // namespace facebook::logdevice
