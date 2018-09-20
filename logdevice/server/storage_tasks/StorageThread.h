/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include "logdevice/common/Thread.h"

namespace facebook { namespace logdevice {

class StorageThreadPool;

/**
 * @file A template for different types of threads belonging to a
 * StorageThreadPool.  The most important type is ExecStorageThread which
 * executes tasks.  SyncingStorageThread is an auxiliary thread that ensures
 * writes get synced to storage, for writes that need that.
 */

class StorageThread : public Thread {
 public:
  /**
   * Does not do much, start() actually creates the thread.
   *
   * @param pool  pool this thread belongs to
   */
  explicit StorageThread(StorageThreadPool* pool) : pool_(pool) {
    ld_check(pool);
  }

 protected:
  // Parent pointer to owning StorageThreadPool
  StorageThreadPool* pool_;
};
}} // namespace facebook::logdevice
