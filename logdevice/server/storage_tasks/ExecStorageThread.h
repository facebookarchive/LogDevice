/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <memory>

#include "logdevice/server/storage_tasks/StorageTask.h"
#include "logdevice/server/storage_tasks/StorageThread.h"
#include "logdevice/server/storage_tasks/StorageThreadPool.h"

namespace facebook { namespace logdevice {

/**
 * @file Repeatedly pulls tasks from the StorageThreadPool's task queue and
 * executes them.
 */

class ExecStorageThread : public StorageThread {
 public:
  ExecStorageThread(StorageThreadPool* pool,
                    StorageTask::ThreadType thread_type,
                    int idx)
      : StorageThread(pool), thread_type_(thread_type), idx_(idx) {}

  StorageThreadPool& getThreadPool() {
    return *pool_;
  }

  // Called by StopExecStorageTask to stop run().  Must already be on this
  // thread.
  void stopProcessingTasks() {
    shouldProcessTasks_ = false;
  }

 protected:
  void run() override;

  std::string threadName() override {
    char buf[16];

    snprintf(buf,
             sizeof buf,
             "ld:s%d:%s:%d",
             pool_->getShardIdx(),
             storageTaskThreadTypeName(thread_type_),
             idx_);
    return buf;
  }

 private:
  // What kind of storage tasks does this thread execute
  StorageTask::ThreadType thread_type_;

  // Index within the StorageThreadPool (used just for naming)
  int idx_;

  bool shouldProcessTasks_ = true;
};
}} // namespace facebook::logdevice
