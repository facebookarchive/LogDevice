/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <folly/MPMCQueue.h>

#include "logdevice/server/storage_tasks/StorageThread.h"
#include "logdevice/server/storage_tasks/StorageThreadPool.h"

namespace facebook { namespace logdevice {

/**
 * @file Storage thread that makes sure writes get synced before
 * acknowledgement (when required).  Makes periodic calls to
 * LocalLogStore::sync().
 */

class StorageTask;

class SyncingStorageThread : public StorageThread {
 public:
  SyncingStorageThread(StorageThreadPool* parent, size_t queue_size);
  ~SyncingStorageThread() override;

  /**
   * Hold the StorageTask until the next LocalLogStore::sync() call, then pass
   * the task back to the Worker.
   */
  void enqueueForSync(std::unique_ptr<StorageTask> task);

  /**
   * Instructs the thread to stop processing tasks and break the run() loop as
   * soon as possible.  Can be called on any thread.
   */
  void stopProcessingTasks();

 protected:
  void run() override;

  std::string threadName() override {
    char buf[16];
    snprintf(buf, sizeof buf, "ld:s%d:sync", pool_->getShardIdx());
    return buf;
  }

 private:
  folly::MPMCQueue<std::unique_ptr<StorageTask>> queue_;
  /**
   * Flag to signal the thread that undelayable task has been enqueued.
   */
  bool sync_immediately_;
  std::condition_variable delay_cv_;
  /**
   * Mutex for sync_immediately_ and delay_cv_
   */
  std::mutex delay_cv_mutex_;
};
}} // namespace facebook::logdevice
