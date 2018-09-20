/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/server/storage_tasks/SyncingStorageThread.h"

#include <chrono>
#include <deque>

#include "logdevice/common/debug.h"
#include "logdevice/server/locallogstore/LocalLogStore.h"
#include "logdevice/server/storage_tasks/StorageTask.h"
#include "logdevice/server/storage_tasks/StorageTaskResponse.h"
#include "logdevice/server/storage_tasks/StorageThreadPool.h"

#include "logdevice/common/stats/Stats.h"

namespace facebook { namespace logdevice {

SyncingStorageThread::SyncingStorageThread(StorageThreadPool* pool,
                                           size_t queue_size)
    : StorageThread(pool), queue_(queue_size) {}

SyncingStorageThread::~SyncingStorageThread() {}

void SyncingStorageThread::enqueueForSync(std::unique_ptr<StorageTask> task) {
  // Tasks should be non-null so we can abuse null in stopProcessingTasks()
  ld_check(task);

  if (!queue_.writeIfNotFull(std::move(task))) {
    RATELIMIT_WARNING(
        std::chrono::seconds(60),
        1,
        "Failed to enqueue.  This should never happen if the queue is properly "
        "sized.  Reverting to blockingWrite().");
    queue_.blockingWrite(std::move(task));
  }
}

void SyncingStorageThread::stopProcessingTasks() {
  queue_.write(std::unique_ptr<StorageTask>());
}

void SyncingStorageThread::run() {
  std::deque<std::unique_ptr<StorageTask>> batch;
  bool stop = false;
  auto got_task = [&](std::unique_ptr<StorageTask> task) {
    if (task) {
      ld_check(task->durability() == Durability::SYNC_WRITE);
      auto sync_token = task->syncToken();
      if (sync_token == FlushToken_INVALID ||
          sync_token > pool_->getLocalLogStore().walSyncedUpThrough()) {
        batch.push_back(std::move(task));
      } else {
        task->onSynced();
        StorageTaskResponse::sendBackToWorker(std::move(task));
        STAT_INCR(pool_->stats(), write_ops_sync_already_done);
      }
    } else {
      // Null indicates need to stop
      stop = true;
    }
  };

  while (!stop) {
    std::unique_ptr<StorageTask> task;

    // Some tasks may have been waiting for a sync we just completed.
    // Loop until got_task() finds a task that still needs a sync to
    // be issued.
    while (!stop && batch.empty()) {
      queue_.blockingRead(task);
      got_task(std::move(task));
    }

    // We got one task off the incoming queue, now pull as much as possible to
    // sync in the same batch.  If the typical sync() time is `SYNC', most
    // tasks will wait between 0 and 2*SYNC before we can confirm they got
    // synced.  (The worst case is when a task comes in just as we started
    // syncing a previous batch, so it has to wait for two syncs.)
    //
    // The `batch.size()' check guards against the theoretical possibility of
    // tasks coming in faster than we can drain them, although this should be
    // impossible in practice because of limits on how many tasks can be in
    // flight.
    while (batch.size() < queue_.capacity() && queue_.read(task)) {
      got_task(std::move(task));
    }

    if (!batch.empty()) {
      using namespace std::chrono;
      auto t1 = steady_clock::now();
      int rv = pool_->getLocalLogStore().sync(Durability::ASYNC_WRITE);
      if (rv != 0) {
        // Handling this properly would require expanding the StorageTask
        // interface further to allow this failure.  Seems unlikely?
        RATELIMIT_ERROR(std::chrono::seconds(60), 1, "Sync failed!?");
      }
      ld_debug("Shard %d: Synced %zu tasks in %ld ms",
               pool_->getLocalLogStore().getShardIdx(),
               batch.size(),
               duration_cast<milliseconds>(steady_clock::now() - t1).count());

      for (auto& ptr : batch) {
        if (ptr) {
          ptr->onSynced();
          StorageTaskResponse::sendBackToWorker(std::move(ptr));
        } else {
        }
      }
      batch.clear();
    }
  }
}
}} // namespace facebook::logdevice
