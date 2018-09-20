/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "ExecStorageThread.h"

#include <chrono>

#include "logdevice/common/SlowStorageTasksTracer.h"
#include "logdevice/common/StorageTask-enums.h"
#include "logdevice/common/Timestamp.h"
#include "logdevice/common/stats/PerShardHistograms.h"
#include "logdevice/common/stats/Stats.h"
#include "logdevice/server/ServerProcessor.h"
#include "logdevice/server/locallogstore/LocalLogStore.h"
#include "logdevice/server/storage_tasks/StorageTask.h"
#include "logdevice/server/storage_tasks/StorageTaskResponse.h"
#include "logdevice/server/storage_tasks/StorageThreadPool.h"

namespace facebook { namespace logdevice {

void ExecStorageThread::run() {
  pool_->getLocalLogStore().onStorageThreadStarted();

  auto settings = pool_->getSettings().get();
  // both slow and metadata threads are using the lower io priority
  if ((thread_type_ == StorageTask::ThreadType::SLOW ||
       thread_type_ == StorageTask::ThreadType::METADATA) &&
      settings->slow_ioprio.hasValue()) {
    set_io_priority_of_this_thread(settings->slow_ioprio.value());
  }

  while (shouldProcessTasks_) {
    std::unique_ptr<StorageTask> task = pool_->blockingGetTask(thread_type_);
    task->setStorageThread(this);

    // Maintain stats for queueing latency.
    auto queueing_usec = usec_since(task->enqueue_time_);
    if (task->reply_shard_idx_ != -1) {
      PER_SHARD_HISTOGRAM_ADD(
          pool_->stats(),
          storage_threads_queue_time[static_cast<int>(thread_type_)],
          task->reply_shard_idx_,
          queueing_usec);
      PER_SHARD_HISTOGRAM_ADD(
          pool_->stats(),
          storage_task_queue_time[static_cast<int>(task->getType())],
          task->reply_shard_idx_,
          queueing_usec);
    }

    auto execution_start_time = std::chrono::steady_clock::now();
    task->execute();
    auto execution_end_time = std::chrono::steady_clock::now();
    auto usec = SystemTimestamp(execution_end_time - execution_start_time)
                    .toMicroseconds()
                    .count();
    STORAGE_TASK_TYPE_STAT_INCR(
        pool_->stats(), task->getType(), storage_tasks_executed);
    STORAGE_TASK_TYPE_STAT_ADD(
        pool_->stats(), task->getType(), storage_thread_usec, usec);

    // Maintaining stats for execution latency.
    if (task->reply_shard_idx_ != -1) {
      if (task->getType() != StorageTask::Type::UNKNOWN) {
        PER_SHARD_HISTOGRAM_ADD(
            pool_->stats(),
            storage_tasks[static_cast<int>(task->getType())],
            task->reply_shard_idx_,
            usec);
      }
    }

    // If we are running on a slow thread, post samples to Scuba
    if (task->getThreadType() == StorageTask::ThreadType::SLOW) {
      SlowStorageTasksTracer logger{pool_->getTraceLogger()};
      StorageTaskDebugInfo info = task->getDebugInfo();
      info.execution_start_time =
          toSystemTimestamp(execution_start_time).toMilliseconds();
      info.execution_end_time =
          toSystemTimestamp(execution_end_time).toMilliseconds();
      logger.traceStorageTask(info);
    }

    if (task->durability() == Durability::SYNC_WRITE) {
      pool_->enqueueForSync(std::move(task));
    } else {
      StorageTaskResponse::sendBackToWorker(std::move(task));
    }
  }
}
}} // namespace facebook::logdevice
