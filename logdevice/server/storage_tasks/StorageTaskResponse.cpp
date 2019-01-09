/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/server/storage_tasks/StorageTaskResponse.h"

#include <folly/Memory.h>

#include "logdevice/common/RequestPump.h"
#include "logdevice/common/stats/Stats.h"
#include "logdevice/server/ServerWorker.h"
#include "logdevice/server/storage_tasks/PerWorkerStorageTaskQueue.h"

namespace facebook { namespace logdevice {

void StorageTaskResponse::sendBackToWorker(std::unique_ptr<StorageTask> task) {
  std::shared_ptr<RequestPump> reply_pump = std::move(task->reply_pump_);
  if (!reply_pump) {
    // Not all storage tasks need to get sent back to workers
    return;
  }

  auto stats = task->stats_;
  auto worker_idx = task->reply_worker_idx_;

  // Tasks not associated with any worker are completed directly.
  if (worker_idx == WORKER_ID_INVALID) {
    task->onDone();
    return;
  }

  std::unique_ptr<Request> req =
      std::make_unique<StorageTaskResponse>(std::move(task));
  auto req_type = req->type_;
  auto worker_type = req->getWorkerTypeAffinity();

  req->enqueue_time_ = std::chrono::steady_clock::now();
  int rv = reply_pump->forcePost(req);
  ld_check(rv == 0);

  Request::bumpStatsWhenPosted(stats, req_type, worker_type, worker_idx, true);
}

void StorageTaskResponse::sendDroppedToWorker(
    std::unique_ptr<StorageTask> task) {
  task->dropped_from_storage_thread_queue_ = true;
  task->onStorageThreadDrop();
  sendBackToWorker(std::move(task));
}

Request::Execution StorageTaskResponse::execute() {
  ServerWorker* worker = ServerWorker::onThisThread();
  worker->getStorageTaskQueueForShard(task_->reply_shard_idx_)->onReply(*task_);

  if (task_->dropped_from_storage_thread_queue_) {
    WORKER_STORAGE_TASK_STAT_INCR(
        task_->getThreadType(), storage_tasks_dropped);
    task_->onDropped();
  } else {
    task_->onDone();
  }

  return Execution::COMPLETE;
}

std::string StorageTaskResponse::describe() const {
  return requestTypeNames[type_] + "(" +
      storageTaskTypeNames[task_->getType()] + ")";
}
}} // namespace facebook::logdevice
