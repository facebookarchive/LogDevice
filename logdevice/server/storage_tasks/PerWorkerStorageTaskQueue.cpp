/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/server/storage_tasks/PerWorkerStorageTaskQueue.h"

#include <map>
#include <unistd.h>

#include <folly/Memory.h>
#include <folly/small_vector.h>

#include "logdevice/common/MetaDataLog.h"
#include "logdevice/common/debug.h"
#include "logdevice/common/stats/Stats.h"
#include "logdevice/server/ServerProcessor.h"
#include "logdevice/server/ServerWorker.h"
#include "logdevice/server/storage_tasks/ShardedStorageThreadPool.h"
#include "logdevice/server/storage_tasks/StorageTask.h"
#include "logdevice/server/storage_tasks/StorageThreadPool.h"
#include "logdevice/server/storage_tasks/WriteBatchStorageTask.h"
#include "logdevice/server/storage_tasks/WriteStorageTask.h"

namespace facebook { namespace logdevice {

static size_t inflight_slots_for_task(const StorageTask& task) {
  // Writes count 2x (see docblock for WriteBatchStorageTask)
  return task.isWriteTask() ? 2 : 1;
}

PerWorkerStorageTaskQueue::PerWorkerStorageTaskQueue(shard_index_t shard_idx,
                                                     size_t max_tasks_in_flight,
                                                     size_t max_buffered_tasks)
    : shard_idx_(shard_idx),
      max_tasks_in_flight_(max_tasks_in_flight),
      max_buffered_tasks_(max_buffered_tasks)

{}

PerWorkerStorageTaskQueue::~PerWorkerStorageTaskQueue() {
  for (int thread_type = 0; thread_type < (int)StorageTask::ThreadType::MAX;
       ++thread_type) {
    if (taskBuffer_[thread_type].size() > 0) {
      ld_debug("Dropping %lu queued %s storage tasks on shard %d",
               taskBuffer_[thread_type].size(),
               storageTaskThreadTypeName(
                   static_cast<StorageTask::ThreadType>(thread_type)),
               shard_idx_);
      drop((StorageTask::ThreadType)thread_type);
    }
  }
}

Status PerWorkerStorageTaskQueue::acceptingWrites() const {
  StorageThreadPool& pool =
      ServerWorker::onThisThread()
          ->processor_->sharded_storage_thread_pool_->getByIndex(shard_idx_);

  LocalLogStore* local_logstore = &pool.getLocalLogStore();
  ld_check(local_logstore);

  return local_logstore->acceptingWrites();
}

bool PerWorkerStorageTaskQueue::isWriteExempt(WriteStorageTask* write,
                                              Status status) const {
  if (status == E::NOSPC) {
    // Allow writes to metadata logs and internal logs even though local log
    // store crossed the used disk space threshold. This is important because in
    // order to update nodesets, we have to write into metadata logs; if a node
    // storing both data and metadata logs is full, we might not be able to
    // remove it from some nodesets unless we can write to metadata logs.
    // Another example is to recover the system from a bad stat we may need to
    // write events to the internal log, which would not be possible if the
    // system is full.
    size_t num_write_ops = write->getNumWriteOps();
    folly::small_vector<const WriteOp*, 16> write_ops(num_write_ops);
    size_t write_ops_written =
        write->getWriteOps(write_ops.data(), write_ops.size());
    ld_check(num_write_ops == write_ops_written);
    for (const WriteOp* op : write_ops) {
      auto record_op = dynamic_cast<const RecordWriteOp*>(op);
      if (record_op != nullptr &&
          (MetaDataLog::isMetaDataLog(record_op->log_id) ||
           configuration::InternalLogs::isInternal(record_op->log_id))) {
        return true;
      }
    }
  }

  return write->allowIfStoreIsNotAcceptingWrites(status);
}

void PerWorkerStorageTaskQueue::putTask(std::unique_ptr<StorageTask>&& task) {
  task->reply_executor_ = Worker::onThisThread();
  task->reply_shard_idx_ = shard_idx_;
  task->reply_worker_idx_ = Worker::onThisThread()->idx_;
  task->stats_ = Worker::stats();
  task->enqueue_time_ = std::chrono::steady_clock::now();

  StorageThreadPool* pool =
      &ServerWorker::onThisThread()
           ->processor_->sharded_storage_thread_pool_->getByIndex(shard_idx_);
  task->setStorageThreadPool(pool);

  auto type = task->getThreadType();

  // check to see if the underlying storage is accepting writes
  // if not, we abort early
  if (task->isWriteTask()) {
    ld_assert(task->isDroppable());
    WriteStorageTask* write = static_cast<WriteStorageTask*>(task.get());

    Status accepting = acceptingWrites();
    if (accepting != E::OK && accepting != E::LOW_ON_SPC) {
      ld_assert(dynamic_cast<WriteStorageTask*>(task.get()) != nullptr);
      ld_check_in(accepting, ({E::NOSPC, E::DISABLED, E::FAILED}));

      if (!isWriteExempt(write, accepting)) {
        write->status_ = accepting;
        write->onDone();
        // reply sent, free the task
        task.reset();
        return;
      }
    }

    size_t payload_size = write->getPayloadSize();
    if (payload_size > 0) {
      auto token = pool->getMemoryBudget(type).acquireToken(payload_size);
      if (!token.valid()) {
        // The in-flight StoreStorageTask-s are using a lot of memory.
        // Don't enqueue any more of them.
        write->onDropped();
        if (type == StorageTask::ThreadType::FAST_STALLABLE) {
          PER_SHARD_STAT_INCR(
              Worker::stats(), rebuilding_stores_over_mem_limit, shard_idx_);
        } else {
          PER_SHARD_STAT_INCR(
              Worker::stats(), append_stores_over_mem_limit, shard_idx_);
        }
        task.reset();
        return;
      }
      write->memToken_ = std::move(token);
    }
  }

  WORKER_STAT_INCR(storage_tasks_queued);

  if (canSendToStorageThread(*task)) {
    ld_spew("tasks_in_flight_ = %zu, sending immediately",
            taskBuffer_[(int)type].tasks_in_flight);
    sendTaskToStorageThread(std::move(task));
  } else {
    ld_spew("tasks_in_flight_ = %zu, queueing",
            taskBuffer_[(int)type].tasks_in_flight);
    if (taskBuffer_[(int)type].normal.size() >= max_buffered_tasks_) {
      onBufferFull(type);
    }
    auto& buffer = taskBuffer_[(int)type];
    auto& queue = task->isDroppable() ? buffer.normal : buffer.non_droppable;
    queue.push(std::move(task));
    WORKER_STORAGE_TASK_STAT_INCR(type, storage_task_buffer_size);
  }
}

void PerWorkerStorageTaskQueue::onReply(const StorageTask& task) {
  auto type = task.getThreadType();

  // NOTE: here we always subtract 1 (not inflight_slots_for_task()) because
  // we expect two replies per write, each releasing one inflight slot.
  --taskBuffer_[(int)type].tasks_in_flight;

  auto check_queue = [=](std::queue<std::unique_ptr<StorageTask>>& queue) {
    while (!queue.empty() && canSendToStorageThread(*queue.front())) {
      sendTaskToStorageThread(std::move(queue.front()));
      queue.pop();
      WORKER_STORAGE_TASK_STAT_DECR(type, storage_task_buffer_size);
    }
  };
  check_queue(taskBuffer_[(int)type].non_droppable);
  check_queue(taskBuffer_[(int)type].normal);
}

bool PerWorkerStorageTaskQueue::isOverloaded() const {
  int64_t p = Worker::settings().queue_size_overload_percentage;
  // Since the overloaded status is only used on the append path to determine
  // viable candidate nodes, only consider the buffer consumed by "fast" storage
  // threads (those writing records from Appenders) here. In the future we may
  // also want to expose the state of the read path.
  auto& buffer = taskBuffer_[(int)StorageTask::ThreadType::FAST_TIME_SENSITIVE];
  double mem_budget_used =
      ServerWorker::onThisThread()
          ->processor_->sharded_storage_thread_pool_->getByIndex(shard_idx_)
          .getMemoryBudget(StorageTask::ThreadType::FAST_TIME_SENSITIVE)
          .fractionUsed();

  if (100 * buffer.size() > p * max_buffered_tasks_ ||
      100 * mem_budget_used > p) {
    return true; // too many buffered tasks
  }

  auto elapsed = std::chrono::steady_clock::now() - buffer.last_queue_drop_time;
  return std::chrono::duration_cast<std::chrono::milliseconds>(elapsed) <=
      Worker::settings().queue_drop_overload_time;
}

bool PerWorkerStorageTaskQueue::canSendToStorageThread(
    const StorageTask& task) const {
  const size_t cur = taskBuffer_[(int)task.getThreadType()].tasks_in_flight;
  return cur + inflight_slots_for_task(task) <= max_tasks_in_flight_;
}

void PerWorkerStorageTaskQueue::sendTaskToStorageThread(
    std::unique_ptr<StorageTask>&& task) {
  ld_check(canSendToStorageThread(*task));

  auto type = task->getThreadType();
  taskBuffer_[(int)type].tasks_in_flight += inflight_slots_for_task(*task);

  ServerWorker* worker = ServerWorker::onThisThread();
  ld_check(worker);
  StorageThreadPool* pool =
      &worker->processor_->sharded_storage_thread_pool_->getByIndex(shard_idx_);
  ld_check(pool);
  int rv;

  if (!task->isWriteTask()) {
    rv = pool->tryPutTask(std::move(task));
    // Transferring the task to the storage pool queue must always succeed.  We
    // have a per-worker limit and the storage thread pool queue's capacity is
    // big enough to fit inflight requests of all workers.
    ld_check(rv == 0 || err == E::SHUTDOWN);
  } else {
    // Writes get special handling for batching.  For details see docblock for
    // WriteBatchStorageTask.

    // In debug builds, use RTTI to check the cast
    ld_assert(dynamic_cast<WriteStorageTask*>(task.get()) != nullptr);

    // First put the individual write onto the write queue
    WriteStorageTask* raw = static_cast<WriteStorageTask*>(task.release());
    auto thread_type = raw->getThreadType();
    rv = pool->tryPutWrite(std::unique_ptr<WriteStorageTask>(raw));
    ld_check(rv == 0 || err == E::SHUTDOWN);

    // Now put a blank WriteBatchStorageTask onto the main queue
    auto batch_task = std::make_unique<WriteBatchStorageTask>(thread_type);
    batch_task->reply_executor_ = Worker::onThisThread();
    batch_task->reply_shard_idx_ = shard_idx_;
    batch_task->reply_worker_idx_ = Worker::onThisThread()->idx_;
    batch_task->stats_ = Worker::stats();
    batch_task->enqueue_time_ = std::chrono::steady_clock::now();
    rv = pool->tryPutTask(std::unique_ptr<StorageTask>(std::move(batch_task)));
    ld_check(rv == 0 || err == E::SHUTDOWN);
  }
}

void PerWorkerStorageTaskQueue::drop(StorageTask::ThreadType thread_type) {
  std::vector<std::unique_ptr<StorageTask>> to_notify;
  std::map<StorageTaskType, int> count_by_type;

  auto do_queue = [&](std::queue<std::unique_ptr<StorageTask>>& queue,
                      bool notify) {
    WORKER_STORAGE_TASK_STAT_ADD(
        thread_type, storage_tasks_dropped, queue.size());
    WORKER_STORAGE_TASK_STAT_SUB(
        thread_type, storage_task_buffer_size, queue.size());
    while (!queue.empty()) {
      std::unique_ptr<StorageTask> task = std::move(queue.front());
      queue.pop();
      ++count_by_type[task->getType()];
      if (notify) {
        to_notify.push_back(std::move(task));
      }
    }
  };

  do_queue(taskBuffer_[(int)thread_type].normal, true);
  // Non-droppable tasks are only discarded if shutting down
  if (Worker::onThisThread()->shuttingDown()) {
    // Don't call onDropped() for undroppable tasks because their
    // implementations don't expect this method to be called.
    do_queue(taskBuffer_[(int)thread_type].non_droppable, false);
  }

  if (!count_by_type.empty()) {
    RATELIMIT_INFO(std::chrono::seconds(1),
                   2,
                   "Dropping storage tasks in shard %d: %s",
                   (int)shard_idx_,
                   toString(count_by_type).c_str());
  }

  for (auto& task : to_notify) {
    task->onDropped();
  }
}

void PerWorkerStorageTaskQueue::onBufferFull(StorageTask::ThreadType type) {
  RATELIMIT_WARNING(std::chrono::seconds(10),
                    2,
                    "per-worker %s storage task buffer of worker %s full for "
                    "shard %i, dropping. "
                    "taskBuffer_ size[normal:%lu, non-droppable:%lu],"
                    " max_buffered_tasks_:%lu",
                    storageTaskThreadTypeName(type),
                    Worker::onThisThread()->getName().c_str(),
                    shard_idx_,
                    taskBuffer_[(int)type].normal.size(),
                    taskBuffer_[(int)type].non_droppable.size(),
                    max_buffered_tasks_);

  // Tell the storage thread queue (shared by all workers) to drop all tasks.
  // We do this because a single worker's queue filling up is a good indicator
  // that the system is overloaded.
  ServerWorker* worker = ServerWorker::onThisThread();
  ld_check(worker);
  StorageThreadPool& pool =
      worker->processor_->sharded_storage_thread_pool_->getByIndex(shard_idx_);
  pool.dropTaskQueue(type);

  drop(type);

  auto now = std::chrono::steady_clock::now();
  taskBuffer_[(int)type].last_queue_drop_time = now;
}
}} // namespace facebook::logdevice
