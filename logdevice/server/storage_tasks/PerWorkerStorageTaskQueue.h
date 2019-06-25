/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <chrono>
#include <memory>
#include <queue>
#include <utility>

#include "logdevice/include/Err.h"
#include "logdevice/server/storage_tasks/StorageTask.h"

namespace facebook { namespace logdevice {

/**
 * @file Helper class of Worker that facilitates communication between the
 *       worker and storage threads when reading from or writing to the local
 *       log store.  The worker creates StorageTask instances and hands them
 *       off to this class.  If needed, this class buffers tasks (in order to
 *       keep a hard limit on the number of StorageTasks in flight) and hands
 *       them off to storage threads.  As replies come back from storage
 *       threads, more requests that were buffered get sent out.
 */

class WriteStorageTask;

class PerWorkerStorageTaskQueue {
 public:
  /**
   * @param shard_idx index in worker's vector of queues, matches
   *                  ShardedStorageThreadPool shard index
   * @param base  event base (passed to RequestPump) for responses
   * @param zero  base's zero timeout (passed to RequestPump)
   * @param max_tasks_in_flight  max tasks to pass to storage thread pool at
   *                             once
   * @param max_buffered_tasks  max tasks to buffer in this class (when we
   *                            are already at the inflight limit)
   * @param requests_per_iteration  Number of requests that RequestPump would
   *                                run on a single EventLoop iteration
   */
  PerWorkerStorageTaskQueue(shard_index_t shard_idx,
                            size_t max_tasks_in_flight,
                            size_t max_buffered_tasks);

  ~PerWorkerStorageTaskQueue();

  /**
   * Claims ownership of a task and eventually has it processed by a storage
   * thread.
   *
   * This might immediately hand off the task to the storage thread pool or,
   * if there are already too many tasks in flight, buffer the task for later.
   *
   * If the queue is full, may synchronously call onDropped() on the given
   * task and/or on other tasks in the queue.
   */
  void putTask(std::unique_ptr<StorageTask>&& task);

  /**
   * Called when a response for a storage task is received from a storage
   * thread.
   *
   * This updates the tasks-in-flight counter and possibly sends out a new
   * task if any were buffered.
   */
  void onReply(const StorageTask& task);

  /**
   * Drop tasks queued in this PerWorkerStorageTaskQueue for a specified
   * thread pool to pick up.  Normally (if the worker is not shutting down),
   * discards only droppable tasks.
   */
  void drop(StorageTask::ThreadType thread_type);

  /**
   * Check if this queue is taking too much work.
   */
  bool isOverloaded() const;

 private:
  // Index in worker's storage_task_queues_ vector, used to send requests to
  // StorageThreadPool with the same index
  const shard_index_t shard_idx_;

  // Can we send the task to a storage thread, without going over the limit on
  // inflight tasks?
  bool canSendToStorageThread(const StorageTask& task) const;

  // Check to see if the underlying storage is currently accepting writes
  Status acceptingWrites() const;

  // Should we still try to post a write task, even though local log store
  // is not normally accepting more writes?
  bool isWriteExempt(WriteStorageTask* write, Status status) const;

  // Helper method that hands off a task to the storage thread pool and
  // updates the inflight task count.
  void sendTaskToStorageThread(std::unique_ptr<StorageTask>&& task);

  // Called when putTask() needs to buffer but the buffer is full. Drops
  // normal-priority tasks buffered in this queue and requests
  // StorageThreadPool corresponding to shard_idx_ to drop tasks as well.
  void onBufferFull(StorageTask::ThreadType thread_type);

  const size_t max_tasks_in_flight_;
  const size_t max_buffered_tasks_;

  // Buffered tasks when tasks_in_flight_ == max
  struct TaskBuffer {
    size_t size() const {
      return non_droppable.size() + normal.size();
    }

    std::queue<std::unique_ptr<StorageTask>> non_droppable;
    std::queue<std::unique_ptr<StorageTask>> normal;

    // Counter of tasks that have been put on the storage thread pool queue and
    // for which a response has not been received yet.  The tasks may currently
    // be processing, or sitting on the thread pool task queue.
    size_t tasks_in_flight{0};

    // last time drop() was called
    std::chrono::steady_clock::time_point last_queue_drop_time{};
  } taskBuffer_[(int)StorageTask::ThreadType::MAX];

};
}} // namespace facebook::logdevice
