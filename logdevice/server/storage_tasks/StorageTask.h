/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <memory>
#include <string>

#include "logdevice/common/AdminCommandTable-fwd.h"
#include "logdevice/common/DRRScheduler.h"
#include "logdevice/common/StorageTask-enums.h"
#include "logdevice/common/StorageTaskDebugInfo.h"
#include "logdevice/common/Timestamp.h"
#include "logdevice/common/settings/Durability.h"

namespace folly {
class Executor;
}

namespace facebook { namespace logdevice {

/**
 * @file Interface for tasks created by worker threads for storage threads,
 *       mostly reading from and writing to the local log store.
 */

class ExecStorageThread;
class StatsHolder;
class StorageThreadPool;

class StorageTask {
 public:
  using Type = StorageTaskType;
  using ThreadType = StorageTaskThreadType;
  using Principal = StorageTaskPrincipal;

  StorageTask() = delete;
  explicit StorageTask(Type type) : type_(type) {
    // UNKNOWN is allowable in tests
    ld_check(type_ >= StorageTask::Type::UNKNOWN &&
             type_ < StorageTask::Type::MAX);
  }

  Type getType() const {
    return type_;
  }

  /**
   * Executes the task on a storage thread.  Upon completion, the task is
   * passed back to the worker thread.
   */
  virtual void execute() = 0;

  /**
   * The durability requirement for this store request (i.e. should the log
   * store sync changes before this task is considered done)?  This is called
   * after execute() has finished and may consult internal state.
   *
   * If the value is ASYNC_WRITE or SYNC_WRITE after execute completes(), it
   * is assumed that execute() has already made the write ASYNC_WRITE-durable
   * (i.e. performed a write syscall to enqueue the data for a write-durable
   * device).
   *
   * If the value is SYNC_WRITE after execute() completes, a "sync(ASYNC_WRITE)"
   * operation must be issued on the log store to guarantee the SYNC_WRITE
   * durability requirement. This sync operation must complete before the
   * task's onDone() callback is invoked.
   *
   * For all other durability values, no further action is required after
   * execute() completes in order to satisfy the task's durabilty requirement.
   *
   * NOTE: Returns Durability::INVALID if the storage task has no writes to
   *       issue or the task has failed.
   *
   * NOTE: For a task with SYNC_WRITE durability, although the task won't
   *       complete until after data is persistently stored, the effect of
   *       the write can become externally visible any time after execute()
   *       is called and before it is committed to stable storage.
   */
  virtual Durability durability() const {
    return Durability::ASYNC_WRITE;
  }

  /**
   * For tasks that return durability() == Durability::SYNC_WRITE, the
   * optinal FlushToken that indicates the lowest value returned by
   * store->wasSyncedUpThrough() which indicates that this store is
   * durably stored.
   *
   * Tasks that want to wait for the next sync batch regardless
   * of the value of store->wasSyncedUpThrough() should return
   * FlushToken_INVALID from this method.
   */
  virtual FlushToken syncToken() const {
    return FlushToken_INVALID;
  }

  /**
   * For tasks that return durability() == Durability::SYNC_WRITE to request
   * syncing, this hook is called on an unspecified thread after execute()
   * and after the sync has completed, but before the task is passed back to
   * the worker thread.  Mostly useful for testing.
   */
  virtual void onSynced() {}

  /**
   * Called on a worker thread after execute() has finished running on a
   * storage thread.
   */
  virtual void onDone() = 0;

  /**
   * Called on a worker thread when the storage task is dropped under load.
   * Don't post more storage tasks from inside this callback.
   */
  virtual void onDropped() = 0;

  /**
   * This should be overridden to return true iff the subclass is
   * WriteStorageTask (or a subclass of that).  This makes the batching
   * mechanism for writes kick in.
   */
  virtual bool isWriteTask() const {
    return false;
  }

  /**
   * Threads of what type should run this task?
   */
  virtual ThreadType getThreadType() const {
    return ThreadType::DEFAULT;
  }

  /**
   * What is the priority of this storage task?
   */
  virtual StorageTaskPriority getPriority() const {
    // all tasks execute at mid priority by default
    return StorageTaskPriority::MID;
  }

  /**
   * Principal: context for the IO task.
   */
  virtual Principal getPrincipal() const {
    // Tasks get the MISC principal by default.
    // It has the lowest disk share.
    return Principal::MISC;
  }

  /**
   * Can this storage task be dropped if the node is overloaded?
   *
   * Some tasks must not be dropped and have to be executed before normal tasks.
   * There is a risk of accumulating many of such tasks and running out of
   * memory.  However, if the only alternative would be to keep retrying the
   * task (individually), which requires yet more memory, it is likely superior
   * to just not drop it.
   */
  virtual bool isDroppable() const {
    return true;
  }

  /**
   * Whether to allow delayed sync with LocalLogStore in SyncingStorageThread
   */
  virtual bool allowDelayingSync() const {
    return true;
  }

  /**
   * Hook called on a storage thread when the task is dropped during a queue
   * drop.  Subclasses can override to perform extra processing.
   */
  virtual void onStorageThreadDrop() {}

  /**
   * Called by StorageThreadPool as soon as the task gets handed to the
   * storage thread pool to provide tasks access to the local log store etc.
   */
  void setStorageThreadPool(StorageThreadPool* ptr) {
    ld_check(storageThreadPool_ == nullptr || storageThreadPool_ == ptr);
    storageThreadPool_ = ptr;
  }

  /**
   * Called by StorageThread before execute() to provide tasks access to the
   * StorageThread instance processing the task.
   */
  void setStorageThread(ExecStorageThread* ptr) {
    storageThread_ = ptr;
  }

  inline uint64_t reqSize() {
    return reqSize_;
  }

  inline void reqSize(uint64_t reqSize) {
    reqSize_ = reqSize;
  }

  /**
   * Fetches debug information that is common for all task types, then calls
   * into a virtual function that can fetch type-specific fields
   */
  StorageTaskDebugInfo getDebugInfo() const;
  void getDebugInfo(InfoStorageTasksTable& table) const;

  virtual ~StorageTask() {}

  /**
   * State Machine that will process the response of this storage task.
   * It is set by PerWorkerStorageTaskQueue::putTask() when issuing the task to
   * storage threads.
   */
  folly::Executor* reply_executor_;

  /**
   * Index of database shard that this task is going to.  Set by
   * PerWorkerStorageTaskQueue::putTask().  Used to direct the reply to the
   * appropriate PerWorkerStorageTaskQueue (in charge of that shard).
   */
  int reply_shard_idx_ = -1;

  /**
   * Index of the worker to which to send the reply. Used for stats.
   */
  worker_id_t reply_worker_idx_ = WORKER_ID_INVALID;

  /**
   * The pointer to stats.
   */
  StatsHolder* stats_;

  /**
   * Flag set when the task is dropped from the storage thread queue.  Once
   * the task is back on the worker thread, we'll know to call onDropped()
   * instead of onDone().
   */
  bool dropped_from_storage_thread_queue_ = false;

  // Time this task was queued for execution.
  // Used to maintain histograms of queueing time of storage tasks.
  std::chrono::steady_clock::time_point enqueue_time_;

  // Time this task execution was started
  folly::Optional<std::chrono::steady_clock::time_point> execution_start_time_;
  // Time this task execution was finished
  folly::Optional<std::chrono::steady_clock::time_point> execution_end_time_;

  // This field is useful for IO scheduling. The default size of the task is 1.
  // This implies request based scheduling as each request has the same size.
  // Different Principals get different requests/sec. The caller may update the
  // default size to the actual IO size in bytes. This will lead to throughput
  // based scheduling: each principal will get bytes/sec proportional to their
  // share.
  uint32_t reqSize_{1};

  folly::IntrusiveListHook schedulerQHook_;

 protected:
  Type type_ = Type::UNKNOWN;

  // Pointer to StorageThreadPool instance
  StorageThreadPool* storageThreadPool_ = nullptr;

  // Pointer to ExecStorageThread instance which picked up the Task and called
  // execute()
  ExecStorageThread* storageThread_ = nullptr;

 private:
  // Fetches fields that are specific to every task type. Should be overridden
  // in specific task implementations that want to provide this
  virtual void getDebugInfoDetailed(StorageTaskDebugInfo&) const {}
};

// Helper macros for manipulating stats related to storage task
// on different thread types

#define STORAGE_TASK_STAT_ADD(stats, thread_type, prefix, val)  \
  do {                                                          \
    switch (thread_type) {                                      \
      case StorageTask::ThreadType::FAST_TIME_SENSITIVE:        \
        STAT_ADD((stats), prefix##_fast_time_sensitive, (val)); \
        break;                                                  \
      case StorageTask::ThreadType::FAST_STALLABLE:             \
        STAT_ADD((stats), prefix##_fast_stallable, (val));      \
        break;                                                  \
      case StorageTask::ThreadType::SLOW:                       \
        STAT_ADD((stats), prefix##_slow, (val));                \
        break;                                                  \
      case StorageTask::ThreadType::DEFAULT:                    \
        STAT_ADD((stats), prefix##_default, (val));             \
        break;                                                  \
      case StorageTask::ThreadType::MAX:                        \
        ld_check(false);                                        \
        break;                                                  \
    }                                                           \
  } while (0);

#define STORAGE_TASK_STAT_SUB(stats, thread_type, prefix, val) \
  STORAGE_TASK_STAT_ADD(stats, thread_type, prefix, -(val))
#define STORAGE_TASK_STAT_INCR(stats, thread_type, prefix) \
  STORAGE_TASK_STAT_ADD(stats, thread_type, prefix, 1)
#define STORAGE_TASK_STAT_DECR(stats, thread_type, prefix) \
  STORAGE_TASK_STAT_SUB(stats, thread_type, prefix, 1)

#define WORKER_STORAGE_TASK_STAT_ADD(thread_type, prefix, val) \
  STORAGE_TASK_STAT_ADD(Worker::stats(), thread_type, prefix, val)
#define WORKER_STORAGE_TASK_STAT_SUB(thread_type, prefix, val) \
  STORAGE_TASK_STAT_SUB(Worker::stats(), thread_type, prefix, val)
#define WORKER_STORAGE_TASK_STAT_INCR(thread_type, prefix) \
  STORAGE_TASK_STAT_INCR(Worker::stats(), thread_type, prefix)
#define WORKER_STORAGE_TASK_STAT_DECR(thread_type, prefix) \
  STORAGE_TASK_STAT_DECR(Worker::stats(), thread_type, prefix)
}} // namespace facebook::logdevice
