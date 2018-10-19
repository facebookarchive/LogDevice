/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "StorageThreadPool.h"

#include <folly/Memory.h>

#include "logdevice/common/AdminCommandTable.h"
#include "logdevice/common/ConstructorFailed.h"
#include "logdevice/common/StorageTask-enums.h"
#include "logdevice/common/debug.h"
#include "logdevice/common/stats/Stats.h"
#include "logdevice/server/RecordCachePersistence.h"
#include "logdevice/server/locallogstore/LocalLogStore.h"
#include "logdevice/server/storage_tasks/ExecStorageThread.h"
#include "logdevice/server/storage_tasks/StorageTask.h"
#include "logdevice/server/storage_tasks/StorageTaskResponse.h"
#include "logdevice/server/storage_tasks/SyncingStorageThread.h"
#include "logdevice/server/storage_tasks/WriteStorageTask.h"

namespace facebook { namespace logdevice {

using ThreadType = StorageTask::ThreadType;

namespace {
/**
 * Dummy task to wake up the storage thread that processes the task.
 * StorageThreadPool uses this task in its destructor to stop all threads.
 */
class StopExecStorageTask : public StorageTask {
 public:
  StopExecStorageTask(shard_index_t shard_idx,
                      std::shared_ptr<std::atomic<int>> remaining_threads,
                      bool persist_record_caches)
      : StorageTask(StorageTask::Type::STOP_EXEC),
        shard_idx_(shard_idx),
        remaining_threads_(remaining_threads),
        persist_record_caches_(persist_record_caches) {}
  void execute() override {
    // Persist record caches in the very last storage task to execute.
    if (--*remaining_threads_ == 0 && persist_record_caches_) {
      RecordCachePersistence::persistRecordCaches(
          shard_idx_, storageThreadPool_);
    }

    storageThread_->stopProcessingTasks();
  }
  void onDone() override {}
  void onDropped() override {
    ld_check(false);
  }
  bool isDroppable() const override {
    return false;
  }

  shard_index_t shard_idx_;
  std::shared_ptr<std::atomic<int>> remaining_threads_;
  bool persist_record_caches_;
};
} // namespace

StorageThreadPool::StorageThreadPool(
    shard_index_t shard_idx,
    const StorageThreadPool::Params& params,
    UpdateableSettings<Settings> settings,
    LocalLogStore* local_log_store,
    size_t task_queue_size,
    StatsHolder* stats,
    const std::shared_ptr<TraceLogger> trace_logger)
    : settings_(settings),
      nthreads_slow_(params[(size_t)ThreadType::SLOW].nthreads),
      nthreads_fast_stallable_(
          params[(size_t)ThreadType::FAST_STALLABLE].nthreads),
      nthreads_fast_time_sensitive_(
          params[(size_t)ThreadType::FAST_TIME_SENSITIVE].nthreads),
      nthreads_metadata_(params[(size_t)ThreadType::METADATA].nthreads),
      local_log_store_(local_log_store),
      processor_(nullptr),
      trace_logger_(trace_logger),
      stats_(stats),
      shard_idx_(shard_idx),
      taskQueues_([&, task_queue_size]() {
        const auto actual_queue_sizes =
            computeActualQueueSizes(task_queue_size);
        for (int type = 0; type < (int)ThreadType::MAX; ++type) {
          const size_t size = actual_queue_sizes[type];
          taskQueues_.emplace_back(size, stats);
        }
      }) {
  ld_check(local_log_store != nullptr);
  ld_check(nthreads_slow_ > 0);

  // If you're adding a new ThreadType, please search this file for 'nthreads'
  // to find what need updating here.
  static_assert((int)ThreadType::SLOW == 0 &&
                    (int)ThreadType::FAST_STALLABLE == 1 &&
                    (int)ThreadType::FAST_TIME_SENSITIVE == 2 &&
                    (int)ThreadType::METADATA == 3,
                "");
  static_assert((int)ThreadType::MAX == 4, "");

  // Find an upper limit on the number of tasks in flight for this thread
  // pool, to size the syncing thread's queue
  size_t max_tasks_in_flight = 0;

  for (int type = 0; type < (int)ThreadType::MAX; ++type) {
    max_tasks_in_flight +=
        taskQueues_[static_cast<ThreadType>(type)].queue.max_capacity() /
        (ssize_t)StorageTask::Priority::NUM_PRIORITIES;
  }

  // Start ExecStorageThread instances
  for (int type = 0; type < (int)ThreadType::MAX; ++type) {
    for (int i = 0; i < params[type].nthreads; ++i) {
      auto thread =
          std::make_unique<ExecStorageThread>(this, (ThreadType)type, i);
      if (thread->start() != 0) {
        this->shutDown(); // shut down any already started threads
        this->join();
        throw ConstructorFailed();
      }
      exec_threads_.push_back(std::move(thread));
    }
  }

  // Start syncing thread
  {
    auto thread =
        std::make_unique<SyncingStorageThread>(this, max_tasks_in_flight);
    if (thread->start() != 0) {
      this->shutDown(); // shut down any already started threads
      this->join();
      throw ConstructorFailed();
    }
    syncing_thread_ = std::move(thread);
  }
}

std::array<size_t, (size_t)ThreadType::MAX>
StorageThreadPool::computeActualQueueSizes(size_t task_queue_size) const {
  std::array<size_t, (size_t)ThreadType::MAX> actual_queue_sizes;
  std::fill(
      actual_queue_sizes.begin(), actual_queue_sizes.end(), task_queue_size);
  // If we're not configured to run some type x of threads, their tasks will
  // go to threads of another type y (see getThreadType()).
  // Increase y's queue size to accommodate tasks of both types.
  if (nthreads_fast_stallable_ == 0) {
    actual_queue_sizes[(int)ThreadType::FAST_TIME_SENSITIVE] +=
        actual_queue_sizes[(int)ThreadType::FAST_STALLABLE];
    // Since we're not configured to run with this type of threads,
    // their queue will be unused. Make it as small as possible.
    // Could be 0 but MPMCQueue doesn't consider it a valid size.
    actual_queue_sizes[(int)ThreadType::FAST_STALLABLE] = 1;
  }
  if (nthreads_fast_time_sensitive_ == 0) {
    actual_queue_sizes[(int)ThreadType::SLOW] +=
        actual_queue_sizes[(int)ThreadType::FAST_TIME_SENSITIVE];
    // Since we're not configured to run with this type of threads,
    // their queue will be unused. Make it as small as possible.
    // Could be 0 but MPMCQueue doesn't consider it a valid size.
    actual_queue_sizes[(int)ThreadType::FAST_TIME_SENSITIVE] = 1;
  }
  if (nthreads_metadata_ == 0) {
    actual_queue_sizes[(int)ThreadType::SLOW] +=
        actual_queue_sizes[(int)ThreadType::METADATA];
    // Since we're not configured to run with this type of threads,
    // their queue will be unused. Make it as small as possible.
    // Could be 0 but MPMCQueue doesn't consider it a valid size.
    actual_queue_sizes[(int)ThreadType::METADATA] = 1;
  }
  return actual_queue_sizes;
}

StorageThreadPool::~StorageThreadPool() {
  // Join all the pthreads to complete shutdown.  This is necessary because
  // StorageThreads have a pointer to us, so they have to finish before we're
  // destroyed.  If shutdown/join were already called, this should be a no-op
  // (`shutting_down_' is true, `threads_' is empty).
  shutDown();
  join();
}

void StorageThreadPool::shutDown(bool persist_record_caches) {
  if (shutting_down_.exchange(true)) {
    return;
  }

  int total_num_threads = nthreads_slow_ + nthreads_fast_stallable_ +
      nthreads_metadata_ + nthreads_fast_time_sensitive_;
  auto remaining_threads =
      std::make_shared<std::atomic<int>>(total_num_threads);
  int stop_tasks_enqueued = 0;

  auto do_stop = [&](PerTypeTaskQueue& task_queue, size_t nthreads) {
    // Ask storage threads to drop as many tasks as possible in case the queue
    // is backed up
    const size_t ndrop = task_queue.queue.max_capacity();
    task_queue.tasks_to_drop.store(ndrop);

    for (size_t i = 0; i < nthreads; ++i) {
      std::unique_ptr<StorageTask> task(new StopExecStorageTask(
          shard_idx_, remaining_threads, persist_record_caches));
      task->setStorageThreadPool(this);
      task_queue.queue.blockingWrite(task.release());
      ++stop_tasks_enqueued;
    }
  };

  // Instruct all exec threads to stop soon.  Enqueue one StopExecStorageTask
  // per thread.  Since each thread will stop after processing the task, each
  // thread will consume exactly one of them.
  static_assert((int)StorageTask::ThreadType::MAX == 4, "");

  if (persist_record_caches) {
    ld_info("Persisting record caches for shard %d", shard_idx_);
  } else {
    ld_info("Skipping persistence of record caches for shard %d", shard_idx_);
  }
  do_stop(taskQueues_[StorageTask::ThreadType::SLOW], nthreads_slow_);
  do_stop(taskQueues_[StorageTask::ThreadType::FAST_STALLABLE],
          nthreads_fast_stallable_);
  do_stop(taskQueues_[StorageTask::ThreadType::METADATA], nthreads_metadata_);
  do_stop(taskQueues_[StorageTask::ThreadType::FAST_TIME_SENSITIVE],
          nthreads_fast_time_sensitive_);

  // Unstall stalled low priority writes if they were stuck waiting for flushed
  // data to complete.
  local_log_store_->adviseUnstallingLowPriWrites(/*dont_stall_anymore*/ true);

  ld_check_eq(stop_tasks_enqueued, total_num_threads);
}

void StorageThreadPool::join() {
  for (const auto& thread : exec_threads_) {
    int rv = pthread_join(thread->getThreadHandle(), nullptr);
    ld_check(rv == 0);
  }
  exec_threads_.clear();

  // Now that all exec threads are done, also stop the syncing thread.  We do
  // it here not in shutDown() because exec threads shutting down may have
  // generated work for the syncing thread; if not, it will shut down quickly.
  if (syncing_thread_) {
    syncing_thread_->stopProcessingTasks();
    int rv = pthread_join(syncing_thread_->getThreadHandle(), nullptr);
    ld_check(rv == 0);
    syncing_thread_.reset();
  }
}

int StorageThreadPool::tryPutTask(std::unique_ptr<StorageTask>&& task) {
  if (shutting_down_.load()) {
    err = E::SHUTDOWN;
    return -1;
  }

  auto task_type = task->getType();
  auto thread_type = getThreadType(*task);
  auto& queue = taskQueues_[thread_type].queue;

  task->setStorageThreadPool(this);
  if (!queue.writeIfNotFull(task.get())) {
    err = E::INTERNAL;
    return -1;
  }
  task.release();

  STORAGE_TASK_STAT_INCR(stats_, thread_type, num_storage_tasks);
  STORAGE_TASK_TYPE_STAT_INCR(stats_, task_type, storage_tasks_posted);
  return 0;
}

int StorageThreadPool::tryPutWrite(std::unique_ptr<WriteStorageTask>&& task) {
  if (shutting_down_.load()) {
    err = E::SHUTDOWN;
    return -1;
  }

  auto thread_type = getThreadType(*task);
  auto& queue = taskQueues_[thread_type].write_queue;

  task->setStorageThreadPool(this);
  if (!queue.writeIfNotFull(task.get())) {
    err = E::INTERNAL;
    return -1;
  }

  task.release();
  return 0;
}

void StorageThreadPool::blockingPutTask(std::unique_ptr<StorageTask>&& task) {
  if (shutting_down_.load()) {
    return;
  }

  auto task_type = task->getType();
  auto thread_type = getThreadType(*task);
  auto& queue = taskQueues_[thread_type].queue;

  task->setStorageThreadPool(this);
  queue.blockingWrite(task.release());
  STORAGE_TASK_STAT_INCR(stats_, thread_type, num_storage_tasks);
  STORAGE_TASK_TYPE_STAT_INCR(stats_, task_type, storage_tasks_posted);
}

std::unique_ptr<StorageTask>
StorageThreadPool::blockingGetTask(StorageTask::ThreadType type) {
  auto& task_queue = taskQueues_[getThreadType(type)];

  while (true) {
    StorageTask* rawptr;
    task_queue.queue.blockingRead(rawptr);
    std::unique_ptr<StorageTask> task(rawptr);

    STORAGE_TASK_STAT_DECR(stats_, type, num_storage_tasks);

    // Check if we should drop the task.  Doing a load first to avoid an
    // std::atomic write in the common case when there is nothing to drop.
    if (task_queue.tasks_to_drop.load() > 0 && tryDropOneTask(task)) {
      continue;
    }

    STORAGE_TASK_STAT_INCR(stats_, type, storage_tasks_dequeued);
    return task;
  }
}

folly::small_vector<std::unique_ptr<WriteStorageTask>, 4>
StorageThreadPool::tryGetWriteBatch(StorageTask::ThreadType thread_type,
                                    size_t max_count,
                                    size_t max_bytes) {
  thread_type = getThreadType(thread_type);

  auto& task_queue = taskQueues_[thread_type];
  folly::small_vector<WriteStorageTask*, 4> raw_tasks =
      task_queue.write_queue.readBatchSinglePriority(max_count, max_bytes);
  folly::small_vector<std::unique_ptr<WriteStorageTask>, 4> res;
  res.reserve(raw_tasks.size());
  for (auto rawptr : raw_tasks) {
    res.emplace_back(rawptr);
  }
  return res;
}

void StorageThreadPool::enqueueForSync(std::unique_ptr<StorageTask> task) {
  syncing_thread_->enqueueForSync(std::move(task));
}

void StorageThreadPool::dropTaskQueue(StorageTask::ThreadType type) {
  auto& task_queue = taskQueues_[getThreadType(type)];
  ssize_t ntasks = task_queue.queue.size();
  if (ntasks > 0) {
    // Drop however many tasks there are currently in the queue.  This is
    // approximate (tasks are coming and going) but we don't need it to be
    // precise.
    task_queue.tasks_to_drop.store(ntasks);
  }
}

bool StorageThreadPool::tryDropOneTask(std::unique_ptr<StorageTask>& task) {
  if (!task->isDroppable()) {
    return false;
  }

  auto& task_queue = taskQueues_[getThreadType(*task)];

  // Try to acquire a ticket
  if (task_queue.tasks_to_drop.fetch_sub(1) <= 0) {
    // Give the ticket back.  This is not perfect; a dropTaskQueue() call may
    // have squeezed in since we tried to acquire the ticket, but the
    // consequence is one extra task dropped later, not a big deal.
    ++task_queue.tasks_to_drop;
    return false;
  }

  StorageTaskResponse::sendDroppedToWorker(std::move(task));
  return true;
}

StorageTask::ThreadType
StorageThreadPool::getThreadType(const StorageTask& task) const {
  return getThreadType(task.getThreadType());
}

StorageTask::ThreadType
StorageThreadPool::getThreadType(StorageTask::ThreadType type) const {
  // If there are no specialized threads of this type configured, pick a thread
  // of different type:
  // FAST_STALLABLE -> FAST_TIME_SENSITIVE -> SLOW
  // METADATA -> SLOW

  if (type == StorageTask::ThreadType::FAST_STALLABLE &&
      nthreads_fast_stallable_ == 0) {
    type = StorageTask::ThreadType::FAST_TIME_SENSITIVE;
  }
  if (type == StorageTask::ThreadType::FAST_TIME_SENSITIVE &&
      nthreads_fast_time_sensitive_ == 0) {
    type = StorageTask::ThreadType::SLOW;
  }
  if (type == StorageTask::ThreadType::METADATA && nthreads_metadata_ == 0) {
    type = StorageTask::ThreadType::SLOW;
  }
  return type;
}

void StorageThreadPool::getStorageTaskDebugInfo(InfoStorageTasksTable& table) {
  size_t seq_counter = 0;
  auto cb = [&](StorageTask* task,
                StorageTask::ThreadType /*thread_type*/,
                bool is_write_queue) {
    using F = InfoStorageTasksTableFieldOffsets;
    StorageTaskDebugInfo info = task->getDebugInfo();
    // Set common fields from `info` structure
    table.next()
        .set<F::SHARD_ID>(info.shard_id)
        .set<F::PRIORITY>(info.priority)
        .set<F::THREAD_TYPE>(info.thread_type)
        .set<F::TASK_TYPE>(info.task_type)
        .set<F::ENQUEUE_TIME>(info.enqueue_time)
        .set<F::DURABILITY>(info.durability);
    // Set thread-specific fields
    table.set<F::IS_WRITE_QUEUE>(is_write_queue)
        .set<F::SEQUENCE_NO>(++seq_counter);
    // Set optional fields if they are non-empty
    table.setOptional<F::LOG_ID>(info.log_id);
    table.setOptional<F::LSN>(info.lsn);
    table.setOptional<F::CLIENT_ID>(info.client_id);
    table.setOptional<F::CLIENT_ADDRESS>(info.client_address);
    table.setOptional<F::EXTRA_INFO>(info.extra_info);
  };

  for (int type = 0; type < (int)ThreadType::MAX; ++type) {
    const auto eType = static_cast<ThreadType>(type);
    taskQueues_[eType].queue.introspect_contents(
        std::bind(cb, std::placeholders::_1, eType, false));
    taskQueues_[eType].write_queue.introspect_contents(
        std::bind(cb, std::placeholders::_1, eType, true));
  }
}
}} // namespace facebook::logdevice
