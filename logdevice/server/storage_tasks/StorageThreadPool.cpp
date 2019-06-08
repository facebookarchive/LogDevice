/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/server/storage_tasks/StorageThreadPool.h"

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
    size_t num_shards,
    const ServerSettings::StoragePoolParams& params,
    UpdateableSettings<ServerSettings> server_settings,
    UpdateableSettings<Settings> settings,
    LocalLogStore* local_log_store,
    size_t task_queue_size,
    StatsHolder* stats,
    const std::shared_ptr<TraceLogger> trace_logger)
    : server_settings_(server_settings),
      settings_(settings),
      nthreads_slow_(params[(size_t)ThreadType::SLOW].nthreads),
      nthreads_fast_stallable_(
          params[(size_t)ThreadType::FAST_STALLABLE].nthreads),
      nthreads_fast_time_sensitive_(
          params[(size_t)ThreadType::FAST_TIME_SENSITIVE].nthreads),
      nthreads_default_(params[(size_t)ThreadType::DEFAULT].nthreads),
      useDRR_(settings->storage_tasks_use_drr),
      local_log_store_(local_log_store),
      processor_(nullptr),
      trace_logger_(trace_logger),
      stats_(stats),
      shard_idx_(shard_idx),
      num_shards_(num_shards),
      taskQueues_([&, task_queue_size]() {
        const auto actual_queue_sizes =
            computeActualQueueSizes(task_queue_size);

        std::vector<DRRPrincipal> principals;
        buildSchedulerPrincipals(principals);
        // All ThreadType get both the DRR queue and the Priority queue.
        // But DRR is implemented only for the SLOW ThreadType for now.
        for (int type = 0; type < (int)ThreadType::MAX; ++type) {
          const size_t size = actual_queue_sizes[type];
          taskQueues_.emplace_back(
              size, std::numeric_limits<size_t>::max(), stats);
          const auto eType = static_cast<ThreadType>(type);
          taskQueues_[eType].drrQueue.initShares(
              storageTaskThreadTypeName(eType),
              settings_->storage_tasks_drr_quanta,
              principals);
        }
      }) {
  ld_check(local_log_store != nullptr);
  ld_check(nthreads_slow_ > 0);

  // If you're adding a new ThreadType, please search this file for 'nthreads'
  // to find what need updating here.
  static_assert((int)ThreadType::SLOW == 0 &&
                    (int)ThreadType::FAST_STALLABLE == 1 &&
                    (int)ThreadType::FAST_TIME_SENSITIVE == 2 &&
                    (int)ThreadType::DEFAULT == 3,
                "");
  static_assert((int)ThreadType::MAX == 4, "");

  // Set memory budgets from settings.
  settings_subscription_ = settings_.callAndSubscribeToUpdates([this] {
    taskQueues_[ThreadType::FAST_TIME_SENSITIVE].memory_budget.setLimit(
        settings_->append_stores_max_mem_bytes / num_shards_);
    taskQueues_[ThreadType::FAST_STALLABLE].memory_budget.setLimit(
        settings_->rebuilding_stores_max_mem_bytes / num_shards_);

    std::vector<DRRPrincipal> principals;
    buildSchedulerPrincipals(principals);
    taskQueues_[ThreadType::SLOW].drrQueue.setShares(
        settings_->storage_tasks_drr_quanta, principals);
  });

  // Find an upper limit on the number of tasks in flight for this thread
  // pool, to size the syncing thread's queue
  size_t max_tasks_in_flight = 0;

  for (int type = 0; type < (int)ThreadType::MAX; ++type) {
    max_tasks_in_flight +=
        taskQueues_[static_cast<ThreadType>(type)].queue.max_capacity() /
        (ssize_t)StorageTaskPriority::NUM_PRIORITIES;
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
  if (nthreads_default_ == 0) {
    actual_queue_sizes[(int)ThreadType::SLOW] +=
        actual_queue_sizes[(int)ThreadType::DEFAULT];
    // Since we're not configured to run with this type of threads,
    // their queue will be unused. Make it as small as possible.
    // Could be 0 but MPMCQueue doesn't consider it a valid size.
    actual_queue_sizes[(int)ThreadType::DEFAULT] = 1;
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
      nthreads_default_ + nthreads_fast_time_sensitive_;
  auto remaining_threads =
      std::make_shared<std::atomic<int>>(total_num_threads);
  int stop_tasks_enqueued = 0;

  auto do_stop = [&](PerTypeTaskQueue& task_queue, size_t nthreads, bool drr) {
    // Ask storage threads to drop as many tasks as possible in case the queue
    // is backed up
    const size_t ndrop =
        std::max(task_queue.drrQueue.size(), task_queue.queue.max_capacity());
    task_queue.tasks_to_drop.store(ndrop);

    for (size_t i = 0; i < nthreads; ++i) {
      std::unique_ptr<StorageTask> task = std::make_unique<StopExecStorageTask>(
          shard_idx_, remaining_threads, persist_record_caches);
      task->setStorageThreadPool(this);
      if (drr) {
        uint64_t principal = static_cast<uint64_t>(task->getPrincipal());
        task_queue.drrQueue.enqueue(task.release(), principal);
      } else {
        task_queue.queue.blockingWrite(task.release());
      }
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

  do_stop(taskQueues_[StorageTask::ThreadType::SLOW], nthreads_slow_, useDRR_);
  do_stop(taskQueues_[StorageTask::ThreadType::FAST_STALLABLE],
          nthreads_fast_stallable_,
          false);
  do_stop(
      taskQueues_[StorageTask::ThreadType::DEFAULT], nthreads_default_, false);
  do_stop(taskQueues_[StorageTask::ThreadType::FAST_TIME_SENSITIVE],
          nthreads_fast_time_sensitive_,
          false);

  // Unstall stalled low priority writes if they were stuck waiting for flushed
  // data to complete.
  local_log_store_->disableWriteStalling();

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
  shutdown_complete_.store(true);
}

int StorageThreadPool::tryPutTask(std::unique_ptr<StorageTask>&& task) {
  if (shutting_down_.load()) {
    err = E::SHUTDOWN;
    return -1;
  }

  auto task_type = task->getType();
  auto thread_type = getThreadType(*task);

  task->setStorageThreadPool(this);

  bool ret = true;
  if (useDRR_ && (thread_type == StorageTask::ThreadType::SLOW)) {
    uint64_t principal = static_cast<uint64_t>(task->getPrincipal());
    taskQueues_[thread_type].drrQueue.enqueue(task.get(), principal);
  } else {
    ret = taskQueues_[thread_type].queue.writeIfNotFull(task.get());
  }

  if (!ret) {
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

bool StorageThreadPool::blockingPutTask(std::unique_ptr<StorageTask>&& task) {
  if (shutting_down_.load()) {
    err = E::SHUTDOWN;
    return false;
  }

  auto task_type = task->getType();
  auto thread_type = getThreadType(*task);

  task->setStorageThreadPool(this);
  if (useDRR_ && (thread_type == StorageTask::ThreadType::SLOW)) {
    uint64_t principal = static_cast<uint64_t>(task->getPrincipal());
    taskQueues_[thread_type].drrQueue.enqueue(task.release(), principal);
  } else {
    taskQueues_[thread_type].queue.blockingWrite(task.release());
  }
  STORAGE_TASK_STAT_INCR(stats_, thread_type, num_storage_tasks);
  STORAGE_TASK_TYPE_STAT_INCR(stats_, task_type, storage_tasks_posted);
  return true;
}

std::unique_ptr<StorageTask>
StorageThreadPool::blockingGetTask(StorageTask::ThreadType type) {
  auto& task_queue = taskQueues_[getThreadType(type)];
  std::map<StorageTaskType, int> dropped_by_type;

  while (true) {
    StorageTask* rawptr;
    if (useDRR_ && (type == StorageTask::ThreadType::SLOW)) {
      rawptr = task_queue.drrQueue.blockingDequeue();
    } else {
      task_queue.queue.blockingRead(rawptr);
    }

    std::unique_ptr<StorageTask> task(rawptr);

    STORAGE_TASK_STAT_DECR(stats_, type, num_storage_tasks);

    // Check if we should drop the task.  Doing a load first to avoid an
    // std::atomic write in the common case when there is nothing to drop.
    if (task_queue.tasks_to_drop.load() > 0 &&
        tryDropOneTask(task, dropped_by_type)) {
      continue;
    }

    if (!dropped_by_type.empty()) {
      RATELIMIT_INFO(std::chrono::seconds(1),
                     2,
                     "Dropping storage tasks in shard %d: %s",
                     (int)shard_idx_,
                     toString(dropped_by_type).c_str());
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
  ssize_t ntasks;
  if (useDRR_ && (type == StorageTask::ThreadType::SLOW)) {
    ntasks = task_queue.drrQueue.size();
  } else {
    ntasks = task_queue.queue.size();
  }

  if (ntasks > 0) {
    // Drop however many tasks there are currently in the queue.  This is
    // approximate (tasks are coming and going) but we don't need it to be
    // precise.
    task_queue.tasks_to_drop.store(ntasks);
  }
}

bool StorageThreadPool::tryDropOneTask(
    std::unique_ptr<StorageTask>& task,
    std::map<StorageTaskType, int>& dropped_by_type) {
  auto& task_queue = taskQueues_[getThreadType(*task)];

  // Try to acquire a ticket
  if (task_queue.tasks_to_drop.fetch_sub(1) <= 0) {
    // Give the ticket back.  This is not perfect; a dropTaskQueue() call may
    // have squeezed in since we tried to acquire the ticket, but the
    // consequence is one extra task dropped later, not a big deal.
    ++task_queue.tasks_to_drop;
    return false;
  }

  if (!task->isDroppable()) {
    // Don't drop the task but decrease tasks_to_drop.
    return false;
  }

  ++dropped_by_type[task->getType()];
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
  if (type == StorageTask::ThreadType::DEFAULT && nthreads_default_ == 0) {
    type = StorageTask::ThreadType::SLOW;
  }
  return type;
}

ResourceBudget&
StorageThreadPool::getMemoryBudget(StorageTask::ThreadType thread_type) {
  return taskQueues_[thread_type].memory_budget;
}

void StorageThreadPool::getStorageTaskDebugInfo(InfoStorageTasksTable& table) {
  size_t seq_counter = 0;
  auto cb = [&](const StorageTask* task,
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
    if (useDRR_ && (eType == StorageTask::ThreadType::SLOW)) {
      taskQueues_[eType].drrQueue.introspect_contents(
          std::bind(cb, std::placeholders::_1, eType, false));
    } else {
      taskQueues_[eType].queue.introspect_contents(
          std::bind(cb, std::placeholders::_1, eType, false));
    }
    taskQueues_[eType].write_queue.introspect_contents(
        std::bind(cb, std::placeholders::_1, eType, true));
  }
}
}} // namespace facebook::logdevice
