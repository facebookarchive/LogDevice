/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include <atomic>
#include <semaphore.h>

#include <folly/Memory.h>
#include <gtest/gtest.h>

#include "logdevice/common/NoopTraceLogger.h"
#include "logdevice/common/Request.h"
#include "logdevice/common/debug.h"
#include "logdevice/server/ServerProcessor.h"
#include "logdevice/server/ServerWorker.h"
#include "logdevice/server/locallogstore/LocalLogStore.h"
#include "logdevice/server/locallogstore/test/TemporaryLogStore.h"
#include "logdevice/server/storage_tasks/PerWorkerStorageTaskQueue.h"
#include "logdevice/server/storage_tasks/ShardedStorageThreadPool.h"
#include "logdevice/server/storage_tasks/StorageTask.h"
#include "logdevice/server/storage_tasks/StorageThreadPool.h"
#include "logdevice/server/test/TestUtil.h"

using namespace facebook::logdevice;
using Params = ServerSettings::StoragePoolParams;

class FakeShardedLocalLogStore : public ShardedLocalLogStore {
  int numShards() const override {
    return 1;
  }
  LocalLogStore* getByIndex(int /*idx*/) override {
    return &store_;
  }

  TemporaryRocksDBStore store_;
};

Semaphore task_ready_sem, task_start_sem, task_done_sem, task_dropped_sem;
std::atomic<int> done(0), dropped(0);

class TestStorageTask : public StorageTask {
 public:
  TestStorageTask() : StorageTask(StorageTask::Type::UNKNOWN) {}

  void execute() override {
    task_ready_sem.post();
    task_start_sem.wait();
  }
  void onDone() override {
    ++done;
    task_done_sem.post();
  }
  void onDropped() override {
    ++dropped;
    task_dropped_sem.post();
  }
  void onStorageThreadDrop() override {
    // This should have been set
    EXPECT_NE(nullptr, storageThreadPool_);
    // This should not be set because we never executed the task
    EXPECT_EQ(nullptr, storageThread_);
  }
};

class MakeTasksRequest : public Request {
 public:
  explicit MakeTasksRequest(int ntasks)
      : Request(RequestType::TEST_STORAGE_THREADPOOL_MAKE_TASK_REQUEST),
        ntasks_(ntasks) {}

  Execution execute() override {
    PerWorkerStorageTaskQueue* queue =
        ServerWorker::onThisThread()->getStorageTaskQueueForShard(0);

    for (int i = 0; i < ntasks_; ++i) {
      queue->putTask(std::make_unique<TestStorageTask>());
    }
    return Execution::COMPLETE;
  }

 private:
  int ntasks_;
};

TEST(StorageThreadPoolTest, QueueDrop) {
  Settings settings = create_default_settings<Settings>(); // default settings
  settings.num_workers = 1;
  settings.per_worker_storage_task_queue_size = 1000;
  settings.max_inflight_storage_tasks = 1000;
  UpdateableSettings<Settings> updateable_settings(settings);
  ServerSettings server_settings =
      create_default_settings<ServerSettings>(); // default settings
  UpdateableSettings<ServerSettings> updateable_server_settings(
      server_settings);

  FakeShardedLocalLogStore store;

  const int nstorage_threads = 8;
  Params params;
  params[(size_t)StorageTaskThreadType::SLOW].nthreads = nstorage_threads;
  size_t shared_task_queue_size =
      settings.num_workers * settings.max_inflight_storage_tasks;
  ShardedStorageThreadPool sharded_pool(&store,
                                        params,
                                        updateable_server_settings,
                                        updateable_settings,
                                        shared_task_queue_size,
                                        nullptr // stats
  );

  auto processor_builder = TestServerProcessorBuilder{settings}
                               .setServerSettings(server_settings)
                               .setShardedStorageThreadPool(&sharded_pool);
  auto processor = std::move(processor_builder).build();
  sharded_pool.setProcessor(processor.get());

  Alarm alarm(std::chrono::seconds(5));

  ld_check(task_ready_sem.value() == 0);
  ld_check(task_start_sem.value() == 0);
  ld_check(task_done_sem.value() == 0);
  ld_check(task_dropped_sem.value() == 0);

  // Create 2000 tasks:
  // - 1000 (max_inflight_storage_tasks) of them will go on the storage thread
  //   queue
  // - 8 of the above will start executing on storage threads and block on
  //   task_start_sem
  // - Another 1000 will get buffered inside the worker
  {
    std::unique_ptr<Request> req = std::make_unique<MakeTasksRequest>(2000);
    processor->blockingRequest(req);
  }

  EXPECT_EQ(0, done.load());
  EXPECT_EQ(0, dropped.load());

  // Wait for the 8 tasks to get picked up by storage threads
  for (int i = 0; i < nstorage_threads; ++i) {
    task_ready_sem.wait();
  }

  // One more task should trigger a queue drop inside the worker
  {
    std::unique_ptr<Request> req = std::make_unique<MakeTasksRequest>(1);
    processor->blockingRequest(req);
  }
  EXPECT_EQ(0, done.load());
  EXPECT_EQ(1000, dropped.load());

  // Now there is a TestStorageTask blocking every storage thread that will
  // execute successfully.  There is also the one that triggered the queue
  // drop.
  int expected_done = nstorage_threads + 1;
  int expected_dropped = 2001 - expected_done;

  // When we let the tasks blocking storage threads complete ...
  for (int i = 0; i < expected_done; ++i) {
    task_start_sem.post();
  }
  for (int i = 0; i < expected_done; ++i) {
    task_done_sem.wait();
  }
  EXPECT_EQ(expected_done, done.load());

  // ... the 992 that were waiting in the storage thread pool queue should
  // have got dropped.
  for (int i = 0; i < expected_dropped; ++i) {
    task_dropped_sem.wait();
  }
  EXPECT_EQ(expected_dropped, dropped.load());

  shutdown_test_server(processor);
}

// Trivial task that can be made non-droppable and invokes a callback if
// dropped.
class PrioritizableStorageTask : public StorageTask {
 public:
  PrioritizableStorageTask(bool droppable,
                           std::function<void()> drop_cb,
                           std::function<void()> destroy_cb)
      : StorageTask(StorageTask::Type::UNKNOWN),
        droppable_(droppable),
        drop_cb_(std::move(drop_cb)),
        destroy_cb_(std::move(destroy_cb)) {}
  ~PrioritizableStorageTask() override {
    destroy_cb_();
  }

  void execute() override {}
  void onDone() override {}
  void onDropped() override {
    drop_cb_();
  }
  bool isDroppable() const override {
    return droppable_;
  }

 private:
  bool droppable_;
  std::function<void()> drop_cb_;
  std::function<void()> destroy_cb_;
};

class PutStorageTaskRequest : public Request {
 public:
  explicit PutStorageTaskRequest(std::unique_ptr<StorageTask> task)
      : Request(RequestType::TEST_STORAGE_THREADPOOL_PUT_STORAGE_TASK_REQUEST),
        task_(std::move(task)) {}

  Execution execute() override {
    PerWorkerStorageTaskQueue* queue =
        ServerWorker::onThisThread()->getStorageTaskQueueForShard(0);
    queue->putTask(std::move(task_));
    return Execution::COMPLETE;
  }

 private:
  std::unique_ptr<StorageTask> task_;
};

TEST(StorageThreadPoolTest, DontDropHighPriority) {
  std::set<int> cb_dropped_contexts;
  std::set<int> cb_destroyed_contexts;
  std::mutex cb_mutex;
  Semaphore sem;

  Settings settings = create_default_settings<Settings>(); // default settings
  settings.num_workers = 1;
  // Queue at most one normal priority task
  settings.per_worker_storage_task_queue_size = 1;
  // Don't send any tasks to storage threads
  settings.max_inflight_storage_tasks = 0;
  UpdateableSettings<Settings> updateable_settings(settings);
  ServerSettings server_settings =
      create_default_settings<ServerSettings>(); // default settings
  UpdateableSettings<ServerSettings> updateable_server_settings(
      server_settings);

  Params params;
  params[(size_t)StorageTaskThreadType::SLOW].nthreads = 1;

  FakeShardedLocalLogStore store;
  ShardedStorageThreadPool sharded_pool(&store,
                                        params,
                                        updateable_server_settings,
                                        updateable_settings,
                                        100,    // shared task queue size
                                        nullptr // stats
  );

  auto processor_builder = TestServerProcessorBuilder{settings}
                               .setServerSettings(server_settings)
                               .setShardedStorageThreadPool(&sharded_pool);
  auto processor = std::move(processor_builder).build();
  sharded_pool.setProcessor(processor.get());

  Alarm alarm(std::chrono::seconds(5));

  auto drop_cb = [&](int context) {
    std::lock_guard<std::mutex> guard(cb_mutex);
    cb_dropped_contexts.insert(context);
    sem.post();
  };
  auto destroy_cb = [&](int context) {
    std::lock_guard<std::mutex> guard(cb_mutex);
    cb_destroyed_contexts.insert(context);
    sem.post();
  };
  std::vector<std::unique_ptr<StorageTask>> tasks;
  auto add_task = [&](bool droppable, int context) {
    tasks.push_back(std::make_unique<PrioritizableStorageTask>(
        droppable,
        std::bind(drop_cb, context),
        std::bind(destroy_cb, context)));
  };
  // Put three tasks: one normal, one hipri and another normal.  The third
  // should cause a queue drop (settings.per_worker_storage_task_queue_size is
  // 1) but only the first task should be dropped (since the second is hipri)
  add_task(true, 1);
  add_task(false, 2);
  add_task(true, 3);
  for (auto& task : tasks) {
    std::unique_ptr<Request> req =
        std::make_unique<PutStorageTaskRequest>(std::move(task));
    processor->blockingRequest(req);
  }

  // Check that only task 1 (the first normal-pri) was dropped
  {
    sem.wait();
    sem.wait();
    std::lock_guard<std::mutex> guard(cb_mutex);
    std::set<int> expected_contexts = {1};
    ASSERT_EQ(expected_contexts, cb_dropped_contexts);
    ASSERT_EQ(expected_contexts, cb_destroyed_contexts);
  }

  // Processor shutdown should cause the other two tasks to be destroyed,
  // calling onDropped() only on droppable task
  shutdown_test_server(processor);
  processor.reset();
  {
    sem.wait();
    sem.wait();
    sem.wait();
    std::lock_guard<std::mutex> guard(cb_mutex);
    std::set<int> expected_dropped_contexts = {1, 3};
    std::set<int> expected_destroyed_contexts = {1, 2, 3};
    ASSERT_EQ(expected_dropped_contexts, cb_dropped_contexts);
    ASSERT_EQ(expected_destroyed_contexts, cb_destroyed_contexts);
  }

  ASSERT_EQ(0, sem.value());
}
