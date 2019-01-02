/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/server/storage_tasks/StorageThreadPool.h"

#include <atomic>

#include <folly/Memory.h>
#include <folly/synchronization/Baton.h>
#include <gtest/gtest.h>

#include "logdevice/common/Semaphore.h"
#include "logdevice/common/test/TestUtil.h"
#include "logdevice/server/locallogstore/test/TemporaryLogStore.h"
#include "logdevice/server/storage_tasks/StorageTask.h"
#include "logdevice/server/storage_tasks/WriteStorageTask.h"

using namespace facebook::logdevice;
using Params = ServerSettings::StoragePoolParams;

namespace {

struct SimpleStorageTask : public StorageTask {
  explicit SimpleStorageTask(Semaphore* sem)
      : StorageTask(StorageTask::Type::UNKNOWN), sem_(sem) {}
  void execute() override {
    sem_->post();
  }
  void onDone() override {}
  void onDropped() override {
    ld_check(false);
  }

  Semaphore* sem_;
};

struct TestTask : public StorageTask {
  TestTask(ThreadType thread_type, std::function<void()> fn)
      : StorageTask(StorageTask::Type::UNKNOWN),
        thread_type(thread_type),
        fn(fn) {}

  void execute() override {
    fn();
  }
  ThreadType getThreadType() const override {
    return thread_type;
  }
  void onDone() override {}
  void onDropped() override {
    ld_check(false && "Storage task dropped");
  }

  ThreadType thread_type;
  std::function<void()> fn;
};

} // namespace

class MockWriteStorageTask : public WriteStorageTask {
 public:
  explicit MockWriteStorageTask(ThreadType thread_type,
                                size_t mock_payload_size = 0)
      : WriteStorageTask(StorageTask::Type::UNKNOWN),
        thread_type_(thread_type) {
    payload_size_ = mock_payload_size;
  }

  size_t getPayloadSize() const override {
    return payload_size_;
  }

  ThreadType getThreadType() const override {
    return thread_type_;
  }

  void onDone() override {}

  void onDropped() override {}

  size_t getNumWriteOps() const override {
    return 0;
  }

  size_t getWriteOps(const WriteOp**, size_t) const override {
    return 0;
  }

 private:
  ThreadType thread_type_;
  size_t payload_size_;
};

/**
 * Spins up storage thread pool, has it do some trivial tasks, verifies that
 * the pool can cleanly shut down. The seconds iteration drives the DRR
 * scheduler code path.
 */
TEST(StorageThreadPoolTest, Basic) {
  for (int testIter = 0; testIter < 2; testIter++) {
    ld_info("starting test iter %d", testIter);
    Settings init_settings = create_default_settings<Settings>();
    if (testIter == 1) {
      init_settings.storage_tasks_use_drr = true;
    }
    UpdateableSettings<Settings> settings(init_settings);
    ServerSettings init_server_settings =
        create_default_settings<ServerSettings>();
    UpdateableSettings<ServerSettings> server_settings(init_server_settings);

    Params params;
    params[(size_t)StorageTaskThreadType::SLOW].nthreads = 4;
    const int task_queue_slots = 16;
    // Number of tasks intentionally more than task queue slots
    const int ntasks = 3 * task_queue_slots;

    TemporaryRocksDBStore store;
    auto pool = std::make_unique<StorageThreadPool>(
        0, 1, params, server_settings, settings, &store, task_queue_slots);

    Semaphore sem;
    for (int i = 0; i < ntasks; ++i) {
      bool ret =
          pool->blockingPutTask(std::make_unique<SimpleStorageTask>(&sem));
      ASSERT_TRUE(ret);
    }
    // Wait until all tasks have finished
    for (int i = 0; i < ntasks; ++i) {
      sem.wait();
    }
    pool.reset();
  }
}

// A slow storage task that needs syncing should not be dropped on the floor
// during shutdown
TEST(StorageThreadPoolTest, SyncingShutdown) {
  class SyncingTestTask : public StorageTask {
   public:
    explicit SyncingTestTask(folly::Baton<>* started,
                             std::atomic<bool>* executed,
                             std::atomic<bool>* synced)
        : StorageTask(StorageTask::Type::UNKNOWN),
          started_(started),
          executed_(executed),
          synced_(synced) {}

    void execute() override {
      started_->post();
      // Simulate a slow I/O op
      std::this_thread::sleep_for(std::chrono::milliseconds(200));
      executed_->store(true);
    }
    Durability durability() const override {
      return Durability::SYNC_WRITE;
    }
    void onSynced() override {
      synced_->store(true);
    }
    void onDone() override {}
    void onDropped() override {}

   private:
    folly::Baton<>* started_;
    std::atomic<bool>* executed_;
    std::atomic<bool>* synced_;
  };

  for (int testIter = 0; testIter < 2; testIter++) {
    ld_info("starting test iter %d", testIter);
    Settings init_settings = create_default_settings<Settings>();
    if (testIter == 1) {
      init_settings.storage_tasks_use_drr = true;
    }
    UpdateableSettings<Settings> settings(init_settings);
    ServerSettings init_server_settings =
        create_default_settings<ServerSettings>();
    UpdateableSettings<ServerSettings> server_settings(init_server_settings);

    Params params;
    params[(size_t)StorageTaskThreadType::SLOW].nthreads = 1;
    const int task_queue_slots = 16;

    TemporaryRocksDBStore store;
    auto pool = std::make_unique<StorageThreadPool>(
        0, 1, params, server_settings, settings, &store, task_queue_slots);

    folly::Baton<> started;
    std::atomic<bool> executed(false);
    std::atomic<bool> synced(false);
    bool ret = pool->blockingPutTask(
        std::make_unique<SyncingTestTask>(&started, &executed, &synced));
    ASSERT_TRUE(ret);
    started.wait();
    // Shut down the pool.  Since the task has already started executing, it has
    // to finish before the pool can shut down.
    pool.reset();

    ASSERT_TRUE(executed);
    ASSERT_TRUE(synced);
  }
}

// Creates a storage thread pool consisting of two threads, one for slow, and
// the other for fast tasks. Verifies that two tasks of different types can
// run at the same time.
TEST(StorageThreadPoolTest, DifferentPriorities) {
  Alarm alarm(std::chrono::seconds(60));
  Params params;
  params[(size_t)StorageTaskThreadType::SLOW].nthreads = 1;
  params[(size_t)StorageTaskThreadType::FAST_TIME_SENSITIVE].nthreads = 1;
  params[(size_t)StorageTaskThreadType::FAST_STALLABLE].nthreads = 1;
  params[(size_t)StorageTaskThreadType::DEFAULT].nthreads = 1;
  Semaphore sem1;
  TemporaryRocksDBStore store;

  for (int testIter = 0; testIter < 2; testIter++) {
    ld_info("starting test iter %d", testIter);
    Settings init_settings = create_default_settings<Settings>();
    if (testIter == 1) {
      init_settings.storage_tasks_use_drr = true;
    }
    UpdateableSettings<Settings> settings(init_settings);
    ServerSettings init_server_settings =
        create_default_settings<ServerSettings>();
    UpdateableSettings<ServerSettings> server_settings(init_server_settings);
    StorageThreadPool pool(0, // shard idx
                           1, // num shards
                           params,
                           server_settings,
                           settings,
                           &store,
                           16); // task queue size

    const int ntasks = 10;

    // Create several "slow" tasks that block (wait on a semaphore).
    for (int i = 0; i < ntasks; ++i) {
      auto task = std::make_unique<TestTask>(
          StorageTask::ThreadType::SLOW, [&]() { sem1.wait(); });
      ASSERT_EQ(0, pool.tryPutTask(std::move(task)));
    }

    // A fast "write" task ought to complete first because there's a dedicated
    // thread eager to execute it.
    Semaphore sem2;
    auto task = std::make_unique<TestTask>(
        StorageTask::ThreadType::FAST_TIME_SENSITIVE, [&]() { sem2.post(); });
    ASSERT_EQ(0, pool.tryPutTask(std::move(task)));

    sem2.wait();
    for (int i = 0; i < ntasks; ++i) {
      sem1.post();
    }
  }
}

TEST(StorageThreadPoolTest, IOPrio) {
  Settings init_settings = create_default_settings<Settings>();
  init_settings.slow_ioprio = std::make_pair(2, 2);
  Params params;
  params[(size_t)StorageTaskThreadType::SLOW].nthreads = 1;
  params[(size_t)StorageTaskThreadType::FAST_TIME_SENSITIVE].nthreads = 1;
  params[(size_t)StorageTaskThreadType::FAST_STALLABLE].nthreads = 1;
  params[(size_t)StorageTaskThreadType::DEFAULT].nthreads = 1;

  TemporaryRocksDBStore store;
  for (int testIter = 0; testIter < 2; testIter++) {
    ld_info("starting test iter %d", testIter);
    if (testIter == 1) {
      init_settings.storage_tasks_use_drr = true;
    }
    UpdateableSettings<Settings> settings(init_settings);
    ServerSettings init_server_settings =
        create_default_settings<ServerSettings>();
    UpdateableSettings<ServerSettings> server_settings(init_server_settings);

    StorageThreadPool pool(0, // shard idx
                           1, // num shards
                           params,
                           server_settings,
                           settings,
                           &store,
                           16); // task queue size

    std::pair<int, int> default_prio(-1, -1);

    // Get default IO priority by creating a new thread and calling
    // get_io_priority_of_this_thread() on it. It's better than calling
    // get_io_priority_of_this_thread() on the main thread because other tests
    // could have changed it.
    std::thread([&default_prio] {
      int rv = get_io_priority_of_this_thread(&default_prio);
      EXPECT_EQ(0, rv);
    })
        .join();

    Semaphore sem;
    auto task =
        std::make_unique<TestTask>(StorageTask::ThreadType::SLOW, [&]() {
          std::pair<int, int> prio(-1, -1);
          int rv = get_io_priority_of_this_thread(&prio);
          EXPECT_EQ(0, rv);
          EXPECT_EQ(std::make_pair(2, 2), prio);
          sem.post();
        });
    ASSERT_EQ(0, pool.tryPutTask(std::move(task)));

    task = std::make_unique<TestTask>(
        StorageTask::ThreadType::FAST_TIME_SENSITIVE, [&]() {
          std::pair<int, int> prio(-1, -1);
          int rv = get_io_priority_of_this_thread(&prio);
          EXPECT_EQ(0, rv);
          EXPECT_EQ(default_prio, prio);
          sem.post();
        });
    ASSERT_EQ(0, pool.tryPutTask(std::move(task)));

    task = std::make_unique<TestTask>(
        StorageTask::ThreadType::FAST_STALLABLE, [&]() {
          std::pair<int, int> prio(-1, -1);
          int rv = get_io_priority_of_this_thread(&prio);
          EXPECT_EQ(0, rv);
          EXPECT_EQ(default_prio, prio);
          sem.post();
        });
    ASSERT_EQ(0, pool.tryPutTask(std::move(task)));

    // default thread should have the same io priority as the slow thread
    task = std::make_unique<TestTask>(StorageTask::ThreadType::DEFAULT, [&]() {
      std::pair<int, int> prio(-1, -1);
      int rv = get_io_priority_of_this_thread(&prio);
      EXPECT_EQ(0, rv);
      EXPECT_EQ(std::make_pair(2, 2), prio);
      sem.post();
    });
    ASSERT_EQ(0, pool.tryPutTask(std::move(task)));

    sem.wait();
    sem.wait();
    sem.wait();
    sem.wait();
  }
}

TEST(StorageThreadPoolTest, BatchLimits) {
  Settings init_settings = create_default_settings<Settings>();
  Params params;
  params[(size_t)StorageTaskThreadType::SLOW].nthreads = 1;
  params[(size_t)StorageTaskThreadType::FAST_TIME_SENSITIVE].nthreads = 1;
  params[(size_t)StorageTaskThreadType::FAST_STALLABLE].nthreads = 1;
  params[(size_t)StorageTaskThreadType::DEFAULT].nthreads = 1;
  auto limit = init_settings.write_batch_size;
  auto byte_limit = init_settings.write_batch_bytes;

  TemporaryRocksDBStore store;
  for (int testIter = 0; testIter < 2; testIter++) {
    ld_info("starting test iter %d", testIter);
    if (testIter == 1) {
      init_settings.storage_tasks_use_drr = true;
    }
    UpdateableSettings<Settings> settings(init_settings);
    ServerSettings init_server_settings =
        create_default_settings<ServerSettings>();
    UpdateableSettings<ServerSettings> server_settings(init_server_settings);

    StorageThreadPool pool(0, // shard idx
                           1, // num shards
                           params,
                           server_settings,
                           settings,
                           &store,
                           limit + 1); // task queue size

    auto ttype = MockWriteStorageTask::ThreadType::SLOW;

    // Make sure byte_limit is respected
    ASSERT_EQ(0,
              pool.tryPutWrite(std::make_unique<MockWriteStorageTask>(
                  ttype, byte_limit - 1)));
    ASSERT_EQ(
        0, pool.tryPutWrite(std::make_unique<MockWriteStorageTask>(ttype, 2)));
    ASSERT_EQ(
        0, pool.tryPutWrite(std::make_unique<MockWriteStorageTask>(ttype)));
    auto res = pool.tryGetWriteBatch(ttype, limit, byte_limit);
    ASSERT_EQ(2, res.size());

    // Make sure (task) limit is respected
    for (auto i = 0; i < limit; i++) {
      ASSERT_EQ(
          0, pool.tryPutWrite(std::make_unique<MockWriteStorageTask>(ttype)));
    }
    res = pool.tryGetWriteBatch(ttype, limit, byte_limit);
    ASSERT_EQ(limit, res.size());

    // Make sure we get the rest of the queue even though neither limit reached
    res = pool.tryGetWriteBatch(ttype, limit, byte_limit);
    ASSERT_EQ(1, res.size());
  }
}
