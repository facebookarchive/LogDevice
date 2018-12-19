/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <atomic>

#include <folly/Memory.h>
#include <folly/synchronization/Baton.h>
#include <gtest/gtest.h>

#include "logdevice/common/Semaphore.h"
#include "logdevice/common/test/TestUtil.h"
#include "logdevice/server/locallogstore/test/TemporaryLogStore.h"
#include "logdevice/server/storage_tasks/StorageTask.h"
#include "logdevice/server/storage_tasks/StorageThreadPool.h"
#include "logdevice/server/storage_tasks/WriteStorageTask.h"

using namespace facebook::logdevice;
using Params = ServerSettings::StoragePoolParams;
using SteadyClock = std::chrono::steady_clock;

namespace {

static const int64_t TIMEOUT_MS = 1000;
static const int64_t ARRIVAL_TIMEOUT_MS = 500;

struct TestStorageTaskBase : public StorageTask {
  TestStorageTaskBase(Semaphore* sem, int64_t min_timeout, int64_t max_timeout)
      : StorageTask(StorageTask::Type::UNKNOWN),
        sem_(sem),
        start_(SteadyClock::now()),
        min_timeout_(min_timeout),
        max_timeout_(max_timeout) {}
  void execute() override {}
  void onDone() override {}
  void onDropped() override {
    ld_check(false);
  }
  Durability durability() const override {
    return Durability::SYNC_WRITE;
  }
  void onSynced() override {
    auto end = SteadyClock::now();
    EXPECT_GE(
        std::chrono::duration_cast<std::chrono::milliseconds>(end - start_)
            .count(),
        min_timeout_);
    EXPECT_LT(
        std::chrono::duration_cast<std::chrono::milliseconds>(end - start_)
            .count(),
        max_timeout_);
    sem_->post();
  }

  Semaphore* sem_;
  SteadyClock::time_point start_;
  int64_t min_timeout_;
  int64_t max_timeout_;
};

struct DelayableStorageTask : public TestStorageTaskBase {
  explicit DelayableStorageTask(Semaphore* sem,
                                int64_t min_timeout,
                                int64_t max_timeout)
      : TestStorageTaskBase(sem, min_timeout, max_timeout) {}
};

struct UndelayableStorageTask : public TestStorageTaskBase {
  explicit UndelayableStorageTask(Semaphore* sem,
                                  int64_t min_timeout,
                                  int64_t max_timeout)
      : TestStorageTaskBase(sem, min_timeout, max_timeout) {}
  bool allowDelayingSync() const override {
    return false;
  }
};
} // namespace

/**
 * Run thread, put some delayable tasks, wait for completion
 * check that more than TIMEOUT has passed
 */
TEST(SyncingStorageThreadTest, BurstDelayableTasks) {
  UpdateableSettings<Settings> settings;
  ServerSettings init_server_settings =
      create_default_settings<ServerSettings>();
  init_server_settings.storage_thread_delaying_sync_interval =
      std::chrono::milliseconds(TIMEOUT_MS);
  UpdateableSettings<ServerSettings> server_settings(init_server_settings);

  Params params;
  params[(size_t)StorageTaskThreadType::SLOW].nthreads = 4;
  const int task_queue_slots = 4;
  const int ntasks = 16;

  TemporaryRocksDBStore store;
  auto pool = std::make_unique<StorageThreadPool>(
      0, 1, params, server_settings, settings, &store, task_queue_slots);

  Semaphore sem;
  for (int i = 0; i < ntasks; ++i) {
    pool->enqueueForSync(std::make_unique<DelayableStorageTask>(
        &sem, TIMEOUT_MS, TIMEOUT_MS * 3));
  }
  // Wait until all tasks have finished
  for (int i = 0; i < ntasks; ++i) {
    sem.wait();
  }

  pool.reset();
}

/**
 * Run thread, put some undelayable tasks, wait for completion
 * check that less than ARRIVAL_TIMEOUT passed
 */
TEST(SyncingStorageThreadTest, BurstUndelayableTasks) {
  UpdateableSettings<Settings> settings;
  ServerSettings init_server_settings =
      create_default_settings<ServerSettings>();
  init_server_settings.storage_thread_delaying_sync_interval =
      std::chrono::milliseconds(TIMEOUT_MS);
  UpdateableSettings<ServerSettings> server_settings(init_server_settings);

  Params params;
  params[(size_t)StorageTaskThreadType::SLOW].nthreads = 4;
  const int task_queue_slots = 4;
  const int ntasks = 16;

  TemporaryRocksDBStore store;
  auto pool = std::make_unique<StorageThreadPool>(
      0, 1, params, server_settings, settings, &store, task_queue_slots);

  Semaphore sem;
  for (int i = 0; i < ntasks; ++i) {
    pool->enqueueForSync(
        std::make_unique<UndelayableStorageTask>(&sem, 0, ARRIVAL_TIMEOUT_MS));
  }
  // Wait until all tasks have finished
  for (int i = 0; i < ntasks; ++i) {
    sem.wait();
  }

  pool.reset();
}

/**
 * Run thread, put some delayable tasks, then undelayable after some time,
 * wait for completion
 * check that execution time is between ARRIVAL_TIMEOUT and TIMEOUT
 * for delayable part and immediately for undelayable
 */
TEST(SyncingStorageThreadTest, DelayableAndLaterUndelayableTasks) {
  UpdateableSettings<Settings> settings;
  ServerSettings init_server_settings =
      create_default_settings<ServerSettings>();
  init_server_settings.storage_thread_delaying_sync_interval =
      std::chrono::milliseconds(TIMEOUT_MS);
  UpdateableSettings<ServerSettings> server_settings(init_server_settings);

  Params params;
  params[(size_t)StorageTaskThreadType::SLOW].nthreads = 4;
  const int task_queue_slots = 4;
  const int ntasks = 8;

  TemporaryRocksDBStore store;
  auto pool = std::make_unique<StorageThreadPool>(
      0, 1, params, server_settings, settings, &store, task_queue_slots);

  Semaphore sem;
  for (int i = 0; i < ntasks; ++i) {
    pool->enqueueForSync(std::make_unique<DelayableStorageTask>(
        &sem, ARRIVAL_TIMEOUT_MS, TIMEOUT_MS));
  }
  /* sleep override */
  std::this_thread::sleep_for(std::chrono::milliseconds(ARRIVAL_TIMEOUT_MS));
  for (int i = 0; i < ntasks; ++i) {
    pool->enqueueForSync(
        std::make_unique<UndelayableStorageTask>(&sem, 0, ARRIVAL_TIMEOUT_MS));
  }
  // Wait until all tasks have finished
  for (int i = 0; i < 2 * ntasks; ++i) {
    sem.wait();
  }

  pool.reset();
}
