/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/server/storage_tasks/WriteStorageTask.h"

#include <vector>

#include <folly/Memory.h>
#include <gtest/gtest.h>

#include "logdevice/common/MetaDataLog.h"
#include "logdevice/common/NoopTraceLogger.h"
#include "logdevice/common/Request.h"
#include "logdevice/common/debug.h"
#include "logdevice/server/ServerProcessor.h"
#include "logdevice/server/ServerWorker.h"
#include "logdevice/server/locallogstore/LocalLogStore.h"
#include "logdevice/server/locallogstore/WriteOps.h"
#include "logdevice/server/locallogstore/test/TemporaryLogStore.h"
#include "logdevice/server/storage_tasks/PerWorkerStorageTaskQueue.h"
#include "logdevice/server/storage_tasks/ShardedStorageThreadPool.h"
#include "logdevice/server/storage_tasks/StorageTask.h"
#include "logdevice/server/storage_tasks/StorageThreadPool.h"
#include "logdevice/server/test/TestUtil.h"

using namespace facebook::logdevice;
using Params = ServerSettings::StoragePoolParams;

class TestWriteRequest;

/**
 * WriteStorageTask with onDone() overridden to inform TestWriteRequest.
 */
class TestWriteStorageTask : public WriteStorageTask {
 public:
  explicit TestWriteStorageTask(
      const WriteOp* op,
      TestWriteRequest* req,
      StorageTask::ThreadType type = StorageTask::ThreadType::DEFAULT)
      : WriteStorageTask(StorageTask::Type::UNKNOWN),
        op_(op),
        req_(req),
        type_(type) {}

  void onDone() override;
  void onDropped() override {}

  size_t getNumWriteOps() const override {
    return 1;
  }

  StorageTask::ThreadType getThreadType() const override {
    return type_;
  }

  size_t getWriteOps(const WriteOp** write_ops,
                     size_t write_ops_len) const override {
    if (write_ops_len > 0) {
      write_ops[0] = op_;
      return 1;
    } else {
      return 0;
    }
  }

 private:
  const WriteOp* op_;
  TestWriteRequest* req_;
  const StorageTask::ThreadType type_;
};

class TestStorageTaskFactory {
 public:
  virtual ~TestStorageTaskFactory() {}
  virtual std::unique_ptr<StorageTask>
  createStorageTask(const WriteOp* op, TestWriteRequest* req) {
    return std::make_unique<TestWriteStorageTask>(op, req);
  }
};

/**
 * Request that creates one WriteStorageTask per write, then waits for storage
 * threads to complete all of the tasks.
 */
class TestWriteRequest : public Request {
 public:
  explicit TestWriteRequest(std::vector<const WriteOp*> ops,
                            std::vector<Status> results = std::vector<Status>(),
                            std::unique_ptr<TestStorageTaskFactory> factory =
                                std::make_unique<TestStorageTaskFactory>())
      : Request(RequestType::TEST_WRITE_STORAGE_TASK_REQUEST),
        write_ops_(ops),
        expected_(results),
        task_factory_(std::move(factory)) {}

  Request::Execution execute() override {
    outstanding_writes_ = write_ops_.size();
    for (const WriteOp* op : write_ops_) {
      auto task = task_factory_->createStorageTask(op, this);
      ServerWorker::onThisThread()->getStorageTaskQueueForShard(0)->putTask(
          std::move(task));
    }
    // NOTE: request is self-owned, not done until onWriteDone() is called
    // enough times
    return Execution::CONTINUE;
  }

  void onWriteDone(const WriteOp* op, Status status) {
    if (expected_.size() > 0) {
      auto it = std::find(write_ops_.begin(), write_ops_.end(), op);
      ld_check(it != write_ops_.end());
      auto pos = std::distance(write_ops_.begin(), it);
      ld_check(pos < expected_.size());
      EXPECT_EQ(expected_[pos], status);
    }

    if (--outstanding_writes_ == 0) {
      delete this;
    }
  }

 private:
  std::vector<const WriteOp*> write_ops_;
  std::vector<Status> expected_;
  int outstanding_writes_;
  std::unique_ptr<TestStorageTaskFactory> task_factory_;
};

void TestWriteStorageTask::onDone() {
  req_->onWriteDone(op_, status_);
}

class OutOfSpaceRocksDBStore : public TemporaryRocksDBStore {
 public:
  Status acceptingWrites() const override {
    return E::NOSPC;
  }
};

template <typename T>
class ShardedStoreWrapper : public ShardedLocalLogStore {
 public:
  int numShards() const override {
    return 1;
  }

  LocalLogStore* getByIndex(int idx) override {
    ld_check(idx == 0);
    return &store_;
  }

 private:
  T store_;
};

TEST(WriteStorageTaskTest, Simple) {
  const int nwrites = 10000;
  const int nworkers = 1;

  Settings settings = create_default_settings<Settings>();
  ServerSettings server_settings = create_default_settings<ServerSettings>();

  settings.num_workers = nworkers;
  settings.max_inflight_storage_tasks = 512;
  // Make sure the Worker can buffer all writes
  settings.per_worker_storage_task_queue_size = nwrites;
  UpdateableSettings<Settings> updateable_settings(settings);
  UpdateableSettings<ServerSettings> updateable_server_settings(
      server_settings);

  Params params;
  params[(size_t)StorageTaskThreadType::SLOW].nthreads = 1;
  ShardedStoreWrapper<TemporaryRocksDBStore> sharded_store;
  ShardedStorageThreadPool sharded_storage_thread_pool(
      &sharded_store,
      params,
      updateable_server_settings,
      updateable_settings,
      nworkers * settings.max_inflight_storage_tasks,
      nullptr);

  auto processor_builder =
      TestServerProcessorBuilder{settings}
          .setServerSettings(server_settings)
          .setShardedStorageThreadPool(&sharded_storage_thread_pool);
  auto processor = std::move(processor_builder).build();

  std::vector<std::string> datas(nwrites); // needs to live long enough
  std::vector<PutWriteOp> write_ops;
  for (int i = 0; i < nwrites; ++i) {
    std::array<ShardID, 2> cs = {ShardID(41, 0), ShardID(42, 0)};
    LocalLogStoreRecordFormat::formRecordHeader(
        0,
        esn_t(0),
        LocalLogStoreRecordFormat::FLAG_CHECKSUM_PARITY,
        0,
        folly::Range<const ShardID*>(cs.begin(), cs.end()),
        OffsetMap(),
        std::map<KeyType, std::string>(),
        &datas[i]);
    PutWriteOp op{
        logid_t(1), lsn_t(i + 1), Slice(datas[i].data(), datas[i].size())};
    write_ops.push_back(op);
  }

  std::vector<const WriteOp*> ops;
  for (auto& x : write_ops) {
    ops.push_back(&x);
  }

  std::unique_ptr<Request> req = std::make_unique<TestWriteRequest>(ops);
  processor->blockingRequest(req);

  // Check that all writes made it through
  auto it = sharded_store.getByIndex(0)->read(
      logid_t(1), LocalLogStore::ReadOptions("Simple"));
  it->seek(0);
  int nread = 0;
  for (nread = 0; it->state() == IteratorState::AT_RECORD; ++nread, it->next())
    ;
  EXPECT_EQ(nwrites, nread);

  // Shutdown ShardedStorageThreadPool while Processor is alive: remaining
  // WriteBatchStorageTasks may try to send response to a worker.
  sharded_storage_thread_pool.shutdown();

  shutdown_test_server(processor);
}

// Checks that writes into metadata logs are executed even if log store reports
// that it's out of space.
TEST(WriteStorageTaskTest, MetadataLogNOSPC) {
  Settings settings = create_default_settings<Settings>();
  UpdateableSettings<Settings> updateable_settings(settings);
  ServerSettings server_settings = create_default_settings<ServerSettings>();
  UpdateableSettings<ServerSettings> updateable_server_settings(
      server_settings);
  ShardedStoreWrapper<OutOfSpaceRocksDBStore> sharded_store;
  Params params;
  params[(size_t)StorageTaskThreadType::SLOW].nthreads = 1;
  ShardedStorageThreadPool sharded_storage_thread_pool(
      &sharded_store,
      params,
      updateable_server_settings,
      updateable_settings,
      1000,
      nullptr);
  auto processor_builder =
      TestServerProcessorBuilder{settings}
          .setServerSettings(server_settings)
          .setShardedStorageThreadPool(&sharded_storage_thread_pool);
  auto processor = std::move(processor_builder).build();

  std::vector<std::string> data(3);
  for (size_t i = 0; i < data.size(); ++i) {
    std::array<ShardID, 3> cs = {
        ShardID(12, 0), ShardID(11, 0), ShardID(10, 0)};
    LocalLogStoreRecordFormat::formRecordHeader(
        i,
        esn_t(0),
        LocalLogStoreRecordFormat::FLAG_CHECKSUM_PARITY,
        0,
        folly::Range<const ShardID*>(cs.begin(), cs.end()),
        OffsetMap(),
        std::map<KeyType, std::string>(),
        &data[i]);
  }

  const logid_t log_1 = logid_t(1);
  const logid_t log_2 = MetaDataLog::metaDataLogID(log_1);
  const logid_t log_3 = logid_t(USER_LOGID_MAX.val_ + 100);

  std::vector<PutWriteOp> write_ops{
      PutWriteOp{log_1, 1, Slice(data[0].data(), data[0].size())},
      PutWriteOp{log_2, 1, Slice(data[1].data(), data[1].size())},
      PutWriteOp{log_3, 1, Slice(data[2].data(), data[2].size())},
  };

  std::vector<const WriteOp*> ops;
  for (auto& x : write_ops) {
    ops.push_back(&x);
  }

  // Writes to the metadata and internal log should make it through.
  std::vector<Status> expected = {E::NOSPC, E::OK, E::OK};

  std::unique_ptr<Request> req =
      std::make_unique<TestWriteRequest>(std::move(ops), std::move(expected));
  processor->blockingRequest(req);

  // Shutdown ShardedStorageThreadPool while Processor is alive.
  sharded_storage_thread_pool.shutdown();

  shutdown_test_server(processor);
}
class ThrottlingRocksDBStore : public TemporaryRocksDBStore {
 public:
  ~ThrottlingRocksDBStore() override {
    EXPECT_TRUE(stall_invoked_);
  }
  WriteThrottleState getWriteThrottleState() override {
    return WriteThrottleState::REJECT_WRITE;
  }
  void stallLowPriWrite() override {
    stall_invoked_ = true;
  }

 private:
  bool stall_invoked_{false};
};

class ThrottlingStorageTaskFactory : public TestStorageTaskFactory {
 public:
  ~ThrottlingStorageTaskFactory() override {}
  std::unique_ptr<StorageTask>
  createStorageTask(const WriteOp* op, TestWriteRequest* req) override {
    StorageTask::ThreadType type = StorageTask::ThreadType::FAST_TIME_SENSITIVE;
    ++counter_;
    if (counter_ == 1) {
      type = StorageTask::ThreadType::FAST_STALLABLE;
    }
    if (counter_ == 3) {
      type = StorageTask::ThreadType::DEFAULT;
    }
    return std::make_unique<TestWriteStorageTask>(op, req, type);
  }

 private:
  int counter_{0};
};

// Create 3 write ops. All three writes are for different threads first one will
// be from stallable and it mimics rebuilding write, second is from
// fast_time_sensitive and it mimics normal appends, and third is from default
// not sure if we have writes on that thread. getWriteThrottleState always
// returns REJECT_WRITE status and expectation is that write from fast_stallable
// thread should stall, write from fast_time_sensitive should be rejected while
// write on default thread should be allowed to proceed.
TEST(WriteStorageTaskTest, ThrottleWritesTest) {
  const int nwrites = 3;
  const int nworkers = 1;

  Settings settings = create_default_settings<Settings>();
  ServerSettings server_settings = create_default_settings<ServerSettings>();

  settings.num_workers = nworkers;
  settings.max_inflight_storage_tasks = 512;
  // Make sure the Worker can buffer all writes
  settings.per_worker_storage_task_queue_size = nwrites;
  settings.write_batch_size = 1;
  UpdateableSettings<Settings> updateable_settings(settings);
  UpdateableSettings<ServerSettings> updateable_server_settings(
      server_settings);

  Params params;
  params[(size_t)StorageTaskThreadType::SLOW].nthreads = 1;
  params[(size_t)StorageTaskThreadType::DEFAULT].nthreads = 1;
  params[(size_t)StorageTaskThreadType::FAST_STALLABLE].nthreads = 1;
  params[(size_t)StorageTaskThreadType::FAST_TIME_SENSITIVE].nthreads = 1;
  ShardedStoreWrapper<ThrottlingRocksDBStore> sharded_store;
  ShardedStorageThreadPool sharded_storage_thread_pool(
      &sharded_store,
      params,
      updateable_server_settings,
      updateable_settings,
      nworkers * settings.max_inflight_storage_tasks,
      nullptr);

  auto processor_builder =
      TestServerProcessorBuilder{settings}
          .setServerSettings(server_settings)
          .setShardedStorageThreadPool(&sharded_storage_thread_pool);
  auto processor = std::move(processor_builder).build();

  std::vector<std::string> datas(nwrites); // needs to live long enough
  std::vector<PutWriteOp> write_ops;
  for (int i = 0; i < nwrites; ++i) {
    std::array<ShardID, 2> cs = {ShardID(41, 0), ShardID(42, 0)};
    LocalLogStoreRecordFormat::formRecordHeader(
        0,
        esn_t(0),
        LocalLogStoreRecordFormat::FLAG_CHECKSUM_PARITY,
        0,
        folly::Range<const ShardID*>(cs.begin(), cs.end()),
        OffsetMap(),
        std::map<KeyType, std::string>(),
        &datas[i]);
    PutWriteOp op{
        logid_t(1), lsn_t(i + 1), Slice(datas[i].data(), datas[i].size())};
    write_ops.push_back(op);
  }

  std::vector<const WriteOp*> ops;
  for (auto& x : write_ops) {
    ops.push_back(&x);
  }

  std::vector<Status> expected = {E::OK, E::DROPPED, E::DROPPED};
  std::unique_ptr<Request> req = std::make_unique<TestWriteRequest>(
      ops,
      std::move(expected),
      std::make_unique<ThrottlingStorageTaskFactory>());

  // Mark the node as rejecting writes.
  processor->blockingRequest(req);

  // Shutdown ShardedStorageThreadPool while Processor is alive: remaining
  // WriteBatchStorageTasks may try to send response to a worker.
  sharded_storage_thread_pool.shutdown();

  shutdown_test_server(processor);
}
