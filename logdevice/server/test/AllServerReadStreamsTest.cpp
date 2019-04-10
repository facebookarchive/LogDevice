/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/server/read_path/AllServerReadStreams.h"

#include <folly/Memory.h>
#include <gtest/gtest.h>

#include "logdevice/common/debug.h"
#include "logdevice/common/protocol/RELEASE_Message.h"
#include "logdevice/common/settings/util.h"
#include "logdevice/server/read_path/CatchupQueue.h"
#include "logdevice/server/read_path/LocalLogStoreReader.h"
#include "logdevice/server/read_path/LogStorageStateMap.h"
#include "logdevice/server/storage_tasks/ReadStorageTask.h"

using namespace facebook::logdevice;

using InterceptedTasks = std::vector<std::unique_ptr<ReadStorageTask>>;
using LocalLogStoreReader::ReadPointer;

namespace {

const shard_index_t SHARD_IDX = 0;

class TestAllServerReadStreams : public AllServerReadStreams {
 public:
  TestAllServerReadStreams(UpdateableSettings<Settings> settings,
                           size_t max_read_storage_tasks_mem,
                           worker_id_t worker_id,
                           LogStorageStateMap* log_storage_state_map,
                           ServerProcessor* processor,
                           StatsHolder* stats,
                           bool on_worker_thread)
      : AllServerReadStreams(settings,
                             max_read_storage_tasks_mem,
                             worker_id,
                             log_storage_state_map,
                             processor,
                             stats,
                             on_worker_thread) {
    dbg::assertOnData = true;
  }
  ~TestAllServerReadStreams() override {
    EXPECT_FALSE(sendDelayedStorageTasksPending_);
  }

  void sendStorageTask(std::unique_ptr<ReadStorageTask>&& task,
                       shard_index_t shard) override {
    ASSERT_EQ(SHARD_IDX, shard);
    tasks_.push_back(std::move(task));
  }

  void scheduleSendDelayedStorageTasks() override {
    sendDelayedStorageTasksPending_ = true;
  }

  InterceptedTasks& getTasks() {
    return tasks_;
  }

  void fireSendDelayedStorageTasksTimer() {
    ASSERT_TRUE(sendDelayedStorageTasksPending_);
    sendDelayedStorageTasksPending_ = false;
    sendDelayedReadStorageTasks();
  }

 private:
  InterceptedTasks tasks_;
  bool sendDelayedStorageTasksPending_ = false;
};

} // namespace

/**
 * Test that AllServerReadStreams maintains the right subscriptions to RELEASE
 * messages.
 */
TEST(AllServerReadStreamsTest, Subscriptions) {
  LogStorageStateMap map(1);
  const worker_id_t worker_id(1);
  Settings settings = create_default_settings<Settings>();
  AllServerReadStreams streams(
      settings, 9999, worker_id, &map, nullptr, nullptr, false);

  const ClientID c1(111), c2(112);
  const logid_t log1(222), log2(223);

  // Have a client start reading, check that we subscribed
  streams.insertOrGet(c1, log1, SHARD_IDX, read_stream_id_t(1));
  ASSERT_TRUE(map.get(log1, SHARD_IDX).isWorkerSubscribed(worker_id));

  // Have another client start and stop reading the same log, should still be
  // subscribed
  streams.insertOrGet(c2, log1, SHARD_IDX, read_stream_id_t(1));
  ASSERT_TRUE(map.get(log1, SHARD_IDX).isWorkerSubscribed(worker_id));
  streams.erase(c2, log1, read_stream_id_t(1), SHARD_IDX);
  ASSERT_TRUE(map.get(log1, SHARD_IDX).isWorkerSubscribed(worker_id));

  // Erase the original, should unsubscribe
  streams.erase(c1, log1, read_stream_id_t(1), SHARD_IDX);
  ASSERT_FALSE(map.get(log1, SHARD_IDX).isWorkerSubscribed(worker_id));

  // Have c2 subscribe to both logs, c1 to one
  streams.insertOrGet(c2, log1, SHARD_IDX, read_stream_id_t(2));
  streams.insertOrGet(c2, log2, SHARD_IDX, read_stream_id_t(2));
  streams.insertOrGet(c1, log2, SHARD_IDX, read_stream_id_t(2));
  // Now have c2 disconnect
  streams.eraseAllForClient(c2);
  // Should still be subscribed to log2
  ASSERT_FALSE(map.get(log1, SHARD_IDX).isWorkerSubscribed(worker_id));
  ASSERT_TRUE(map.get(log2, SHARD_IDX).isWorkerSubscribed(worker_id));

  streams.clear();
}

/**
 * insertOrGet() should fail when we reach the capacity specified at
 * construction time.
 */
TEST(AllServerReadStreamsTest, Capacity) {
  LogStorageStateMap map(1);
  const worker_id_t worker_id(1);
  Settings settings = create_default_settings<Settings>();
  settings.max_server_read_streams = 4;
  AllServerReadStreams streams(
      settings, 99999, worker_id, &map, nullptr, nullptr, false);

  const logid_t log_id(1);
  const read_stream_id_t rs1(1), rs2(2);
  const ClientID c1(111), c2(222), c3(333);

  ASSERT_NE(streams.insertOrGet(c1, log_id, SHARD_IDX, rs1).first, nullptr);
  ASSERT_NE(streams.insertOrGet(c1, log_id, SHARD_IDX, rs2).first, nullptr);
  ASSERT_NE(streams.insertOrGet(c2, log_id, SHARD_IDX, rs1).first, nullptr);
  ASSERT_NE(streams.insertOrGet(c2, log_id, SHARD_IDX, rs2).first, nullptr);

  // We are at capacity now, insert should fail
  ASSERT_EQ(streams.insertOrGet(c3, log_id, SHARD_IDX, rs1).first, nullptr);

  // But a lookup of an existing stream should succeed
  ASSERT_NE(streams.insertOrGet(c2, log_id, SHARD_IDX, rs2).first, nullptr);

  // If one of the clients disconnects, inserting should succeed again
  streams.eraseAllForClient(c1);
  ASSERT_NE(streams.insertOrGet(c3, log_id, SHARD_IDX, rs1).first, nullptr);
  ASSERT_NE(streams.insertOrGet(c3, log_id, SHARD_IDX, rs2).first, nullptr);

  streams.clear();
}

// #5357883 AllServerReadStreams::onRelease() should not risk a use-after-free
TEST(AllServerReadStreams, OnReleaseUseAfterFree) {
  class TestAllServerReadStreams : public AllServerReadStreams {
    using AllServerReadStreams::AllServerReadStreams;

   protected:
    // Override to trigger the bug
    void scheduleForCatchup(ServerReadStream& stream,
                            bool,
                            CatchupEventTrigger) override {
      // CatchupQueue::pushRecords() may erase arbitrary read streams which used
      // to trigger the use-after-free in onRelease().  Simulate that here.
      this->eraseAllForClient(stream.client_id_);
    }
  };

  LogStorageStateMap map(1);
  Settings settings = create_default_settings<Settings>();
  settings.max_server_read_streams = 4;
  TestAllServerReadStreams streams(
      settings, 99999, worker_id_t(1), &map, nullptr, nullptr, false);

  const logid_t LOG_ID(1);
  const read_stream_id_t rs1(1), rs2(2);
  const ClientID c1(111);

  ASSERT_NE(streams.insertOrGet(c1, LOG_ID, SHARD_IDX, rs1).first, nullptr);
  ASSERT_NE(streams.insertOrGet(c1, LOG_ID, SHARD_IDX, rs2).first, nullptr);

  // boom!
  streams.onRelease(RecordID(LSN_MAX, LOG_ID), SHARD_IDX, false);

  streams.clear();
}

// Verify that AllServerReadStreams ensures that we do not reach the limit of
// bytes allocated by ReadStorageTasks.
TEST(AllServerReadStreams, ReadStorageTasksMemoryLimit) {
  LogStorageStateMap map(1);
  const worker_id_t worker_id(1);
  Settings settings = create_default_settings<Settings>();

  // There is a limit of 100 bytes for memory allocated by ReadStorageTasks.
  TestAllServerReadStreams streams(
      settings, 100, worker_id, &map, nullptr, nullptr, false);

  const logid_t log_id(1);
  const read_stream_id_t rs1(1);
  const ClientID c1(111), c2(222), c3(333), c4(444), c5(555);

  // create 3 read streams.
  auto stream1 = streams.insertOrGet(c1, log_id, SHARD_IDX, rs1).first;
  ASSERT_NE(stream1, nullptr);
  auto stream2 = streams.insertOrGet(c2, log_id, SHARD_IDX, rs1).first;
  ASSERT_NE(stream2, nullptr);
  auto stream3 = streams.insertOrGet(c3, log_id, SHARD_IDX, rs1).first;
  ASSERT_NE(stream3, nullptr);
  auto stream4 = streams.insertOrGet(c4, log_id, SHARD_IDX, rs1).first;
  ASSERT_NE(stream4, nullptr);
  auto stream5 = streams.insertOrGet(c5, log_id, SHARD_IDX, rs1).first;
  ASSERT_NE(stream5, nullptr);

  auto createTask = [&](ClientID /*client*/,
                        ServerReadStream* s,
                        uint64_t bytes) -> std::unique_ptr<ReadStorageTask> {
    using LLSFilter = LocalLogStoreReadFilter;
    LocalLogStoreReader::ReadContext read_ctx(log_id,
                                              ReadPointer{LSN_OLDEST},
                                              LSN_MAX,
                                              LSN_MAX,
                                              std::chrono::milliseconds::max(),
                                              LSN_MAX,
                                              bytes,
                                              true,  // first_record_any_size
                                              false, // is_rebuilding
                                              std::make_shared<LLSFilter>(),
                                              CatchupEventTrigger::OTHER);
    LocalLogStore::ReadOptions options("ReadStorageTasksMemoryLimit");
    std::weak_ptr<LocalLogStore::ReadIterator> it;
    auto task = std::make_unique<ReadStorageTask>(
        s->createRef(),
        WeakRef<CatchupQueue>(), // invalid ref to CatchupQueue
        server_read_stream_version_t{1},
        filter_version_t{1},
        read_ctx,
        options,
        it,
        0,
        s->getReadPriority(),
        StorageTaskType::READ_TAIL,
        StorageTaskThreadType::SLOW,
        StorageTaskPriority::MID,
        StorageTaskPrincipal::READ_TAIL);
    task->total_bytes_ = bytes;
    return task;
  };

  // A task comes in and allocates 60 bytes from the budget.
  auto t1 = createTask(c1, stream1, 60);
  streams.putStorageTask(std::move(t1), SHARD_IDX);
  streams.eraseAllForClient(c1);
  ASSERT_EQ(1, streams.getTasks().size());
  t1 = std::move(streams.getTasks()[0]);
  streams.getTasks().clear();
  ASSERT_EQ(40, streams.getMemoryBudget().available());

  // Another task comes in and allocates 10 bytes from the budget.
  auto t2 = createTask(c2, stream2, 10);
  streams.putStorageTask(std::move(t2), SHARD_IDX);
  streams.eraseAllForClient(c2);
  ASSERT_EQ(1, streams.getTasks().size());
  t2 = std::move(streams.getTasks()[0]);
  streams.getTasks().clear();
  ASSERT_EQ(30, streams.getMemoryBudget().available());

  // There are 30 bytes left. A task that wants to allocate 40 bytes is delayed.
  auto t3 = createTask(c3, stream3, 40);
  streams.putStorageTask(std::move(t3), SHARD_IDX);
  streams.eraseAllForClient(c3);
  ASSERT_EQ(0, streams.getTasks().size());
  ASSERT_EQ(30, streams.getMemoryBudget().available());

  // There are 30 bytes left. A task that wants to allocate 90 bytes is delayed.
  auto t4 = createTask(c4, stream4, 90);
  streams.putStorageTask(std::move(t4), SHARD_IDX);
  streams.eraseAllForClient(c4);
  ASSERT_EQ(0, streams.getTasks().size());
  ASSERT_EQ(30, streams.getMemoryBudget().available());

  // There are 30 bytes left. A task that wants to allocate 10 bytes is delayed.
  auto t5 = createTask(c5, stream5, 10);
  streams.putStorageTask(std::move(t5), SHARD_IDX);
  streams.eraseAllForClient(c5);
  ASSERT_EQ(0, streams.getTasks().size());
  ASSERT_EQ(30, streams.getMemoryBudget().available());

  // t2 comes back, releasing 10 bytes of memory. Giving just enough for t3 to
  // be sent, but not t4.
  streams.onReadTaskDone(*t2);
  ASSERT_EQ(1, streams.getTasks().size());
  t3 = std::move(streams.getTasks()[0]);
  streams.getTasks().clear();
  ASSERT_EQ(0, streams.getMemoryBudget().available());

  // t3 comes back (it is dropped).
  streams.onReadTaskDropped(*t3);
  streams.fireSendDelayedStorageTasksTimer();
  ASSERT_EQ(0, streams.getTasks().size());
  ASSERT_EQ(40, streams.getMemoryBudget().available());

  // t1 comes back. t4 and t5 can be sent.
  streams.onReadTaskDone(*t1);
  ASSERT_EQ(2, streams.getTasks().size());
  t4 = std::move(streams.getTasks()[0]);
  t5 = std::move(streams.getTasks()[1]);
  streams.getTasks().clear();
  ASSERT_EQ(0, streams.getMemoryBudget().available());

  // t4 and t5 come back
  streams.onReadTaskDone(*t4);
  streams.onReadTaskDone(*t5);
  ASSERT_EQ(0, streams.getTasks().size());
  ASSERT_EQ(100, streams.getMemoryBudget().available());

  streams.clear();
}
