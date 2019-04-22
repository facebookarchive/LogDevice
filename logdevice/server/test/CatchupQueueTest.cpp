/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/server/read_path/CatchupQueue.h"

#include <memory>
#include <utility>
#include <vector>

#include <folly/Memory.h>
#include <gtest/gtest.h>

#include "logdevice/common/DataRecordOwnsPayload.h"
#include "logdevice/common/FlowGroup.h"
#include "logdevice/common/LocalLogStoreRecordFormat.h"
#include "logdevice/common/Sender.h"
#include "logdevice/common/protocol/Compatibility.h"
#include "logdevice/common/protocol/GAP_Message.h"
#include "logdevice/common/protocol/Message.h"
#include "logdevice/common/protocol/RECORD_Message.h"
#include "logdevice/common/protocol/STARTED_Message.h"
#include "logdevice/common/protocol/STORE_Message.h"
#include "logdevice/common/protocol/WINDOW_Message.h"
#include "logdevice/common/stats/Stats.h"
#include "logdevice/common/test/MockBackoffTimer.h"
#include "logdevice/common/test/MockTimer.h"
#include "logdevice/server/ServerRecordFilterFactory.h"
#include "logdevice/server/read_path/AllServerReadStreams.h"
#include "logdevice/server/read_path/LogStorageStateMap.h"
#include "logdevice/server/storage_tasks/ReadStorageTask.h"

namespace facebook { namespace logdevice {

#define N0 ShardID(0, 0)
#define N1 ShardID(1, 0)
#define N2 ShardID(2, 0)
#define N3 ShardID(3, 0)
#define N4 ShardID(4, 0)
#define N5 ShardID(5, 0)

/**
 * @file Unit tests for CatchupQueue, part of the server-side read path that
 *       manages all read streams for one client.
 */

using InterceptedTasks = std::vector<std::unique_ptr<ReadStorageTask>>;
using InterceptedMessages =
    std::vector<std::pair<std::unique_ptr<Message>, ClientID>>;

namespace {

const shard_index_t SHARD_IDX = 0;

class TestAllServerReadStreams : public AllServerReadStreams {
 public:
  TestAllServerReadStreams(LogStorageStateMap* log_storage_state_map,
                           InterceptedTasks* tasks)
      : AllServerReadStreams(UpdateableSettings<Settings>(),
                             1ul << 30,
                             worker_id_t(0),
                             log_storage_state_map,
                             nullptr,
                             nullptr,
                             false),
        tasks_(tasks) {}
  ~TestAllServerReadStreams() override {
    EXPECT_FALSE(sendDelayedStorageTasksPending_);
  }

  void sendStorageTask(std::unique_ptr<ReadStorageTask>&& task,
                       shard_index_t shard) override {
    ASSERT_EQ(SHARD_IDX, shard);
    tasks_->push_back(std::move(task));
  }

  void scheduleSendDelayedStorageTasks() override {
    sendDelayedStorageTasksPending_ = true;
  }

  InterceptedTasks* getTasks() {
    return tasks_;
  }

  void fireSendDelayedStorageTasksTimer() {
    ASSERT_TRUE(sendDelayedStorageTasksPending_);
    sendDelayedStorageTasksPending_ = false;
    sendDelayedReadStorageTasks();
  }

 private:
  InterceptedTasks* tasks_;
  bool sendDelayedStorageTasksPending_ = false;
};

} // namespace

class MockCatchupQueueDependencies;

/**
 * Test fixture class for testing CatchupQueue behaviour with a single client
 * and a single read stream.
 */
class CatchupQueueTest : public ::testing::Test {
 public:
  CatchupQueueTest()
      : log_storage_state_map_(1),
        streams_(&log_storage_state_map_, &tasks_),
        flow_group_(std::make_unique<FlowGroup>(
            std::make_unique<NwShapingFlowGroupDeps>(nullptr))) {
    dbg::assertOnData = true;

    // Ensure we have LogStorageState for our log.  Set the last released LSN
    // to 100 (most tests don't care about it).
    LogStorageState* log_state =
        log_storage_state_map_.insertOrGet(log_id_, SHARD_IDX);
    log_state->updateLastReleasedLSN(
        lsn_t(100), LogStorageState::LastReleasedSource::RELEASE);
    log_state->updateTrimPoint(lsn_t(0));

    resetCatchupQueue();
  }

  ~CatchupQueueTest() override {
    streams_.clear();
  }

  void resetCatchupQueue();

  void setTryNonBlockingRead(bool value = true) {
    getClientStateMap()
        .find(client_id_)
        ->second.catchup_queue->try_non_blocking_read_ = value;
  }

  AllServerReadStreams::ClientStateMap& getClientStateMap() {
    return streams_.client_states_;
  }

  Stats& getStats(ClientID client_id) {
    auto it = streams_.client_states_.find(client_id);
    ld_check(it != streams_.client_states_.end());
    return it->second.catchup_queue->deps_->getStatsHolder()->get();
  }

  size_t getCatchupQueueRecordBytesQueued(ClientID client_id) {
    auto it = streams_.client_states_.find(client_id);
    ld_check(it != streams_.client_states_.end());
    return it->second.catchup_queue->record_bytes_queued_;
  }

  size_t getCatchupQueueSize(ClientID client_id) {
    auto it = streams_.client_states_.find(client_id);
    ld_check(it != streams_.client_states_.end());
    return it->second.catchup_queue->queue_.size();
  }

  BackoffTimer* getPingTimer(ClientID client_id) {
    auto it = streams_.client_states_.find(client_id);
    ld_check(it != streams_.client_states_.end());
    return it->second.catchup_queue->ping_timer_.get();
  }

  ServerReadStream& createStream(read_stream_id_t id) {
    auto insert_result =
        streams_.insertOrGet(client_id_, log_id_, SHARD_IDX, id);
    ld_check(insert_result.second);
    ServerReadStream* stream = insert_result.first;
    stream->setTrafficClass(TrafficClass::READ_TAIL);
    stream->setReadPtr(1);
    stream->last_delivered_record_ = 0;
    stream->last_delivered_lsn_ = 0;
    stream->until_lsn_ = LSN_MAX;
    stream->setWindowHigh(LSN_MAX);
    stream->proto_ = Compatibility::MAX_PROTOCOL_SUPPORTED;
    stream->needs_started_message_ = true;
    return *stream;
  }

  void setTrimPoint(lsn_t trim_point) {
    LogStorageState& state = log_storage_state_map_.get(log_id_, SHARD_IDX);
    state.updateTrimPoint(trim_point);
  }

  void setLastReleasedLSN(lsn_t last_released) {
    LogStorageState& state = log_storage_state_map_.get(log_id_, SHARD_IDX);
    state.updateLastReleasedLSN(
        last_released, LogStorageState::LastReleasedSource::RELEASE);
  }

  /**
   * Creates a fake record as if it was read from the local log store
   */
  RawRecord createFakeRecord(lsn_t lsn,
                             size_t payload_size,
                             std::initializer_list<ShardID> copyset = {N1},
                             uint32_t wave = 1,
                             STORE_flags_t flags = 0,
                             folly::StringPiece key = folly::StringPiece(),
                             bool include_filter = false) const {
    std::chrono::milliseconds timestamp(0);

    STORE_Header header;
    header.rid = RecordID{lsn_to_esn(lsn), lsn_to_epoch(lsn), log_id_};
    header.timestamp = timestamp.count();
    header.last_known_good = esn_t(0);
    header.wave = wave;
    header.flags = flags;
    header.copyset_size = copyset.size();

    // Create a ChainLink array from the supplied list of node indices
    std::vector<StoreChainLink> chain(copyset.size());
    std::transform(copyset.begin(), copyset.end(), chain.data(), [](ShardID s) {
      return StoreChainLink{s, ClientID()};
    });

    std::string buf;
    std::map<KeyType, std::string> optional_keys;
    if (include_filter) {
      optional_keys.insert(std::make_pair(KeyType::FILTERABLE, key.str()));
    }

    const bool shard_id_in_copyset = true;
    Slice header_blob = LocalLogStoreRecordFormat::formRecordHeader(
        header, chain.data(), &buf, shard_id_in_copyset, optional_keys);

    size_t log_store_size = header_blob.size + payload_size;
    void* log_store_data = malloc(log_store_size);
    ld_check(log_store_data);
    memcpy(log_store_data, header_blob.data, header_blob.size);
    return RawRecord(lsn,
                     Slice(log_store_data, log_store_size),
                     /* owned payload */ true);
  }

  RECORD_Message* createFakeRecordMessage(read_stream_id_t id,
                                          lsn_t lsn,
                                          size_t payload_size) const {
    RECORD_Header header{log_id_, id, lsn, 0};
    auto msg = new RECORD_Message(header,
                                  TrafficClass::READ_TAIL,
                                  Payload(nullptr, payload_size),
                                  nullptr);
    return msg;
  }

  // Takes advantage of friend declaration in RECORD_Message to expose header
  // and payload
  const RECORD_Header& getHeader(const RECORD_Message& msg) {
    return msg.header_;
  }
  const Payload& getPayload(const RECORD_Message& msg) {
    return msg.payload_;
  }

  std::unique_ptr<GAP_Message> createFakeGapMessage(read_stream_id_t id,
                                                    lsn_t start_lsn,
                                                    lsn_t end_lsn,
                                                    GapReason reason) const {
    GAP_Header header{log_id_, id, start_lsn, end_lsn, reason, 0, 0};
    return std::make_unique<GAP_Message>(header, TrafficClass::READ_TAIL);
  }

  std::unique_ptr<STARTED_Message>
  createFakeStartedMessage(read_stream_id_t id,
                           filter_version_t fv,
                           Status status) const {
    STARTED_Header header{log_id_, id, status, fv, LSN_INVALID, 0};
    return std::make_unique<STARTED_Message>(header, TrafficClass::READ_TAIL);
  }

  const size_t getStreamsSize() const {
    return streams_.streams_.size();
  }

  void notifyNeedsCatchup(ServerReadStream& stream,
                          read_stream_id_t /*rsid*/,
                          bool more_data = false) {
    streams_.notifyNeedsCatchup(stream, more_data);
  }

  void invokeCallback() {
    ld_check(callback_);
    can_send_ = true;
    callback_->deactivate();
    (*callback_)(*flow_group_, callback_mutex_);
    callback_ = nullptr;
  }

  void filterTestHelper(ServerRecordFilterType type,
                        folly::StringPiece key1,
                        folly::StringPiece key2,
                        folly::StringPiece record_key1,
                        folly::StringPiece record_key2);

  LogStorageStateMap log_storage_state_map_;
  InterceptedTasks tasks_;
  TestAllServerReadStreams streams_;
  const ClientID client_id_{9999};
  const logid_t log_id_{1};
  BWAvailableCallback* callback_{nullptr};
  InterceptedMessages messages_;
  bool delay_read_{false};
  Status read_return_status_{E::WOULDBLOCK};
  // When read_return_status_ == E::CBREGISTERED, we create a fake message to
  // send to ReadingCallback::processRecord().  This is the LSN of that record.
  int next_read_lsn_{LSN_INVALID};

  bool can_send_{true};
  // Can be used by a test to simulate sendMessage failing. Number of messages
  // to be sent successfully before sendMessage fails with E::NOBUFS.
  int n_messages_before_fail_{-1};
  // Number of times CatchupQueue tries to do a non-blocking read.  If
  // try_non_blocking_read is true, we always try a non-blocking read before
  // creating a StorageTask.
  int n_non_blocking_read_attempts_{0};

  // Used when calling callback_.  Currently, the callback doesn't actually use
  // it.
  std::unique_ptr<FlowGroup> flow_group_{nullptr};
  std::mutex callback_mutex_;
};

/**
 * Mock implementation of CatchupQueueDependencies that captures all outgoing
 * communication from CatchupQueue/CatchupOneStream to allow inspection by
 * tests.
 */
class MockCatchupQueueDependencies : public CatchupQueueDependencies {
  using MockSender = SenderTestProxy<MockCatchupQueueDependencies>;

 public:
  explicit MockCatchupQueueDependencies(CatchupQueueTest& test)
      : CatchupQueueDependencies(&test.streams_, &server_stats_),
        test_(test),
        server_stats_(StatsParams().setIsServer(true)) {
    sender_ = std::make_unique<MockSender>(this);
  }

  MockCatchupQueueDependencies(const MockCatchupQueueDependencies&) = delete;
  MockCatchupQueueDependencies& operator=(const MockCatchupQueueDependencies&) =
      delete;

  // The following methods keep their default implementations (for now):
  // - getStream()
  // - eraseStream()
  // - invalidateIterators()

  std::unique_ptr<BackoffTimer>
  createPingTimer(std::function<void()> callback) override {
    auto timer = std::make_unique<MockBackoffTimer>();
    timer->setCallback(callback);
    return std::move(timer);
  }

  std::unique_ptr<Timer>
  createIteratorTimer(std::function<void()> callback) override {
    auto timer = std::make_unique<MockTimer>();
    timer->setCallback(callback);
    return std::move(timer);
  }

  std::chrono::milliseconds iteratorTimerTTL() const override {
    return std::chrono::milliseconds::zero();
  }

  folly::Optional<std::chrono::milliseconds>
  getDeliveryLatency(logid_t /*log_id*/) override {
    if (test_.delay_read_) {
      return std::chrono::milliseconds(1000000);
    }
    return folly::Optional<std::chrono::milliseconds>();
  }

  bool canSendToImpl(const Address&,
                     TrafficClass tc,
                     BWAvailableCallback& callback) {
    if (!test_.can_send_) {
      saveCallback(callback, PriorityMap::fromTrafficClass()[tc]);
    }
    return test_.can_send_;
  }

  int sendMessageImpl(std::unique_ptr<Message>&& msg,
                      const Address& addr,
                      BWAvailableCallback* callback,
                      SocketCallback*) {
    ld_check(msg->priority() != Priority::INVALID);
    ld_check(addr.isClientAddress());
    if (!addr.isClientAddress()) {
      err = E::INTERNAL;
      return -1;
    }

    if (!test_.can_send_ && callback) {
      saveCallback(*callback, msg->priority());
      err = E::CBREGISTERED;
      return -1;
    }

    if (test_.n_messages_before_fail_ >= 0) {
      (test_.n_messages_before_fail_)--;
      if (test_.n_messages_before_fail_ == -1) {
        err = E::NOBUFS;
        return -1;
      }
    }

    test_.messages_.emplace_back(std::move(msg), addr.id_.client_);
    return 0;
  }

  Status read(LocalLogStore::ReadIterator* /*read_iterator*/,
              LocalLogStoreReader::Callback& callback,
              LocalLogStoreReader::ReadContext* /*read_ctx*/) override {
    test_.n_non_blocking_read_attempts_++;

    if (test_.read_return_status_ == E::CBREGISTERED) {
      // Reads should only be attempted if we thought we had bandwidth ...
      EXPECT_TRUE(test_.can_send_);
      // ... but now we simulate bandwidth not being available.
      test_.can_send_ = false;
      int rv = callback.processRecord(
          test_.createFakeRecord(++test_.next_read_lsn_, 500));
      EXPECT_NE(0, rv);
      EXPECT_EQ(E::CBREGISTERED, err);
      EXPECT_NE(nullptr, test_.callback_);
    }
    return test_.read_return_status_;
  }

  LogStorageStateMap& getLogStorageStateMap() override {
    return test_.log_storage_state_map_;
  }

  int recoverLogState(logid_t /*log_id*/,
                      shard_index_t /*shard*/,
                      bool /*force_ask_sequencer*/ = false) override {
    return 0;
  }

  NodeID getMyNodeID() const override {
    return NodeID(0, 1);
  };

  size_t getMaxRecordBytesQueued(ClientID) override {
    return 128 * 1024;
  }

  const Settings& getSettings() const override {
    return *settings_.get();
  }

 private:
  void saveCallback(BWAvailableCallback& callback, Priority priority) {
    EXPECT_EQ(nullptr, test_.callback_);
    test_.callback_ = &callback;
    test_.flow_group_->push(callback, priority);
  }

  UpdateableSettings<Settings> settings_; // never updated
  CatchupQueueTest& test_;
  StatsHolder server_stats_;
};

void CatchupQueueTest::resetCatchupQueue() {
  streams_.eraseAllForClient(client_id_);

  // Inject a CatchupQueue with mocked dependencies into AllServerReadStreams.
  auto insert_result =
      streams_.client_states_.emplace(std::piecewise_construct,
                                      std::forward_as_tuple(client_id_),
                                      std::forward_as_tuple());
  ld_check(insert_result.second);

  auto it = insert_result.first;
  AllServerReadStreams::ClientState& client_state = it->second;

  auto deps = std::make_unique<MockCatchupQueueDependencies>(*this);
  client_state.catchup_queue.reset(
      new CatchupQueue(std::move(deps), client_id_));
  client_state.catchup_queue->try_non_blocking_read_ = false;
}

/**
 * Tests a scenario where the window is updated while a storage task is in
 * flight.
 */
TEST_F(CatchupQueueTest, WindowUpdatedWhileProcessingStorageTask) {
  read_stream_id_t read_stream_id(123123);
  // Initialize a server-side read stream with window_high=100.
  ServerReadStream& stream = createStream(read_stream_id);
  stream.setWindowHigh(100);
  notifyNeedsCatchup(stream, read_stream_id);

  // CatchupQueue is expected to issue a storage task to read some data.
  ASSERT_EQ(1, tasks_.size());
  std::unique_ptr<ReadStorageTask> task = std::move(tasks_.front());
  tasks_.clear();

  // Update the window at the request of the client.
  WINDOW_Header window_header{log_id_, read_stream_id, {50, 200}};
  streams_.onWindowMessage(client_id_, window_header);

  // Suppose storage task comes back with status WINDOW_END_REACHED...
  task->status_ = E::WINDOW_END_REACHED;
  task->records_ = ReadStorageTask::RecordContainer();
  streams_.onReadTaskDone(*task);

  // Despite the WINDOW_END_REACHED status, a new task should be created
  // because the window was updated since the storage task had been issued.
  ASSERT_EQ(1, tasks_.size());
}

/**
 * Tests a scenario where there are two streams and:
 * - one stream queues a lot of records for sending, reaching the
 *   max_record_bytes_queued limit
 * - we shouldn't read from the local log store for the second stream until
 *   there is more room in the output evbuffer
 */
TEST_F(CatchupQueueTest, LargeRecordPauses) {
  read_stream_id_t id1(1);
  read_stream_id_t id2(2);

  // assume all relevant records have been released
  setLastReleasedLSN(LSN_MAX);

  // Initialize two streams.
  ServerReadStream& stream1 = createStream(id1);
  stream1.setReadPtr(100);
  stream1.last_delivered_lsn_ = 100 - 1;
  notifyNeedsCatchup(stream1, id1);
  ServerReadStream& stream2 = createStream(id2);
  stream2.setReadPtr(200);
  stream2.last_delivered_lsn_ = 200 - 1;
  notifyNeedsCatchup(stream2, id2);

  // CatchupQueue is expected to issue a storage task to read some data from
  // stream1.
  ASSERT_EQ(1, tasks_.size());
  std::unique_ptr<ReadStorageTask> task = std::move(tasks_.front());
  ASSERT_EQ(100, task->read_ctx_.read_ptr_.lsn);
  tasks_.clear();

  // Suppose reading from the local log store came close to the limit (another
  // record would have taken us over).  ReadStorageTask came back with status
  // E::BYTE_LIMIT_REACHED.
  ReadStorageTask::RecordContainer records;
  records.push_back(createFakeRecord(100, 5000));
  records.push_back(createFakeRecord(101, 5000));
  task->status_ = E::BYTE_LIMIT_REACHED;
  task->records_ = std::move(records);
  task->read_ctx_.read_ptr_ = {lsn_t{102}};
  streams_.onReadTaskDone(*task);
  // 2 STARTED messages. 2 Records.
  ASSERT_EQ(4, messages_.size());

  // Since we know we're close to the byte limit, we shouldn't schedule any
  // more reading until the queued records have drained.
  ASSERT_EQ(0, tasks_.size());

  // Now suppose the two RECORD messages drain...
  SteadyTimestamp enqueue_time = SteadyTimestamp::now();
  auto msg = createFakeStartedMessage(id1, filter_version_t(0), E::OK);
  streams_.onStartedSent(client_id_, *msg, enqueue_time);
  std::unique_ptr<RECORD_Message> msg1(createFakeRecordMessage(id1, 100, 5000));
  std::unique_ptr<RECORD_Message> msg2(createFakeRecordMessage(id1, 101, 5000));
  streams_.onRecordSent(client_id_, *msg1, enqueue_time);
  ASSERT_EQ(0, tasks_.size()); // still nothing until we drain both

  streams_.onRecordSent(client_id_, *msg2, enqueue_time);
  ASSERT_EQ(1, tasks_.size());
  {
    task = std::move(tasks_.front());
    ASSERT_EQ(200, task->read_ctx_.read_ptr_.lsn);
  }
}

/**
 * Verifies fix for #2856309.
 */
TEST_F(CatchupQueueTest, RecordBytesQueuedCounterPersists) {
  read_stream_id_t read_stream_id(1);
  ServerReadStream& stream = createStream(read_stream_id);
  notifyNeedsCatchup(stream, read_stream_id);

  ASSERT_EQ(1, tasks_.size());
  std::unique_ptr<ReadStorageTask> task = std::move(tasks_.front());
  tasks_.clear();

  // Suppose two records come back with a status of CAUGHT_UP, which used to
  // cause the CatchupQueue to get deleted (because all streams were caught
  // up).
  ReadStorageTask::RecordContainer records;
  records.push_back(createFakeRecord(1, 1000));
  records.push_back(createFakeRecord(2, 2000));
  task->status_ = E::CAUGHT_UP;
  task->records_ = std::move(records);
  task->read_ctx_.read_ptr_ = {lsn_t{3}};
  streams_.onReadTaskDone(*task);

  // Check that record_bytes_queued_ accounts for both RECORD messages
  size_t expected_sz =
      RECORD_Message::expectedSize(1000) + RECORD_Message::expectedSize(2000);
  ASSERT_EQ(expected_sz, getCatchupQueueRecordBytesQueued(client_id_));

  // Suppose just one of the two RECORD messages drains
  SteadyTimestamp enqueue_time = SteadyTimestamp::now();
  auto msg =
      createFakeStartedMessage(read_stream_id, filter_version_t(0), E::OK);
  streams_.onStartedSent(client_id_, *msg, enqueue_time);
  std::unique_ptr<RECORD_Message> msg1(
      createFakeRecordMessage(read_stream_id, 1, 1000));
  streams_.onRecordSent(client_id_, *msg1, enqueue_time);

  // Check that the CatchupQueue instance still exists and that
  // record_bytes_queued_ accounts for the remaining queued RECORD message
  auto& map = getClientStateMap();
  auto it = map.find(client_id_);
  ASSERT_NE(it, map.end());
  ASSERT_NE(nullptr, it->second.catchup_queue);
  expected_sz = RECORD_Message::expectedSize(2000);
  ASSERT_EQ(expected_sz, getCatchupQueueRecordBytesQueued(client_id_));
}

TEST_F(CatchupQueueTest, ByteLimitReachedButOutputBufferEmptied) {
  read_stream_id_t read_stream_id(1);
  ServerReadStream& stream = createStream(read_stream_id);
  notifyNeedsCatchup(stream, read_stream_id);

  // CatchupQueue is expected to issue a storage task to read some data.
  ASSERT_EQ(1, tasks_.size());
  std::unique_ptr<ReadStorageTask> task = std::move(tasks_.front());
  tasks_.clear();

  // The task will respond with E::CAUGHT_UP, but we update the stream's version
  // in the mean time, so that CatchupQueue will ignore it and continue issuing
  // another storage task.
  stream.version_.val_++;

  // Suppose a big record comes back.
  {
    ReadStorageTask::RecordContainer records;
    records.push_back(createFakeRecord(1, 100000));
    task->status_ = E::CAUGHT_UP;
    task->records_ = std::move(records);
    task->read_ctx_.read_ptr_ = {lsn_t{2}};
    streams_.onReadTaskDone(*task);
  }

  // CatchupQueue is expected to issue another storage task to read some data.
  ASSERT_EQ(1, tasks_.size());
  task = std::move(tasks_.front());
  tasks_.clear();

  // Suppose the first record drains now.
  SteadyTimestamp enqueue_time = SteadyTimestamp::now();
  auto msg =
      createFakeStartedMessage(read_stream_id, filter_version_t(0), E::OK);
  streams_.onStartedSent(client_id_, *msg, enqueue_time);
  std::unique_ptr<RECORD_Message> msg1(
      createFakeRecordMessage(read_stream_id, 1, 100000));
  streams_.onRecordSent(client_id_, *msg1, enqueue_time);

  // Suppose the storage thread couldn't read any records without exceeding
  // the byte limit (which was set before the record drained).
  {
    task->status_ = E::BYTE_LIMIT_REACHED;
    task->records_ = ReadStorageTask::RecordContainer();
    task->read_ctx_.read_ptr_ = stream.getReadPtr();
    streams_.onReadTaskDone(*task);
  }

  // Despite the BYTE_LIMIT_REACHED status, CatchupQueue is expected to issue
  // another storage task because the record that was taking up buffer space
  // has drained.
  ASSERT_EQ(1, tasks_.size());
}

TEST_F(CatchupQueueTest, UntilLSNReached) {
  read_stream_id_t read_stream_id(1);
  ServerReadStream& stream = createStream(read_stream_id);
  stream.until_lsn_ = 5;
  notifyNeedsCatchup(stream, read_stream_id);

  // CatchupQueue is expected to issue a storage task to read some data.
  ASSERT_EQ(1, tasks_.size());
  std::unique_ptr<ReadStorageTask> task = std::move(tasks_.front());
  tasks_.clear();
  messages_.clear();

  {
    ReadStorageTask::RecordContainer records;
    records.push_back(createFakeRecord(1, 100));
    records.push_back(createFakeRecord(5, 100));
    task->status_ = E::UNTIL_LSN_REACHED;
    task->records_ = std::move(records);
    task->read_ctx_.read_ptr_ = {lsn_t{6}};
    streams_.onReadTaskDone(*task);
  }

  // The stream should have been deleted and the queue be empty.
  ASSERT_EQ(
      nullptr, streams_.get(client_id_, log_id_, read_stream_id, SHARD_IDX));
  ASSERT_EQ(0, getCatchupQueueSize(client_id_));

  ASSERT_EQ(2, messages_.size());
  RECORD_Message* r1 = dynamic_cast<RECORD_Message*>(messages_[0].first.get());
  RECORD_Message* r2 = dynamic_cast<RECORD_Message*>(messages_[1].first.get());
  ASSERT_NE(r1, nullptr);
  ASSERT_NE(r2, nullptr);
  ASSERT_EQ(1, getHeader(*r1).lsn);
  ASSERT_EQ(5, getHeader(*r2).lsn);
}

/**
 * CatchupQueue should send a GAP message to the client if the next available
 * record is past until_lsn.
 */
TEST_F(CatchupQueueTest, UntilLsnGap) {
  read_stream_id_t read_stream_id(42);
  ServerReadStream& stream = createStream(read_stream_id);
  stream.until_lsn_ = 100;
  stream.setWindowHigh(100);
  notifyNeedsCatchup(stream, read_stream_id);

  // CatchupQueue is expected to issue a storage task to read some data.
  ASSERT_EQ(1, tasks_.size());
  std::unique_ptr<ReadStorageTask> task = std::move(tasks_.front());
  tasks_.clear();
  messages_.clear();

  task->status_ = E::UNTIL_LSN_REACHED;
  streams_.onReadTaskDone(*task);

  ASSERT_EQ(1, messages_.size());
  GAP_Message* msg = dynamic_cast<GAP_Message*>(messages_[0].first.get());
  ASSERT_NE(msg, nullptr);

  EXPECT_EQ(1, msg->getHeader().start_lsn);
  EXPECT_EQ(100, msg->getHeader().end_lsn);
  EXPECT_EQ(GapReason::NO_RECORDS, msg->getHeader().reason);

  // stream should be erased after reaching until_lsn
  EXPECT_EQ(0, getStreamsSize());
}

/**
 * CatchupQueue should also send a GAP message when no records were read, but
 * last_released_lsn > until_lsn.
 */
TEST_F(CatchupQueueTest, LastReleasedGreaterThanUntilLsnGap) {
  read_stream_id_t read_stream_id(42);
  ServerReadStream& stream = createStream(read_stream_id);
  stream.until_lsn_ = 50; // last_released_lsn == 100
  stream.setWindowHigh(50);
  notifyNeedsCatchup(stream, read_stream_id);

  // CatchupQueue is expected to issue a storage task to read some data.
  ASSERT_EQ(1, tasks_.size());
  std::unique_ptr<ReadStorageTask> task = std::move(tasks_.front());
  tasks_.clear();
  messages_.clear();

  task->status_ = E::UNTIL_LSN_REACHED;
  streams_.onReadTaskDone(*task);

  ASSERT_EQ(1, messages_.size());
  GAP_Message* msg = dynamic_cast<GAP_Message*>(messages_[0].first.get());
  ASSERT_NE(msg, nullptr);
  EXPECT_EQ(GapReason::NO_RECORDS, msg->getHeader().reason);
  EXPECT_EQ(1, msg->getHeader().start_lsn);
  EXPECT_EQ(50, msg->getHeader().end_lsn);
}

/**
 * A test for the intersection of copyset filtering and gap handling near
 * until LSN.  If there is a record in the local log store near until_lsn but
 * it gets filtered, we need to send a gap to the client so that it knows we
 * are done.
 */
TEST_F(CatchupQueueTest, CopysetFiltersOutUntil) {
  for (const lsn_t UNTIL_LSN : {1, 10}) {
    read_stream_id_t read_stream_id(1);
    ServerReadStream& stream = createStream(read_stream_id);
    stream.until_lsn_ = UNTIL_LSN;
    notifyNeedsCatchup(stream, read_stream_id);

    // CatchupQueue is expected to issue a storage task to read some data.
    ASSERT_EQ(1, tasks_.size());
    std::unique_ptr<ReadStorageTask> task = std::move(tasks_.front());
    tasks_.clear();
    messages_.clear();

    {
      // Task comes back empty
      task->records_ = ReadStorageTask::RecordContainer();
      task->status_ = E::UNTIL_LSN_REACHED;
      // read_ptr advanced to lsn 2, which means record with lsn 1 was filtered.
      task->read_ctx_.read_ptr_ = {lsn_t{2}};
      streams_.onReadTaskDone(*task);
    }

    ASSERT_EQ(1, messages_.size());
    GAP_Message* msg = dynamic_cast<GAP_Message*>(messages_[0].first.get());
    ASSERT_NE(msg, nullptr);
    EXPECT_EQ(GapReason::NO_RECORDS, msg->getHeader().reason);
    ASSERT_EQ(1, msg->getHeader().start_lsn);
    ASSERT_EQ(UNTIL_LSN, msg->getHeader().end_lsn);
  }
}

/**
 * If one or more records at the end of the client's window get filtered, we
 * need to send a gap message to ensure that the client can skip the range
 * and continue reading.
 */
TEST_F(CatchupQueueTest, CopysetFiltersOutWindow) {
  read_stream_id_t read_stream_id(1);
  ServerReadStream& stream = createStream(read_stream_id);

  stream.setWindowHigh(2);
  notifyNeedsCatchup(stream, read_stream_id);

  // CatchupQueue is expected to issue a storage task to read some data.
  ASSERT_EQ(1, tasks_.size());
  std::unique_ptr<ReadStorageTask> task = std::move(tasks_.front());
  tasks_.clear();
  messages_.clear();

  // Task comes back empty
  task->status_ = E::WINDOW_END_REACHED;
  task->records_ = ReadStorageTask::RecordContainer();
  // read_ptr advanced to lsn 3, which means record with lsns 1, 2 were
  // filtered.
  task->read_ctx_.read_ptr_ = {lsn_t{3}};
  streams_.onReadTaskDone(*task);

  ASSERT_EQ(1, messages_.size());
  GAP_Message* msg = dynamic_cast<GAP_Message*>(messages_[0].first.get());
  ASSERT_NE(msg, nullptr);
  EXPECT_EQ(GapReason::NO_RECORDS, msg->getHeader().reason);
  EXPECT_EQ(1, msg->getHeader().start_lsn);
  EXPECT_EQ(2, msg->getHeader().end_lsn);
}

/**
 * If we send a WINDOW_END gap message, but then client updates its window,
 * setting window_high_ to a smaller lsn, we don't need to issue another storage
 * task.
 */
TEST_F(CatchupQueueTest, ReadPtrPastNewWindow) {
  read_stream_id_t read_stream_id(1);
  ServerReadStream& stream = createStream(read_stream_id);
  stream.setWindowHigh(11);
  notifyNeedsCatchup(stream, read_stream_id);

  // CatchupQueue is expected to issue a storage task to read some data.
  ASSERT_EQ(1, tasks_.size());
  std::unique_ptr<ReadStorageTask> task = std::move(tasks_.front());
  tasks_.clear();

  ReadStorageTask::RecordContainer records;
  for (int i = 1; i <= 5; ++i) {
    records.push_back(createFakeRecord(i, 100));
  }
  task->status_ = E::WINDOW_END_REACHED;
  task->records_ = std::move(records);
  task->read_ctx_.read_ptr_ = {lsn_t{21}};
  streams_.onReadTaskDone(*task);

  ASSERT_EQ(7, messages_.size()); // started, five records and a gap message
  for (int i = 1; i < 6; ++i) {
    RECORD_Message* record_msg =
        dynamic_cast<RECORD_Message*>(messages_[i].first.get());
    ASSERT_NE(record_msg, nullptr);
    EXPECT_EQ(i, getHeader(*record_msg).lsn);
  }
  GAP_Message* gap_msg = dynamic_cast<GAP_Message*>(messages_[6].first.get());
  ASSERT_NE(gap_msg, nullptr);
  EXPECT_EQ(GapReason::NO_RECORDS, gap_msg->getHeader().reason);
  EXPECT_EQ(6, gap_msg->getHeader().start_lsn);
  EXPECT_EQ(20, gap_msg->getHeader().end_lsn);
  messages_.clear();

  // client updates its window
  WINDOW_Header window_header{log_id_, read_stream_id, {6, 16}};
  streams_.onWindowMessage(client_id_, window_header);

  // However, read_ptr is already past the new window, we already informed the
  // client of what we have and there is no point in issuing a storage task.
  // In fact, ClientReadStream should be smart enough to not bother sliding our
  // window in this case.
  ASSERT_EQ(0, tasks_.size());
}

/**
 * CatchupQueue is expected to send a GAP message when there are no records
 * available, but read_ptr.lsn <= last_released_lsn.
 */
TEST_F(CatchupQueueTest, CaughtUpGap) {
  read_stream_id_t read_stream_id(42);
  ServerReadStream& stream = createStream(read_stream_id);
  stream.until_lsn_ = 200;
  stream.setWindowHigh(200);
  notifyNeedsCatchup(stream, read_stream_id);

  // CatchupQueue is expected to issue a storage task to read some data.
  ASSERT_EQ(1, tasks_.size());
  std::unique_ptr<ReadStorageTask> task = std::move(tasks_.front());
  tasks_.clear();
  messages_.clear();

  task->status_ = E::CAUGHT_UP;
  task->read_ctx_.read_ptr_ = {lsn_t{101}}; // last_released_ln + 1
  streams_.onReadTaskDone(*task);

  ASSERT_EQ(1, messages_.size());
  GAP_Message* msg = dynamic_cast<GAP_Message*>(messages_[0].first.get());
  ASSERT_NE(msg, nullptr);
  EXPECT_EQ(GapReason::NO_RECORDS, msg->getHeader().reason);
  EXPECT_EQ(1, msg->getHeader().start_lsn);
  EXPECT_EQ(100, msg->getHeader().end_lsn);
}

/**
 * This test is similar to CaughtUpGap but this time the trim point is set to
 * 50. Two gaps should be issued:
 * - One TRIM gap in the range [1, 50];
 * - One NO_RECORDS gap in the range [51, 100].
 */
TEST_F(CatchupQueueTest, CaughtUpGapWithTrimPoint) {
  read_stream_id_t read_stream_id(42);
  ServerReadStream& stream = createStream(read_stream_id);
  stream.until_lsn_ = 200;
  stream.setWindowHigh(200);
  notifyNeedsCatchup(stream, read_stream_id);

  // CatchupQueue is expected to issue a storage task to read some data.
  ASSERT_EQ(1, tasks_.size());
  std::unique_ptr<ReadStorageTask> task = std::move(tasks_.front());
  tasks_.clear();
  messages_.clear();

  task->status_ = E::CAUGHT_UP;
  task->read_ctx_.read_ptr_ = {lsn_t{101}}; // last_released_lsn + 1
  // Before the storage task comes back trim point was updated.
  setTrimPoint(50);
  streams_.onReadTaskDone(*task);

  ASSERT_EQ(2, messages_.size());

  // First, a TRIM gap should be issued for range [1, 50].
  GAP_Message* gap1 = dynamic_cast<GAP_Message*>(messages_[0].first.get());
  ASSERT_NE(gap1, nullptr);
  EXPECT_EQ(GapReason::TRIM, gap1->getHeader().reason);
  EXPECT_EQ(1, gap1->getHeader().start_lsn);
  EXPECT_EQ(50, gap1->getHeader().end_lsn);

  // Then, a NO_RECORDS gap should be issued for range [51, 100].
  GAP_Message* gap2 = dynamic_cast<GAP_Message*>(messages_[1].first.get());
  ASSERT_NE(gap2, nullptr);
  EXPECT_EQ(GapReason::NO_RECORDS, gap2->getHeader().reason);
  EXPECT_EQ(51, gap2->getHeader().start_lsn);
  EXPECT_EQ(100, gap2->getHeader().end_lsn);
}

// If we start reading a batch and the trim point is past window_high, we should
// send a big gap and not bother issueing a storage task.
TEST_F(CatchupQueueTest, TrimPointPastWindowHigh) {
  read_stream_id_t read_stream_id(42);
  ServerReadStream& stream = createStream(read_stream_id);
  stream.until_lsn_ = 1000;
  stream.setWindowHigh(200);
  setTrimPoint(500);
  notifyNeedsCatchup(stream, read_stream_id);

  ASSERT_TRUE(!stream.isCatchingUp());
  ASSERT_EQ(2, messages_.size());
  GAP_Message* gap = dynamic_cast<GAP_Message*>(messages_[1].first.get());
  ASSERT_NE(gap, nullptr);
  EXPECT_EQ(GapReason::TRIM, gap->getHeader().reason);
  EXPECT_EQ(1, gap->getHeader().start_lsn);
  EXPECT_EQ(500, gap->getHeader().end_lsn);
}

/**
 * When recovery flag is set, last_released_lsn should not have any effect.
 */
TEST_F(CatchupQueueTest, RecoveryFlag) {
  read_stream_id_t read_stream_id(42);
  ServerReadStream& stream = createStream(read_stream_id);
  stream.until_lsn_ = 1000;
  stream.ignore_released_status_ = true;
  notifyNeedsCatchup(stream, read_stream_id);

  ASSERT_EQ(1, tasks_.size());
  std::unique_ptr<ReadStorageTask> task = std::move(tasks_.front());
  tasks_.clear();

  EXPECT_GE(task->read_ctx_.last_released_lsn_, task->read_ctx_.until_lsn_);
}

/**
 * Tests that the HOLE store flag is propagated to clients via the RECORD
 * message.
 */
TEST_F(CatchupQueueTest, RecordHoleFlag) {
  read_stream_id_t read_stream_id(42);
  ServerReadStream& stream = createStream(read_stream_id);
  notifyNeedsCatchup(stream, read_stream_id);

  ASSERT_EQ(1, tasks_.size());
  std::unique_ptr<ReadStorageTask> task = std::move(tasks_.front());
  tasks_.clear();

  ReadStorageTask::RecordContainer records;
  records.push_back(createFakeRecord(1, 100, {N1}, 1, STORE_Header::HOLE));
  task->status_ = E::CAUGHT_UP;
  task->records_ = std::move(records);
  task->read_ctx_.read_ptr_ = {lsn_t{2}};
  streams_.onReadTaskDone(*task);

  // verify that the RECORD message contains the HOLE flag
  ASSERT_EQ(2, messages_.size());
  RECORD_Message* msg = dynamic_cast<RECORD_Message*>(messages_[1].first.get());
  ASSERT_NE(nullptr, msg);
  ASSERT_TRUE(getHeader(*msg).flags & RECORD_Header::HOLE);
}

// The STARTED message should be dispatched even when the starting LSN of
// a START message is beyond the current window.
TEST_F(CatchupQueueTest, StartedEvenIfPastWindow) {
  read_stream_id_t read_stream_id(43);
  ServerReadStream& stream = createStream(read_stream_id);
  stream.last_delivered_lsn_ = 19;
  stream.setWindowHigh(19);
  stream.setReadPtr(20);
  notifyNeedsCatchup(stream, read_stream_id);

  // verify that the STARTED message was dispatched
  ASSERT_EQ(1, messages_.size());
  auto* msg = dynamic_cast<STARTED_Message*>(messages_[0].first.get());
  ASSERT_NE(nullptr, msg);
}

/**
 * Tests a scenario where a read stream is deleted while a storage task for it
 * is in flight.  There was a bug where this could cause the CatchupQueue to
 * stall.
 */
TEST_F(CatchupQueueTest, ReadStreamDeletedWhileStorageTaskInFlight) {
  read_stream_id_t rs1(123987), rs2(123988);
  ServerReadStream& s1 = createStream(rs1);
  notifyNeedsCatchup(s1, rs1);

  // CatchupQueue is expected to issue a storage task to read some data.
  ASSERT_EQ(1, tasks_.size());
  std::unique_ptr<ReadStorageTask> task = std::move(tasks_.front());
  tasks_.clear();

  // Suppose a second ServerReadStream is created while the storage task is in
  // flight.
  ServerReadStream& s2 = createStream(rs2);
  notifyNeedsCatchup(s2, rs2);

  // Nothing is expected to happen right away because there is a storage task
  // in flight.
  ASSERT_EQ(0, tasks_.size());

  // Suppose the first ServerReadStream is deleted because the client got its
  // data from other storage nodes.
  streams_.erase(client_id_, log_id_, rs1, SHARD_IDX);

  // Suppose the storage task comes back ...
  task->status_ = E::CAUGHT_UP;
  task->records_ = ReadStorageTask::RecordContainer();
  streams_.onReadTaskDone(*task);

  // CatchupQueue should create a task for the second read stream.
  ASSERT_EQ(1, tasks_.size());
}

// Tests that no read tasks are issued if the log is fully trimmed
TEST_F(CatchupQueueTest, TrimPointLsnMax) {
  setTrimPoint(LSN_MAX);

  read_stream_id_t read_stream_id(1);
  ServerReadStream& stream = createStream(read_stream_id);
  stream.setReadPtr(LSN_OLDEST);
  stream.until_lsn_ = LSN_MAX;
  notifyNeedsCatchup(stream, read_stream_id);

  // nothing to read
  ASSERT_EQ(0, tasks_.size());

  ASSERT_EQ(2, messages_.size());
  GAP_Message* msg = dynamic_cast<GAP_Message*>(messages_[1].first.get());
  ASSERT_NE(nullptr, msg);

  EXPECT_EQ(LSN_MAX, msg->getHeader().end_lsn);
  EXPECT_EQ(GapReason::TRIM, msg->getHeader().reason);
}

// Tests that no read tasks are issued unless some new records have been
// released
TEST_F(CatchupQueueTest, NoReadsAfterLastReleased) {
  setLastReleasedLSN(200);

  read_stream_id_t read_stream_id(1);
  ServerReadStream& stream = createStream(read_stream_id);
  stream.setReadPtr(LSN_OLDEST);
  stream.setWindowHigh(100);
  stream.until_lsn_ = LSN_MAX;
  notifyNeedsCatchup(stream, read_stream_id);

  {
    ASSERT_EQ(1, tasks_.size());
    auto task = std::move(tasks_.front());
    task->read_ctx_.read_ptr_ = {101};
    task->status_ = E::WINDOW_END_REACHED;
    streams_.onReadTaskDone(*task);
    tasks_.clear();
  }
  {
    WINDOW_Header window_header{log_id_, read_stream_id, {101, 200}};
    streams_.onWindowMessage(client_id_, window_header);

    ASSERT_EQ(1, tasks_.size());
    auto task = std::move(tasks_.front());
    task->read_ctx_.read_ptr_ = {201};
    task->status_ = E::CAUGHT_UP;
    streams_.onReadTaskDone(*task);
    tasks_.clear();
  }
  {
    WINDOW_Header window_header{log_id_, read_stream_id, {201, 300}};
    streams_.onWindowMessage(client_id_, window_header);
    // no released records in the new range
    ASSERT_EQ(0, tasks_.size());
  }
}

TEST_F(CatchupQueueTest, SkipReads) {
  read_stream_id_t read_stream_id(1);
  ServerReadStream& stream = createStream(read_stream_id);

  // new records are released for delivery
  notifyNeedsCatchup(stream, read_stream_id, /* more_data */ true);

  // a storage task is supposed to be created to read those records
  ASSERT_EQ(1, tasks_.size());
  auto task = std::move(tasks_.front());
  task->status_ = E::CAUGHT_UP;
  task->records_ = ReadStorageTask::RecordContainer();
  streams_.onReadTaskDone(*task);
  tasks_.clear();

  // new records are released, but we're not supposed to read them immediately
  delay_read_ = true;
  notifyNeedsCatchup(stream, read_stream_id, /* more_data */ true);
  ASSERT_EQ(0, tasks_.size());
}

// Test that if a storage task is dropped, the catchup queue will activate the
// ping timer and when the ping timer triggers the catchup queue will process
// the next stream.
TEST_F(CatchupQueueTest, StorageTaskDropped) {
  // assume all relevant records have been released
  setLastReleasedLSN(LSN_MAX);

  read_stream_id_t read_stream_id(1);
  ServerReadStream& stream = createStream(read_stream_id);
  stream.setReadPtr(100);
  notifyNeedsCatchup(stream, read_stream_id);

  // CatchupQueue is expected to issue a storage task to read some data from
  // stream1.
  ASSERT_EQ(1, tasks_.size());
  std::unique_ptr<ReadStorageTask> dropped_task = std::move(tasks_.front());
  ASSERT_EQ(100, dropped_task->read_ctx_.read_ptr_.lsn);
  tasks_.clear();

  // Suppose the storage task is dropped.
  streams_.onReadTaskDropped(*dropped_task);
  dropped_task.reset();
  streams_.fireSendDelayedStorageTasksTimer();
  ASSERT_EQ(0, tasks_.size());

  // Trigger the ping timer.
  MockBackoffTimer* ping_timer =
      dynamic_cast<MockBackoffTimer*>(getPingTimer(client_id_));
  ASSERT_TRUE(ping_timer->isActive());
  ping_timer->trigger();

  // CatchupQueue is expected to issue a storage task again.
  ASSERT_EQ(1, tasks_.size());
  {
    std::unique_ptr<ReadStorageTask> task = std::move(tasks_.front());
    ASSERT_EQ(100, task->read_ctx_.read_ptr_.lsn);
  }
  tasks_.clear();
}

// Test the scenario where a storage task comes back but cannot be processed
// entirely (E::ABORTED) because sendMessage failed for one of the records.
// In that case, we expect a new storage task to be issued once the output
// evbuffer is drained. This storage task should read records starting from
// where we left.
TEST_F(CatchupQueueTest, SendMessageFailed) {
  read_stream_id_t read_stream_id(1);
  ServerReadStream& stream = createStream(read_stream_id);
  stream.setReadPtr(1);
  notifyNeedsCatchup(stream, read_stream_id);

  // CatchupQueue is expected to issue a storage task to read some data from
  // stream1.
  ASSERT_EQ(1, tasks_.size());
  std::unique_ptr<ReadStorageTask> task = std::move(tasks_.front());
  ASSERT_EQ(1, task->read_ctx_.read_ptr_.lsn);
  tasks_.clear();
  // Suppose 5 records come back with a status of CAUGHT_UP.
  ReadStorageTask::RecordContainer records;
  records.push_back(createFakeRecord(1, 1000));
  records.push_back(createFakeRecord(2, 2000));
  records.push_back(createFakeRecord(3, 2000));
  records.push_back(createFakeRecord(4, 2000));
  records.push_back(createFakeRecord(5, 2000));
  task->status_ = E::CAUGHT_UP;
  task->records_ = std::move(records);
  task->read_ctx_.read_ptr_ = {lsn_t{6}};

  // Simulate that only the first two messages will be sent because sendMessage
  // will fail after 2 messages.
  n_messages_before_fail_ = 2;
  streams_.onReadTaskDone(*task);

  ASSERT_EQ(3, messages_.size());
  RECORD_Message* r1 = dynamic_cast<RECORD_Message*>(messages_[1].first.get());
  RECORD_Message* r2 = dynamic_cast<RECORD_Message*>(messages_[2].first.get());
  ASSERT_NE(r1, nullptr);
  ASSERT_NE(r2, nullptr);
  ASSERT_EQ(1, getHeader(*r1).lsn);
  ASSERT_EQ(2, getHeader(*r2).lsn);

  MockBackoffTimer* ping_timer =
      dynamic_cast<MockBackoffTimer*>(getPingTimer(client_id_));

  // There is no more storage task and the timer is not active because there are
  // still records to be drained.
  ASSERT_EQ(0, tasks_.size());
  ASSERT_FALSE(ping_timer->isActive());

  // Drain the two records.
  SteadyTimestamp enqueue_time = SteadyTimestamp::now();
  auto msg =
      createFakeStartedMessage(read_stream_id, filter_version_t(0), E::OK);
  streams_.onStartedSent(client_id_, *msg, enqueue_time);
  std::unique_ptr<RECORD_Message> msg1(
      createFakeRecordMessage(read_stream_id, 100, 1000));
  std::unique_ptr<RECORD_Message> msg2(
      createFakeRecordMessage(read_stream_id, 101, 2000));
  streams_.onRecordSent(client_id_, *msg1, enqueue_time);
  streams_.onRecordSent(client_id_, *msg2, enqueue_time);

  // CatchupQueue is expected to have issued a storage task again.
  ASSERT_EQ(1, tasks_.size());
  task = std::move(tasks_.front());
  // The task should contain records starting from lsn 3.
  ASSERT_EQ(3, task->read_ctx_.read_ptr_.lsn);
}

// Test a scenario where a storage task comes back but did not process any
// record because sendMessage failed() (E::ABORTED). This time, no message was
// sent, the output evbuffer is empty. In order to not stall, CatchupQueue
// should activate the ping timer and when it triggers a new storage task will
// be issued.
TEST_F(CatchupQueueTest, SendMessageFailedOutputBufferEmpty) {
  read_stream_id_t read_stream_id(1);
  ServerReadStream& stream = createStream(read_stream_id);
  stream.setReadPtr(1);
  notifyNeedsCatchup(stream, read_stream_id);

  // CatchupQueue is expected to issue a storage task to read some data from
  // stream1.
  ASSERT_EQ(1, tasks_.size());
  std::unique_ptr<ReadStorageTask> task = std::move(tasks_.front());
  ASSERT_EQ(1, task->read_ctx_.read_ptr_.lsn);
  tasks_.clear();
  // Suppose 5 records come back with a status of CAUGHT_UP.
  ReadStorageTask::RecordContainer records;
  records.push_back(createFakeRecord(1, 1000));
  records.push_back(createFakeRecord(2, 2000));
  records.push_back(createFakeRecord(3, 2000));
  records.push_back(createFakeRecord(4, 2000));
  records.push_back(createFakeRecord(5, 2000));
  task->status_ = E::CAUGHT_UP;
  task->records_ = std::move(records);
  task->read_ctx_.read_ptr_ = {lsn_t{6}};

  // Simulate that no message could be sent.
  n_messages_before_fail_ = 0;
  streams_.onReadTaskDone(*task);

  ASSERT_EQ(/*STARTED*/ 1, messages_.size());

  MockBackoffTimer* ping_timer =
      dynamic_cast<MockBackoffTimer*>(getPingTimer(client_id_));

  // There is no more storage task and the timer should be active because the
  // output evbuffer is empty.
  ASSERT_EQ(0, tasks_.size());
  ASSERT_TRUE(ping_timer->isActive());
  // Trigger the ping timer.
  ping_timer->trigger();

  // CatchupQueue is expected to have issued a storage task again.
  ASSERT_EQ(1, tasks_.size());
  task = std::move(tasks_.front());
  // The task should contain records starting from the beginning.
  ASSERT_EQ(1, task->read_ctx_.read_ptr_.lsn);
}

// A simple test that checks that if the stream rewound we do not ship records
// that were fetched by the storage task.
TEST_F(CatchupQueueTest, SingleCopyDeliveryRewind) {
  read_stream_id_t read_stream_id(123123);
  ServerReadStream& stream = createStream(read_stream_id);
  stream.setWindowHigh(100);
  stream.enableSingleCopyDelivery(small_shardset_t{}, node_index_t{0});

  notifyNeedsCatchup(stream, read_stream_id);
  ASSERT_EQ(1, tasks_.size());
  std::unique_ptr<ReadStorageTask> task = std::move(tasks_.front());
  ASSERT_EQ(1, task->read_ctx_.read_ptr_.lsn);
  tasks_.clear();
  messages_.clear();

  ReadStorageTask::RecordContainer records;
  records.push_back(createFakeRecord(4, 50, {N0, N3, N1, N5, N2}));
  records.push_back(createFakeRecord(6, 50, {N0, N3, N2, N4, N1}));
  task->status_ = E::CAUGHT_UP;
  task->records_ = std::move(records);
  task->read_ctx_.read_ptr_ = {lsn_t{7}};

  // In between the stream rewinds to be in all send all mode and the
  // filter_version is increased.
  stream.disableSingleCopyDelivery();
  ++stream.filter_version_.val_;
  // no op because there is a storage task in flight.
  notifyNeedsCatchup(stream, read_stream_id);

  // The storage task comes back but nothing should be shipped since the filter
  // version changed.
  streams_.onReadTaskDone(*task);
  ASSERT_EQ(0, messages_.size());

  // Another task is started since we rewinded.
  ASSERT_EQ(1, tasks_.size());
  task = std::move(tasks_.front());
  ASSERT_EQ(1, task->read_ctx_.read_ptr_.lsn);
  tasks_.clear();

  records.push_back(createFakeRecord(1, 50, {N1, N2, N3, N4, N5}));
  records.push_back(createFakeRecord(2, 50, {N2, N5, N0, N4, N3}));
  records.push_back(createFakeRecord(3, 50, {N3, N4, N1, N5, N2}));
  records.push_back(createFakeRecord(4, 50, {N0, N3, N1, N5, N2}));
  records.push_back(createFakeRecord(5, 50, {N5, N0, N4, N2, N3}));
  records.push_back(createFakeRecord(6, 50, {N0, N3, N2, N4, N1}));
  task->status_ = E::CAUGHT_UP;
  task->records_ = std::move(records);
  task->read_ctx_.read_ptr_ = {lsn_t{7}};

  streams_.onReadTaskDone(*task);
  ASSERT_EQ(6, messages_.size());
  ASSERT_EQ(0, tasks_.size());
}

// A test that verifies that CatchupOneStream will advance read_ptr_ if the
// last record in a batch was filtered.
// (Note: this test may not make much sense after T10357210)
TEST_F(CatchupQueueTest, LastRecordsInBatchFiltered) {
  read_stream_id_t read_stream_id(123123);
  ServerReadStream& stream = createStream(read_stream_id);
  stream.setWindowHigh(100);

  notifyNeedsCatchup(stream, read_stream_id);
  ASSERT_EQ(1, tasks_.size());
  std::unique_ptr<ReadStorageTask> task = std::move(tasks_.front());
  ASSERT_EQ(1, task->read_ctx_.read_ptr_.lsn);
  tasks_.clear();
  messages_.clear();

  ReadStorageTask::RecordContainer records;
  records.push_back(createFakeRecord(1, 50, {N1, N2, N3, N4, N5}, 1));
  records.push_back(createFakeRecord(2, 50, {N2, N5, N0, N4, N3}, 1));
  task->status_ = E::CAUGHT_UP;
  task->records_ = std::move(records);
  // Let's imagine that records with lsns 3 and 4 were encountered but filtered.
  // The last record had lsn 4. The storage task comes back with a
  // read ptr pointing to lsn 5.
  task->read_ctx_.read_ptr_ = {lsn_t{5}};
  streams_.onReadTaskDone(*task);
  // The two records came back and CatchupOneStream should add a gap record for
  // the range [3, 4].
  ASSERT_EQ(3, messages_.size());
  GAP_Message* msg = dynamic_cast<GAP_Message*>(messages_[2].first.get());
  ASSERT_NE(msg, nullptr);
  EXPECT_EQ(GapReason::NO_RECORDS, msg->getHeader().reason);
  EXPECT_EQ(3, msg->getHeader().start_lsn);
  EXPECT_EQ(4, msg->getHeader().end_lsn);
  ASSERT_EQ(0, tasks_.size());

  notifyNeedsCatchup(stream, read_stream_id);
  ASSERT_EQ(1, tasks_.size());
  task = std::move(tasks_.front());

  ASSERT_EQ(5, task->read_ctx_.read_ptr_.lsn);
  tasks_.clear();
}

// Many records are filtered by a first batch until we eventually reach the
// limit on the number of bytes that can be read per batch. LocalLogStoreReader
// completes the batch with E::PARTIAL, so CatchupOneStream should issue a gap
// to the client and use Action::REQUEUE_AND_CONTINUE.
TEST_F(CatchupQueueTest, MaxRecordsReadLimitReached) {
  // Stream 1 needs to catchup.
  ServerReadStream& stream1 = createStream(read_stream_id_t(1));
  stream1.setWindowHigh(100);
  notifyNeedsCatchup(stream1, read_stream_id_t(1));

  // Stream 2 needs to catchup.
  ServerReadStream& stream2 = createStream(read_stream_id_t(2));
  stream2.setWindowHigh(100);
  notifyNeedsCatchup(stream2, read_stream_id_t(2));

  // There should be a task for stream 1.
  ASSERT_EQ(1, tasks_.size());
  std::unique_ptr<ReadStorageTask> task = std::move(tasks_.front());
  ASSERT_EQ(1, task->read_ctx_.read_ptr_.lsn);
  tasks_.clear();
  messages_.clear();

  // The stream did not ship any record and the batch completed because we
  // reached the limit of bytes read from the local log store.
  // We suppose the last filtered record was record with lsn 41.
  // read_ptr is updated past it.
  ReadStorageTask::RecordContainer records;
  task->status_ = E::PARTIAL;
  task->records_ = std::move(records);
  task->read_ctx_.read_ptr_ = {lsn_t{42}};
  streams_.onReadTaskDone(*task);

  // CatchupOneStream should send a gap for all the filtered records so that the
  // client can make progress.
  ASSERT_EQ(1, messages_.size());
  GAP_Message* msg = dynamic_cast<GAP_Message*>(messages_[0].first.get());
  ASSERT_NE(msg, nullptr);
  EXPECT_EQ(GapReason::NO_RECORDS, msg->getHeader().reason);
  EXPECT_EQ(1, msg->getHeader().start_lsn);
  EXPECT_EQ(41, msg->getHeader().end_lsn);

  // CatchupQueue should have immediately started a batch for stream 2.
  ASSERT_EQ(1, tasks_.size());
  task = std::move(tasks_.front());
  ASSERT_EQ(1, task->read_ctx_.read_ptr_.lsn);
}

// Verify that a stream is not marked caught up if a task came back with
// E::CAUGHT_UP because read_ptr was advanced past the value of
// last_released_lsn at the time the task was issued but this value since then
// was increased.
TEST_F(CatchupQueueTest, LastReleasedLSNIncreasedWhileTaskInFlight) {
  setLastReleasedLSN(100);

  read_stream_id_t read_stream_id(123123);
  ServerReadStream& stream = createStream(read_stream_id);
  stream.setWindowHigh(1000);

  // A task comes back with no records and moves read_ptr to
  // last_released_lsn+1, completing with E::CAUGHT_UP.
  notifyNeedsCatchup(stream, read_stream_id);
  ASSERT_EQ(1, tasks_.size());
  std::unique_ptr<ReadStorageTask> task = std::move(tasks_.front());
  ASSERT_EQ(1, task->read_ctx_.read_ptr_.lsn);
  tasks_.clear();
  task->status_ = E::CAUGHT_UP;
  task->records_ = ReadStorageTask::RecordContainer{};
  task->read_ctx_.read_ptr_ = {lsn_t{101}};

  // Before the task comes back, the version was bumped because
  // last_released_lsn was moved forward.
  stream.version_.val_++;
  setLastReleasedLSN(142);
  streams_.onReadTaskDone(*task);

  // Normally, the stream would be marked as caught up. But actually
  // last_released_lsn is now 142 which is greater than read_ptr.
  // So a new task should be issued.
  // If CatchupOneStream was not checking stream.version_, the stream would stay
  // caught up until a new record is written to the log or a new WINDOW message,
  // which is bad.
  EXPECT_EQ(1, tasks_.size());
  std::unique_ptr<ReadStorageTask> t = std::move(tasks_.front());
  ASSERT_EQ(101, t->read_ctx_.read_ptr_.lsn);
}

TEST_F(CatchupQueueTest, FixT8640159_1) {
  setLastReleasedLSN(10000);

  read_stream_id_t read_stream_id(123123);
  ServerReadStream& stream = createStream(read_stream_id);
  stream.setWindowHigh(100);

  // We mark the stream for catchup. It will start reading from lsn 1. There is
  // no record up to lsn 4000 which is past the window high. So the task comes
  // back with nothing, moves read_ptr to 4000 and completes with
  // E::WINDOW_END_REACHED.

  notifyNeedsCatchup(stream, read_stream_id);
  ASSERT_EQ(1, tasks_.size());
  std::unique_ptr<ReadStorageTask> task = std::move(tasks_.front());
  ASSERT_EQ(1, task->read_ctx_.read_ptr_.lsn);
  tasks_.clear();
  messages_.clear();
  task->status_ = E::WINDOW_END_REACHED;
  task->records_ = ReadStorageTask::RecordContainer{};
  task->read_ctx_.read_ptr_ = {lsn_t{4000}};

  // Before the task comes back, the version was bumped because
  // last_released_lsn was moved forward.
  stream.version_.val_++;
  setLastReleasedLSN(10042);
  streams_.onReadTaskDone(*task);

  // The stream should be caught up.
  EXPECT_EQ(0, tasks_.size());

  // We should receive a gap [1, 3999]
  ASSERT_EQ(1, messages_.size());
  GAP_Message* msg = dynamic_cast<GAP_Message*>(messages_[0].first.get());
  ASSERT_NE(msg, nullptr);
  EXPECT_EQ(GapReason::NO_RECORDS, msg->getHeader().reason);
  EXPECT_EQ(1, msg->getHeader().start_lsn);
  EXPECT_EQ(3999, msg->getHeader().end_lsn);

  // The issue in #8640159 was that the above gap was not sent, and the stream
  // was not marked caught up: a new task was issued. If the log was getting
  // frequent RELEASES, frequently enough so that it happened almost always
  // each time a read storage task was inflight, the stream would be stuck in a
  // cycle where it would keep issueing read storage task that read nothing and
  // don't update read_ptr... while no gap was sent to help ClientReadStream
  // move the window forward.
}

// Same as FixT8640159_1 but this time window_high_ was updated instead of
// last_released_lsn.
TEST_F(CatchupQueueTest, FixT8640159_2) {
  setLastReleasedLSN(10000);

  read_stream_id_t read_stream_id(123123);
  ServerReadStream& stream = createStream(read_stream_id);
  stream.setWindowHigh(100);

  notifyNeedsCatchup(stream, read_stream_id);
  ASSERT_EQ(1, tasks_.size());
  std::unique_ptr<ReadStorageTask> task = std::move(tasks_.front());
  ASSERT_EQ(1, task->read_ctx_.read_ptr_.lsn);
  tasks_.clear();
  messages_.clear();
  task->status_ = E::WINDOW_END_REACHED;
  task->records_ = ReadStorageTask::RecordContainer{};
  task->read_ctx_.read_ptr_ = {lsn_t{4000}};

  // Before the task comes back, the version was bumped because
  // window_high was moved forward.
  stream.version_.val_++;
  stream.setWindowHigh(200);
  streams_.onReadTaskDone(*task);

  // The stream should still be caught up because window_high_ < read_ptr
  EXPECT_EQ(0, tasks_.size());

  // We should receive a gap [1, 3999]
  ASSERT_EQ(1, messages_.size());
  GAP_Message* msg = dynamic_cast<GAP_Message*>(messages_[0].first.get());
  ASSERT_NE(msg, nullptr);
  EXPECT_EQ(GapReason::NO_RECORDS, msg->getHeader().reason);
  EXPECT_EQ(1, msg->getHeader().start_lsn);
  EXPECT_EQ(3999, msg->getHeader().end_lsn);
}

/**
 * Tests a scenario where there are two streams and:
 *
 * - The first one issues a blocking read.
 *
 * - The second one should still attempt a non-blocking read before the first is
 *   done, but not a blocking read.
 */
TEST_F(CatchupQueueTest, DoNonBlockingAfterBlocking) {
  // This test needs non-blocking reads.
  setTryNonBlockingRead();

  read_stream_id_t id1(1);
  read_stream_id_t id2(2);

  // assume all relevant records have been released
  setLastReleasedLSN(LSN_MAX);

  // Initialize a stream.
  ServerReadStream& stream1 = createStream(id1);
  stream1.setReadPtr(100);
  stream1.last_delivered_lsn_ = 100 - 1;
  read_return_status_ = E::WOULDBLOCK;
  notifyNeedsCatchup(stream1, id1);

  // Should have tried a non-blocking read, then a blocking read.
  EXPECT_EQ(1, n_non_blocking_read_attempts_);
  n_non_blocking_read_attempts_ = 0;
  ASSERT_EQ(1, tasks_.size());
  ASSERT_EQ(100, tasks_.front()->read_ctx_.read_ptr_.lsn);
  tasks_.clear();

  // Now add another stream, before the first storage task finishes.
  ServerReadStream& stream2 = createStream(id2);
  stream2.setReadPtr(200);
  stream2.last_delivered_lsn_ = 200 - 1;
  notifyNeedsCatchup(stream2, id2);

  // It should try a non-blocking read, but not a blocking read.
  EXPECT_EQ(1, n_non_blocking_read_attempts_);
  EXPECT_TRUE(tasks_.empty());
}

/**
 * Tests a scenario where there are two streams and:
 *
 * - The first one issues a blocking read.
 *
 * - The second one should still attempt a non-blocking read before the first is
 *   done, but not a blocking read.
 */
TEST_F(CatchupQueueTest, NoBandwidthAfterBlocking) {
  // This test needs non-blocking reads.
  setTryNonBlockingRead();

  read_stream_id_t id1(1);
  read_stream_id_t id2(2);

  // assume all relevant records have been released
  setLastReleasedLSN(LSN_MAX);

  // Initialize a stream.
  ServerReadStream& stream1 = createStream(id1);
  stream1.setReadPtr(100);
  stream1.last_delivered_lsn_ = 100 - 1;
  read_return_status_ = E::WOULDBLOCK;
  notifyNeedsCatchup(stream1, id1);

  // Should have tried a non-blocking read, then a blocking read.
  EXPECT_EQ(1, n_non_blocking_read_attempts_);
  n_non_blocking_read_attempts_ = 0;
  EXPECT_EQ(/*STARTED*/ 1, messages_.size());
  ASSERT_EQ(nullptr, callback_);
  ASSERT_EQ(1, tasks_.size());
  EXPECT_EQ(100, tasks_.front()->read_ctx_.read_ptr_.lsn);
  std::unique_ptr<ReadStorageTask> task = std::move(tasks_.front());
  EXPECT_EQ(100, task->read_ctx_.read_ptr_.lsn);
  tasks_.clear();
  messages_.clear();

  // Now add another stream, before the first storage task finishes.  This time,
  // even though a storage task is in flight, we try a non-blocking read.
  //
  // The non-blocking read succeeds and it sends a message.
  ServerReadStream& stream2 = createStream(id2);
  stream2.setReadPtr(200);
  stream2.last_delivered_lsn_ = 200 - 1;
  next_read_lsn_ = stream2.getReadPtr().lsn;
  notifyNeedsCatchup(stream2, id2);

  EXPECT_EQ(1, n_non_blocking_read_attempts_);
  n_non_blocking_read_attempts_ = 0;
  EXPECT_EQ(nullptr, callback_);
  EXPECT_EQ(0, tasks_.size());
  EXPECT_EQ(/*STARTED*/ 1, messages_.size());
  messages_.clear();

  // Now the storage task is completed (with records).
  ReadStorageTask::RecordContainer records;
  records.push_back(createFakeRecord(100, 500));
  records.push_back(createFakeRecord(101, 500));
  task->status_ = E::PARTIAL;
  task->records_ = std::move(records);
  task->read_ctx_.read_ptr_ = {lsn_t{102}};
  // Simulate traffic shaping hitting a limit.  We should send the messages
  // anyway.
  can_send_ = false;
  streams_.onReadTaskDone(*task);

  // Make sure messages were sent.
  EXPECT_EQ(2, messages_.size());
  // But when we tried to read some more, we noticed there was no available
  // bandwidth so didn't do any actual reading.
  EXPECT_NE(nullptr, callback_);
  EXPECT_EQ(0, n_non_blocking_read_attempts_);
  EXPECT_EQ(0, tasks_.size());
}

/*  Used by test cases below to test server-side filtering.
 *   @param:
 *          filter_type: the type of filter to construct
 *          filter_key1: key1 for constructing filter
 *          filter_key2: key2 for constructing filter
 *          record_key1: key you expect to pass the filter
 *          record_key2: key you expect not to pass the filter
 */
void CatchupQueueTest::filterTestHelper(ServerRecordFilterType filter_type,
                                        folly::StringPiece filter_key1,
                                        folly::StringPiece filter_key2,
                                        folly::StringPiece record_key1,
                                        folly::StringPiece record_key2) {
  resetCatchupQueue();
  read_stream_id_t read_stream_id(1);
  ServerReadStream& stream = createStream(read_stream_id);

  stream.filter_pred_ =
      ServerRecordFilterFactory::create(filter_type, filter_key1, filter_key2);

  if (filter_type == ServerRecordFilterType::RANGE &&
      filter_key1 > filter_key2) {
    ASSERT_EQ(nullptr, stream.filter_pred_);
    // We cannot proceed to test filtering because we do not have a filter.
    return;
  }

  // new records are released for delivery
  notifyNeedsCatchup(stream, read_stream_id, /* more_data */ true);
  ASSERT_EQ(/*STARTED*/ 1, messages_.size());
  messages_.clear();

  // a storage task is supposed to be created to read those records
  ASSERT_EQ(1, tasks_.size());
  auto task = std::move(tasks_.front());
  task->status_ = E::CAUGHT_UP;
  task->records_ = ReadStorageTask::RecordContainer();

  // Add more records: half with correct custom key and the other half
  // with custom key that can pass filter.
  // We should only send record with payload when they pass the
  // string based filter.

  for (int i = 1; i <= 100; i++) {
    if (i % 2 == 0) {
      task->records_.push_back(
          createFakeRecord(i, 100, {N1}, 1, 0, record_key1, true));
    } else {
      task->records_.push_back(
          createFakeRecord(i, 100, {N1}, 1, 0, record_key2, true));
    }
  }

  streams_.onReadTaskDone(*task);

  for (int i = 0; i < 100; i++) {
    if (i % 2 == 0) {
      GAP_Message* gap_msg =
          dynamic_cast<GAP_Message*>(messages_[i].first.get());
      ASSERT_EQ(GapReason::FILTERED_OUT, gap_msg->getHeader().reason);
    } else {
      RECORD_Message* record_msg =
          dynamic_cast<RECORD_Message*>(messages_[i].first.get());
      ASSERT_NE((size_t)0, record_msg->payload_.size());
    }
  }
  tasks_.clear();
  messages_.clear();
  ASSERT_EQ(0, tasks_.size());
}

/**
 * Tests functionality of ServerRecordFilter
 */
TEST_F(CatchupQueueTest, ServerRecordFilterFeatureTest) {
  // Test case 1: EQUALITY filter    check: == "pass"
  // Expect: filter out records with key ""
  //         records with "pass" will pass
  filterTestHelper(
      ServerRecordFilterType::EQUALITY, "pass", "not used", "pass", "");

  // Test case 2: EQUALITY filter   check:  == ""
  // Expect: filter out records with key "not used"
  //         records with "" will pass
  filterTestHelper(
      ServerRecordFilterType::EQUALITY, "", "not used", "", "not pass");

  // Test case 3: EQUALITY filter   check: == "123"
  // Expect: filter out records with key "989"
  //         records with "123" will pass
  filterTestHelper(
      ServerRecordFilterType::EQUALITY, "123", "456", "123", "989");

  // Test case 4: RANGE filter     check: >= "" && <= "test"
  // Expect: records with "should pass" pass
  //         records with "test2" will be filtered out
  filterTestHelper(
      ServerRecordFilterType::RANGE, "", "test", "should pass", "test2");

  // Test case 5: RANGE filter   check: >= "" && <= "test"
  // Expect: filter out records with "zero pass"
  //         records with "" will pass
  filterTestHelper(ServerRecordFilterType::RANGE, "", "test", "", "zero pass");

  // Test case 6: RANGE filter   check: >= "20170623" && <= "20170711"
  // Expect: filter out records with "zero pass"
  //         records with "" will pass
  filterTestHelper(ServerRecordFilterType::RANGE,
                   "20170623",
                   "20170711",
                   "20170630",
                   "20170822");
  // Test case 7: RANGE filter   check: >= "20170623" && <= "20170711"
  // Expect: filter out records with "zero pass"
  //         records with "20170630" will pass
  filterTestHelper(
      ServerRecordFilterType::RANGE, "", "20180101", "20170630", "20180822");

  // Test case 8: EQUALITY filter   check: == "'"(escaple character)
  // Expect: filter out records with key "."
  //         records with "'" will pass
  filterTestHelper(ServerRecordFilterType::EQUALITY, "'", ".", "'", ",");

  // Test case 9: RANGE filter   check: >= "" && <= ""
  // Expect: filter out records with ")"
  //         records with "" will pass
  filterTestHelper(ServerRecordFilterType::RANGE, "", "", "", ")");

  // Test case 9: RANGE filter
  // filter_key1 > filter_key2, an error message should be printed
  // ServerRecordFilterFactory should return a nullptr
  filterTestHelper(ServerRecordFilterType::RANGE, "b", "a", "", ")");
}

TEST_F(CatchupQueueTest, MergeFilteredOutGapOnServerSide1) {
  resetCatchupQueue();
  read_stream_id_t read_stream_id(1);
  ServerReadStream& stream = createStream(read_stream_id);
  stream.setWindowHigh(2);
  stream.filter_pred_ = ServerRecordFilterFactory::create(
      ServerRecordFilterType::EQUALITY, "PASS", "PASS");
  // new records are released for delivery
  notifyNeedsCatchup(stream, read_stream_id, /* more_data */ true);
  // a storage task is supposed to be created to read those records
  ASSERT_EQ(1, tasks_.size());
  auto task = std::move(tasks_.front());
  task->status_ = E::CAUGHT_UP;
  task->records_ = ReadStorageTask::RecordContainer();
  ASSERT_EQ(/*STARTED*/ 1, messages_.size());
  messages_.clear();

  task->records_.push_back(createFakeRecord(1, 100, {N1}, 1, 0, "FAIL", true));
  task->records_.push_back(createFakeRecord(2, 100, {N1}, 1, 0, "FAIL", true));
  task->records_.push_back(createFakeRecord(3, 100, {N1}, 1, 0, "PASS", true));
  streams_.onReadTaskDone(*task);

  GAP_Message* gap_msg = dynamic_cast<GAP_Message*>(messages_[0].first.get());
  ASSERT_EQ(GapReason::FILTERED_OUT, gap_msg->getHeader().reason);
  ASSERT_EQ(gap_msg->getHeader().start_lsn, 1);
  ASSERT_EQ(gap_msg->getHeader().end_lsn, 2);

  WINDOW_Header window_header{log_id_, read_stream_id, {3, 4}, 0};
  streams_.onWindowMessage(client_id_, window_header);

  RECORD_Message* record_msg =
      dynamic_cast<RECORD_Message*>(messages_[1].first.get());
  ASSERT_NE((size_t)0, record_msg->payload_.size());

  tasks_.clear();
  messages_.clear();
  ASSERT_EQ(0, tasks_.size());
}

TEST_F(CatchupQueueTest, MergeFilteredOutGapOnServerSide2) {
  resetCatchupQueue();
  read_stream_id_t read_stream_id(1);
  ServerReadStream& stream = createStream(read_stream_id);
  stream.setWindowHigh(2);
  stream.filter_pred_ = ServerRecordFilterFactory::create(
      ServerRecordFilterType::EQUALITY, "PASS", "PASS");
  // new records are released for delivery
  notifyNeedsCatchup(stream, read_stream_id, /* more_data */ true);
  // a storage task is supposed to be created to read those records
  ASSERT_EQ(1, tasks_.size());
  auto task = std::move(tasks_.front());
  task->status_ = E::CAUGHT_UP;
  task->records_ = ReadStorageTask::RecordContainer();
  ASSERT_EQ(/*STARTED*/ 1, messages_.size());
  messages_.clear();

  task->records_.push_back(createFakeRecord(1, 100, {N1}, 1, 0, "FAIL", true));
  task->records_.push_back(createFakeRecord(2, 100, {N1}, 1, 0, "FAIL", true));
  task->records_.push_back(createFakeRecord(3, 100, {N1}, 1, 0, "FAIL", true));
  streams_.onReadTaskDone(*task);

  GAP_Message* gap_msg = dynamic_cast<GAP_Message*>(messages_[0].first.get());
  ASSERT_EQ(GapReason::FILTERED_OUT, gap_msg->getHeader().reason);
  ASSERT_EQ(gap_msg->getHeader().start_lsn, 1);
  ASSERT_EQ(gap_msg->getHeader().end_lsn, 2);

  WINDOW_Header window_header{log_id_, read_stream_id, {3, 4}, 0};
  streams_.onWindowMessage(client_id_, window_header);

  gap_msg = dynamic_cast<GAP_Message*>(messages_[1].first.get());
  ASSERT_EQ(GapReason::FILTERED_OUT, gap_msg->getHeader().reason);
  ASSERT_EQ(gap_msg->getHeader().start_lsn, 3);
  ASSERT_EQ(gap_msg->getHeader().end_lsn, 3);

  tasks_.clear();
  messages_.clear();
  ASSERT_EQ(0, tasks_.size());
}

TEST_F(CatchupQueueTest, MergeFilteredOutGapOnServerSide3) {
  resetCatchupQueue();
  read_stream_id_t read_stream_id(1);
  ServerReadStream& stream = createStream(read_stream_id);
  stream.setWindowHigh(2);
  stream.filter_pred_ = ServerRecordFilterFactory::create(
      ServerRecordFilterType::EQUALITY, "PASS", "PASS");
  // new records are released for delivery
  notifyNeedsCatchup(stream, read_stream_id, /* more_data */ true);
  // a storage task is supposed to be created to read those records
  ASSERT_EQ(1, tasks_.size());
  auto task = std::move(tasks_.front());
  task->status_ = E::CAUGHT_UP;
  task->records_ = ReadStorageTask::RecordContainer();
  ASSERT_EQ(/*STARTED*/ 1, messages_.size());
  messages_.clear();

  task->records_.push_back(createFakeRecord(1, 100, {N1}, 1, 0, "FAIL", true));
  task->records_.push_back(createFakeRecord(2, 100, {N1}, 1, 0, "FAIL", true));
  stream.setReadPtr(4);
  streams_.onReadTaskDone(*task);

  GAP_Message* gap_msg = dynamic_cast<GAP_Message*>(messages_[0].first.get());
  ASSERT_EQ(GapReason::FILTERED_OUT, gap_msg->getHeader().reason);
  ASSERT_EQ(gap_msg->getHeader().start_lsn, 1);
  ASSERT_EQ(gap_msg->getHeader().end_lsn, 2);

  // Update the window at the request of the client.
  WINDOW_Header window_header{log_id_, read_stream_id, {3, 4}, 0};
  streams_.onWindowMessage(client_id_, window_header);

  gap_msg = dynamic_cast<GAP_Message*>(messages_[1].first.get());
  ASSERT_EQ(GapReason::NO_RECORDS, gap_msg->getHeader().reason);
  ASSERT_EQ(gap_msg->getHeader().start_lsn, 3);
  ASSERT_EQ(gap_msg->getHeader().end_lsn, 3);

  tasks_.clear();
  messages_.clear();
  ASSERT_EQ(0, tasks_.size());
}

TEST_F(CatchupQueueTest, MergeFilteredOutGapOnServerSide4) {
  resetCatchupQueue();
  read_stream_id_t read_stream_id(1);
  ServerReadStream& stream = createStream(read_stream_id);
  stream.setWindowHigh(4);
  stream.filter_pred_ = ServerRecordFilterFactory::create(
      ServerRecordFilterType::EQUALITY, "PASS", "PASS");
  // new records are released for delivery
  notifyNeedsCatchup(stream, read_stream_id, /* more_data */ true);
  // a storage task is supposed to be created to read those records
  ASSERT_EQ(1, tasks_.size());
  auto task = std::move(tasks_.front());
  task->status_ = E::CAUGHT_UP;
  task->records_ = ReadStorageTask::RecordContainer();
  ASSERT_EQ(/*STARTED*/ 1, messages_.size());
  messages_.clear();

  task->records_.push_back(createFakeRecord(1, 100, {N1}, 1, 0, "FAIL", true));
  task->records_.push_back(createFakeRecord(2, 100, {N1}, 1, 0, "FAIL", true));
  stream.setReadPtr(5);
  task->records_.push_back(createFakeRecord(5, 100, {N1}, 1, 0, "FAIL", true));
  streams_.onReadTaskDone(*task);

  GAP_Message* gap_msg = dynamic_cast<GAP_Message*>(messages_[0].first.get());
  ASSERT_EQ(GapReason::FILTERED_OUT, gap_msg->getHeader().reason);
  ASSERT_EQ(gap_msg->getHeader().start_lsn, 1);
  ASSERT_EQ(gap_msg->getHeader().end_lsn, 2);

  // Update the window at the request of the client.
  WINDOW_Header window_header{log_id_, read_stream_id, {5, 8}, 0};
  streams_.onWindowMessage(client_id_, window_header);

  gap_msg = dynamic_cast<GAP_Message*>(messages_[1].first.get());
  ASSERT_EQ(GapReason::NO_RECORDS, gap_msg->getHeader().reason);
  ASSERT_EQ(gap_msg->getHeader().start_lsn, 3);
  ASSERT_EQ(gap_msg->getHeader().end_lsn, 4);

  gap_msg = dynamic_cast<GAP_Message*>(messages_[2].first.get());
  ASSERT_EQ(GapReason::FILTERED_OUT, gap_msg->getHeader().reason);
  ASSERT_EQ(gap_msg->getHeader().start_lsn, 5);
  ASSERT_EQ(gap_msg->getHeader().end_lsn, 5);

  tasks_.clear();
  messages_.clear();
  ASSERT_EQ(0, tasks_.size());
}

TEST_F(CatchupQueueTest, StartedStreamValidations) {
  SteadyTimestamp enqueue_time;

  resetCatchupQueue();
  read_stream_id_t id1(1);
  ServerReadStream& stream = createStream(id1);
  stream.setReadPtr(1);
  stream.start_lsn_ = 1;

  auto& stats = getStats(client_id_);
  auto clear_task_queue = [this](lsn_t read_ptr) {
    ASSERT_EQ(1, tasks_.size());
    std::unique_ptr<ReadStorageTask> task = std::move(tasks_.front());
    tasks_.clear();
    task->status_ = E::CAUGHT_UP;
    task->read_ctx_.read_ptr_ = {read_ptr};
    streams_.onReadTaskDone(*task);
  };

  notifyNeedsCatchup(stream, id1);
  // CatchupQueue is expected to issue a storage task to read some data.
  clear_task_queue(lsn_t(1));

  // STARTED should have been queued.
  ASSERT_EQ(1, messages_.size());
  messages_.clear();

  // Complete the STARTED
  {
    enqueue_time = SteadyTimestamp::now();
    auto msg = createFakeStartedMessage(id1, filter_version_t(0), E::OK);
    streams_.onStartedSent(client_id_, *msg, enqueue_time);
    ASSERT_EQ(0, stats.read_stream_started_violations);
  }

  // Simulate the client resending START multiple times. (Client side
  // STARTED timeout).
  stream.needs_started_message_ = true;
  notifyNeedsCatchup(stream, id1);
  clear_task_queue(lsn_t(1));
  stream.needs_started_message_ = true;
  notifyNeedsCatchup(stream, id1);
  clear_task_queue(lsn_t(1));

  // STARTED should have been queued multiple times..
  ASSERT_EQ(2, messages_.size());
  messages_.clear();

  // Sending 2 STARTED messages should be fine, but not a 3rd.
  {
    enqueue_time = SteadyTimestamp::now();
    auto msg = createFakeStartedMessage(id1, filter_version_t(0), E::OK);
    streams_.onStartedSent(client_id_, *msg, enqueue_time);
    streams_.onStartedSent(client_id_, *msg, enqueue_time);
    ASSERT_EQ(0, stats.read_stream_started_violations);

    streams_.onStartedSent(client_id_, *msg, enqueue_time);
    ASSERT_EQ(1, stats.read_stream_started_violations);
  }

  // Simulate a stream rewind. This will bump the filter version.
  stream.filter_version_ = filter_version_t(1);
  stream.needs_started_message_ = true;
  notifyNeedsCatchup(stream, id1);
  ASSERT_EQ(1, messages_.size());

  // Complete a downrev STARTED.
  {
    enqueue_time = SteadyTimestamp::now();
    auto msg = createFakeStartedMessage(id1, filter_version_t(0), E::OK);
    streams_.onStartedSent(client_id_, *msg, enqueue_time);
    ASSERT_EQ(2, stats.read_stream_started_violations);
  }
}

TEST_F(CatchupQueueTest, StreamValidations) {
  SteadyTimestamp before_start = SteadyTimestamp::now();
  SteadyTimestamp enqueue_time;

  resetCatchupQueue();
  read_stream_id_t id1(1);
  ServerReadStream& stream = createStream(id1);
  stream.setReadPtr(1);
  stream.start_lsn_ = 1;

  auto& stats = getStats(client_id_);

  notifyNeedsCatchup(stream, id1);
  // CatchupQueue is expected to issue a storage task to read some data.
  ASSERT_EQ(1, tasks_.size());
  std::unique_ptr<ReadStorageTask> task = std::move(tasks_.front());
  tasks_.clear();

  // STARTED should have been queued.
  ASSERT_EQ(1, messages_.size());
  messages_.clear();

  // Complete the read task and ship a record.
  // Since STARTED hasn't been sent yet, this should be flagged as an error
  {
    ReadStorageTask::RecordContainer records;
    records.push_back(createFakeRecord(1, 100));
    task->status_ = E::CAUGHT_UP;
    task->records_ = std::move(records);
    task->read_ctx_.read_ptr_ = {lsn_t{2}};
    streams_.onReadTaskDone(*task);
    ASSERT_EQ(1, messages_.size());
    enqueue_time = SteadyTimestamp::now();
    std::unique_ptr<RECORD_Message> msg(createFakeRecordMessage(id1, 1, 100));
    streams_.onRecordSent(client_id_, *msg, enqueue_time);
    ASSERT_EQ(1, stats.read_stream_record_violations);
  }

  // Same thing for a gap.
  {
    enqueue_time = SteadyTimestamp::now();
    auto msg = createFakeGapMessage(id1, 1, 10, GapReason::NO_RECORDS);
    streams_.onGapSent(client_id_, *msg, enqueue_time);
    ASSERT_EQ(1, stats.read_stream_gap_violations);
  }
}

}} // namespace facebook::logdevice
