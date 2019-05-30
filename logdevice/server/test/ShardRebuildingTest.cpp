/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include <gtest/gtest.h>

#include "logdevice/common/settings/SettingsUpdater.h"
#include "logdevice/common/test/MockTimer.h"
#include "logdevice/server/rebuilding/ShardRebuildingV2.h"

namespace facebook { namespace logdevice {

static const node_index_t MY_NODE_IDX = 13;
static const shard_index_t SHARD_IDX = 4;
static const lsn_t REBUILDING_VERSION = 42;
static const lsn_t RESTART_VERSION = 420;

class MockedShardRebuilding : public ShardRebuildingV2,
                              public ShardRebuildingInterface::Listener {
 public:
  struct ChunkInfo {
    log_rebuilding_id_t id;
    std::unique_ptr<ChunkData> data;
    worker_id_t worker;
  };

  StatsHolder stats;
  bool taskInFlight = false;
  bool waitingForGlobalWindow = false;
  bool completed = false;

  // Populated by ShardRebuilding and emptied by simulateChunkRebuildingDone().
  std::deque<ChunkInfo> chunkRebuildings;
  // Populated by ShardRebuilding and emptied by tests.
  std::deque<RecordTimestamp> donorProgress;

  // These are used for verifying stats about waiting for global window.
  SteadyTimestamp beforeStartedWaiting;
  SteadyTimestamp afterStartedWaiting;
  int64_t waitedForGlobalWindow = 0;
  // Call this after any operation that may change
  // rebuilding_global_window_waiting_flag stat.
  // Test cases are also encouraged to sleep for 1ms before each operation that
  // starts waiting for global window and after each operation that stops
  // waiting (i.e. advances global window far enough).
  void globalWindowWaitingMayHaveChanged(SteadyTimestamp before_op) {
    bool waiting = stats.get()
                       .per_shard_stats->get(SHARD_IDX)
                       ->rebuilding_global_window_waiting_flag;
    int64_t waited_ms =
        stats.get().per_shard_stats->get(SHARD_IDX)->rebuilding_ms_stalled;
    if (waiting == waitingForGlobalWindow) {
      EXPECT_EQ(waited_ms, waitedForGlobalWindow);
      return;
    }
    if (waiting) {
      beforeStartedWaiting = before_op;
      afterStartedWaiting = SteadyTimestamp::now();
      EXPECT_EQ(waited_ms, waitedForGlobalWindow);
    } else {
      auto min_expected = to_msec(before_op - afterStartedWaiting).count();
      auto max_expected =
          to_msec(SteadyTimestamp::now() - beforeStartedWaiting).count();
      int64_t delta = waited_ms - waitedForGlobalWindow;
      EXPECT_GE(delta, min_expected);
      EXPECT_LE(delta, max_expected);
      waitedForGlobalWindow = waited_ms;
    }
    waitingForGlobalWindow = waiting;
  }

  // ShardRebuildingV2 doesn't directly use rebuilding set and config, it just
  // passes them through to storage task and chunk rebuildings, which are mocked
  // out in this test. So we just use nullptrs here.
  explicit MockedShardRebuilding(
      UpdateableSettings<RebuildingSettings> rebuilding_settings)
      : ShardRebuildingV2(SHARD_IDX,
                          REBUILDING_VERSION,
                          RESTART_VERSION,
                          /* rebuilding_set */ nullptr,
                          rebuilding_settings,
                          /* my_node_id */ NodeID(),
                          /* listener */ this),
        stats(StatsParams().setIsServer(true)) {}

  ~MockedShardRebuilding() override {
    // Make sure ~ShardRebuilding doesn't try to post any
    // AbortChunkRebuildingRequest-s to Processor, since we don't have a
    // Processor.
    chunkRebuildings_.clear();
  }

  StatsHolder* getStats() override {
    return &stats;
  }
  std::unique_ptr<TimerInterface>
  createTimer(std::function<void()> cb) override {
    return std::make_unique<MockTimer>(cb);
  }
  std::chrono::milliseconds getIteratorTTL() override {
    return std::chrono::seconds(20);
  }
  node_index_t getMyNodeIndex() override {
    return MY_NODE_IDX;
  }
  worker_id_t startChunkRebuilding(std::unique_ptr<ChunkData> chunk,
                                   log_rebuilding_id_t chunk_id) override {
    worker_id_t worker{(int)(chunk_id.val() % 10)};
    chunkRebuildings.emplace_back(
        ChunkInfo{.id = chunk_id, .data = std::move(chunk), .worker = worker});
    return worker;
  }
  void putStorageTask() override {
    EXPECT_FALSE(taskInFlight);
    taskInFlight = true;
  }

  void onShardRebuildingComplete(uint32_t shard_idx) override {
    EXPECT_EQ(shard_idx, SHARD_IDX);
    EXPECT_FALSE(completed);
    completed = true;
  }

  void notifyShardDonorProgress(uint32_t shard,
                                RecordTimestamp next_ts,
                                lsn_t version,
                                double progress_estimate) override {
    EXPECT_EQ(shard, SHARD_IDX);
    EXPECT_EQ(version, REBUILDING_VERSION);
    if (next_ts != RecordTimestamp::min()) {
      donorProgress.push_back(next_ts);
    }
  }

  // Use these simulate*() wrappers instead of calling the methods directly.
  // The wrappers update some fields and verify some stats.

  void simulateAdvanceGlobalWindow(RecordTimestamp window_end) {
    auto before = SteadyTimestamp::now();
    advanceGlobalWindow(window_end);
    globalWindowWaitingMayHaveChanged(before);
  }

  void simulateReadTaskDone(std::vector<ChunkData*> chunks,
                            bool reached_end = false) {
    ld_check(taskInFlight);
    taskInFlight = false;

    ld_check(!readContext_->reachedEnd);
    readContext_->reachedEnd = reached_end;
    auto before = SteadyTimestamp::now();
    readContext_->onDone(
        std::vector<std::unique_ptr<ChunkData>>(chunks.begin(), chunks.end()));
    globalWindowWaitingMayHaveChanged(before);
  }

  void simulatePersistentError() {
    ld_check(taskInFlight);
    taskInFlight = false;

    readContext_->persistentError = true;
    auto before = SteadyTimestamp::now();
    readContext_->onDone({});
    globalWindowWaitingMayHaveChanged(before);
  }

  // idx is index in chunkRebuildings.
  void simulateChunkRebuildingDone(size_t idx) {
    auto before = SteadyTimestamp::now();
    onChunkRebuildingDone(chunkRebuildings.at(idx).id,
                          chunkRebuildings.at(idx).data->oldestTimestamp);
    globalWindowWaitingMayHaveChanged(before);
    chunkRebuildings.erase(chunkRebuildings.begin() + idx);
  }
};

class ShardRebuildingTest : public ::testing::Test {
 public:
  ShardRebuildingTest() {
    rebuildingSettingsUpdater_.registerSettings(rebuildingSettings_);
  }

  UpdateableSettings<RebuildingSettings> rebuildingSettings_;
  SettingsUpdater rebuildingSettingsUpdater_;
};

const RecordTimestamp BASE_TIME =
    RecordTimestamp(std::chrono::milliseconds(946713600000));
const std::chrono::milliseconds MINUTE = std::chrono::minutes(1);

// We use raw pointer here because unique_ptr doesn't work in initializer lists
// (initializer lists don't support rvalue references for some reason).
ChunkData* makeChunk(logid_t log,
                     lsn_t min_lsn,
                     lsn_t max_lsn,
                     size_t bytes,
                     RecordTimestamp oldest_timestamp = BASE_TIME,
                     size_t block_id = 11) {
  auto c = new ChunkData();
  c->address.log = log;
  c->address.min_lsn = min_lsn;
  c->address.max_lsn = max_lsn;
  c->blockID = block_id;
  c->oldestTimestamp = oldest_timestamp;
  // ShardRebuilding doesn't use it directly, use nullptr.
  c->replication = nullptr;

  std::string blob((bytes - 1) / (max_lsn - min_lsn + 1) + 1, 'x');
  for (lsn_t lsn = min_lsn; lsn <= max_lsn; ++lsn) {
    c->addRecord(lsn,
                 Slice(blob.data(),
                       bytes / (max_lsn - min_lsn + 1) +
                           (lsn - min_lsn < bytes % (max_lsn - min_lsn + 1))));
  }

  return c;
}

#define EXPECT_DONOR_PROGRESS(ts)                                    \
  do {                                                               \
    ASSERT_EQ(std::deque<RecordTimestamp>({ts}), reb.donorProgress); \
    reb.donorProgress.clear();                                       \
  } while (false)

#define PER_SHARD_STAT(s) reb.stats.get().per_shard_stats->get(SHARD_IDX)->s

TEST_F(ShardRebuildingTest, Basic) {
  MockedShardRebuilding reb(rebuildingSettings_);
  // ShardRebuilding doesn't directly use rebuilding plan, it just passes it to
  // readContext_. So we can pass an empty plan.
  reb.start({});
  EXPECT_TRUE(reb.taskInFlight);
  EXPECT_EQ(0, reb.chunkRebuildings.size());
  EXPECT_EQ(0, reb.donorProgress.size());

  // Read one chunk. Chunk rebuilding and another storage task will be started.
  reb.simulateReadTaskDone({makeChunk(logid_t(1), 100, 101, 10, BASE_TIME)});
  EXPECT_TRUE(reb.taskInFlight);
  ASSERT_EQ(1, reb.chunkRebuildings.size());
  EXPECT_EQ(101, reb.chunkRebuildings[0].data->address.max_lsn);
  EXPECT_DONOR_PROGRESS(BASE_TIME);

  // Chunk rebuilding done. Just waiting for storage task now.
  reb.simulateChunkRebuildingDone(0);
  EXPECT_TRUE(reb.taskInFlight);
  ASSERT_EQ(0, reb.chunkRebuildings.size());
  ASSERT_EQ(0, reb.donorProgress.size());

  // Read 2 chunks, reached end.
  reb.simulateReadTaskDone(
      {makeChunk(logid_t(2), 50, 52, 30, BASE_TIME + MINUTE * 2),
       makeChunk(logid_t(1), 102, 110, 40, BASE_TIME + MINUTE)},
      true);
  EXPECT_FALSE(reb.taskInFlight);
  ASSERT_EQ(2, reb.chunkRebuildings.size());
  EXPECT_EQ(50, reb.chunkRebuildings[0].data->address.min_lsn);
  EXPECT_EQ(1, reb.chunkRebuildings[1].data->address.log.val());
  EXPECT_DONOR_PROGRESS(BASE_TIME + MINUTE);

  // The two chunk rebuildings come back out of order.

  reb.simulateChunkRebuildingDone(1);
  EXPECT_FALSE(reb.taskInFlight);
  ASSERT_EQ(1, reb.chunkRebuildings.size());
  EXPECT_DONOR_PROGRESS(BASE_TIME + MINUTE * 2);

  EXPECT_FALSE(reb.completed);

  reb.simulateChunkRebuildingDone(0);
  EXPECT_FALSE(reb.taskInFlight);
  ASSERT_EQ(0, reb.chunkRebuildings.size());
  ASSERT_EQ(0, reb.donorProgress.size());

  EXPECT_TRUE(reb.completed);
}

TEST_F(ShardRebuildingTest, NothingToRebuild) {
  MockedShardRebuilding reb(rebuildingSettings_);
  reb.start({});
  reb.simulateReadTaskDone({}, true);
  EXPECT_FALSE(reb.taskInFlight);
  ASSERT_EQ(0, reb.chunkRebuildings.size());
  ASSERT_EQ(0, reb.donorProgress.size());
  EXPECT_TRUE(reb.completed);
}

TEST_F(ShardRebuildingTest, WindowsAndLimits) {
  rebuildingSettingsUpdater_.setFromCLI(
      // Keep the difference between the newest and oldest ChunkRebuilding below
      // 30 minutes.
      {{"rebuilding-local-window", "30min"},
       // At most 1 KB's worth of ChunkRebuildings at a time.
       {"rebuilding-max-record-bytes-in-flight", "1000"},
       // At most 3 ChunkRebuildings at a time.
       {"rebuilding-max-records-in-flight", "6"},
       // Stop sending storage tasks if read buffer gets bigger than 1500 B
       // (750*(3-1), the 3 being hardcoded in ShardRebuilding).
       {"rebuilding-max-batch-bytes", "750"}});

  MockedShardRebuilding reb(rebuildingSettings_);
  reb.simulateAdvanceGlobalWindow(BASE_TIME - MINUTE);
  reb.start({});

  /* sleep override */
  std::this_thread::sleep_for(std::chrono::milliseconds(1));

  // Read 3 records, all above global window.
  reb.simulateReadTaskDone(
      {makeChunk(logid_t(2), 50, 51, 600, BASE_TIME + MINUTE),
       makeChunk(logid_t(1), 102, 103, 100, BASE_TIME),
       makeChunk(logid_t(1), 120, 121, 200, BASE_TIME + MINUTE * 10)});
  EXPECT_TRUE(reb.taskInFlight);
  EXPECT_FALSE(reb.waitingForGlobalWindow);
  ASSERT_EQ(0, reb.chunkRebuildings.size());
  EXPECT_DONOR_PROGRESS(BASE_TIME + MINUTE);

  /* sleep override */
  std::this_thread::sleep_for(std::chrono::milliseconds(1));

  // Move global window to allow 2 out of 3 records to proceed.
  reb.simulateAdvanceGlobalWindow(BASE_TIME + MINUTE * 5);
  EXPECT_FALSE(reb.waitingForGlobalWindow);
  ASSERT_EQ(2, reb.chunkRebuildings.size());
  EXPECT_DONOR_PROGRESS(BASE_TIME);

  // Read some more records. Now the read buffer is too big to read more.
  reb.simulateReadTaskDone(
      {makeChunk(logid_t(3), 1, 2, 500, BASE_TIME + MINUTE * 2),
       makeChunk(logid_t(1), 130, 131, 300, BASE_TIME + MINUTE * 20),
       makeChunk(logid_t(2), 53, 54, 800, BASE_TIME + MINUTE * 35)});
  EXPECT_FALSE(reb.taskInFlight);
  ASSERT_EQ(2, reb.chunkRebuildings.size());
  EXPECT_DONOR_PROGRESS(BASE_TIME);

  /* sleep override */
  std::this_thread::sleep_for(std::chrono::milliseconds(1));

  // Move global window to above all the buffered records.
  // One chunk rebuilding starts, and the max-records-in-flight is hit.
  reb.simulateAdvanceGlobalWindow(BASE_TIME + MINUTE * 40);
  EXPECT_FALSE(reb.taskInFlight);
  ASSERT_EQ(3, reb.chunkRebuildings.size());
  EXPECT_DONOR_PROGRESS(BASE_TIME);

  // A chunk rebuilding completes, one new one starts before
  // max-record-bytes-in-flight and max-records-in-flight are both hit.
  // Also read buffer gets small enough to start a read task.
  reb.simulateChunkRebuildingDone(1);
  EXPECT_TRUE(reb.taskInFlight);
  ASSERT_EQ(3, reb.chunkRebuildings.size());
  EXPECT_DONOR_PROGRESS(BASE_TIME + MINUTE);

  // A chunk rebuilding completes, and nothing happens because we're still
  // over max-record-bytes-in-flight.
  reb.simulateChunkRebuildingDone(1);
  ASSERT_EQ(2, reb.chunkRebuildings.size());
  EXPECT_DONOR_PROGRESS(BASE_TIME + MINUTE);

  // A chunk rebuilding completes, one new one starts before
  // rebuilding-local-window is hit.
  reb.simulateChunkRebuildingDone(0);
  ASSERT_EQ(2, reb.chunkRebuildings.size());
  EXPECT_DONOR_PROGRESS(BASE_TIME + MINUTE * 2);

  // Read one last record, above global window.
  reb.simulateReadTaskDone(
      {makeChunk(logid_t(1), 300, 301, 10, BASE_TIME + MINUTE * 45)}, true);
  EXPECT_FALSE(reb.taskInFlight);
  ASSERT_EQ(2, reb.chunkRebuildings.size());
  EXPECT_DONOR_PROGRESS(BASE_TIME + MINUTE * 2);

  // A chunk rebuilding completes, one new one starts before global window is
  // hit.
  reb.simulateChunkRebuildingDone(0);
  EXPECT_FALSE(reb.waitingForGlobalWindow);
  ASSERT_EQ(2, reb.chunkRebuildings.size());
  EXPECT_DONOR_PROGRESS(BASE_TIME + MINUTE * 20);

  // Still waiting for global window.
  reb.simulateChunkRebuildingDone(0);
  ASSERT_EQ(1, reb.chunkRebuildings.size());
  EXPECT_DONOR_PROGRESS(BASE_TIME + MINUTE * 35);

  /* sleep override */
  std::this_thread::sleep_for(std::chrono::milliseconds(1));

  // Still waiting for global window, but this time there's nothing in flight,
  // so it counts as a stall.
  reb.simulateChunkRebuildingDone(0);
  EXPECT_TRUE(reb.waitingForGlobalWindow);
  ASSERT_EQ(0, reb.chunkRebuildings.size());
  EXPECT_DONOR_PROGRESS(BASE_TIME + MINUTE * 45);

  /* sleep override */
  std::this_thread::sleep_for(std::chrono::milliseconds(1));

  // Move global window. The last chunk rebuilding starts.
  reb.simulateAdvanceGlobalWindow(BASE_TIME + MINUTE * 50);
  EXPECT_FALSE(reb.waitingForGlobalWindow);
  ASSERT_EQ(1, reb.chunkRebuildings.size());
  EXPECT_DONOR_PROGRESS(BASE_TIME + MINUTE * 45);

  /* sleep override */
  std::this_thread::sleep_for(std::chrono::milliseconds(1));

  // The last chunk rebuilding completes, and we're all done.
  reb.simulateChunkRebuildingDone(0);
  EXPECT_FALSE(reb.taskInFlight);
  ASSERT_EQ(0, reb.chunkRebuildings.size());
  EXPECT_EQ(0, reb.donorProgress.size());
  EXPECT_TRUE(reb.completed);
}

TEST_F(ShardRebuildingTest, PersistentError) {
  MockedShardRebuilding reb(rebuildingSettings_);
  reb.start({});

  reb.simulateReadTaskDone({makeChunk(logid_t(1), 300, 301, 10, BASE_TIME)});
  EXPECT_TRUE(reb.taskInFlight);
  ASSERT_EQ(1, reb.chunkRebuildings.size());
  EXPECT_DONOR_PROGRESS(BASE_TIME);

  reb.simulatePersistentError();
  EXPECT_FALSE(reb.taskInFlight);
  ASSERT_EQ(1, reb.chunkRebuildings.size());
  EXPECT_DONOR_PROGRESS(BASE_TIME);

  reb.simulateChunkRebuildingDone(0);
  EXPECT_FALSE(reb.taskInFlight);
  ASSERT_EQ(0, reb.chunkRebuildings.size());
  EXPECT_EQ(0, reb.donorProgress.size());
  EXPECT_FALSE(reb.completed);
}

TEST_F(ShardRebuildingTest, TestStallRebuilding) {
  rebuildingSettingsUpdater_.setFromCLI({{"test-stall-rebuilding", "true"}});

  MockedShardRebuilding reb(rebuildingSettings_);
  reb.start({});
  EXPECT_FALSE(reb.taskInFlight);
  EXPECT_FALSE(reb.completed);
  ASSERT_EQ(0, reb.chunkRebuildings.size());
  ASSERT_EQ(0, reb.donorProgress.size());
}

// TODO: getDebugInfo()
// TODO: getDebugInfo() while waiting for global window
// TODO: getDebugInfo() while have and don't have storage task in flight
// TODO: noteConfigurationChanged()

}} // namespace facebook::logdevice
