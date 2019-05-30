/**
 * Copyright (c) 2017-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include "logdevice/common/Processor.h"
#include "logdevice/common/RateLimiter.h"
#include "logdevice/common/RebuildingTypes.h"
#include "logdevice/common/SimpleEnumMap.h"
#include "logdevice/common/configuration/UpdateableConfig.h"
#include "logdevice/common/settings/RebuildingSettings.h"
#include "logdevice/server/locallogstore/LocalLogStore.h"
#include "logdevice/server/rebuilding/RebuildingReadStorageTaskV2.h"

namespace facebook { namespace logdevice {

class ShardRebuildingV2 : public ShardRebuildingInterface {
 public:
  ShardRebuildingV2(shard_index_t shard,
                    lsn_t rebuilding_version,
                    lsn_t restart_version,
                    std::shared_ptr<const RebuildingSet> rebuilding_set,
                    UpdateableSettings<RebuildingSettings> rebuilding_settings,
                    NodeID my_node_id,
                    ShardRebuildingInterface::Listener* listener);
  ~ShardRebuildingV2() override;

  void start(std::unordered_map<logid_t, std::unique_ptr<RebuildingPlan>> plan)
      override;
  void advanceGlobalWindow(RecordTimestamp new_window_end) override;
  void noteConfigurationChanged() override;
  void noteRebuildingSettingsChanged() override;

  void onReadTaskDone(std::vector<std::unique_ptr<ChunkData>> chunks);
  void onChunkRebuildingDone(log_rebuilding_id_t chunk_id,
                             RecordTimestamp oldest_timestamp);

  void getDebugInfo(InfoRebuildingShardsTable& table) const override;

  // To collect per-log debug info, call this from the worker thread, pass
  // the returned function to some non-worker thread and call it from there.
  // This is needed because the per-log states may be in use by the storage
  // task; in this case the returned function waits for the storage task to
  // complete, which is not a good thing to do on a worker thread.
  std::function<void(InfoRebuildingLogsTable&)> beginGetLogsDebugInfo() const;

 protected:
  // The following may be overridden by tests.
  virtual StatsHolder* getStats();
  virtual node_index_t getMyNodeIndex();
  virtual worker_id_t startChunkRebuilding(std::unique_ptr<ChunkData> chunk,
                                           log_rebuilding_id_t chunk_id);
  virtual std::chrono::milliseconds getIteratorTTL();
  virtual void putStorageTask();
  virtual std::unique_ptr<TimerInterface> createTimer(std::function<void()> cb);

 protected:
  // Key in the ordered map of in-flight chunk rebuildings.
  struct ChunkRebuildingKey {
    RecordTimestamp oldestTimestamp;
    log_rebuilding_id_t chunkID;

    ChunkRebuildingKey() = default;
    ChunkRebuildingKey(RecordTimestamp oldest_timestamp,
                       log_rebuilding_id_t chunk_id)
        : oldestTimestamp(oldest_timestamp), chunkID(chunk_id) {}

    // Compares oldestTimestamp first.
    bool operator<(const ChunkRebuildingKey& rhs) const;
  };

  struct ChunkRebuildingInfo {
    ChunkAddress address;
    size_t numRecords;
    size_t totalBytes;
    worker_id_t workerID;
  };

  lsn_t rebuildingVersion_{LSN_INVALID};
  lsn_t restartVersion_{LSN_INVALID};
  uint32_t shard_;
  // Rebuilding set and config can be nullptr in tests.
  std::shared_ptr<const RebuildingSet> rebuildingSet_;
  UpdateableSettings<RebuildingSettings> rebuildingSettings_;
  NodeID my_node_id_;
  Listener* listener_;
  bool completed_ = false;

  RecordTimestamp globalWindowEnd_{RecordTimestamp::max()};

  // There's at most one RebuildingReadStorageTaskV2 in flight at any time.
  bool storageTaskInFlight_ = false;
  // The reading context is shared between us and the storage task.
  // When a storage task is in flight, we're not allowed to access the context.
  std::shared_ptr<RebuildingReadStorageTaskV2::Context> readContext_;

  RateLimiter readRateLimiter_;
  // The timer is used when readRateLimiter_ tells us to wait before reading.
  std::unique_ptr<TimerInterface> delayedReadTimer_;

  // Records we've read but haven't started ChunkRebuilding yet.
  std::deque<std::unique_ptr<ChunkData>> readBuffer_;
  size_t bytesInReadBuffer_ = 0;

  // Information about in-flight ChunkRebuildings.
  // Used for finding the timestamp of the oldest record being rebuilt, to make
  // sure it's not too far behind.
  // The ChunkRebuildings themselves are not directly accessible from here
  // because live on different worker threads.
  std::map<ChunkRebuildingKey, ChunkRebuildingInfo> chunkRebuildings_;

  // Sum of numRecords and totalBytes in chunkRebuildings_.
  size_t chunkRebuildingRecordsInFlight_ = 0;
  size_t chunkRebuildingBytesInFlight_ = 0;

  // If iterator doesn't get seeked for some time, this timer fires and
  // invalidates it. For rocksdb-based LocalLogStore implementations the
  // invalidation prevents the iterator from pinning old versions of
  // data indefinitely.
  std::unique_ptr<TimerInterface> iteratorInvalidationTimer_;

  WorkerCallbackHelper<ShardRebuildingV2> callbackHelper_;

  static std::atomic<log_rebuilding_id_t::raw_type> nextChunkID_;

  // Posts requests to abort state machines listed in chunkRebuildings_.
  void abortChunkRebuildings();

  void sendStorageTaskIfNeeded();
  void startSomeChunkRebuildingsIfNeeded();
  void finalizeIfNeeded();

  void invalidateIterator();

  void tryMakeProgress();

  // Stuff below is for instrumentation and stats.

  // At each moment in time shard rebuilding is in one of these states.
  // Might be useful for finding bottlenecks and tweaking settings.
  enum class ProfilingState {
    // There's a read storage task in flight and at least one ChunkRebuilding
    // in flight.
    FULLY_OCCUPIED,
    // There's a read storage task in flight but no ChunkRebuildings in flight.
    WAITING_FOR_READ,
    // Waiting for rate limiter to allow us to send a read storage task.
    RATE_LIMITED,
    // There are ChunkRebuildings in flight but no read task (because read
    // buffer is full, i.e. ChunkRebuildings can't keep up with reads).
    WAITING_FOR_REREPLICATION,
    // There are neither ChunkRebuildings nor read task in flight.
    // We either reached the end of global window or hit a permanent error.
    STALLED,

    // Not a state.
    MAX,
  };
  static const SimpleEnumMap<ProfilingState, std::string>&
  profilingStateNames();

  SteadyTimestamp startTime_;
  // Current state.
  ProfilingState profilingState_ = ProfilingState::MAX;
  // Time spent in each state since the beginning of rebuilding, as of time
  // currentStateStartTime_.
  std::array<std::chrono::milliseconds, (size_t)ProfilingState::MAX>
      totalTimeByState_{};
  SteadyTimestamp currentStateStartTime_ = SteadyTimestamp::min();
  size_t chunksRebuilt_ = 0;
  size_t recordsRebuilt_ = 0;
  size_t bytesRebuilt_ = 0;
  size_t readTasksDone_ = 0;
  // These are duplicated from readContext_ to make sure we always have
  // lock-free access to them.
  size_t numLogs_;
  std::shared_ptr<LocalLogStore::AllLogsIterator::Location> nextLocation_;
  // Calls flushCurrentStateTime() every minute, to make sure we're publishing
  // accurate time spent in each state even when state doesn't change often.
  std::unique_ptr<TimerInterface> profilingTimer_;

  // How far the iterator has read, approximately.
  // Note that this may not correspond to any record.
  // In particular, if we're filtering out very long ranges of data, this
  // iterator will show progress of the filtering, while any record-based
  // indicators would stand still until we find a record that passes filter.
  RecordTimestamp readingProgressTimestamp_ = RecordTimestamp::min();
  // Value between 0 and 1 indicating approximately what fraction of the data
  // we have read. -1 means not supported.
  double readingProgress_ = 0;

  // Advances currentStateStartTime_ to current time, updating totalTimeByState_
  // and stats as needed.
  void flushCurrentStateTime();
  void updateProfilingState();
  std::string describeTimeByState() const;
};

}} // namespace facebook::logdevice
