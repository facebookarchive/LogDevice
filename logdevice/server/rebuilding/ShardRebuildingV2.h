/**
 * Copyright (c) 2017-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include "logdevice/common/Processor.h"
#include "logdevice/common/RebuildingTypes.h"
#include "logdevice/common/configuration/UpdateableConfig.h"
#include "logdevice/common/settings/RebuildingSettings.h"
#include "logdevice/server/rebuilding/RebuildingReadStorageTaskV2.h"

namespace facebook { namespace logdevice {

class ShardRebuildingV2 : public ShardRebuildingInterface {
 public:
  ShardRebuildingV2(shard_index_t shard,
                    lsn_t rebuilding_version,
                    lsn_t restart_version,
                    std::shared_ptr<const RebuildingSet> rebuilding_set,
                    UpdateableSettings<RebuildingSettings> rebuilding_settings,
                    std::shared_ptr<UpdateableConfig> config,
                    ShardRebuildingInterface::Listener* listener);
  ~ShardRebuildingV2() override;

  void start(std::unordered_map<logid_t, std::unique_ptr<RebuildingPlan>> plan)
      override;
  void advanceGlobalWindow(RecordTimestamp new_window_end) override;
  void noteConfigurationChanged() override;
  void getDebugInfo(InfoShardsRebuildingTable& table) const override;

  void onReadTaskDone(std::vector<std::unique_ptr<ChunkData>> chunks);
  void onChunkRebuildingDone(log_rebuilding_id_t chunk_id,
                             RecordTimestamp oldest_timestamp);

 protected:
  // The following may be overridden by tests.

  virtual StatsHolder* getStats();
  virtual node_index_t getMyNodeIndex();
  virtual worker_id_t startChunkRebuilding(std::unique_ptr<ChunkData> chunk,
                                           log_rebuilding_id_t chunk_id);

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
  std::shared_ptr<UpdateableConfig> config_;
  Listener* listener_;
  bool completed_ = false;

  RecordTimestamp globalWindowEnd_{RecordTimestamp::max()};
  // If we are waiting for global window to slide, this is when we started
  // waiting.
  SteadyTimestamp waitingOnGlobalWindowSince_ = SteadyTimestamp::max();

  // There's at most one RebuildingReadStorageTaskV2 in flight at any time.
  bool storageTaskInFlight_ = false;
  // The reading context is shared between us and the storage task.
  // When a storage task is in flight, we're not allowed to access the context.
  std::shared_ptr<RebuildingReadStorageTaskV2::Context> readContext_;

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

  WorkerCallbackHelper<ShardRebuildingV2> callbackHelper_;

  static std::atomic<log_rebuilding_id_t::raw_type> nextChunkID_;

  // Posts requests to abort state machines listed in chunkRebuildings_.
  void abortChunkRebuildings();

  virtual void putStorageTask();

  void sendStorageTaskIfNeeded();
  void startSomeChunkRebuildingsIfNeeded();
  void finalizeIfNeeded();

  void tryMakeProgress() {
    startSomeChunkRebuildingsIfNeeded();
    sendStorageTaskIfNeeded();
    finalizeIfNeeded();
  }
};

}} // namespace facebook::logdevice
