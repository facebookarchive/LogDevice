/**
 * Copyright (c) 2017-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include "logdevice/server/locallogstore/LocalLogStore.h"
#include "logdevice/server/rebuilding/ChunkRebuilding.h"
#include "logdevice/server/rebuilding/RebuildingPlan.h"
#include "logdevice/server/storage_tasks/StorageTask.h"

namespace facebook { namespace logdevice {

/**
 * @file Task created by LogRebuilding state machines when they need data read
 *       from the local log store.  Upon completion, the task (including the
 *       result) gets sent back to the worker thread.
 */

class RebuildingReadStorageTaskV2 : public StorageTask {
 public:
  struct Context {
    struct LogState {
      RebuildingPlan plan;

      lsn_t lastSeenLSN = LSN_INVALID;

      // The copyset of the last record we've read.
      std::vector<ShardID> lastSeenCopyset;

      // Whenever the copyset of a record doesn't match the copyset of the
      // previous one, this counter is bumped. It is used to hash the copyset
      // selector's RNG to preserve sticky copyset blocks after rebuilding.
      size_t currentBlockID{0};

      // A counter we keep to split blocks that get too large. Without this
      // blocks that get accidentally "fused" together because they happen to
      // have identical copysets will never get split. In a cluster with a
      // small number of nodes this could lead to very large block sizes after
      // rebuilding a significant part of the cluster, which will create an
      // unbalanced distribution of data.
      size_t bytesInCurrentBlock{0};

      // Cached information about epoch metadata for last seen record, and the
      // range of epochs it covers (right-open interval), as looked up in
      // `plan`. If currentEpochRange is valid but currentEpochMetadata is
      // nullptr, it's a cached negative result: it means this epoch range is
      // not covered by `plan`.
      std::pair<epoch_t, epoch_t> currentEpochRange{EPOCH_INVALID,
                                                    EPOCH_INVALID};
      std::shared_ptr<EpochMetaData> currentEpochMetadata;
      // ReplicationScheme corresponding to currentEpochMetadata.
      // Initialized lazily, can be nullptr even if currentEpochMetadata is not.
      std::shared_ptr<ReplicationScheme> currentReplication;
    };

    // Immutable parameters.
    // The onDone callback is called from worker thread.
    std::function<void(std::vector<std::unique_ptr<ChunkData>>)> onDone;
    std::shared_ptr<const RebuildingSet> rebuildingSet;
    UpdateableSettings<RebuildingSettings> rebuildingSettings;
    ShardID myShardID;

    // The mutable fields are accessed by storage task on storage thread and
    // by ShardRebuilding on worker thread. The ShardRebuilding only accesses
    // them when there's no storage task in flight or in queue.

    // What to read.
    std::unordered_map<logid_t, LogState> logs;
    // A long-living main iterator. If nullptr, the storage task will create it.
    // After storage task is done, iterator is either invalid or points
    // at nextLocation.
    std::unique_ptr<LocalLogStore::AllLogsIterator> iterator;
    // The first location not processed yet.
    // Next storage task needs to start reading from here.
    std::unique_ptr<LocalLogStore::AllLogsIterator::Location> nextLocation;
    // If we encounter too many invalid records, stall rebuilding just in case.
    size_t numMalformedRecordsSeen{0};

    // true if we're finished reading.
    bool reachedEnd = false;
    // true if we're not going to be able to read everything we need.
    bool persistentError = false;
  };

  class Filter : public LocalLogStoreReadFilter {
   public:
    enum class FilteredReason {
      SCD,
      NOT_DIRTY,
      DRAINED,
      TIMESTAMP,
      EPOCH_RANGE
    };

    Filter(RebuildingReadStorageTaskV2* task, Context* context);

    bool operator()(logid_t log,
                    lsn_t lsn,
                    const ShardID* copyset,
                    const copyset_size_t copyset_size,
                    const csi_flags_t csi_flags,
                    RecordTimestamp min_ts,
                    RecordTimestamp max_ts) override;

    bool shouldProcessTimeRange(RecordTimestamp min,
                                RecordTimestamp max) override;

    /**
     * Update stats regarding skipped records.
     * @param late  true if the filter was called on the full record rather than
     *              CSI entry.
     */
    void noteRecordFiltered(FilteredReason reason, bool late);

    RebuildingReadStorageTaskV2* task;
    Context* context;

    // Just a cache to avoid lookup in context->logs.
    // If currentLog is valid but currentLogState is nullptr, it means this log
    // is not in context->logs, i.e. we're not interested in it.
    logid_t currentLog = LOGID_INVALID;
    Context::LogState* currentLogState = nullptr;

    // Cached set of shards that are effectively not in the rebuilding set,
    // as long as the given time range is concerned.
    // This struct uses the fact that operator() is usually called many times
    // in a row with the same min_ts and max_ts.
    struct {
      RecordTimestamp minTs = RecordTimestamp::min();
      RecordTimestamp maxTs = RecordTimestamp::max();
      // ShardID+DataClass pairs whose dirty ranges have no intersection with
      // time range [minTs, maxTs].
      // The dirty ranges are rebuildingSet.shards[s].dc_dirty_ranges[dc].
      std::unordered_set<std::pair<ShardID, DataClass>> shardsOutsideTimeRange;

      bool valid(RecordTimestamp min_ts, RecordTimestamp max_ts) const {
        return min_ts == minTs && max_ts == maxTs;
      }

      void clear() {
        minTs = RecordTimestamp::min();
        maxTs = RecordTimestamp::max();
        shardsOutsideTimeRange.clear();
      }
    } timeRangeCache;

    // How many records we filtered out for various reasons. Used for logging.
    size_t nRecordsLateFiltered{0};
    size_t nRecordsSCDFiltered{0};
    size_t nRecordsNotDirtyFiltered{0};
    size_t nRecordsDrainedFiltered{0};
    size_t nRecordsTimestampFiltered{0};
    size_t nRecordsEpochRangeFiltered{0};
  };

  explicit RebuildingReadStorageTaskV2(std::weak_ptr<Context> context);

  void execute() override;

  void onDone() override;
  void onDropped() override;

  // There can be at most one task of this type in queue at any given time.
  // Dropping it won't achieve much in way of clearing the queue.
  bool isDroppable() const override {
    return false;
  }

  ThreadType getThreadType() const override {
    // Read tasks may take a while to execute, so they shouldn't block fast
    // write operations.
    return ThreadType::SLOW;
  }

  Priority getPriority() const override {
    // Rebuilding reads should be lo-pri compared to regular reads
    return Priority::LOW;
  }

 protected:
  // Can be overridden in tests.
  virtual UpdateableSettings<Settings> getSettings();
  virtual std::shared_ptr<UpdateableConfig> getConfig();
  virtual StatsHolder* getStats();

  virtual std::unique_ptr<LocalLogStore::AllLogsIterator>
  createIterator(const LocalLogStore::ReadOptions& opts);

 private:
  std::weak_ptr<Context> context_;
  std::vector<std::unique_ptr<ChunkData>> result_;

  // Checks if the copyset has changed compared to the last seen record and
  // bumps currentBlockID if it has.
  // @param temp_copyset is just a scratch buffer for use inside the function.
  //   The function will resize it as needed. The caller can reuse the buffer
  //   between calls as an optimization to avoid a memory allocation.
  // If record is invalid, sets erro to E::MALFORMED_RECORD and returns -1.
  int checkRecordForBlockChange(logid_t log,
                                lsn_t lsn,
                                Slice record,
                                Context* context,
                                Context::LogState* log_state,
                                std::vector<ShardID>* temp_copyset,
                                RecordTimestamp* out_timestamp);

  // Makes sure that log_state->currentEpochMetadata covers `lsn`.
  // Returns false if `lsn` is not covered by RebuildingPlan and should be
  // skipped.
  // If `create_replication_scheme` is true, also creates
  // log_state->currentReplication if it's null.
  bool lookUpEpochMetadata(logid_t log,
                           lsn_t lsn,
                           Context* context,
                           Context::LogState* log_state,
                           bool create_replication_scheme);

  /**
   * Mark all nodes in the rebuilding set as not available to receive copies.
   *
   * While a storage node is rebuilding, it replies to STORE messages with
   * STORED(status=E::DISABLED). We mark these recipients as unavailable so that
   * RecordRebuildingStore does not try to store copies on them.
   */
  void markNodesInRebuildingSetNotAvailable(NodeSetState* nodeset_state,
                                            Context* context);

  void getDebugInfoDetailed(StorageTaskDebugInfo&) const override;
};

}} // namespace facebook::logdevice
