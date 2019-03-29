/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <unordered_map>
#include <unordered_set>

#include "logdevice/common/AdminCommandTable-fwd.h"
#include "logdevice/common/DataClass.h"
#include "logdevice/common/ShardID.h"
#include "logdevice/common/Timestamp.h"
#include "logdevice/common/WorkerCallbackHelper.h"
#include "logdevice/common/debug.h"
#include "logdevice/common/types_internal.h"
#include "logdevice/include/Err.h"
#include "logdevice/include/types.h"

/**
 * @file This file contains some rebuilding types that workers should know
 * about, as well as some abstract interfaces to rebuilding classes for use by
 * workers.
 */

namespace facebook { namespace logdevice {

/**
 * Different rebuilding modes.
 */
enum class RebuildingMode {
  RELOCATE, // In this mode, nodes for which we rebuild are known to still have
            // their data and are asked to participate as donor nodes.
            // This mode can be used when rebuilding a node before it gets
            // decommissioned.
  RESTORE,  // In this mode, nodes for which we rebuild are not donor nodes.
            // All of their data will be rebuilt by other donors. This mode
            // should be used if the node for which we are rebuilding lost its
            // data.
  INVALID
};

std::string toString(const RebuildingMode&);

using PerDataClassTimeRanges =
    std::unordered_map<DataClass, RecordTimeIntervals>;

std::string toString(const PerDataClassTimeRanges&);

struct RebuildingNodeInfo {
  explicit RebuildingNodeInfo(RebuildingMode mode) : mode(mode) {}
  RebuildingNodeInfo(const PerDataClassTimeRanges& dcdr, RebuildingMode mode)
      : dc_dirty_ranges(dcdr), mode(mode) {}
  PerDataClassTimeRanges dc_dirty_ranges;
  RebuildingMode mode;

  bool operator==(const RebuildingNodeInfo& other) const;

  /**
   * @returns true if records matching the provided data class/timestamp
   *          tuple have potentially been lost on the node represented
   *          by this RebuildingNodeInfo.
   */
  bool isUnderReplicated(DataClass, RecordTimestamp) const;
};

// A list of shards that are rebuilding a log.
struct RebuildingSet {
  using map_type =
      std::unordered_map<ShardID, RebuildingNodeInfo, ShardID::Hash>;
  RebuildingSet() {}
  explicit RebuildingSet(map_type data) : shards(std::move(data)) {}
  map_type shards;
  // Shards inside `shards` that have already been rebuilt and thus are empty.
  // Rebuilding state machines should not try to use these shards as recipient
  // and not consider them to decide whether or not the current rebuilding is
  // authoritative.
  // TODO: currently LogRebuilding will look for records that contain these
  // shards in their copyset, which is useless since these shards have already
  // been authoritatively rebuilt.
  std::unordered_set<ShardID, ShardID::Hash> empty;

  RecordTimeIntervals all_dirty_time_intervals;

  // The output string is truncated to `max_size`. Useful when writing the
  // resulting string to error log, which truncates each line to ~2 KB anyway.
  // E.g. in extreme cases, full output of describe() can take tens of MBs;
  // it would be super wasteful to produce such a long string and then only
  // use the first 2 KB.
  std::string describe(size_t max_size = 2048) const;

  bool operator==(const RebuildingSet& other) const;
};
using RebuildingSets = std::unordered_map<shard_index_t, RebuildingSet>;

class UpdateableConfig;
class RebuildingPlan;
class RebuildingReadStorageTask;
struct STORED_Header;
struct STORE_Header;

class ShardRebuildingV1;
using ShardRebuildingV1Ref = WorkerCallbackHelper<ShardRebuildingV1>::Ticket;

class ShardRebuildingV2;
using ShardRebuildingV2Ref = WorkerCallbackHelper<ShardRebuildingV2>::Ticket;

class RecordRebuildingInterface {
 public:
  virtual void onStored(const STORED_Header& header,
                        ShardID from,
                        lsn_t rebuilding_version,
                        uint32_t rebuilding_wave,
                        log_rebuilding_id_t rebuilding_id,
                        ServerInstanceId server_instance_id,
                        FlushToken flush_token) = 0;

  virtual void onStoreSent(Status st,
                           const STORE_Header& header,
                           ShardID to,
                           lsn_t rebuilding_version,
                           uint32_t rebuilding_wave) = 0;
  virtual ~RecordRebuildingInterface() {}

  // This is used to seed the RNG for the copyset selector. Public and included
  // here for CopySetSelector tests. The copyset and the salt are hashed into a
  // single 128-bit hash into 'out'.
  static void getRNGSeedFromRecord(uint32_t (&out)[4],
                                   const ShardID* copyset,
                                   size_t copyset_size,
                                   size_t salt);
};

class LogRebuildingInterface {
 public:
  virtual void abort(bool notify_complete = true) = 0;
  virtual void onReadTaskDone(RebuildingReadStorageTask& task) = 0;
  virtual void onReadTaskDropped(RebuildingReadStorageTask& task) = 0;
  virtual RecordRebuildingInterface* findRecordRebuilding(lsn_t lsn) = 0;
  virtual void onMemtableFlushed(node_index_t node_index,
                                 ServerInstanceId server_instance_id,
                                 FlushToken flushToken) = 0;
  virtual void onGracefulShutdown(node_index_t node_index,
                                  ServerInstanceId server_instance_id) = 0;
  virtual ~LogRebuildingInterface() {}
};

class RebuildingCoordinatorInterface {
 public:
  virtual void noteConfigurationChanged() = 0;
  virtual void shutdown() = 0;
  virtual void onDirtyStateChanged() = 0;
  virtual lsn_t getLastSeenEventLogVersion() const = 0;
  virtual ~RebuildingCoordinatorInterface() {}
};

/**
 * A map from (logid, shard) to LogRebuildingInterface.
 */
struct LogRebuildingMap {
 public:
  using Key = std::pair<logid_t, shard_index_t>;
  struct KeyHasher {
    size_t operator()(const Key& k) const;
  };
  using Map = std::
      unordered_map<Key, std::unique_ptr<LogRebuildingInterface>, KeyHasher>;
  LogRebuildingInterface* find(logid_t logid, shard_index_t shard);
  void erase(logid_t logid, shard_index_t shard);
  void insert(logid_t logid,
              shard_index_t shard,
              std::unique_ptr<LogRebuildingInterface> log);
  Map map;
};

/**
 * Object in charge of the data movement during rebuilding.
 * It reads and re-replicates the records.
 * There are currently two implementations: the old rebuilding that uses a
 * LogRebuilding for each log, and the new rebuilding partition by partition
 * that reads all logs together.
 *
 * TODO (#T24665001): After rebuilding partition by partition is good and
 *                    stable, remove this interface and have
 *                    RebuildingCoordinator just use ShardRebuilding directly.
 */
class ShardRebuildingInterface {
 public:
  // Interface for the owner of a ShardRebuilding instance.
  // Gets notified when ShardRebuilding completes and when it makes enough
  // progress to notify other donors through event log.
  // RebuildingCoordinator if the only implementation of this interface outside
  // tests; see comments there for documentation of the methods' arguments.
  struct Listener {
    virtual void onShardRebuildingComplete(uint32_t shard_idx) = 0;
    virtual void notifyShardDonorProgress(uint32_t shard,
                                          RecordTimestamp next_ts,
                                          lsn_t version,
                                          double progress_estimate) = 0;
    virtual ~Listener() = default;
  };

  // Most of the parameters describing what to rebuild are passed to
  // constructor.
  // Destructor aborts the re-replication if it's running.

  virtual ~ShardRebuildingInterface() = default;

  // Must be called exactly once. Starts the re-replication work.
  virtual void
  start(std::unordered_map<logid_t, std::unique_ptr<RebuildingPlan>> plan) = 0;

  // Notification that other donors made enough progress to allow us to advance
  // the global window to the given point. Note that the window can move
  // backwards (if window size setting was changed at runtime).
  // Can be called before start().
  virtual void advanceGlobalWindow(RecordTimestamp new_window_end) = 0;

  virtual void noteConfigurationChanged() = 0;
  virtual void noteRebuildingSettingsChanged() = 0;

  // Fills the current row of @param table with debug information about the
  // state of rebuilding for this shard. Used by admin commands.
  virtual void getDebugInfo(InfoRebuildingShardsTable& table) const = 0;
};

}} // namespace facebook::logdevice
