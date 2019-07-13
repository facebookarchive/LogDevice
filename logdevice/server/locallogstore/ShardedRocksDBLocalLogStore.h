/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <memory>
#include <vector>

#include <boost/filesystem.hpp>
#include <rocksdb/env.h>

#include "logdevice/common/SingleEvent.h"
#include "logdevice/common/configuration/UpdateableConfig.h"
#include "logdevice/common/debug.h"
#include "logdevice/common/settings/RebuildingSettings.h"
#include "logdevice/common/settings/Settings.h"
#include "logdevice/common/settings/UpdateableSettings.h"
#include "logdevice/include/types.h"
#include "logdevice/server/locallogstore/LocalLogStore.h"
#include "logdevice/server/locallogstore/RocksDBCompactionFilter.h"
#include "logdevice/server/locallogstore/RocksDBEnv.h"
#include "logdevice/server/locallogstore/RocksDBLogStoreConfig.h"
#include "logdevice/server/locallogstore/RocksDBSettings.h"

namespace facebook { namespace logdevice {

/**
 * @file
 *
 * Implementation of ShardedLocalLogStore where each shard is a RocksDB
 * instance.  The instances share a background thread pool and block cache.
 *
 * Directory structure:
 *   + base_path
 *   +-- NSHARDS                // control file so we don't accidentally reshard
 *   +-- shard0                 // first RocksDB instance
 *   +-- shard1
 *   ...
 *   +-- shard<n-1>
 */

struct RocksDBCachesInfo;
class ShardedStorageThreadPool;

class ShardedRocksDBLocalLogStore : public ShardedLocalLogStore {
 public:
  /**
   * Constructor.
   *
   * base_path must:
   * - not exist (a new sharded database is created), or
   * - be an empty directory (a new sharded database is created), or
   * - contain a previously created sharded database with the same number of
   *   shards
   *
   * @param num_shards  number of shards to create database with, if -1
   *                    NSHARDS file must already exist
   * @param config  current configuration; pointer is only guaranted to be
   *                valid during the call; can be nullptr in tests
   * @param caches  if not null, this object will be updated with pointers to
   *                RocksDB block caches
   *
   * @throws ConstructorFailed
   */
  ShardedRocksDBLocalLogStore(
      const std::string& base_path,
      shard_size_t num_shards,
      Settings settings,
      UpdateableSettings<RocksDBSettings> db_settings,
      UpdateableSettings<RebuildingSettings> rebuilding_settings,
      std::shared_ptr<UpdateableConfig> updateable_config,
      RocksDBCachesInfo* caches,
      StatsHolder* stats = nullptr);

  ~ShardedRocksDBLocalLogStore() override;

  int numShards() const override {
    return shards_.size();
  }

  LocalLogStore* getByIndex(int idx) override {
    ld_check(idx >= 0 && idx < numShards());
    if (idx < 0 || idx >= shards_.size()) {
      return nullptr;
    }
    return shards_[idx].get();
  }

  void setShardedStorageThreadPool(const ShardedStorageThreadPool*);

  struct DiskShardMappingEntry {
    // Canonical path to one of the shards.  The path can be used to find the
    // free space on the device.
    boost::filesystem::path example_path;
    // List of shards on the disk
    std::vector<int> shards;
    std::atomic<bool> sequencer_initiated_space_based_retention{false};
  };
  /**
   * Generates a DiskShardMappingEntry for each disk/device that some of our
   * shards live on.  (Typical configurations are one disk for all shards, or
   * one per shard.)
   *
   * Returns the number of shards for which the disk was successfully
   * determined.  Full success is indicated by rv == numShards(), anything
   * less means there were issues.
   */
  size_t createDiskShardMapping();

  const std::unordered_map<dev_t, DiskShardMappingEntry>&
  getShardToDiskMapping();

  /**
   * If the shards use LogsDB, per-disk space-based trimming is enabled, and
   * space usage has reached that limit, trim logs on the given disk until that
   * disk's space usage no longer exceeds the given space-based retention
   * threshold. The low pri thread in each shard will be told to drop the
   * partitions and trim logs accordingly.
   *
   * @param mapping Name of the mount point(example path) to perform trimming on
   * @param info    Filesystem space info
   * @param full    Indicates if coordinated space-based retention threshold
   *                was exceeded AND the sequencer had not initiated trimming.
   *                In this case, the disk should be marked full by monitor.
   */
  int trimLogsBasedOnSpaceIfNeeded(const DiskShardMappingEntry& mapping,
                                   boost::filesystem::space_info info,
                                   bool* full);

  /**
   * Adjust DiskInfo to indicate sequencer initiated space-based retention.
   */
  void setSequencerInitiatedSpaceBasedRetention(int shard_idx) override;

  struct DiskInfo {
    DiskInfo() : sequencer_initiated_space_based_retention(false), shards() {}
    std::atomic<bool> sequencer_initiated_space_based_retention;
    std::vector<int> shards;
  };

 private:
  void printDiskShardMapping();

  void refreshIOTracingSettings();

  void onSettingsUpdated();

  // Shutdown event to indicate that the sharded store is closing down.
  SingleEvent shutdown_event_;

  StatsHolder* stats_;

  // Shards that use FailingLocalLogStore because they failed to open DB.
  std::set<int> failing_log_store_shards_;

  // Per-shard objects holding IO tracing context. Index in vector is shard idx.
  //
  // Note: they could live inside LocalLogStore, but having them here is more
  // convenient because (a) they're available throughout env_'s lifetime, so
  // no need to worry e.g. about construction and destruction order of IOContext
  // vs rocksdb::DB, or about whether it's possibile for Env to be used after
  // rocksdb::DB is destroyed, (b) in future we'll probably allow switching
  // LocalLogStore at runtime (e.g. closing the DB before disk repair and
  // opening after), which would further complicate IOTracing lifetime and its
  // interaction with Env if IOContext were to be owned by LocalLogStore.
  std::vector<std::unique_ptr<IOTracing>> io_tracing_by_shard_;

  std::unique_ptr<RocksDBEnv> env_;

  RocksDBLogStoreConfig rocksdb_config_;

  // subscription to update db settings on update of RocksDB settings.
  UpdateableSettings<RocksDBSettings>::SubscriptionHandle
      rocksdb_settings_handle_;

  UpdateableSettings<RocksDBSettings> db_settings_;

  // Compaction filter factories, which we need to keep around so that we can
  // propagate pointers to StorageThreadPool instances after the pools are
  // created.
  std::vector<std::shared_ptr<RocksDBCompactionFilterFactory>> filters_;

  // Actual RocksDBLogStoreBase instances
  std::vector<std::unique_ptr<LocalLogStore>> shards_;

  // For each shard, the directory containing the shard's database
  std::vector<boost::filesystem::path> shard_paths_;
  std::vector<dev_t> shard_to_devt_;

  // Mapping between shard idx, and the disk on which it resides
  std::unordered_map<dev_t, DiskShardMappingEntry> fspath_to_dsme_;

  // Indicating if shards are partitioned
  const bool partitioned_;
};

}} // namespace facebook::logdevice
