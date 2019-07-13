/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/server/locallogstore/ShardedRocksDBLocalLogStore.h"

#include <array>
#include <cerrno>
#include <cstdio>
#include <cstdlib>
#include <future>
#include <thread>

#include <folly/FileUtil.h>
#include <folly/Memory.h>
#include <folly/ScopeGuard.h>
#include <rocksdb/db.h>
#include <rocksdb/statistics.h>
#include <sys/stat.h>

#include "logdevice/common/ConstructorFailed.h"
#include "logdevice/common/RandomAccessQueue.h"
#include "logdevice/common/ThreadID.h"
#include "logdevice/common/settings/RebuildingSettings.h"
#include "logdevice/common/types_internal.h"
#include "logdevice/server/ServerProcessor.h"
#include "logdevice/server/fatalsignal.h"
#include "logdevice/server/locallogstore/FailingLocalLogStore.h"
#include "logdevice/server/locallogstore/IOTracing.h"
#include "logdevice/server/locallogstore/PartitionedRocksDBStore.h"
#include "logdevice/server/locallogstore/RocksDBKeyFormat.h"
#include "logdevice/server/locallogstore/RocksDBListener.h"
#include "logdevice/server/locallogstore/RocksDBLogStoreFactory.h"
#include "logdevice/server/locallogstore/ShardToPathMapping.h"
#include "logdevice/server/storage_tasks/ShardedStorageThreadPool.h"

namespace facebook { namespace logdevice {

namespace fs = boost::filesystem;

// Returns 1/0 if base_path looks like it contains an existing database, -1 on
// error
static int database_exists(const std::string& base_path);

// Called when the database directory exists. Expect a NSHARDS file that
// contains the number of shards this directory was created for. Verify that
// this number matches `nshards_expected` which is retrieved from the
// "num_shards" property of this node in the config.
//
// Returns 0 on success, or -1 on any error.
static int check_nshards(const std::string& base_path,
                         shard_size_t nshards_expected);

// Tries to create a new sharded database at the given path, returns 0 on
// success
static int create_new_sharded_db(const std::string& base_path,
                                 shard_size_t nshards);

using DiskShardMappingEntry =
    ShardedRocksDBLocalLogStore::DiskShardMappingEntry;

ShardedRocksDBLocalLogStore::ShardedRocksDBLocalLogStore(
    const std::string& base_path,
    shard_size_t nshards,
    Settings settings,
    UpdateableSettings<RocksDBSettings> db_settings,
    UpdateableSettings<RebuildingSettings> rebuilding_settings,
    std::shared_ptr<UpdateableConfig> updateable_config,
    RocksDBCachesInfo* caches,
    StatsHolder* stats)
    : stats_(stats),
      db_settings_(db_settings),
      partitioned_(db_settings->partitioned) {
  if (db_settings->num_levels < 2 &&
      db_settings->compaction_style == rocksdb::kCompactionStyleLevel) {
    ld_error("Level-style compaction requires at least 2 levels. %d given.",
             db_settings->num_levels);
    throw ConstructorFailed();
  }

  int rv = database_exists(base_path);
  if (rv < 0) {
    throw ConstructorFailed();
  }

  // This value should have been validated by the Configuration module.
  ld_check(nshards > 0 && nshards <= MAX_SHARDS);

  if (rv) {
    // Database exists, check that the number of shards matches.  We do not
    // want to start up otherwise.
    if (check_nshards(base_path, nshards) < 0) {
      throw ConstructorFailed();
    }
  } else {
    if (db_settings->auto_create_shards == false) {
      ld_error("Auto creation of shards & directories is not enabled and "
               "they do not exist");
      throw ConstructorFailed();
    }
    // Directory does not exist.  Create a new sharded database.
    if (nshards < 0) {
      ld_error("Number of shards was not specified and database does not "
               "exist.  Don't know how many shards to create!");
      throw ConstructorFailed();
    }

    if (create_new_sharded_db(base_path, nshards) != 0) {
      throw ConstructorFailed();
    }
  }

  std::shared_ptr<Configuration> config =
      updateable_config ? updateable_config->get() : nullptr;

  std::vector<IOTracing*> tracing_ptrs;
  for (shard_index_t i = 0; i < nshards; ++i) {
    io_tracing_by_shard_.push_back(std::make_unique<IOTracing>(i));
    tracing_ptrs.push_back(io_tracing_by_shard_[i].get());
  }
  // If tracing is enabled in settings, enable it before opening the DBs.
  refreshIOTracingSettings();

  env_ = std::make_unique<RocksDBEnv>(db_settings, stats, tracing_ptrs);
  {
    int num_bg_threads_lo = db_settings->num_bg_threads_lo;
    if (num_bg_threads_lo == -1) {
      num_bg_threads_lo = nshards * db_settings->max_background_compactions;
    }
    env_->SetBackgroundThreads(num_bg_threads_lo, rocksdb::Env::LOW);
  }
  {
    int num_bg_threads_hi = db_settings->num_bg_threads_hi;
    if (num_bg_threads_hi == -1) {
      num_bg_threads_hi = nshards * db_settings->max_background_flushes;
    }
    env_->SetBackgroundThreads(num_bg_threads_hi, rocksdb::Env::HIGH);
  }

  rocksdb_config_ = RocksDBLogStoreConfig(
      db_settings, rebuilding_settings, env_.get(), updateable_config, stats);

  // save the rocksdb cache information to be used by the SIGSEGV handler
  if (caches) {
    caches->block_cache = rocksdb_config_.table_options_.block_cache;
    caches->block_cache_compressed =
        rocksdb_config_.table_options_.block_cache_compressed;
    if (rocksdb_config_.metadata_table_options_.block_cache !=
        rocksdb_config_.table_options_.block_cache) {
      caches->metadata_block_cache =
          rocksdb_config_.metadata_table_options_.block_cache;
    }
  }

  rv = ShardToPathMapping(base_path, nshards).get(&shard_paths_);
  if (rv != 0) {
    throw ConstructorFailed();
  }
  ld_check(static_cast<int>(shard_paths_.size()) == nshards);

  // Create shards in multiple threads since it's a bit slow
  using FutureResult =
      std::pair<std::unique_ptr<LocalLogStore>,
                std::shared_ptr<RocksDBCompactionFilterFactory>>;

  std::vector<std::future<FutureResult>> futures;
  for (shard_index_t shard_idx = 0; shard_idx < nshards; ++shard_idx) {
    fs::path shard_path = shard_paths_[shard_idx];
    ld_check(!shard_path.empty());
    futures.push_back(std::async(std::launch::async, [=]() {
      ThreadID::set(
          ThreadID::UTILITY, folly::sformat("ld:open-rocks{}", shard_idx));

      // Make a copy of RocksDBLogStoreConfig for this shard.
      RocksDBLogStoreConfig shard_config = rocksdb_config_;
      shard_config.createMergeOperator(shard_idx);

      // Create SstFileManager for this shard
      shard_config.addSstFileManagerForShard();

      // If rocksdb statistics are enabled, create a Statistics object for
      // each shard.
      if (db_settings->statistics) {
        shard_config.options_.statistics = rocksdb::CreateDBStatistics();
      }

      // Create a compaction filter factory.  Later (in
      // setShardedStorageThreadPool()) we'll link it to the storage
      // thread pool so that it can see the world.
      auto filter_factory =
          std::make_shared<RocksDBCompactionFilterFactory>(db_settings);
      shard_config.options_.compaction_filter_factory = filter_factory;

      if (stats) {
        shard_config.options_.listeners.push_back(
            std::make_shared<RocksDBListener>(
                stats,
                shard_idx,
                env_.get(),
                io_tracing_by_shard_[shard_idx].get()));
      }

      RocksDBLogStoreFactory factory(
          std::move(shard_config), settings, config, stats);
      std::unique_ptr<LocalLogStore> shard_store;

      // Treat the shard as failed if we find a file named
      // LOGDEVICE_DISABLED. Used by tests.
      auto disable_marker = shard_path / fs::path("LOGDEVICE_DISABLED");
      bool should_open_shard = true;

      try {
        if (fs::exists(disable_marker) && fs::is_regular_file(disable_marker)) {
          ld_info("Found %s, not opening shard %d",
                  disable_marker.string().c_str(),
                  shard_idx);
          should_open_shard = false;
        }

        if (fs::exists(shard_path) && !fs::is_directory(shard_path)) {
          ld_info("%s exists but is not a directory; not opening shard %d",
                  shard_path.string().c_str(),
                  shard_idx);
          should_open_shard = false;
        }
      } catch (const fs::filesystem_error& e) {
        ld_error("Error while checking for existence of %s or %s: %s. "
                 "Not opening shard %d",
                 disable_marker.string().c_str(),
                 shard_path.string().c_str(),
                 e.what(),
                 shard_idx);
        should_open_shard = false;
      }

      if (should_open_shard) {
        shard_store = factory.create(shard_idx,
                                     nshards,
                                     shard_path.string(),
                                     io_tracing_by_shard_[shard_idx].get());
      }

      if (shard_store) {
        ld_info("Opened RocksDB instance at %s", shard_path.c_str());
        ld_check(dynamic_cast<RocksDBLogStoreBase*>(shard_store.get()) !=
                 nullptr);
      } else {
        PER_SHARD_STAT_INCR(stats_, failing_log_stores, shard_idx);
        shard_store = std::make_unique<FailingLocalLogStore>();
        ld_info("Opened FailingLocalLogStore instance for shard %d", shard_idx);
      }

      return std::make_pair(std::move(shard_store), std::move(filter_factory));
    }));
  }

  for (int shard_idx = 0; shard_idx < nshards; ++shard_idx) {
    auto& future = futures[shard_idx];

    std::unique_ptr<LocalLogStore> shard_store;
    std::shared_ptr<RocksDBCompactionFilterFactory> filter_factory;
    std::tie(shard_store, filter_factory) = future.get();

    ld_check(shard_store);
    if (dynamic_cast<FailingLocalLogStore*>(shard_store.get()) != nullptr) {
      shards_.push_back(std::move(shard_store));
      filters_.push_back(std::move(filter_factory));
      failing_log_store_shards_.insert(shard_idx);
      continue;
    }

    shards_.push_back(std::move(shard_store));
    filters_.push_back(std::move(filter_factory));
  }

  // Subscribe for rocksdb config updates after initializing shards.
  rocksdb_settings_handle_ = db_settings.callAndSubscribeToUpdates(
      std::bind(&ShardedRocksDBLocalLogStore::onSettingsUpdated, this));

  if (failing_log_store_shards_.size() >= nshards && nshards > 0) {
    ld_critical("All shards failed to open. Not starting the server.");
    throw ConstructorFailed();
  }

  // Check that we can map shards to devices
  if (createDiskShardMapping() != nshards) {
    throw ConstructorFailed();
  }
  printDiskShardMapping();
}

ShardedRocksDBLocalLogStore::~ShardedRocksDBLocalLogStore() {
  shutdown_event_.signal();

  for (shard_index_t shard_idx : failing_log_store_shards_) {
    PER_SHARD_STAT_DECR(stats_, failing_log_stores, shard_idx);
  }

  // Unsubscribe from settings update before destroying shards.
  rocksdb_settings_handle_.unsubscribe();

  // destroy each RocksDBLocalLogStore instance in a separate thread
  std::vector<std::thread> threads;
  for (size_t i = 0; i < numShards(); ++i) {
    threads.emplace_back([this, i]() {
      ThreadID::set(
          ThreadID::Type::UTILITY, folly::sformat("ld:stop-rocks{}", i));
      ld_debug("Destroying RocksDB shard %zd", i);
      shards_[i].reset();
      ld_info("Destroyed RocksDB shard %zd", i);
    });
  }

  for (auto& thread : threads) {
    thread.join();
  }
}

static int database_exists(const std::string& base_path) {
  boost::system::error_code code;

  bool exists = fs::exists(base_path, code);
  if (code.value() != boost::system::errc::success &&
      code.value() != boost::system::errc::no_such_file_or_directory) {
    ld_error("Error checking if path \"%s\" exists: %s",
             base_path.c_str(),
             code.message().c_str());
    return -1;
  }

  if (!exists) {
    return 0;
  }

  bool isdir = fs::is_directory(base_path, code);
  if (code.value() != boost::system::errc::success &&
      code.value() != boost::system::errc::not_a_directory) {
    ld_error("Error checking if path \"%s\" is a directory: %s",
             base_path.c_str(),
             code.message().c_str());
    return -1;
  }

  if (!isdir) {
    ld_error("Path \"%s\" exists but is not a directory, cannot open local "
             "log store",
             base_path.c_str());
    return -1;
  }

  bool isempty = fs::is_empty(base_path, code);
  if (code.value() != boost::system::errc::success) {
    ld_error("Error checking if path \"%s\" is empty: %s",
             base_path.c_str(),
             code.message().c_str());
    return -1;
  }

  return !isempty;
}

static int check_nshards(const std::string& base_path,
                         const shard_size_t nshards_expected) {
  fs::path nshards_path = fs::path(base_path) / fs::path("NSHARDS");
  FILE* fp = std::fopen(nshards_path.c_str(), "r");
  if (fp == nullptr) {
    ld_error("Database directory \"%s\" exists but there was an error opening "
             "the NSHARDS file, errno=%d (%s).  To create a new sharded "
             "database, please delete the directory and try again.",
             base_path.c_str(),
             errno,
             strerror(errno));
    return -1;
  }

  SCOPE_EXIT {
    std::fclose(fp);
  };

  shard_size_t nshards_read;
  if (std::fscanf(fp, "%hd", &nshards_read) != 1) {
    ld_error("Error reading file \"%s\"", nshards_path.c_str());
    return -1;
  }

  if (nshards_read != nshards_expected) {
    ld_error("Tried to open existing sharded database \"%s\" with a different "
             "number of shards (expected %d, found %d)",
             base_path.c_str(),
             nshards_expected,
             nshards_read);
    return -1;
  }

  return 0;
}

static int create_new_sharded_db(const std::string& base_path,
                                 shard_size_t nshards) {
  boost::system::error_code code;
  fs::create_directories(base_path, code);
  if (code.value() != boost::system::errc::success) {
    ld_error("Error creating directory \"%s\": %s",
             base_path.c_str(),
             code.message().c_str());
    return -1;
  }

  fs::path nshards_path = fs::path(base_path) / fs::path("NSHARDS");

  FILE* fp = std::fopen(nshards_path.c_str(), "w");
  if (fp == nullptr) {
    ld_error("Failed to open \"%s\" for writing, errno=%d (%s)",
             nshards_path.c_str(),
             errno,
             strerror(errno));
    return -1;
  }

  SCOPE_EXIT {
    std::fclose(fp);
  };

  if (std::fprintf(fp, "%d", nshards) < 0) {
    ld_error("Error writing to \"%s\"", nshards_path.c_str());
    return -1;
  }

  return 0;
}

int ShardedRocksDBLocalLogStore::trimLogsBasedOnSpaceIfNeeded(
    const DiskShardMappingEntry& mapping,
    boost::filesystem::space_info info,
    bool* full) {
  if (!partitioned_) {
    RATELIMIT_INFO(std::chrono::minutes(1),
                   1,
                   "Space based trimming requested on non-partitioned storage");
    return -1;
  }

  *full = false;

  if (db_settings_->free_disk_space_threshold_low == 0) {
    return 0;
  }
  size_t space_limit_coordinated =
      (1 - db_settings_->free_disk_space_threshold_low) * info.capacity;

  // Get & reset sequencer-initiated flag, so that if the flag was set by a
  // trailing probe after it was not full anymore, that value is not used in
  // the future if it becomes full again.
  ld_check(mapping.shards.size());
  auto disk_info_kv = fspath_to_dsme_.find(shard_to_devt_[mapping.shards[0]]);
  if (disk_info_kv == fspath_to_dsme_.end()) {
    ld_check(false);
    return -1;
  }
  bool sequencer_initiated_trimming =
      disk_info_kv->second.sequencer_initiated_space_based_retention.exchange(
          false);

  if (space_limit_coordinated >= (info.capacity - info.free)) {
    // Not breaking any limits
    return 0;
  }

  using PartitionPtr = std::shared_ptr<PartitionedRocksDBStore::Partition>;
  using PartitionIterator = std::vector<PartitionPtr>::const_iterator;

  struct ShardTrimPoint {
    shard_index_t shard_idx;
    PartitionIterator it;
    // Keeping partition list so it is not freed while we use the iterator
    PartitionedRocksDBStore::PartitionList partition_list;
    size_t space_usage;
    size_t reclaimed;
  };

  // Comparator to get the oldest partition timestamp first
  auto timestamp_cmp = [](const ShardTrimPoint& a, const ShardTrimPoint& b) {
    return (*a.it)->starting_timestamp > (*b.it)->starting_timestamp;
  };
  std::priority_queue<ShardTrimPoint,
                      std::vector<ShardTrimPoint>,
                      decltype(timestamp_cmp)>
      shards_oldest_partitions_queue(timestamp_cmp);

  // Comparator to get lowest shard_idx first, for nicer prints
  auto shard_idx_cmp = [](const ShardTrimPoint& a, const ShardTrimPoint& b) {
    return a.shard_idx > b.shard_idx;
  };
  std::priority_queue<ShardTrimPoint,
                      std::vector<ShardTrimPoint>,
                      decltype(shard_idx_cmp)>
      trim_points_sorted(shard_idx_cmp);

  // 1) Get snapshot of partitions from each shard, and put in priority queue.
  size_t total_space_used_by_partitions = 0;
  for (shard_index_t shard_idx : mapping.shards) {
    auto store = getByIndex(shard_idx);
    auto partitioned_store = dynamic_cast<PartitionedRocksDBStore*>(store);
    ld_check(partitioned_store != nullptr);
    auto partition_list = partitioned_store->getPartitionList();

    // Add its size
    size_t partition_space_usage =
        partitioned_store->getApproximatePartitionSize( // Metadata
            partitioned_store->getMetadataCFHandle()) +
        partitioned_store->getApproximatePartitionSize( // Unpartitioned
            partitioned_store->getUnpartitionedCFHandle());

    for (PartitionPtr partition_ptr : *partition_list) { // Partitions
      partition_space_usage += partitioned_store->getApproximatePartitionSize(
          partition_ptr->cf_->get());
    }

    total_space_used_by_partitions += partition_space_usage;

    ShardTrimPoint stp{
        shard_idx,               // shard id
        partition_list->begin(), // iterator at beginning
        partition_list,          // partition list (to keep in mem)
        partition_space_usage,   // space usage
        0                        // reclaimed space (none yet)
    };

    // Only add shard to priority queue if there are partitions we can drop.
    if (partition_list->size() > 1) {
      shards_oldest_partitions_queue.push(stp);
    } else {
      trim_points_sorted.push(stp);
    }
  }

  double ld_percentage =
      double(total_space_used_by_partitions) / double(info.capacity);
  double actual_percentage =
      double(info.capacity - info.free) / double(info.capacity);
  if (std::abs(actual_percentage - ld_percentage) > 0.3) {
    // TODO: add a stat and raise an alarm?
    RATELIMIT_WARNING(
        std::chrono::seconds(5),
        1,
        "Estimated size differ %d%% from actual size! Would be unsafe "
        "to do space-based trimming, skipping it",
        static_cast<int>(
            round(100 * std::abs(actual_percentage - ld_percentage))));
    return -1;
  }

  bool coordinated_limit_exceeded =
      total_space_used_by_partitions > space_limit_coordinated;
  *full = coordinated_limit_exceeded;

  ld_debug("example path:%s -> coordinated_limit_exceeded:%s, "
           "sequencer_initiated_trimming:%s, sbr_force:%s, "
           "space_limit_coordinated:%lu, coordinated_threshold:%lf"
           "[ld_percentage:%lf, total_space_used_by_partitions:%lu,"
           " actual_percentage:%lf] [info.capacity:%lu, info.free:%lu]",
           mapping.example_path.c_str(),
           coordinated_limit_exceeded ? "yes" : "no",
           sequencer_initiated_trimming ? "yes" : "no",
           db_settings_->sbr_force ? "yes" : "no",
           space_limit_coordinated,
           db_settings_->free_disk_space_threshold_low,
           ld_percentage,
           total_space_used_by_partitions,
           actual_percentage,
           info.capacity,
           info.free);

  if (!coordinated_limit_exceeded) {
    return 0;
  }
  if (!sequencer_initiated_trimming && !db_settings_->sbr_force) {
    return 0;
  }

  // 2) Calculate how much to trim.
  size_t reclaimed_so_far = 0;
  size_t total_to_reclaim =
      total_space_used_by_partitions - space_limit_coordinated;

  // 3) Keep picking the oldest partition until enough space is freed.
  while (reclaimed_so_far <= total_to_reclaim &&
         !shards_oldest_partitions_queue.empty()) {
    ShardTrimPoint current = shards_oldest_partitions_queue.top();
    shards_oldest_partitions_queue.pop();

    auto partitioned_store =
        dynamic_cast<PartitionedRocksDBStore*>(getByIndex(current.shard_idx));
    size_t partition_size = partitioned_store->getApproximatePartitionSize(
        (*current.it)->cf_->get());
    reclaimed_so_far += partition_size;
    current.reclaimed += partition_size;
    current.it++;

    // Don't drop latest partition
    if ((*current.it)->id_ == current.partition_list->nextID() - 1) {
      trim_points_sorted.push(current);
    } else {
      shards_oldest_partitions_queue.push(current);
    }
  }

  // 4) Tell the low-pri thread in each shard to drop the decided partitions.
  while (!shards_oldest_partitions_queue.empty()) {
    trim_points_sorted.push(shards_oldest_partitions_queue.top());
    shards_oldest_partitions_queue.pop();
  }

  // Apply trim points, log stats
  size_t num_trim_points = trim_points_sorted.size();
  while (!trim_points_sorted.empty()) {
    ShardTrimPoint current = trim_points_sorted.top();
    trim_points_sorted.pop();
    auto partitioned_store =
        dynamic_cast<PartitionedRocksDBStore*>(getByIndex(current.shard_idx));
    ld_check(partitioned_store != nullptr);
    partition_id_t first = current.partition_list->firstID();
    partition_id_t target = (*current.it)->id_;
    ld_spew("Setting trim-limit target:%lu for shard:%d. "
            "coordinated threshold: %lf",
            target,
            current.shard_idx,
            db_settings_->free_disk_space_threshold_low);
    partitioned_store->setSpaceBasedTrimLimit(target);

    if (target == first) {
      ld_debug("Space-based trimming of shard%d: %ju used, dropping nothing",
               current.shard_idx,
               current.space_usage);
    } else {
      PER_SHARD_STAT_INCR(stats_, sbt_num_storage_trims, current.shard_idx);
      ld_info("Space-based trimming of shard %d: %ju used, %ju reclaimed, "
              "dropping partitions [%ju,%ju)",
              current.shard_idx,
              current.space_usage,
              current.reclaimed,
              first,
              target);
    }
  }
  if (num_trim_points > 1) {
    ld_info("Space based trimming, total on disk:%s: used:%ju, limit:%ju, "
            "reclaimed:%ju",
            mapping.example_path.c_str(),
            total_space_used_by_partitions,
            space_limit_coordinated,
            reclaimed_so_far);
  }

  return 0;
}

void ShardedRocksDBLocalLogStore::setSequencerInitiatedSpaceBasedRetention(
    int shard_idx) {
  ld_debug("shard_idx:%d, coordinated threshold:%lf",
           shard_idx,
           db_settings_->free_disk_space_threshold_low);

  if (db_settings_->free_disk_space_threshold_low == 0) {
    return;
  }

  auto disk_info_kv = fspath_to_dsme_.find(shard_to_devt_[shard_idx]);
  if (disk_info_kv == fspath_to_dsme_.end()) {
    RATELIMIT_ERROR(std::chrono::seconds(1),
                    5,
                    "Couldn't find disk info for disk %s (containing shard %d)",
                    shard_paths_[shard_idx].c_str(),
                    shard_idx);
    ld_check(false);
  } else {
    ld_debug("Setting sequencer_initiated_space_based_retention for shard%d",
             shard_idx);
    disk_info_kv->second.sequencer_initiated_space_based_retention.store(true);
  }
}

void ShardedRocksDBLocalLogStore::setShardedStorageThreadPool(
    const ShardedStorageThreadPool* sharded_pool) {
  for (size_t i = 0; i < shards_.size(); ++i) {
    filters_[i]->setStorageThreadPool(&sharded_pool->getByIndex(i));
    shards_[i]->setProcessor(checked_downcast<Processor*>(
        &sharded_pool->getByIndex(i).getProcessor()));
  }
}

const std::unordered_map<dev_t, DiskShardMappingEntry>&
ShardedRocksDBLocalLogStore::getShardToDiskMapping() {
  return fspath_to_dsme_;
}

size_t ShardedRocksDBLocalLogStore::createDiskShardMapping() {
  ld_check(!shard_paths_.empty());

  std::unordered_map<dev_t, size_t> dev_to_out_index;
  size_t success = 0, added_to_map = 0;
  shard_to_devt_.resize(shard_paths_.size());

  for (int shard_idx = 0; shard_idx < shard_paths_.size(); ++shard_idx) {
    const fs::path& path = shard_paths_[shard_idx];
    boost::system::error_code ec;
    // Resolve any links and such
    auto db_canonical_path = fs::canonical(path, ec);
    if (ec.value() != boost::system::errc::success) {
      RATELIMIT_ERROR(std::chrono::minutes(10),
                      1,
                      "Failed to find canonical path of shard %d (path %s): %s",
                      shard_idx,
                      path.c_str(),
                      ec.message().c_str());
      continue;
    }
    std::string true_path = db_canonical_path.generic_string();

    struct stat st;
    int rv = ::stat(true_path.c_str(), &st);
    if (rv != 0) {
      RATELIMIT_ERROR(std::chrono::minutes(10),
                      1,
                      "stat(\"%s\") failed with errno %d (%s)",
                      true_path.c_str(),
                      errno,
                      strerror(errno));
      continue;
    }

    shard_to_devt_[shard_idx] = st.st_dev;
    auto insert_result = dev_to_out_index.emplace(st.st_dev, added_to_map);
    if (!insert_result.second) {
      // A previous shard had the same dev_t so they are on the same disk.
      // Just append this shard idx to the list in DiskSpaceInfo.
      fspath_to_dsme_[shard_to_devt_[shard_idx]].shards.push_back(shard_idx);
      ++success;
      continue;
    }

    // First time we're seeing the device.  The index in the output vector
    // was "reserved" by the map::emplace() above.
    fspath_to_dsme_[shard_to_devt_[shard_idx]].example_path = db_canonical_path;
    fspath_to_dsme_[shard_to_devt_[shard_idx]].shards.push_back(shard_idx);
    ++added_to_map;
    ++success;
  }
  return success;
}

void ShardedRocksDBLocalLogStore::printDiskShardMapping() {
  // Format disk -> shard mapping log
  std::stringstream disk_mapping_ss;
  disk_mapping_ss << "Disk -> Shard mapping: ";
  bool first = true;
  for (const auto& kv : fspath_to_dsme_) {
    if (!first) {
      disk_mapping_ss << ", ";
    }
    first = false;
    disk_mapping_ss << kv.second.example_path.c_str() << " -> [";
    auto& last_shard = kv.second.shards.back();
    for (auto& shard_idx : kv.second.shards) {
      disk_mapping_ss << shard_idx << (shard_idx == last_shard ? "]" : ",");
    }
  }
  ld_info("%s", disk_mapping_ss.str().c_str());
}

void ShardedRocksDBLocalLogStore::refreshIOTracingSettings() {
  std::vector<bool> enabled_by_shard(io_tracing_by_shard_.size());
  for (shard_index_t idx : db_settings_.get()->io_tracing_shards) {
    if (idx < 0 || idx >= enabled_by_shard.size()) {
      ld_error("Shard idx out of range in --rocksdb-io-tracing-shards: %d not "
               "in [0, %lu). Ignoring.",
               static_cast<int>(idx),
               io_tracing_by_shard_.size());
      continue;
    }
    enabled_by_shard[idx] = true;
  }
  for (shard_index_t i = 0; i < enabled_by_shard.size(); ++i) {
    io_tracing_by_shard_[i]->setEnabled(enabled_by_shard[i]);
  }
}

void ShardedRocksDBLocalLogStore::onSettingsUpdated() {
  refreshIOTracingSettings();
  for (auto& shard : shards_) {
    auto rocksdb_shard = dynamic_cast<RocksDBLogStoreBase*>(shard.get());
    if (rocksdb_shard == nullptr) {
      continue;
    }
    rocksdb_shard->onSettingsUpdated(db_settings_.get());
  }
}

}} // namespace facebook::logdevice
