/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/server/locallogstore/RocksDBLogStoreConfig.h"

#include <rocksdb/cache.h>
#include <rocksdb/db.h>
#include <rocksdb/filter_policy.h>
#include <rocksdb/options.h>
#include <rocksdb/slice_transform.h>
#include <rocksdb/sst_file_manager.h>
#include <rocksdb/statistics.h>
#include <rocksdb/table.h>
#include <rocksdb/write_buffer_manager.h>

#include "logdevice/server/locallogstore/RocksDBCompactionFilter.h"
#include "logdevice/server/locallogstore/RocksDBEnv.h"
#include "logdevice/server/locallogstore/RocksDBFlushBlockPolicy.h"
#include "logdevice/server/locallogstore/RocksDBKeyFormat.h"
#include "logdevice/server/locallogstore/RocksDBListener.h"
#include "logdevice/server/locallogstore/RocksDBLogger.h"
#include "logdevice/server/locallogstore/RocksDBMemTableRep.h"
#include "logdevice/server/locallogstore/RocksDBWriterMergeOperator.h"

namespace facebook { namespace logdevice {

RocksDBLogStoreConfig::RocksDBLogStoreConfig(
    UpdateableSettings<RocksDBSettings> rocksdb_settings,
    UpdateableSettings<RebuildingSettings> rebuilding_settings,
    rocksdb::EnvWrapper* env,
    std::shared_ptr<UpdateableConfig> updateable_config,
    StatsHolder* stats)
    : rocksdb_settings_(rocksdb_settings),
      rebuilding_settings_(rebuilding_settings) {
  options_ = rocksdb_settings_->toRocksDBOptions();
  options_.allow_mmap_reads = false;
  options_.allow_mmap_writes = false;
  options_.create_if_missing = true;

#ifdef LOGDEVICED_ROCKSDB_HAS_AVOID_UNNECESSARY_BLOCKING_IO
  // When ColumnFamilyHandle is destroyed, defer file deletions to background
  // thread. That's because we sometimes do it on worker threads, e.g. when
  // destroying nonblocking iterators (logsdb iterators hold PartitionPtr,
  // which owns ColumnFamilyHandle).
  options_.avoid_unnecessary_blocking_io = true;
#endif

  table_options_.index_block_restart_interval =
      rocksdb_settings_->index_block_restart_interval;

  table_options_.whole_key_filtering = false;

  if (env) {
    options_.env = env;
  }

  options_.wal_recovery_mode = rocksdb::WALRecoveryMode::kPointInTimeRecovery;

  if (updateable_config) {
    options_.table_properties_collector_factories.push_back(
        std::make_shared<RocksDBTablePropertiesCollectorFactory>(
            updateable_config, stats));
  }

  // Use a prefix extractor which returns the part of the key containing just
  // the log id. Since almost all of our reads are restricted to a particular
  // log, this enables additional optimizations in tailing and skip list
  // iterators.
  using RocksDBKeyFormat::DataKey;
  options_.prefix_extractor.reset(
      rocksdb::NewCappedPrefixTransform(DataKey::PREFIX_LENGTH));

  if (rocksdb_settings_->cache_size_ > 0) {
    if (rocksdb_settings_->cache_numshardbits_ > 0) {
      table_options_.block_cache =
          rocksdb::NewLRUCache(rocksdb_settings_->cache_size_,
                               rocksdb_settings_->cache_numshardbits_,
                               false, /*strict_capacity_limit*/
                               rocksdb_settings_->cache_high_pri_pool_ratio_);
    } else {
      table_options_.block_cache =
          rocksdb::NewLRUCache(rocksdb_settings_->cache_size_);
    }
  }

  if (rocksdb_settings_->compressed_cache_size_ > 0) {
    if (rocksdb_settings_->compressed_cache_numshardbits_ > 0) {
      table_options_.block_cache_compressed = rocksdb::NewLRUCache(
          rocksdb_settings_->compressed_cache_size_,
          rocksdb_settings_->compressed_cache_numshardbits_);
    } else {
      table_options_.block_cache_compressed =
          rocksdb::NewLRUCache(rocksdb_settings_->compressed_cache_size_);
    }
  }

  if (rocksdb_settings_->flush_block_policy_ !=
      RocksDBSettings::FlushBlockPolicyType::DEFAULT) {
    table_options_.flush_block_policy_factory =
        std::make_shared<RocksDBFlushBlockPolicyFactory>(
            rocksdb_settings_->block_size_,
            rocksdb_settings_->min_block_size_,
            rocksdb_settings_->flush_block_policy_ ==
                RocksDBSettings::FlushBlockPolicyType::EACH_COPYSET,
            stats);
  }

  table_options_.block_size = rocksdb_settings_->block_size_;
  table_options_.cache_index_and_filter_blocks =
      rocksdb_settings_->cache_index_;
  table_options_.cache_index_and_filter_blocks_with_high_priority =
      rocksdb_settings_->cache_index_with_high_priority_;
  table_options_.read_amp_bytes_per_bit =
      rocksdb_settings_->read_amp_bytes_per_bit_;

  if (rocksdb_settings_->bloom_bits_per_key_ > 0) {
    table_options_.filter_policy.reset(
        rocksdb::NewBloomFilterPolicy(rocksdb_settings_->bloom_bits_per_key_,
                                      rocksdb_settings_->bloom_block_based_));
  }

  options_.table_factory.reset(
      rocksdb::NewBlockBasedTableFactory(table_options_));

  if (rocksdb_settings_->enable_insert_hint_) {
    // In case data is not partitioned, DataKey::PREFIX_LENGTH (=9) will be
    // used for both DataKey and various metadata keys. This is fine:
    // most types of keys start with 9 bytes (type, log_id).
    options_.memtable_insert_with_hint_prefix_extractor.reset(
        rocksdb::NewCappedPrefixTransform(DataKey::PREFIX_LENGTH));
  }

  // Separate caches and options for metadata column family.
  if (rocksdb_settings_->partitioned) {
    metadata_table_options_ = table_options_;
    bool changed = false;

    if (rocksdb_settings_->metadata_cache_size_ > 0) {
      changed = true;
      if (rocksdb_settings_->metadata_cache_numshardbits_ > 0) {
        metadata_table_options_.block_cache = rocksdb::NewLRUCache(
            rocksdb_settings_->metadata_cache_size_,
            rocksdb_settings_->metadata_cache_numshardbits_);
      } else {
        metadata_table_options_.block_cache =
            rocksdb::NewLRUCache(rocksdb_settings_->metadata_cache_size_);
      }
    }

    if (rocksdb_settings_->metadata_block_size_ > 0) {
      changed = true;
      metadata_table_options_.block_size =
          rocksdb_settings_->metadata_block_size_;
    }

    if (metadata_table_options_.flush_block_policy_factory) {
      changed = true;
      metadata_table_options_.flush_block_policy_factory = nullptr;
    }

    if (rocksdb_settings_->metadata_bloom_bits_per_key_ !=
        rocksdb_settings_->bloom_bits_per_key_) {
      changed = true;
      if (rocksdb_settings_->metadata_bloom_bits_per_key_ > 0) {
        metadata_table_options_.filter_policy.reset(
            rocksdb::NewBloomFilterPolicy(
                rocksdb_settings_->metadata_bloom_bits_per_key_,
                rocksdb_settings_->bloom_block_based_));
      } else {
        metadata_table_options_.filter_policy = nullptr;
      }
    }

    metadata_options_ = options_;
    if (changed) {
      metadata_options_.table_factory.reset(
          rocksdb::NewBlockBasedTableFactory(metadata_table_options_));
    }
    if (rocksdb_settings_->enable_insert_hint_) {
      metadata_options_.memtable_insert_with_hint_prefix_extractor.reset(
          rocksdb::NewNoopTransform());
    }

    // LD managed flushes.
    // Make sure RocksDBSettings::INFINITE_MEMORY_LIMIT value is sufficiently
    // large.
    auto can_ld_manage_flushes = RocksDBSettings::INFINITE_MEMORY_LIMIT >=
        2 * rocksdb_settings_->memtable_size_per_node;

    // db_write_buffer_size should be zero for logdevice to manage flushes.
    // Until the new (per-node) setting is used everywhere, don't override old
    // (per-shard) setting if a custom value was set
    can_ld_manage_flushes &= rocksdb_settings_->db_write_buffer_size == 0;

    // Finally check if ld_managed_flushes is disabled or not.
    can_ld_manage_flushes &= rocksdb_settings_->ld_managed_flushes;

    auto node_mem_limit = RocksDBSettings::INFINITE_MEMORY_LIMIT;
    // Rollback settings before creating or recovering rocksdb instance.
    if (!can_ld_manage_flushes) {
      // TODO Set stat indicating shard cannot come up with ld_managed_flushes.
      options_.write_buffer_size = rocksdb_settings_->write_buffer_size;
      node_mem_limit = rocksdb_settings_->memtable_size_per_node;
      if (rocksdb_settings_->ld_managed_flushes) {
        ld_error("Cannot manage flushing memtable within logdevice because, %s",
                 rocksdb_settings_->db_write_buffer_size != 0
                     ? "db_write_buffer_size is non-zero"
                     : "infinite memory limit value should be atleast twice of "
                       "per node limit.");
      } else {
        ld_info("LD not managing flushing of memtables to sst.");
      }
    }

    if (rocksdb_settings_->db_write_buffer_size == 0) {
      // Enforces total size of DB write buffers per node, not shard
      options_.write_buffer_manager =
          std::make_shared<rocksdb::WriteBufferManager>(node_mem_limit);
    }
  }
}

void RocksDBLogStoreConfig::createMergeOperator(shard_index_t this_shard) {
  options_.merge_operator.reset(new RocksDBWriterMergeOperator(this_shard));
}

void RocksDBLogStoreConfig::addSstFileManagerForShard() {
  if (!options_.env) {
    ld_error("Can't create SstFileManager: no env");
    return;
  }

  rocksdb::Status status; // status of delete of any existing trash
  std::shared_ptr<rocksdb::Logger> logger =
      std::make_shared<RocksDBLogger>(dbg::currentLevel);
  options_.sst_file_manager.reset(
      rocksdb::NewSstFileManager(options_.env,
                                 std::move(logger),
                                 "" /* trash_dir, deprecated */,
                                 rocksdb_settings_->sst_delete_bytes_per_sec,
                                 true, // delete_existing_trash
                                 &status,
                                 1.1)); // max_trash_db_ratio > 100 % to always
                                        // enforce ratelimit

  if (!status.ok()) {
    ld_error("An error occurred when deleting existing trash: %s",
             status.ToString().c_str());
  }
}

}} // namespace facebook::logdevice
