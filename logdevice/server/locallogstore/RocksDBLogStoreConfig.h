/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <rocksdb/env.h>

#include "logdevice/common/configuration/UpdateableConfig.h"
#include "logdevice/common/settings/RebuildingSettings.h"
#include "logdevice/common/settings/UpdateableSettings.h"
#include "logdevice/common/types_internal.h"
#include "logdevice/include/types.h"
#include "logdevice/server/locallogstore/RocksDBSettings.h"

/**
 * RocksDBLogStoreConfig holds all the necessary configuration for a
 * LocalLogStore based on rocksdb.
 */

namespace facebook { namespace logdevice {

struct RocksDBLogStoreConfig {
  RocksDBLogStoreConfig() {}

  /**
   * Initialize a RocksDBLogStoreConfig. Populate several rocksdb data
   * structures such as:
   *
   * - the memtable factory;
   * - the table factory;
   * - a RocksDBTablePropertiesCollectorFactory for collecting stats about table
   *   files;
   * - A prefix extractor that return the logid part of a key to optimize reads;
   * - uncompressed and compressed block caches.
   *
   *  This function overrides FlushBlockPolicy with
   *  logdevice::RocksDBFlushBlockPolicy, if RocksDBSettings asks for it.
   *
   * @param rocksdb_settings    RocksDB settings. This contains settings to
   *                            populate some options inside data structures
   *                            held in `options_`, `table_options_`,
   *                            `metadata_options_` and
   *                            `metadata_table_options_`.
   * @param rebuilding_settings Rebuilding settings.
   * @param env                 If not nullptr, attach an env to `options_`.
   * @param updateable_config   If not nullptr, used to create a
   *                            RocksDBTablePropertiesCollector to collect stats
   *                            about table files. @see RocksDBListener.h
   * @param stats               StatsHolder object.
   */
  RocksDBLogStoreConfig(
      UpdateableSettings<RocksDBSettings> rocksdb_settings,
      UpdateableSettings<RebuildingSettings> rebuilding_settings,
      rocksdb::EnvWrapper* env,
      std::shared_ptr<UpdateableConfig> updateable_config,
      StatsHolder* stats);

  // Copyable.
  RocksDBLogStoreConfig(const RocksDBLogStoreConfig& rhs) = default;
  void createMergeOperator(shard_index_t this_shard);

  std::shared_ptr<const RocksDBSettings> getRocksDBSettings() const {
    return rocksdb_settings_.get();
  }

  std::shared_ptr<const RebuildingSettings> getRebuildingSettings() const {
    return rebuilding_settings_.get();
  }

  rocksdb::Options* getRocksDBOptions() {
    return &options_;
  }

  // NOTE: Add a mutex to protect the fields below.
  rocksdb::Options options_;
  rocksdb::BlockBasedTableOptions table_options_;

  // Options for metadata column family (only if partitioned = true).
  rocksdb::ColumnFamilyOptions metadata_options_;
  rocksdb::BlockBasedTableOptions metadata_table_options_;

  UpdateableSettings<RocksDBSettings> rocksdb_settings_;
  UpdateableSettings<RebuildingSettings> rebuilding_settings_;

  // Whether memtable flush decisions are made by logdevice rather than rocksdb.
  // Based on setting ld_managed_flushes, but takes other factors into account
  // and may be different.
  bool use_ld_managed_flushes_;

  // Create an SstFileManager, used to ratelimit deletes. This should only be
  // called on a copy that has been created for a particular shard.
  void addSstFileManagerForShard();
};

}} // namespace facebook::logdevice
