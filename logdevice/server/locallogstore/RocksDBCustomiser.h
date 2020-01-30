/**
 * Copyright (c) 2020-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <rocksdb/db.h>

#include "logdevice/common/plugin/Plugin.h"
#include "logdevice/server/locallogstore/RocksDBSettings.h"

namespace facebook { namespace logdevice {

/**
 * @file
 * `RocksDBCustomiser` allows changing/wrapping rocksdb::Env (e.g. to use
 * remote storage) and rocksdb::DB (e.g. to publish extra stats).
 * All methods have default implementations, which are also used if no plugin
 * is present.
 */

class RocksDBCustomiser {
 public:
  RocksDBCustomiser() = default;
  virtual ~RocksDBCustomiser() = default;

  // Returns a static uncustomzied RocksDBCustomiser instance.
  static RocksDBCustomiser* defaultInstance();

  // If isDBLocal() is true, the rocksdb files will live in a perfectly normal
  // directory in the local file system, and the shard paths returned from
  // getShardPaths() are the paths to these directories.
  // If isDBLocal() is false, rocksdb will store its data elsewhere, like in a
  // remote storage service or in memory. In this case the Env returned from
  // getEnv() is responsible for forwarding all the IO to wherever the data
  // is actually stored.
  // Logdevice uses this flag to disable some behavior that accesses the local
  // file system directly, e.g. space-based trimming.
  // The returned value shouldn't change over the lifetime of RocksDBCustomiser.
  virtual bool isDBLocal() const;

  // Called once during sharded DB initialization. May modify the DB path if
  // it wants (e.g. to include cluster name and node ID if the files will be
  // stored remotely in a shared service). May do sanity checks and preparation
  // on the directory (e.g. check that the directory exists or create it, or
  // check/create NSHARDS file).
  // The paths to shards will be formed by appending "/shard<idx>/" to
  // out_new_base_path; this behavior is not currently customizable.
  //
  // Return false if some checks failed, and the server should refuse to start.
  // validateAndOverrideBasePath() should log the error details in this case.
  //
  // @param out_new_base_path
  //   If the RocksDBCustomiser wants to override base path, it can assign the
  //   new value here.
  // @param out_disabled_shards
  //   Output parameter with the list of shards that should be considered
  //   disabled, i.e. FailingLocalLogStore will be created. This may happen
  //   e.g. if a disk is missing, or if disabling disk was explicitly requested
  //   from outside.
  virtual bool
  validateAndOverrideBasePath(std::string base_path,
                              shard_size_t num_shards,
                              const RocksDBSettings& db_settings,
                              std::string* out_new_base_path,
                              std::vector<shard_index_t>* out_disabled_shards);

  // Returns a rocksdb::Env or nullptr to use the default one.
  // The returned Env will be wrapped in logdevice::RocksDBEnv and shared
  // across all shards in current process. We'll never call destructor on it,
  // i.e. it should be static or owned by RocksDBCustomiser (or just leak).
  // Custom Env can be used to implement remote storage, by forwarding all
  // file IO to an external service.
  virtual rocksdb::Env* getEnv();

  // Open/create a rocksdb database.
  // Typically a wrapper around rocksdb::DB::Open().
  // May return an instance of some custom type derived from rocksdb::DB,
  // e.g. a wrapper that collects some extra stats.
  virtual rocksdb::Status
  openDB(const rocksdb::DBOptions& db_options,
         const std::string& name, // path, from getShardPaths()
         const std::vector<rocksdb::ColumnFamilyDescriptor>& column_families,
         std::vector<rocksdb::ColumnFamilyHandle*>* handles,
         rocksdb::DB** dbptr);

  // Same but for rocksdb::DB::OpenForReadOnly().
  virtual rocksdb::Status openReadOnlyDB(
      const rocksdb::DBOptions& db_options,
      const std::string& name,
      const std::vector<rocksdb::ColumnFamilyDescriptor>& column_families,
      std::vector<rocksdb::ColumnFamilyHandle*>* handles,
      rocksdb::DB** dbptr,
      bool error_if_log_file_exist = false);
};

class RocksDBCustomiserFactory : public Plugin {
 public:
  PluginType type() const override {
    return PluginType::ROCKSDB_CUSTOMISER_FACTORY;
  }

  // Create a RocksDBCustomiser responsible for a sharded local log store.
  // Called once, during startup. The returned object is used for creating
  // the rocksdb DBs, then is kept around for the uptime of the server, then
  // destroyed after closing all rocksdb DBs.
  virtual std::unique_ptr<RocksDBCustomiser>
  operator()(std::string local_log_store_path,
             std::string cluster_name,
             node_index_t my_node_id,
             shard_size_t num_shards,
             UpdateableSettings<RocksDBSettings> db_settings) = 0;
};

}} // namespace facebook::logdevice
