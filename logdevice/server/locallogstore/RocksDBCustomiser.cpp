/**
 * Copyright (c) 2020-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/server/locallogstore/RocksDBCustomiser.h"

#include <boost/filesystem.hpp>

namespace facebook { namespace logdevice {

RocksDBCustomiser* RocksDBCustomiser::defaultInstance() {
  static RocksDBCustomiser* c = new RocksDBCustomiser(); // leak it
  return c;
}

bool RocksDBCustomiser::isDBLocal() const {
  return true;
}

bool RocksDBCustomiser::validateAndOverrideBasePath(
    std::string base_path,
    shard_size_t num_shards,
    const RocksDBSettings& db_settings,
    std::string* out_new_base_path,
    std::vector<shard_index_t>* out_disabled_shards) {
  ld_check(isDBLocal());
  // TODO: Move stuff from ShardedRocksDBLocalLogStore into here.

  return true;
}

rocksdb::Env* RocksDBCustomiser::getEnv() {
  return rocksdb::Env::Default();
}

rocksdb::Status RocksDBCustomiser::openDB(
    const rocksdb::DBOptions& db_options,
    const std::string& name, // path, from getShardPaths()
    const std::vector<rocksdb::ColumnFamilyDescriptor>& column_families,
    std::vector<rocksdb::ColumnFamilyHandle*>* handles,
    rocksdb::DB** dbptr) {
  return rocksdb::DB::Open(db_options, name, column_families, handles, dbptr);
}

rocksdb::Status RocksDBCustomiser::openReadOnlyDB(
    const rocksdb::DBOptions& db_options,
    const std::string& name,
    const std::vector<rocksdb::ColumnFamilyDescriptor>& column_families,
    std::vector<rocksdb::ColumnFamilyHandle*>* handles,
    rocksdb::DB** dbptr,
    bool error_if_log_file_exist) {
  return rocksdb::DB::OpenForReadOnly(db_options,
                                      name,
                                      column_families,
                                      handles,
                                      dbptr,
                                      error_if_log_file_exist);
}

}} // namespace facebook::logdevice
