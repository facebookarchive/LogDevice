/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include "logdevice/server/locallogstore/ShardedRocksDBLocalLogStore.h"

namespace facebook { namespace logdevice {

// a collection of rocksdb caches
struct RocksDBCachesInfo {
  std::weak_ptr<rocksdb::Cache> block_cache;
  std::weak_ptr<rocksdb::Cache> block_cache_compressed;
  std::weak_ptr<rocksdb::Cache> metadata_block_cache;
};

extern RocksDBCachesInfo g_rocksdb_caches;

/**
 * Handler for signals that causes coredumps (e.g., SIGSEGV, SIGABRT).
 * To reduce the core dump size while still getting information needed
 * for debugging (e.g., stack trace), it unmaps all the virtual memory
 * areas used by rocksdb block caches before triggering the core dump
 * (by raising SIGTRAP).
 *
 * @param sig  signal which triggered dumping core
 *
 */
void handle_fatal_signal(int sig);
}} // namespace facebook::logdevice
