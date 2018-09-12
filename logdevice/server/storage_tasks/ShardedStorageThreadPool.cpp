/**
 * Copyright (c) 2017-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "ShardedStorageThreadPool.h"

#include <folly/Memory.h>

namespace facebook { namespace logdevice {

ShardedStorageThreadPool::ShardedStorageThreadPool(
    ShardedLocalLogStore* store,
    const StorageThreadPool::Params& params,
    UpdateableSettings<Settings> settings,
    size_t task_queue_size,
    StatsHolder* stats,
    const std::shared_ptr<TraceLogger> trace_logger)
    : sharded_log_store_(store) {
  shard_size_t nshards = store->numShards();
  pools_.reserve(nshards);
  for (shard_index_t shard_idx = 0; shard_idx < nshards; ++shard_idx) {
    pools_.push_back(
        // may throw
        std::make_unique<StorageThreadPool>(shard_idx,
                                            params,
                                            settings,
                                            store->getByIndex(shard_idx),
                                            task_queue_size,
                                            stats,
                                            trace_logger));
  }
}
}} // namespace facebook::logdevice
