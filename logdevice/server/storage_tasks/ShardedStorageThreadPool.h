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

#include "logdevice/include/types.h"
#include "logdevice/server/locallogstore/LocalLogStore.h"
#include "logdevice/server/storage_tasks/StorageThreadPool.h"

namespace facebook { namespace logdevice {

/**
 * @file
 * Owns a set of StorageThreadPool instances, one per local log store shard.
 */

struct Settings;
class StatsHolder;

class ShardedStorageThreadPool {
 public:
  /**
   * Constructor.
   *
   * The supplied ShardedLocalLogStore must outlive this instance.  Individual
   * StorageThreadPools contain raw pointers to individual LocalLogStore
   * instances.
   *
   * @throws ConstructorFailed on failure
   */
  ShardedStorageThreadPool(
      ShardedLocalLogStore* store,
      const ServerSettings::StoragePoolParams& params,
      UpdateableSettings<ServerSettings> server_settings,
      UpdateableSettings<Settings> settings,
      size_t task_queue_size,
      StatsHolder* stats,
      const std::shared_ptr<TraceLogger> trace_logger = nullptr);

  void setProcessor(ServerProcessor* processor) {
    for (auto& pool : pools_) {
      pool->setProcessor(processor);
    }
  }

  int numShards() const {
    return pools_.size();
  }

  StorageThreadPool& getByIndex(size_t idx) const {
    ld_check(idx < pools_.size());
    return *pools_[idx];
  }

  void shutdown(bool persist_record_caches = false) {
    for (auto& pool : pools_) {
      pool->shutDown(persist_record_caches);
    }
    for (auto& pool : pools_) {
      pool->join();
    }
  }

  ShardedLocalLogStore* getShardedLocalLogStore() const {
    return sharded_log_store_;
  }

 private:
  std::vector<std::unique_ptr<StorageThreadPool>> pools_;
  ShardedLocalLogStore* const sharded_log_store_;
};
}} // namespace facebook::logdevice
