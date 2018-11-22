/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "ServerProcessor.h"

#include "logdevice/common/EventLoopHandle.h"
#include "logdevice/common/UpdateableSecurityInfo.h"
#include "logdevice/common/stats/Stats.h"
#include "logdevice/server/FailureDetector.h"
#include "logdevice/server/storage/PurgeCoordinator.h"
#include "logdevice/server/storage_tasks/ShardedStorageThreadPool.h"

namespace facebook { namespace logdevice {

ServerWorker* ServerProcessor::createWorker(worker_id_t i, WorkerType type) {
  return new ServerWorker(this, i, config_, stats_, type);
}

std::unique_ptr<LogStorageState_PurgeCoordinator_Bridge>
ServerProcessor::createPurgeCoordinator(logid_t log_id,
                                        shard_index_t shard,
                                        LogStorageState* parent) {
  return std::make_unique<PurgeCoordinator>(log_id, shard, parent);
}

void ServerProcessor::maybeCreateLogStorageStateMap() {
  if (runningOnStorageNode()) {
    // sharded_storage_thread_pool_ may be nullptr in tests, in that case
    // assume there is one shard only.
    const shard_size_t num_shards = sharded_storage_thread_pool_
        ? sharded_storage_thread_pool_->numShards()
        : 1;
    log_storage_state_map_ = std::make_unique<LogStorageStateMap>(
        num_shards, updateableSettings()->log_state_recovery_interval, this);
  }
}

void ServerProcessor::init() {
  Processor::init();

  if (sharded_storage_thread_pool_ != nullptr) {
    // All shards are assumed to be waiting to be rebuilt until
    // markShardAsNotMissingData() is called.
    for (shard_index_t shard_idx = 0;
         shard_idx < sharded_storage_thread_pool_->numShards();
         ++shard_idx) {
      PER_SHARD_STAT_INCR(stats_, shard_missing_all_data, shard_idx);
      PER_SHARD_STAT_INCR(stats_, shard_dirty, shard_idx);
    }
  }
  // It's ok to capture this since ServerProcessor destructor will be called
  // before Processor destructor. ServerProcessor destructor will unsubscrube
  // from settings updates. All accessed variables will still be alive since
  // they are members of base class. Subscribe/unsubscribe/update methods of
  // UpdateableSettings are mutex protected.
  std::function<void()> updateResourceBudget = [this]() {
    conn_budget_backlog_.setLimit(settings()->connection_backlog);
  };
  settings_subscription_ =
      updateableSettings().callAndSubscribeToUpdates(updateResourceBudget);
}

int ServerProcessor::getWorkerCount(WorkerType type) const {
  switch (type) {
    case WorkerType::BACKGROUND:
      return server_settings_->num_background_workers;
    case WorkerType::FAILURE_DETECTOR:
      if (gossip_settings_->enabled) {
        return 1; // failure detector is hard coded to have only one worker.
      }
    default:
      return Processor::getWorkerCount(type);
  }
}

void ServerProcessor::getClusterDeadNodeStats(size_t* effective_dead_cnt,
                                              size_t* effective_cluster_size) {
  if (failure_detector_) {
    failure_detector_->getClusterDeadNodeStats(
        effective_dead_cnt, effective_cluster_size);
  }
}

bool ServerProcessor::isNodeAlive(node_index_t index) const {
  if (failure_detector_) {
    return failure_detector_->isAlive(index);
  }
  return true;
}

bool ServerProcessor::isNodeBoycotted(node_index_t index) const {
  if (failure_detector_) {
    return failure_detector_->isBoycotted(index);
  }
  return false;
}

bool ServerProcessor::isNodeIsolated() const {
  if (failure_detector_) {
    return failure_detector_->isIsolated();
  }
  return false;
}

bool ServerProcessor::isFailureDetectorRunning() const {
  return (failure_detector_ != nullptr);
}

LogStorageStateMap& ServerProcessor::getLogStorageStateMap() const {
  ld_check(runningOnStorageNode());
  ld_check(log_storage_state_map_);
  return *log_storage_state_map_;
}
}} // namespace facebook::logdevice
