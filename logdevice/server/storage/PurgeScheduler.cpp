/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/server/storage/PurgeScheduler.h"

#include "logdevice/common/Worker.h"
#include "logdevice/common/debug.h"
#include "logdevice/common/stats/Stats.h"
#include "logdevice/server/ServerProcessor.h"
#include "logdevice/server/read_path/LogStorageStateMap.h"
#include "logdevice/server/storage/PurgeCoordinator.h"
#include "logdevice/server/storage_tasks/ShardedStorageThreadPool.h"

namespace facebook { namespace logdevice {

PurgeScheduler::PurgeScheduler(ServerProcessor* processor)
    : processor_(processor) {
  ld_check(processor_->runningOnStorageNode());
  // Create a PerWorkerStorageTaskQueue for every database shard
  const shard_size_t nshards =
      processor_->sharded_storage_thread_pool_->numShards();

  // init purging for release limit
  purgeForReleaseQueue_.resize(nshards);
  for (int i = 0; i < nshards; ++i) {
    purgeForReleaseBudget_.push_back(std::make_unique<ResourceBudget>(
        processor_->settings()->max_concurrent_purging_for_release_per_shard));
  }
}

ResourceBudget::Token
PurgeScheduler::tryStartPurgeForRelease(logid_t log_id,
                                        shard_index_t shard_index) {
  ld_check(processor_->runningOnStorageNode());

  ld_check(shard_index < purgeForReleaseBudget_.size());
  ld_check(shard_index < purgeForReleaseQueue_.size());

  ResourceBudget* budget = purgeForReleaseBudget_[shard_index].get();
  ld_check(budget != nullptr);
  ResourceBudget::Token token = budget->acquireToken();
  if (!token.valid()) {
    auto& index =
        purgeForReleaseQueue_[shard_index].q.get<LogIDUniqueQueue::FIFOIndex>();
    auto result = index.push_back(log_id);
    if (result.second) {
      WORKER_STAT_INCR(purging_for_release_enqueued);
    }
  }

  return token;
}

void PurgeScheduler::wakeUpMorePurgingForReleases(shard_index_t shard_index) {
  ld_check(processor_->runningOnStorageNode());

  if (!Worker::onThisThread()->isAcceptingWork()) {
    return;
  }

  ld_check(shard_index < purgeForReleaseBudget_.size());
  ResourceBudget* budget = purgeForReleaseBudget_[shard_index].get();
  ld_check(budget != nullptr);
  ld_check(shard_index < purgeForReleaseQueue_.size());
  auto& index =
      purgeForReleaseQueue_[shard_index].q.get<LogIDUniqueQueue::FIFOIndex>();

  while (budget->available() > 0 && index.size() > 0) {
    auto it = index.begin();
    const logid_t new_logid = *it;
    index.erase(it);
    WORKER_STAT_INCR(purging_for_release_dequeued);

    LogStorageState* log_state =
        processor_->getLogStorageStateMap().find(new_logid, shard_index);
    if (log_state == nullptr) {
      ld_critical("LogID %lu previously enqueued not found in storage state "
                  "map!",
                  new_logid.val_);
      ld_check(false);
      continue;
    }

    checked_downcast<PurgeCoordinator&>(*log_state->purge_coordinator_)
        .startBuffered();
  }
}

}} // namespace facebook::logdevice
