/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/server/RepopulateRecordCachesRequest.h"

#include "logdevice/common/debug.h"
#include "logdevice/server/ServerProcessor.h"
#include "logdevice/server/ServerWorker.h"
#include "logdevice/server/storage_tasks/PerWorkerStorageTaskQueue.h"
#include "logdevice/server/storage_tasks/RecordCacheRepopulationTask.h"
#include "logdevice/server/storage_tasks/ShardedStorageThreadPool.h"
#include "logdevice/server/storage_tasks/StorageThreadPool.h"

namespace facebook { namespace logdevice {

RepopulateRecordCachesRequest::RepopulateRecordCachesRequest(
    std::function<void(Status, int)> callback,
    bool repopulate_record_caches)
    : Request(RequestType::REPOPULATE_RECORD_CACHES),
      callback_(callback),
      ref_holder_(this),
      repopulate_record_caches_(repopulate_record_caches) {}

void RepopulateRecordCachesRequest::onRepopulationTaskDone(
    Status status,
    shard_index_t shard_idx) {
  ld_check(remaining_record_cache_repopulations_ > 0);
  ld_check(status == E::OK || status == E::PARTIAL || status == E::FAILED);

  callback_(status, shard_idx);
  remaining_record_cache_repopulations_--;
  if (remaining_record_cache_repopulations_ == 0) {
    delete this;
  }
}

Request::Execution RepopulateRecordCachesRequest::execute() {
  ServerWorker* w = ServerWorker::onThisThread();

  const auto* sharded_pool = w->processor_->sharded_storage_thread_pool_;
  // must be on a storage node
  ld_check(sharded_pool != nullptr);

  // Start a StorageTask for each shard
  const shard_size_t num_shards = sharded_pool->numShards();
  remaining_record_cache_repopulations_ = num_shards;

  if (!repopulate_record_caches_) {
    ld_info("Not repopulating caches"); // just dropping snapshots col. family
  }

  for (shard_index_t shard_idx = 0; shard_idx < num_shards; shard_idx++) {
    // Skip unwritable shards
    LocalLogStore& shard =
        w->processor_->sharded_storage_thread_pool_->getByIndex(shard_idx)
            .getLocalLogStore();
    if (shard.acceptingWrites() == E::DISABLED) {
      remaining_record_cache_repopulations_--;
      callback_(E::DISABLED, shard_idx);
      continue;
    }

    std::unique_ptr<StorageTask> task =
        std::make_unique<RecordCacheRepopulationTask>(
            shard_idx, ref_holder_.ref(), repopulate_record_caches_);
    w->getStorageTaskQueueForShard(shard_idx)->putTask(std::move(task));
  }

  // We're done if there are no enabled shards
  return remaining_record_cache_repopulations_ == 0 ? Execution::COMPLETE
                                                    : Execution::CONTINUE;
}

}} // namespace facebook::logdevice
