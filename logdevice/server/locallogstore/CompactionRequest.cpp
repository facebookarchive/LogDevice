/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/server/locallogstore/CompactionRequest.h"

#include <folly/Memory.h>

#include "logdevice/common/stats/Stats.h"
#include "logdevice/server/ServerProcessor.h"
#include "logdevice/server/ServerWorker.h"
#include "logdevice/server/locallogstore/RocksDBLocalLogStore.h"
#include "logdevice/server/storage_tasks/PerWorkerStorageTaskQueue.h"
#include "logdevice/server/storage_tasks/ShardedStorageThreadPool.h"

namespace facebook { namespace logdevice {

// storage task for performing manual compaction on LocalLogStores
// This storage task is used for the entire compaction run and is
// different from CompactionThrottleStorageTask, which is used
// for throttling during the filtering process (by getting a share
// for a fixed amount of filtering).
class CompactionStorageTask : public StorageTask {
 public:
  CompactionStorageTask(int idx, std::function<void(Status)> callback)
      : StorageTask(StorageTask::Type::COMPACT_PARTITION),
        shard_idx_(idx),
        callback_(callback) {}

  ThreadType getThreadType() const override {
    return ThreadType::SLOW;
  }

  Principal getPrincipal() const override {
    return Principal::METADATA;
  }

  void execute() override {
    LocalLogStore& store = storageThreadPool_->getLocalLogStore();
    RocksDBLocalLogStore* rocks_store =
        dynamic_cast<RocksDBLocalLogStore*>(&store);
    // currently only supports rocksDB local logstore
    if (rocks_store == nullptr) {
      return;
    }
    // block until compaction is finished
    if (rocks_store->performCompaction() != 0) {
      RATELIMIT_ERROR(std::chrono::seconds(1),
                      1,
                      "Manual Compaction failed on RocksDBLocalLogStore "
                      "shard index: %d",
                      shard_idx_);
    } else {
      STAT_INCR(storageThreadPool_->stats(), manual_compactions);
    }
  }

  void onDone() override {
    if (callback_) {
      callback_(E::OK);
    }
  }

  void onDropped() override {
    if (callback_) {
      callback_(E::FAILED);
    }
  }

 private:
  // used for logging purpose
  shard_index_t shard_idx_;

  // called upon the completion of the storage task
  std::function<void(Status)> callback_;
};

Request::Execution CompactionRequest::execute() {
  const ShardedStorageThreadPool* const sharded_pool =
      ServerWorker::onThisThread()->processor_->sharded_storage_thread_pool_;
  // must be on a storage node
  ld_check(sharded_pool != nullptr);

  auto post_compaction_task = [this](int idx) {
    // the callback_ 'pins' an inflight request slot in the
    // MonitorRequestQueue (See MonitorRequestCallback in LogStoreMonitor.cpp)
    // It will get released once we get a response on the storage task.
    auto task = std::make_unique<CompactionStorageTask>(idx, callback_);
    ServerWorker::onThisThread()->getStorageTaskQueueForShard(idx)->putTask(
        std::move(task));
  };

  if (shard_idx_.hasValue()) {
    int index = shard_idx_.value();
    ld_check(index >= 0 && index < sharded_pool->numShards());
    post_compaction_task(index);
  } else {
    // perform compaction on all shards
    for (size_t i = 0; i < sharded_pool->numShards(); ++i) {
      post_compaction_task(i);
    }
  }

  return Execution::COMPLETE;
}

}} // namespace facebook::logdevice
