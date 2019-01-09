/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/server/storage_tasks/RecordCacheRepopulationTask.h"

#include "logdevice/common/Processor.h"
#include "logdevice/common/Worker.h"
#include "logdevice/common/debug.h"
#include "logdevice/common/stats/Stats.h"
#include "logdevice/common/types_internal.h"
#include "logdevice/server/RecordCache.h"
#include "logdevice/server/ServerProcessor.h"
#include "logdevice/server/locallogstore/LocalLogStore.h"
#include "logdevice/server/read_path/LogStorageStateMap.h"
#include "logdevice/server/storage_tasks/ShardedStorageThreadPool.h"
#include "logdevice/server/storage_tasks/StorageThreadPool.h"

namespace facebook { namespace logdevice {

void RecordCacheRepopulationTask::execute() {
  LocalLogStore& shard = storageThreadPool_->getLocalLogStore();
  // the store should not be disabled since we checked before creating the task,
  // therefore getShardIdx() should yield a valid result
  ld_check(shard.getShardIdx() == shard_idx_);
  ShardedLocalLogStore* sharded_store =
      storageThreadPool_->getProcessor()
          .sharded_storage_thread_pool_->getShardedLocalLogStore();
  LogStorageStateMap& log_storage_state_map =
      storageThreadPool_->getProcessor().getLogStorageStateMap();

  // On exit, always delete all snapshot blobs, so that future instances
  // do not repopulate record caches from old data!
  SCOPE_EXIT {
    int rv = storageThreadPool_->getLocalLogStore().deleteAllLogSnapshotBlobs();
    if (rv != 0) {
      ld_critical(
          "Failed to delete all log snapshot blobs on shard %d", shard_idx_);
      status_ = E::FAILED;
    }
  };

  if (!repopulate_record_caches_) {
    status_ = E::OK;
    return;
  }

  ld_check(sharded_store->numShards() > 0);

  size_t repopulated_caches = 0;
  size_t repopulated_bytes = 0;
  const size_t bytes_limit_per_shard =
      storageThreadPool_->getProcessor().settings()->record_cache_max_size /
      sharded_store->numShards();

  LocalLogStore::LogSnapshotBlobCallback repopulate = [&](logid_t log_id,
                                                          Slice data) {
    if (bytes_limit_per_shard > 0 &&
        repopulated_bytes + data.size > bytes_limit_per_shard) {
      ld_error("Repopulating saved snapshot of record cache on shard %d "
               "reached the byte limit of %lu bytes per-shard. Already "
               "populated %lu bytes. Stop populating record caches on "
               "this shard.",
               shard_idx_,
               bytes_limit_per_shard,
               repopulated_bytes);
      return -1;
    }

    int rv = log_storage_state_map.repopulateRecordCacheFromLinearBuffer(
        log_id,
        shard_idx_,
        reinterpret_cast<const char*>(data.data),
        data.size);
    if (rv == 0) {
      repopulated_caches++;
      repopulated_bytes += data.size;
    }
    return rv;
  };

  int rv = shard.readAllLogSnapshotBlobs(
      LocalLogStore::LogSnapshotBlobType::RECORD_CACHE, repopulate);
  if (rv == 0) {
    status_ = E::OK;
  } else {
    ld_error("Failed to read all snapshots on shard %d. Repopulated caches "
             "for %ju logs so far, totaling %ju bytes",
             shard_idx_,
             repopulated_caches,
             repopulated_bytes);
    status_ = E::PARTIAL;
    STAT_INCR(stats_, record_cache_repopulations_failed);
  }

  ld_info("Repopulated record caches for %ju logs on shard %d, totaling %ju "
          "bytes.",
          repopulated_caches,
          shard_idx_,
          repopulated_bytes);
  STAT_ADD(stats_, record_cache_repopulated_bytes, repopulated_bytes);

  // also bump the record_cache_bytes_cached_estimate stats for accurately
  // keeping track of the record cache size upon restart
  STAT_ADD(stats_, record_cache_bytes_cached_estimate, repopulated_bytes);
}

void RecordCacheRepopulationTask::onDone() {
  ld_check(parent_);
  // Notify parent of result
  parent_->onRepopulationTaskDone(status_, shard_idx_);
}
}} // namespace facebook::logdevice
