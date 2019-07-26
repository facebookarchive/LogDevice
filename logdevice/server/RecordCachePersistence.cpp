/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/server/RecordCachePersistence.h"

#include "logdevice/common/Worker.h"
#include "logdevice/server/RecordCache.h"
#include "logdevice/server/ServerProcessor.h"
#include "logdevice/server/read_path/LogStorageStateMap.h"
#include "logdevice/server/storage_tasks/ExecStorageThread.h"
#include "logdevice/server/storage_tasks/ShardedStorageThreadPool.h"
#include "logdevice/server/storage_tasks/StorageThreadPool.h"

namespace facebook { namespace logdevice { namespace RecordCachePersistence {

// Write in batches of 10Mib
static const size_t SNAPSHOT_BATCH_SIZE_LIMIT = 10 * 1024 * 1024;

void persistRecordCaches(shard_index_t shard_idx,
                         StorageThreadPool* storage_thread_pool) {
  LocalLogStore& shard = storage_thread_pool->getLocalLogStore();
  LogStorageStateMap& log_storage_state_map =
      storage_thread_pool->getProcessor().getLogStorageStateMap();
  ShardedStorageThreadPool* sharded_store =
      storage_thread_pool->getProcessor().sharded_storage_thread_pool_;

  Status accepting = shard.acceptingWrites();
  if (accepting != Status::OK && accepting != Status::LOW_ON_SPC) {
    ld_info("Shard %d not accepting writes, skipping RecordCache persistence",
            shard_idx);
    return;
  }

  ld_check(shard.getShardIdx() == shard_idx);

  ld_check(sharded_store->numShards() > 0);
  const size_t bytes_limit_per_shard =
      storage_thread_pool->getProcessor().settings()->record_cache_max_size /
      sharded_store->numShards();

  // Keep owners of RecordCache blobs in memory so contents aren't freed
  std::vector<std::unique_ptr<uint8_t[]>> record_cache_snapshot_owners;
  std::vector<std::pair<logid_t, Slice>> record_cache_snapshot_batch;
  size_t logs_in_current_batch = 0;
  size_t total_persisted_logs = 0;
  size_t bytes_in_current_batch = 0;
  size_t total_bytes = 0;

  auto commit_batch = [&]() -> int {
    int rv = shard.writeLogSnapshotBlobs(
        LocalLogStore::LogSnapshotBlobType::RECORD_CACHE,
        record_cache_snapshot_batch);
    if (rv != 0) {
      ld_error("Failed to write a batch of log snapshot blobs to shard "
               "%d: %s. Written in previous batches: %ju logs, totaling %ju "
               "bytes",
               shard_idx,
               error_name(err),
               total_persisted_logs,
               total_bytes);
    } else {
      total_bytes += bytes_in_current_batch;
      total_persisted_logs += logs_in_current_batch;
    }

    bytes_in_current_batch = logs_in_current_batch = 0;
    record_cache_snapshot_batch.clear();
    record_cache_snapshot_owners.clear();
    return rv;
  };

  // A callback function called for each log, which will write batches of
  // serialized representations of record caches to disk.
  auto callback = [&](logid_t log_id, const LogStorageState& state) {
    // Serialize the record cache
    RecordCache* record_cache = state.record_cache_.get();
    if (!record_cache) {
      return 0;
    }
    ssize_t size = record_cache->sizeInLinearBuffer();
    if (size == -1) {
      ld_error("Failed to calculate size of RecordCache in linear buffer on "
               "shard %d",
               shard_idx);
      return -1;
    }

    if (bytes_limit_per_shard > 0 &&
        total_bytes + bytes_in_current_batch + size >= bytes_limit_per_shard) {
      // the snapshot is about to exceed the byte limit, commit the current
      // batch and abort the operation
      commit_batch();
      ld_error("Snapshot of record cache on shard %d reached the byte limit of "
               "%lu bytes per-shard. Already persisted %lu, current batch %lu. "
               "Stop persisting record caches on this shard.",
               shard_idx,
               bytes_limit_per_shard,
               total_bytes,
               bytes_in_current_batch);
      return -1;
    }

    auto buffer = std::make_unique<uint8_t[]>(size);
    ssize_t linear_size = record_cache->toLinearBuffer(
        reinterpret_cast<char*>(buffer.get()), size);
    if (linear_size == -1) {
      ld_error("Failed to linearize RecordCache on shard %d", shard_idx);
      return -1;
    }
    ld_check(size == linear_size);

    // Add serialized representation to batch. If the batch's size now exceeds
    // the batch limit, write it and start a new one.
    // TODO 12730327: make code cleaner without sharing states between two
    // lambdas
    bytes_in_current_batch += linear_size;
    logs_in_current_batch++;
    record_cache_snapshot_batch.emplace_back(
        log_id, Slice(buffer.get(), linear_size));
    record_cache_snapshot_owners.push_back(std::move(buffer));
    if (bytes_in_current_batch >= SNAPSHOT_BATCH_SIZE_LIMIT) {
      return commit_batch();
    }
    return 0;
  };

  // Run above lambda for each log
  int rv = log_storage_state_map.forEachLogOnShard(shard_idx, callback);

  // Write the last batch, unless we've previously encountered an error
  if (rv == 0) {
    commit_batch();
  }

  ld_info("Persisted record caches for %ju logs on shard %d, totaling %ju "
          "bytes.",
          total_persisted_logs,
          shard_idx,
          total_bytes);
}
}}} // namespace facebook::logdevice::RecordCachePersistence
