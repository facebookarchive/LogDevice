/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/server/RecordCacheDisposal.h"

#include "logdevice/common/MetaDataLog.h"
#include "logdevice/common/Worker.h"
#include "logdevice/common/configuration/UpdateableConfig.h"
#include "logdevice/common/stats/Stats.h"
#include "logdevice/server/EpochRecordCache.h"
#include "logdevice/server/ServerProcessor.h"
#include "logdevice/server/locallogstore/LocalLogStore.h"
#include "logdevice/server/read_path/AllServerReadStreams.h"
#include "logdevice/server/read_path/LogStorageState.h"
#include "logdevice/server/read_path/LogStorageStateMap.h"
#include "logdevice/server/storage_tasks/ShardedStorageThreadPool.h"

namespace facebook { namespace logdevice {

RecordCacheDisposal::RecordCacheDisposal(ServerProcessor* processor)
    : processor_(processor) {
  ld_check(processor_ != nullptr);
}

void RecordCacheDisposal::disposeOfCacheEntry(
    std::unique_ptr<EpochRecordCacheEntry> entry) {
  // Since we're going to enqueue the destruction on the disposal thread, this
  // method must be called while the event loop of that worker is still running.
  ld_check(!processor_->isShuttingDown());

  // If we don't do this here, but let the destructor do this instead, then in
  // the case where we're freeing a whole linked list, each element will only be
  // freed when the disposal list of the next Worker is processed.  That can
  // take a long time, and if it happens during shut down, we won't know to wait
  // for all of them.
  entry->next_.reset();

  STAT_SUB(getStatsHolder(),
           record_cache_bytes_cached_estimate,
           entry->getBytesEstimate());
  STAT_INCR(getStatsHolder(), record_cache_records_evicted);

  // Free the payload so that it can deallocated on the right thread.
  // Rest of the record is freed here.
  entry.reset();
}

void RecordCacheDisposal::onRecordsReleased(const EpochRecordCache& cache,
                                            lsn_t begin,
                                            lsn_t end,
                                            const ReleasedVector& entries) {
  if (!processor_->settings()->real_time_reads_enabled) {
    return;
  }

  for (size_t i = 1; i < entries.size(); i++) {
    entries[i - 1]->next_ = entries[i];
  }

  auto entries_list = entries.empty() ? nullptr : entries.front();

  size_t bytes_estimate =
      ReleasedRecords::computeBytesEstimate(entries_list.get());

  LogStorageState& log_state = processor_->getLogStorageStateMap().get(
      cache.getLogId(), cache.getShardIndex());

  processor_->applyToWorkerIdxs([&](worker_id_t index, WorkerType type) {
    if (log_state.isWorkerSubscribed(index)) {
      AllServerReadStreams& streams =
          processor_->getWorker(index, type).serverReadStreams();
      streams.appendReleasedRecords(std::make_unique<ReleasedRecords>(
          cache.getLogId(), begin, end, entries_list, bytes_estimate));
    }
  });
}

folly::Optional<Seal> RecordCacheDisposal::getSeal(logid_t logid,
                                                   shard_index_t shard,
                                                   bool soft) const {
  // log storage state for logid must exist
  LogStorageState& log_state =
      processor_->getLogStorageStateMap().get(logid, shard);
  return log_state.getSeal(soft ? LogStorageState::SealType::SOFT
                                : LogStorageState::SealType::NORMAL);
}

int RecordCacheDisposal::getHighestInsertedLSN(logid_t log_id,
                                               shard_index_t shard,
                                               lsn_t* highest_lsn) const {
  ld_check(highest_lsn);
  ShardedStorageThreadPool* spool = processor_->sharded_storage_thread_pool_;
  ld_check(spool != nullptr);
  LocalLogStore& store = spool->getByIndex(shard).getLocalLogStore();
  return store.getHighestInsertedLSN(log_id, highest_lsn);
}

size_t RecordCacheDisposal::getEpochRecordCacheSize(logid_t logid) const {
  if (MetaDataLog::isMetaDataLog(logid)) {
    // there should be at most one record per epoch for metadata logs
    // (despite their sequencer sliding window size is 2)
    return 1;
  }

  auto config = processor_->config_->get();
  const auto log = config->getLogGroupByIDShared(logid);
  if (log == nullptr) {
    // log does not exist in config, use default fallback value
    return 256;
  }

  // use the `maxWritesInFlight' attribute (sliding window size of
  // sequencer) plus one as the epoch cache capacity.
  // Note: the reason with `plus one' here is because of the special bridge
  // record plugged by epoch recovery, which might be of esn: LNG + window_size
  // + 1. So reserve an extra slot to make sure bridge record can be cached.
  return static_cast<size_t>(log->attrs().maxWritesInFlight().value()) + 1;
}

bool RecordCacheDisposal::tailOptimized(logid_t logid) const {
  if (MetaDataLog::isMetaDataLog(logid)) {
    // metadata logs should be always be tail optimized
    return true;
  }
  auto config = processor_->config_->get();
  const auto log = config->getLogGroupByIDShared(logid);
  if (log == nullptr) {
    return false;
  }

  return log->attrs().tailOptimized().value();
}

StatsHolder* RecordCacheDisposal::getStatsHolder() const {
  return processor_->stats_;
}

}} // namespace facebook::logdevice
