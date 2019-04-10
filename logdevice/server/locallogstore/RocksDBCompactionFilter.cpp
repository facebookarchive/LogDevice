/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/server/locallogstore/RocksDBCompactionFilter.h"

#include <rocksdb/slice.h>

#include "logdevice/common/LocalLogStoreRecordFormat.h"
#include "logdevice/common/MetaDataLog.h"
#include "logdevice/common/Request.h"
#include "logdevice/common/commandline_util_chrono.h"
#include "logdevice/common/configuration/UpdateableConfig.h"
#include "logdevice/common/stats/PerShardHistograms.h"
#include "logdevice/common/stats/Stats.h"
#include "logdevice/server/ServerProcessor.h"
#include "logdevice/server/ServerWorker.h"
#include "logdevice/server/locallogstore/RocksDBKeyFormat.h"
#include "logdevice/server/locallogstore/RocksDBWriterMergeOperator.h"
#include "logdevice/server/read_path/LogStorageStateMap.h"
#include "logdevice/server/storage_tasks/PerWorkerStorageTaskQueue.h"
#include "logdevice/server/storage_tasks/StorageThreadPool.h"

namespace facebook { namespace logdevice {

using TrimPointUpdateMap = RocksDBCompactionFilter::TrimPointUpdateMap;
using PerEpochLogMetadataTrimPointUpdateMap =
    RocksDBCompactionFilter::PerEpochLogMetadataTrimPointUpdateMap;
using Reason = PartitionedRocksDBStore::PartitionToCompact::Reason;

namespace {
/*
 * This storage task is used as a psuedo task so that compaction shares
 * can also be scheduled from the common IO scheduler. It has a small
 * disadvantage to other principals: the dequeueing thread move on to the
 * next task right after dequeueing this one. So it potentially runs in
 * parallel with other tasks when it is its turn, in effect getting a smaller
 * share. But this effect is minor as most of the latency wil be queueing
 * latency on the DRR queue.
 */

class CompactionThrottleStorageTask : public StorageTask {
 public:
  explicit CompactionThrottleStorageTask(StorageTaskType type, Semaphore& sem)
      : StorageTask(type), sem_(sem) {}

  ThreadType getThreadType() const override {
    return ThreadType::SLOW;
  }

  StorageTaskPriority getPriority() const override {
    return StorageTaskPriority::LOW;
  }

  Principal getPrincipal() const override {
    return (type_ == StorageTaskType::COMPACTION_THROTTLE_RETENTION)
        ? Principal::COMPACTION_RETENTION
        : Principal::COMPACTION_PARTIAL;
  }

  void execute() override {
    sem_.post();
  }

  void onDone() override {}
  void onDropped() override {
    ld_check(false);
  }

 private:
  Semaphore& sem_;
};
} // namespace

RocksDBCompactionFilter::Decision
RocksDBCompactionFilter::filterImpl(const rocksdb::Slice& key,
                                    const rocksdb::Slice& value,
                                    std::string* skip_until) {
  using namespace facebook::logdevice::RocksDBKeyFormat;

  if (first_record_) {
    first_record_ = false;
    // Check that this thread has low IO priority (set in RocksDBEnv).
    if (settings_->low_ioprio.hasValue()) {
      std::pair<int, int> io_prio;
      int rv = get_io_priority_of_this_thread(&io_prio);
      if (rv == 0 && io_prio != settings_->low_ioprio.value()) {
        ld_error(
            "unexpected ioprio of compaction thread: %d,%d; expected: %d,%d",
            io_prio.first,
            io_prio.second,
            settings_->low_ioprio.value().first,
            settings_->low_ioprio.value().second);
      }
    }
  }

  uint64_t bytesNeeded = key.size() + value.size();
  if (storage_thread_pool_->useDRR() && context_ &&
      (context_->reason == Reason::PARTIAL ||
       context_->reason == Reason::RETENTION)) {
    Semaphore sem;
    StorageTaskType type = (context_->reason == Reason::RETENTION)
        ? StorageTaskType::COMPACTION_THROTTLE_RETENTION
        : StorageTaskType::COMPACTION_THROTTLE_PARTIAL;
    // TODO (T37200690): better to reset drrBytesAllowd_ before every IO?
    while (drrBytesAllowed_ < bytesNeeded) {
      // Overallocate for the shutdown case.
      uint64_t bytesGained = bytesNeeded - drrBytesAllowed_;
      std::unique_ptr<CompactionThrottleStorageTask> task =
          std::make_unique<CompactionThrottleStorageTask>(type, sem);
      task->setStorageThreadPool(storage_thread_pool_);
      task->enqueue_time_ = std::chrono::steady_clock::now();
      bool ret = storage_thread_pool_->blockingPutTask(std::move(task));
      if (ret) {
        // If the threadpool is shutting down then we just want
        // to complete the work here as fast and possible and
        // return. But we want to wait for the threadpool shutdown
        // to complete so that no one signals on the semaphore
        // anymore and it's okay to unwind this stack. The order
        // of shutdown is:
        // 1. shut down is initiated by marking the shutting_down_ flag.
        // 2. StorageThreadPool threads are joined and shutdown_complete_ flag
        // is set.
        // 3. We unwind from here.
        // 4. localogstore is destroyed and then the StorageThreadPool is
        // destroyed.
        while (!storage_thread_pool_->shutdownComplete()) {
          if ((sem.timedwait(std::chrono::seconds(2))) == 0) {
            bytesGained = settings_->compaction_max_bytes_at_once;
            break;
          }
        }
      }
      drrBytesAllowed_ += bytesGained;
    }
    drrBytesAllowed_ -= bytesNeeded;
  }

  // The throttler above is dynamically throttling wrt. to other
  // storage tasks in the system. This mechanism predates it and
  // was inserted to avoid VM stalls when compaction runs too fast.
  // See T6806782 for context. TODO (T37200690): tracks any
  // improvements to merge the two mechanisms.
  rate_limiter_.waitUntilAllowed(bytesNeeded);

  if (PerEpochLogMetaKey::validAnyType(key.data(), key.size())) {
    // per epoch log metadata needs to be trimmed
    logid_t log_id = PerEpochLogMetaKey::getLogID(key.data());
    epoch_t epoch = PerEpochLogMetaKey::getEpoch(key.data());
    return makeDecisionPerEpochLogMetadata(log_id, epoch);
  }

  char header = key.data()[0];
  if (key.empty() ||
      (header != DataKey::HEADER && header != CopySetIndexKey::HEADER &&
       header != IndexKey::HEADER) ||
      (CopySetIndexKey::valid(key.data(), key.size()) &&
       CopySetIndexKey::isBlockEntry(key.data()))) {
    // Ignore other non-data, non-index or copyset index block-entry keys
    return Decision::kKeep;
  }

  logid_t log_id;
  lsn_t lsn;
  if (header == CopySetIndexKey::HEADER) {
    if (!dd_assert(CopySetIndexKey::valid(key.data(), key.size()),
                   "Invalid CopySetIndexKey: %s",
                   hexdump_buf(key.data(), key.size(), 200).c_str())) {
      return Decision::kKeep;
    }
    log_id = CopySetIndexKey::getLogID(key.data());
    lsn = CopySetIndexKey::getLSN(key.data());
  } else if (header == IndexKey::HEADER) {
    if (!dd_assert(IndexKey::valid(key.data(), key.size()),
                   "Invalid IndexKey: %s",
                   hexdump_buf(key.data(), key.size(), 200).c_str())) {
      return Decision::kKeep;
    }
    log_id = IndexKey::getLogID(key.data());
    lsn = IndexKey::getLSN(key.data(), key.size());
  } else {
    ld_check(header == DataKey::HEADER);
    if (!dd_assert(DataKey::valid(key.data(), key.size()),
                   "Invalid DataKey: %s",
                   hexdump_buf(key.data(), key.size(), 200).c_str())) {
      return Decision::kKeep;
    }
    log_id = DataKey::getLogID(key.data());
    lsn = DataKey::getLSN(key.data());
  }

  getTrimInfo(log_id);
  Decision decision = Decision::kKeep;

  if (cache.trim_point.hasValue() && cache.trim_point.value() >= lsn) {
    // This record/index-entry is behind trim point. Drop it.
    decision = filterOutAndMaybeSkip(log_id, key, skip_until);

    // Update stats.
    if (header == DataKey::HEADER) {
      ++count_removed_;
      if (MetaDataLog::isMetaDataLog(log_id)) {
        ++metadata_log_records_removed_;
      }
      updateRecordAge(log_id, lsn, value);
    }
  } else if (header == DataKey::HEADER) {
    // This record is ahead of trim point, but maybe it's behind cutoff
    // timestamp. If so, advance trim point. Can't remove the record in this
    // compaction because our trim point update will only be persisted after
    // this compaction finishes.
    advanceTrimPointIfNeeded(log_id, lsn, value);
  }

  return decision;
}

RocksDBCompactionFilter::Decision
RocksDBCompactionFilter::filterOutAndMaybeSkip(logid_t log_id,
                                               const rocksdb::Slice& key,
                                               std::string* skip_until) {
  using namespace facebook::logdevice::RocksDBKeyFormat;

  if (!force_no_skips_) {
    // Tell rocksdb to drop this key-value, and also to drop everything up to
    // key skip_until. We'll fill skip_until with some lower bound estimate
    // of the next non-trimmed key of the same kind.
    bool skip_to_end = false;
    logid_t next_log = log_id;
    char header = key.data()[0];

    // If we have a whitelist of logs, skip to the next whitelisted log.
    if (context_ && context_->logs_to_keep.hasValue()) {
      auto it = std::lower_bound(context_->logs_to_keep->begin(),
                                 context_->logs_to_keep->end(),
                                 log_id);
      if (it == context_->logs_to_keep->end()) {
        skip_to_end = true;
        // Could as well use LOGID_MAX_INTERNAL: if we have a whitelist,
        // there should be no metadata or internal logs in this CF.
        // Use LOGID_MAX just in case.
        next_log = logid_t(std::max(log_id.val_, USER_LOGID_MAX.val_ + 1));
      } else {
        // Can be the current log. Then we'll just skip to trim point.
        next_log = *it;
        ld_check(next_log >= log_id);
      }
    }

    lsn_t next_lsn;
    if (skip_to_end && next_log != log_id) {
      // Avoid calling getTrimInfo() for a likely nonexistent log.
      next_lsn = 0;
    } else {
      // Get trim point of the log we're skipping to.
      getTrimInfo(next_log);

      // next_lsn = trim_point + 1, but avoid overflow.
      next_lsn = std::min(std::numeric_limits<lsn_t>::max() - 1,
                          cache.trim_point.value_or(0)) +
          1;
    }

    ld_check(skip_until);

    if (header == CopySetIndexKey::HEADER) {
      const CopySetIndexKey k(
          next_log, next_lsn, CopySetIndexKey::SEEK_ENTRY_TYPE);
      skip_until->assign(reinterpret_cast<const char*>(&k), sizeof(k));
    } else if (header == IndexKey::HEADER) {
      if (next_log == log_id) {
        // Can't seek to an LSN in index, can only seek to a custom key,
        // and we don't know the custom key corresponding to trim point ...
        return Decision::kRemove;
      } else {
        // ... but we can still skip whole logs.
        auto k = IndexKey::create(next_log, 0, "", 0);
        skip_until->assign(k.data(), k.size());
      }
    } else {
      ld_check(header == DataKey::HEADER);
      DataKey k(next_log, next_lsn);
      rocksdb::Slice key_slice = k.sliceForForwardSeek();
      skip_until->assign(key_slice.data(), key_slice.size());
    }

    ld_check(key.compare(*skip_until) < 0);
    ld_check((*skip_until)[0] == key[0]);

    return Decision::kRemoveAndSkipUntil;
  } else {
    return Decision::kRemove;
  }
}

// Trimming per-epoch log metadata
//
// Per-epoch log metadata accumulate as the epoch grows. The unbounded growth
// will cause metadata record space being not compact, and eventually make the
// disk run out-of-space. It is necessary to trim them as data log trims.
//
// Currently there are two concerns for trimming the per-epoch log metadata:
// 1) byte offset requires reading the epoch recovery metadata for epoch `e`-1
//    in order to compute the byte offset for a lsn `l` in epoch `e`. This is
//    because EpochRecoveryMetadata for `e`-1 has the accumulative bytes
//    from the beginning of the log to the end of epoch `e`-1. The byte offset
//    is then computed by combining this value with the in-epoch offset of `l`
//    in epoch `e`. On the other hand, if epoch `e` is completely trimmed,
//    (i.e., trim point moved to an epoch >= `e`+1), it is OK to report the
//    byte offset of `l` as 0 w/o reading EpochRecoveryMetadata.
//
// 2) for epochs that are confirmed empty during epoch recovery, storage nodes
//    in the node set do _not_ store EpochRecoveryMetadata for this epoch as an
//    optimization.
//
// Considering 1) and 2), for any log `L`, auto trimming needs to keep at
// least _one_ record of per-epoch metadata whose epoch is strictly _less_ than
// the current trim point of `L`.
RocksDBCompactionFilter::Decision
RocksDBCompactionFilter::makeDecisionPerEpochLogMetadata(logid_t log_id,
                                                         epoch_t epoch) {
  // get the trim point of the data log
  getTrimInfo(log_id);
  auto metadata_trim_point = cache.per_epoch_log_metadata_trim_point;

  RocksDBCompactionFilter::Decision decision;
  if (metadata_trim_point.hasValue() && epoch <= metadata_trim_point.value()) {
    ++per_epoch_log_metadata_removed_;
    decision = Decision::kRemove;
  } else {
    // we are going to keep the record, and update the next trim point
    // if needed; On the other hand, if a record is being removed, it
    // can never update the next metadata trim point so it is safe to skip
    // them.
    decision = Decision::kKeep;

    if (cache.trim_point.hasValue()) {
      const epoch_t data_trim_epoch = lsn_to_epoch(cache.trim_point.value());
      if (epoch < data_trim_epoch && epoch != EPOCH_INVALID) {
        // metadata trim point can be safely moved to epoch - 1,
        // make sure it is updated in the update map
        const epoch_t next_metadata_trim_point = epoch_t(epoch.val_ - 1);
        epoch_t& value_in_map = next_metadata_trim_points_[log_id];
        value_in_map = std::max(value_in_map, next_metadata_trim_point);
      }
    }
  }

  return decision;
}

void RocksDBCompactionFilter::advanceTrimPointIfNeeded(
    logid_t log_id,
    lsn_t lsn,
    const rocksdb::Slice& record) {
  if (!cache.cutoff_timestamp.hasValue()) {
    return;
  }

  std::chrono::milliseconds timestamp;

  int rv = LocalLogStoreRecordFormat::parseTimestamp(
      Slice(reinterpret_cast<const void*>(record.data()), record.size()),
      &timestamp);

  if (rv != 0) {
    RATELIMIT_ERROR(std::chrono::seconds(1),
                    1,
                    "Unable to parse record for log %lu, lsn %lu: %s",
                    log_id.val_,
                    lsn,
                    error_description(err));
    return;
  }

  // Compute the next trim point for the next compaction, based on timestamp
  // and cutoff timestamp which we computed earlier
  if (timestamp < cache.cutoff_timestamp.value()) {
    // update next_trim_points map if needed
    if (!cache.trim_point.hasValue() || cache.trim_point.value() < lsn) {
      auto result = next_trim_points_.insert(std::make_pair(log_id, lsn));
      if (!result.second) {
        // key already in the map, check if value needs updating
        if (lsn > result.first->second) {
          result.first->second = lsn;
        }
      }
    }
  }
}

RocksDBCompactionFilter::Decision
RocksDBCompactionFilter::filterMergeImpl(const rocksdb::Slice& key,
                                         const rocksdb::Slice& operand,
                                         std::string* skip_until) {
  rocksdb::Slice value = operand;

  // DataKey merge operands are prefixed with a header byte. It's not really
  // used for anything, and other mergable key types don't do that.
  // Remove this byte and carry on, the rest is in the same format
  // as non-Merge writes which filterImpl() can handle.
  if (!key.empty() && key[0] == RocksDBKeyFormat::DataKey::HEADER &&
      !operand.empty() &&
      operand[0] == RocksDBWriterMergeOperator::DATA_MERGE_HEADER) {
    value.remove_prefix(1);
  }

  return filterImpl(key, value, skip_until);
}

void RocksDBCompactionFilter::updateRecordAge(logid_t log_id,
                                              lsn_t lsn,
                                              const rocksdb::Slice& value) {
  std::chrono::milliseconds timestamp;
  int rv = LocalLogStoreRecordFormat::parseTimestamp(
      Slice(reinterpret_cast<const void*>(value.data()), value.size()),
      &timestamp);
  if (rv != 0) {
    RATELIMIT_ERROR(std::chrono::seconds(1),
                    1,
                    "Unable to parse record for log %lu, lsn %lu: %s",
                    log_id.val_,
                    lsn,
                    error_description(err));
  } else {
    int64_t age = std::chrono::duration_cast<std::chrono::seconds>(
                      currentTime() - timestamp)
                      .count();
    PER_SHARD_HISTOGRAM_ADD(storage_thread_pool_->stats(),
                            trimmed_record_age,
                            storage_thread_pool_->getShardIdx(),
                            age);
  }
}

std::chrono::milliseconds RocksDBCompactionFilter::currentTime() {
  return std::chrono::duration_cast<std::chrono::milliseconds>(
      std::chrono::system_clock::now().time_since_epoch());
}

void RocksDBCompactionFilter::getTrimInfo(logid_t log_id) {
  if (log_id == cache.log_id) {
    // Cache hit
    return;
  }

  // initially empty
  folly::Optional<lsn_t> trim_point;
  folly::Optional<std::chrono::milliseconds> cutoff_timestamp;
  folly::Optional<epoch_t> per_epoch_log_metadata_trim_point;

  // Cache miss, query LogStorageStateMap (lives in the storage node's
  // Processor)

  ServerProcessor* processor = &storage_thread_pool_->getProcessor();

  std::shared_ptr<Configuration> current_config = processor->config_->get();

  ld_spew("querying Processor for trim point/cutoff_timestamp of log %lu",
          log_id.val_);

  LogStorageState* log_state = processor->getLogStorageStateMap().find(
      log_id, storage_thread_pool_->getShardIdx());
  if (log_state != nullptr) {
    trim_point = log_state->getTrimPoint();
    per_epoch_log_metadata_trim_point =
        log_state->getPerEpochLogMetadataTrimPoint();
  }

  if (!trim_point.hasValue()) {
    noteLogSkipped(log_id);
  }

  if (!current_config->logsConfig()->isFullyLoaded()) {
    // LogsConfig is not fully loaded, avoid changing the trim point until we
    // have a full config.
    return;
  }

  // TODO: right now, for non-partitioned stores, if a log is removed,
  //       its data will never be deleted.
  const std::shared_ptr<LogsConfig::LogGroupNode> log =
      current_config->getLogGroupByIDShared(log_id);
  folly::Optional<std::chrono::seconds> backlog =
      log ? log->attrs().backlogDuration().value() : folly::none;
  // Don't trim metadata logs based on time.
  if (backlog.hasValue() && !MetaDataLog::isMetaDataLog(log_id)) {
    // compute the cutoff timestamp based on the current local time
    std::chrono::milliseconds now = currentTime();

    cutoff_timestamp = now -
        std::chrono::duration_cast<std::chrono::milliseconds>(backlog.value());
  }

  // Store the result in the cache.  Note that we also cache a negative
  // response (trim point not found).  If the trim point is updated while we
  // are still compacting records for the same log, the change will not be
  // reflected until the next compaction run.
  cache.log_id = log_id;
  cache.trim_point = trim_point;
  cache.cutoff_timestamp = cutoff_timestamp;
  cache.per_epoch_log_metadata_trim_point = per_epoch_log_metadata_trim_point;
}

void RocksDBCompactionFilter::noteLogSkipped(logid_t log_id) {
  logs_skipped_.push_back(log_id);
  if (logs_skipped_.size() == 128) {
    flushSkippedLogs();
  }
}

// helper request used to call LogStorageStateMap::recoverLogState() on a
// worker thread
struct LogStoreRecoveryTask : public Request {
 public:
  explicit LogStoreRecoveryTask(std::vector<logid_t>&& logs,
                                shard_index_t shard_idx)
      : Request(RequestType::LOG_STORE_RECOVERY_TASK),
        logs_(std::move(logs)),
        shard_idx_(shard_idx) {}

  Request::Execution execute() override {
    auto& map =
        ServerWorker::onThisThread()->processor_->getLogStorageStateMap();
    for (logid_t log_id : logs_) {
      map.recoverLogState(
          log_id, shard_idx_, LogStorageState::RecoverContext::ROCKSDB_CF);
    }

    return Execution::COMPLETE;
  }

 private:
  std::vector<logid_t> logs_;
  shard_index_t shard_idx_;
};

void RocksDBCompactionFilter::flushSkippedLogs() {
  if (logs_skipped_.empty()) {
    return;
  }

  ServerProcessor* processor = &storage_thread_pool_->getProcessor();

  ld_debug("looking for trim points of %zu skipped logs", logs_skipped_.size());

  // initiate search for trim points of skipped logs
  std::unique_ptr<Request> recovery_request =
      std::make_unique<LogStoreRecoveryTask>(
          std::move(logs_skipped_), storage_thread_pool_->getShardIdx());
  processor->postRequest(recovery_request);
  ld_check(logs_skipped_.size() == 0);
}

// helper task used to update trim points in LogStorageStateMap
// and LocalLogStore metadata. We defer these updates to a StorageTask
// because it is unsafe to perform write operation in the compaction
// filter.
class UpdateTrimPointsTask : public StorageTask {
 public:
  explicit UpdateTrimPointsTask(
      TrimPointUpdateMap&& trim_points,
      PerEpochLogMetadataTrimPointUpdateMap&& metadata_trim_points)
      : StorageTask(StorageTask::Type::UPDATE_TRIM_POINTS),
        trim_points_(std::move(trim_points)),
        metadata_trim_points_(std::move(metadata_trim_points)) {}

  Principal getPrincipal() const override {
    return Principal::METADATA;
  }

  void execute() override {
    // Update the trim points computed by this compaction.
    // Ignore errors - we'll try again during the next compaction.

    if (!trim_points_.empty()) {
      LocalLogStoreUtils::updateTrimPoints(
          trim_points_,
          &storageThreadPool_->getProcessor(),
          storageThreadPool_->getLocalLogStore(),
          false, // needsSync() returns true to sync asynchronously
          storageThreadPool_->stats());
    }

    if (!metadata_trim_points_.empty()) {
      LocalLogStoreUtils::updatePerEpochLogMetadataTrimPoints(
          storageThreadPool_->getShardIdx(),
          metadata_trim_points_,
          &storageThreadPool_->getProcessor());
    }
  }

  void onDone() override {}

  void onDropped() override {}

  Durability durability() const override {
    return Durability::SYNC_WRITE;
  }

 private:
  TrimPointUpdateMap trim_points_;
  PerEpochLogMetadataTrimPointUpdateMap metadata_trim_points_;
};

// Processor request to send UpdateTrimPointsTask to LocalStorage
class UpdateTrimPointsRequest : public Request {
 public:
  UpdateTrimPointsRequest(
      shard_index_t shard_idx,
      TrimPointUpdateMap&& trim_points,
      PerEpochLogMetadataTrimPointUpdateMap&& metadata_trim_points)
      : Request(RequestType::UPDATE_TRIM_POINT),
        shard_idx_(shard_idx),
        trim_points_(std::move(trim_points)),
        metadata_trim_points_(std::move(metadata_trim_points)) {
    ld_check(shard_idx_ >= 0);
  }

  Request::Execution execute() override {
    auto task = std::make_unique<UpdateTrimPointsTask>(
        std::move(trim_points_), std::move(metadata_trim_points_));

    ServerWorker::onThisThread()
        ->getStorageTaskQueueForShard(shard_idx_)
        ->putTask(std::move(task));

    return Execution::COMPLETE;
  }

 private:
  const shard_index_t shard_idx_;
  TrimPointUpdateMap trim_points_;
  PerEpochLogMetadataTrimPointUpdateMap metadata_trim_points_;
};

void RocksDBCompactionFilter::flushUpdatedTrimPoints() {
  Processor* processor = &storage_thread_pool_->getProcessor();
  int rv;

  if (next_trim_points_.empty() && next_metadata_trim_points_.empty()) {
    // Return if no trim point to update
    return;
  }

  // It is not safe to perform writes in the compaction filter, instead,
  // send a request to eventually let a storage thread execute the operation.
  std::unique_ptr<Request> request = std::make_unique<UpdateTrimPointsRequest>(
      storage_thread_pool_->getShardIdx(),
      std::move(next_trim_points_),
      std::move(next_metadata_trim_points_));

  // we can tolerate the failure here since updated trim points will still be
  // calculated on the next compaction
  processor->postRequest(request);
}

RocksDBCompactionFilter::~RocksDBCompactionFilter() {
  Processor* processor = &storage_thread_pool_->getProcessor();
  STAT_ADD(processor->stats_, records_trimmed_removed, count_removed_);
  STAT_ADD(processor->stats_,
           metadata_log_records_trimmed_removed,
           metadata_log_records_removed_);
  STAT_ADD(processor->stats_,
           per_epoch_log_metadata_trimmed_removed,
           per_epoch_log_metadata_removed_);
  flushSkippedLogs();
  flushUpdatedTrimPoints();
}

}} // namespace facebook::logdevice
