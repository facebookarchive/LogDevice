/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/server/LogRebuilding.h"

#include <folly/Optional.h>

#include "logdevice/common/AdminCommandTable.h"
#include "logdevice/common/EpochMetaData.h"
#include "logdevice/common/FailureDomainNodeSet.h"
#include "logdevice/common/RebuildingEventsTracer.h"
#include "logdevice/common/Timestamp.h"
#include "logdevice/common/configuration/NodeLocation.h"
#include "logdevice/common/debug.h"
#include "logdevice/common/stats/PerShardHistograms.h"
#include "logdevice/server/LogRebuildingCheckpoint.h"
#include "logdevice/server/ServerProcessor.h"
#include "logdevice/server/ServerWorker.h"
#include "logdevice/server/read_path/LogStorageStateMap.h"
#include "logdevice/server/rebuilding/ShardRebuildingV1.h"
#include "logdevice/server/storage_tasks/PerWorkerStorageTaskQueue.h"
#include "logdevice/server/storage_tasks/ShardedStorageThreadPool.h"

namespace facebook { namespace logdevice {

std::atomic<log_rebuilding_id_t::raw_type> LogRebuilding::next_id(1);

namespace {

class ReadingCallback : public LocalLogStoreReader::Callback {
 public:
  int processRecord(const RawRecord& record) override {
    void* blob_copy = malloc(record.blob.size);
    if (blob_copy == nullptr) {
      throw std::bad_alloc();
    }
    memcpy(blob_copy, record.blob.data, record.blob.size);
    totalBytes += record.blob.size;

    buffer.emplace_back(record.lsn,
                        Slice(blob_copy, record.blob.size),
                        true // owned, we malloc-d the memory
    );
    return 0;
  }

  std::vector<RawRecord> buffer;
  size_t totalBytes{0};
};
} // namespace

// Helper to get LogRebuilding from the worker map
static LogRebuilding* getLogRebuilding(logid_t logid, shard_index_t shard) {
  auto log = Worker::onThisThread()->runningLogRebuildings().find(logid, shard);
  if (!log) {
    return nullptr;
  }
  return checked_downcast<LogRebuilding*>(log);
}

LogRebuilding::LogRebuilding(
    ShardRebuildingV1::WeakRef owner,
    logid_t logid,
    shard_index_t shard,
    UpdateableSettings<RebuildingSettings> rebuilding_settings,
    std::shared_ptr<IteratorCache> iterator_cache)
    : owner_(owner),
      logid_(logid),
      shard_(shard),
      id_(next_id++),
      rebuildingSettings_(rebuilding_settings),
      max_records_in_flight_(rebuildingSettings_->max_records_in_flight),
      creation_time_(std::chrono::steady_clock::now()),
      iteratorCache_(iterator_cache),
      rebuilding_events_tracer_(Worker::onThisThread(false)
                                    ? Worker::onThisThread()->getTraceLogger()
                                    : nullptr) {
  readPointer_.lsn = LSN_INVALID;
}

void LogRebuilding::start(std::shared_ptr<const RebuildingSet> rs,
                          RebuildingPlan plan,
                          RecordTimestamp max_ts,
                          lsn_t version,
                          lsn_t restart_version) {
  ld_check(version >= version_);
  ld_check(restart_version > restartVersion_);
  ld_check(!rs->all_dirty_time_intervals.empty());
  // TODO(T20252219): remove this workaround when the bug described in this task
  // is fixed. `all_dirty_time_intervals` should never be empty.
  if (rs->all_dirty_time_intervals.empty()) {
    RATELIMIT_ERROR(std::chrono::seconds(1),
                    1,
                    "INTERNAL ERROR: all_dirty_time_intervals is empty. "
                    "See TT20252219");
    auto modified_rs = std::make_shared<RebuildingSet>(*rs);
    modified_rs->all_dirty_time_intervals.insert(allRecordTimeInterval());
    rs = modified_rs;
  }
  version_ = version;
  restartVersion_ = restart_version;
  storageTaskInFlight_ = false;
  rebuildingSet_ = rs;
  rebuildingPlan_ = std::move(plan);

  // ShardRebuildingV1 should not create this state machine with an empty plan.
  ld_check(!rebuildingPlan_.epochsToRead.empty());

  curDirtyTimeWindow_ = rebuildingSet_->all_dirty_time_intervals.begin();

  if (curDirtyTimeWindow_->lower() != RecordTimestamp::min()) {
    ld_debug("LogID %ju: Setting seekToTimeStamp %s",
             (uintmax_t)logid_.val(),
             toString(curDirtyTimeWindow_->lower()).c_str());
    seekToTimestamp_ = curDirtyTimeWindow_->lower();
  }

  timer_ = createTimer([this]() { timerCallback(); });
  if (iteratorCache_) {
    iteratorInvalidationTimer_ = createIteratorTimer();
  }

  stallTimer_ = createStallTimer();
  notifyRebuildingCoordinatorTimer_ = createNotifyRebuildingCoordinatorTimer();
  readNewBatchTimer_ = createReadNewBatchTimer();

  buffer_.clear();
  recordDurabilityState_.clear();

  windowEnd_ = max_ts;
  nRecordsReplicated_ = 0;
  nBytesReplicated_ = 0;
  bytesRebuiltSinceCheckpoint_ = 0;
  readCheckpointState_ = ReadCheckpointState::NONE;
  lastCheckpoint_ = LSN_INVALID;
  numRecordRebuildingAmendsInFlight_ = 0;
  numRecordRebuildingStoresInFlight_ = 0;
  numRecordRebuildingPendingStoreDurable_ = 0;
  numRecordRebuildingPendingAmendDurable_ = 0;
  numRecordRebuildingPendingAmend_ = 0;

  startTimestamp_ = SystemTimestamp::now();

  ld_debug("Starting LogRebuilding state machine for log %lu "
           "with rebuilding set: %s, maxTimestamp: (%s:%s), "
           "all_dirty_time_intervals: %s",
           logid_.val_,
           rebuildingSet_->describe().c_str(),
           toString(max_ts).c_str(),
           toString(getMaxTimestamp()).c_str(),
           toString(rebuildingSet_->all_dirty_time_intervals).c_str());

  publishTrace(RebuildingEventsTracer::STARTED);
  // Issue a storage task to read a checkpoint for that log and rebuilding
  // version (if any). If we already ran this rebuilding in the past but crashed
  // or were aborted, we will start reading from the latest checkpoint; ...
  readCheckpointState_ = ReadCheckpointState::READING;
  readCheckpoint();

  const lsn_t start = compose_lsn(
      epoch_t(rebuildingPlan_.epochsToRead.begin()->first.lower()), ESN_MIN);
  readPointer_.lsn = std::max(readPointer_.lsn, start);
}

void LogRebuilding::abort(bool notify_complete) {
  ld_info("Aborting LogRebuilding state machine for log %lu", logid_.val_);
  publishTrace(RebuildingEventsTracer::ABORTED);

  if (notify_complete) {
    notifyComplete();
  }
  deleteThis();
}

void LogRebuilding::onCheckpointRead(Status status,
                                     lsn_t restart_version,
                                     lsn_t version,
                                     lsn_t rebuilt_upto,
                                     lsn_t trim_point) {
  if (restart_version != restartVersion_) {
    // We re-started this LogRebuilding state machine.
    return;
  }

  ld_check(readCheckpointState_ == ReadCheckpointState::READING);

  if (status == E::DROPPED) {
    // We will retry later.
    ld_check(timer_); // may be nullptr in tests but we don't test this path.
    readCheckpointState_ = ReadCheckpointState::NONE;
    timer_->activate();
    return;
  }

  ld_check(lastCheckpoint_ == LSN_INVALID);
  readCheckpointState_ = ReadCheckpointState::DONE;

  if (status == E::LOCAL_LOG_STORE_READ) {
    // There was an error reading the checkpoint, let's restart rebuilding from
    // the beginning...
    ld_error("Error while reading rebuilding checkpoint for log %lu, "
             "rebuilding from the beginning.",
             logid_.val_);
  }

  if (status == E::OK && version > version_) {
    // Apparently a rebuilding with greater version already took place. Consider
    // this rebuilding complete, we are probably here because we are reading a
    // backlog of the event log.
    onComplete(false);
    return;
  }

  if (status == E::OK && version == version_) {
    lastCheckpoint_ = rebuilt_upto;
    // Clear any seek timestamp and allow the next read to go to the end of
    // the dirty time limits.  We process this next read batch and then decide
    // if we can skip records based on the timestamps we've read.
    seekToTimestamp_.clear();
    curDirtyTimeWindow_ = --rebuildingSet_->all_dirty_time_intervals.end();
    ld_debug("LogID %ju: Setting last_checkpoint_ to %s.",
             (uintmax_t)logid_.val(),
             lsn_to_string(lastCheckpoint_).c_str());
  }

  const lsn_t start =
      std::max(lastCheckpoint_ == LSN_MAX ? LSN_MAX : lastCheckpoint_ + 1,
               trim_point == LSN_MAX ? LSN_MAX : trim_point + 1);

  // readPointer_.lsn may be higher if we already read the metadata log and
  // determined we can start reading from a higher epoch. This may happen if
  // some epochs (and the corresponding metadata log entries) were trimmed
  // between the moment the checkpoint we just read was written and the moment
  // we read the metadata log.
  readPointer_.lsn = std::max(readPointer_.lsn, start);

  // Start reading now.
  startReading();
}

void LogRebuilding::startReading() {
  ld_check(readCheckpointState_ == ReadCheckpointState::DONE);

  const epoch_t first_epoch_to_read = lsn_to_epoch(readPointer_.lsn);

  // If we did not read any checkpoint, first_epoch_to_read should already match
  // the first epoch in rebuildingPlan_.epochsToRead. However, if we did read a
  // checkpoint, first_epoch_to_read may point to a greater epoch range. That's
  // why here we initialize curEpochRange_ and advance it to the first epoch
  // range that includes it.
  curEpochRange_ = rebuildingPlan_.epochsToRead.begin();
  while (curEpochRange_ != rebuildingPlan_.epochsToRead.end() &&
         curEpochRange_->first.upper() - 1 < first_epoch_to_read.val_) {
    ++curEpochRange_;
  }

  if (curEpochRange_ == rebuildingPlan_.epochsToRead.end()) {
    // There was a checkpoint to a LSN past all the epochs we need to read.
    // Complete the state machine immediately.
    onComplete(false);
    return;
  }

  cur_replication_scheme_ = createReplicationScheme(
      *curEpochRange_->second, rebuildingPlan_.sequencerNodeID);
  readNewBatch();
}

void LogRebuilding::window(RecordTimestamp window_end) {
  // We should have an empty buffer and not be processing anything at this
  // point.
  ld_check(buffer_.empty());
  ld_check(windowEnd_ <= window_end);

  cur_window_total_size_ = 0;
  cur_window_num_batches_ = 0;

  windowEnd_ = window_end;
  if (!storageTaskInFlight_) {
    readNewBatch();
  }
}

void LogRebuilding::aggregateFilterStats() {
  if (filter_) {
    STAT_ADD(getStats(),
             read_streams_num_records_late_filtered_rebuilding,
             filter_->nRecordsLateFiltered);
    nRecordsLateFiltered_ += filter_->nRecordsLateFiltered;
    nRecordsSCDFiltered_ += filter_->nRecordsSCDFiltered;
    nRecordsNotDirtyFiltered_ += filter_->nRecordsNotDirtyFiltered;
    nRecordsDrainedFiltered_ += filter_->nRecordsDrainedFiltered;
    nRecordsTimestampFiltered_ += filter_->nRecordsTimestampFiltered;
    filter_->nRecordsLateFiltered = 0;
    filter_->nRecordsSCDFiltered = 0;
    filter_->nRecordsNotDirtyFiltered = 0;
    filter_->nRecordsDrainedFiltered = 0;
    filter_->nRecordsTimestampFiltered = 0;
  }
}

std::shared_ptr<LocalLogStore::ReadIterator>
LogRebuilding::getIterator(const LocalLogStore::ReadOptions& options) {
  if (iteratorCache_) {
    return iteratorCache_->createOrGet(options);
  }
  auto uptr = ServerWorker::onThisThread()
                  ->processor_->sharded_storage_thread_pool_->getByIndex(shard_)
                  .getLocalLogStore()
                  .read(logid_, options);
  return std::shared_ptr<LocalLogStore::ReadIterator>(uptr.release());
}

void LogRebuilding::readNewBatch() {
  batch_read_start_time_ = std::chrono::steady_clock::now();
  ld_check(!storageTaskInFlight_);

  size_t checkpointThreshold =
      1024 * 1024 * rebuildingSettings_->checkpoint_interval_mb;

  // First, write a checkpoint if needed.
  if (bytesRebuiltSinceCheckpoint_ >= checkpointThreshold) {
    bytesRebuiltSinceCheckpoint_ = 0;
    writeCheckpoint();
  }

  if (getTotalLogRebuildingSize() >= getLogRebuildingSizeThreshold()) {
    // We have hit the memory limit for this log rebuilding state machine.
    // Stall new reads.
    ld_check(!isStallTimerActive());
    ld_check(!nonDurableRecordList_.empty());
    activateStallTimer();
    return;
  }
  if (rebuildingSettings_->test_stall_rebuilding) {
    activateStallTimer();
    return;
  }

  if (!rebuildingSettings_->use_rocksdb_cache ||
      !Worker::settings().allow_reads_on_workers || seekToTimestamp_) {
    storageTaskInFlight_ = true;
    if (seekToTimestamp_) {
      ++nTimeBasedSeeks_;
    }
    putReadStorageTask(createReadContext());

    // Only seek again once we exhaust a dirty time range.
    seekToTimestamp_.clear();
    return;
  }

  LocalLogStore::ReadOptions options("LogRebuilding::readNewBatch", true);
  options.allow_blocking_io = false; // on worker thread, disallow blocking I/O
  options.tailing = true;
  options.fill_cache = rebuildingSettings_->use_rocksdb_cache;
  // Use the copyset index to skip some records
  options.allow_copyset_index = true;

  auto read_iterator = getIterator(options);

  ld_check(read_iterator != nullptr);

  auto read_ctx = createReadContext();

  ReadingCallback callback;

  Status status = LocalLogStoreReader::read(
      *read_iterator, callback, &read_ctx, Worker::stats(), Worker::settings());

  buffer_ = std::move(callback.buffer);
  aggregateFilterStats();
  STAT_ADD(getStats(), rebuilding_records_read_from_cache, buffer_.size());

  ld_spew("Read %lu records from cache for log %lu, status is %s",
          buffer_.size(),
          logid_.val_,
          error_description(status));

  if (status == E::WOULDBLOCK &&
      callback.totalBytes >= read_ctx.max_bytes_to_deliver_) {
    // This should not happen, but let's be resilient against int overflow.
    status = E::PARTIAL;
  }

  if (status == E::WOULDBLOCK) {
    // Let's read the rest of the batch in a storage task.
    ld_check_gt(read_ctx.max_bytes_to_deliver_, callback.totalBytes);
    read_ctx.max_bytes_to_deliver_ -=
        std::min(callback.totalBytes, read_ctx.max_bytes_to_deliver_);
    storageTaskInFlight_ = true;
    ld_check_eq(RecordTimestamp(read_ctx.ts_window_high_),
                getMaxStorageTaskTimestamp());
    putReadStorageTask(read_ctx);
  } else {
    startProcessingBatch(
        status, read_ctx.read_ptr_, RecordTimestamp(read_ctx.ts_window_high_));
    // `this` might be destroyed here.
  }
}

LocalLogStoreReader::ReadContext LogRebuilding::createReadContext() {
  ld_check(!rebuildingSet_->shards.empty());
  auto cfg = getConfig()->get();

  ld_check(cur_replication_scheme_);
  ld_check(cur_replication_scheme_->epoch_metadata == *curEpochRange_->second);

  filter_ = std::make_shared<RebuildingReadFilter>(rebuildingSet_, logid_);
  filter_->scd_my_shard_id_ = getMyShardID();
  filter_->scd_replication_ = cur_replication_scheme_->epoch_metadata
                                  .replication.getReplicationFactor();

  // We set `window_high` to the end of the interval of epochs we are currently
  // reading.
  const lsn_t window_high =
      compose_lsn(epoch_t(curEpochRange_->first.upper() - 1), ESN_MAX);

  // We set `until_lsn` to the last esn in the last epoch in the intervals of
  // epochs we must read.
  const auto last_epoch =
      boost::icl::elements_rbegin(rebuildingPlan_.epochsToRead)->first;
  const lsn_t until_lsn = std::min(
      rebuildingPlan_.untilLSN, compose_lsn(epoch_t(last_epoch), ESN_MAX));

  LocalLogStoreReader::ReadContext read_ctx(
      logid_,
      readPointer_,
      until_lsn,
      window_high,
      getMaxStorageTaskTimestamp().toMilliseconds(),
      LSN_MAX,                              // last_released_lsn
      rebuildingSettings_->max_batch_bytes, // max_record_bytes_queued
      true,                                 // first_record_any_size
      true,                                 // is_rebuilding
      filter_,
      CatchupEventTrigger::OTHER);

  return read_ctx;
}

void LogRebuilding::startProcessingBatch(Status status,
                                         ReadPointer& read_ptr,
                                         RecordTimestamp ts_window_high) {
  batch_process_start_time_ = std::chrono::steady_clock::now();
  ++cur_window_num_batches_;

  ServerWorker* w = ServerWorker::onThisThread(false);
  if (w) {
    PER_SHARD_HISTOGRAM_ADD(Worker::stats(),
                            log_rebuilding_read_batch,
                            shard_,
                            usec_since(batch_read_start_time_));
  }

  readPointer_ = read_ptr;

  // The batch we read, which could be due to resuming from a checkpoint,
  // may be outside of any timestamp range, so we must verify timestamp bounds
  // on any successful read.
  if ((status == E::WINDOW_END_REACHED || status == E::OK) &&
      !boost::icl::contains(*curDirtyTimeWindow_, ts_window_high)) {
    ld_debug("LogID %ju: Ended read at %s which is outside of the current time "
             "range.",
             (uintmax_t)logid_.val(),
             toString(ts_window_high).c_str());
    // Advance to a valid time window. This new window may contain
    // ts_window_high. Only seek if it doesn't.
    curDirtyTimeWindow_ = rebuildingSet_->all_dirty_time_intervals.find(
        RecordTimeInterval(ts_window_high, RecordTimestamp::max()));
    if (curDirtyTimeWindow_ == rebuildingSet_->all_dirty_time_intervals.end()) {
      // No more dirty ranges to process.
      ld_debug(
          "LogID %ju: Exhausted Dirty Time Ranges.", (uintmax_t)logid_.val());
      status = E::UNTIL_LSN_REACHED;
      rebuildingPlan_.untilLSN =
          readPointer_.lsn == LSN_INVALID ? LSN_INVALID : readPointer_.lsn - 1;
    } else if (!boost::icl::contains(*curDirtyTimeWindow_, ts_window_high)) {
      ld_debug("LogID %ju: Ended read at %s which is outside of any time "
               "ranges. Seeking to %s.",
               (uintmax_t)logid_.val(),
               toString(ts_window_high).c_str(),
               toString(curDirtyTimeWindow_->lower()).c_str());
      ts_window_high = curDirtyTimeWindow_->lower();
      seekToTimestamp_ = curDirtyTimeWindow_->lower();
      if (ts_window_high <= getMaxTimestamp()) {
        // We haven't reached the end of the local window. Indicate we should
        // read again.
        status = E::BYTE_LIMIT_REACHED;
      }
    } else if (ts_window_high <= getMaxTimestamp()) {
      ld_debug("LogID %ju: Ended read at %s which is inside a new time range. "
               "No seek required.",
               (uintmax_t)logid_.val(),
               toString(ts_window_high).c_str());
      // We haven't reached the end of the local window. Indicate we should
      // read again.
      status = E::BYTE_LIMIT_REACHED;
    }
  }

  if (status == E::WINDOW_END_REACHED) {
    if (ts_window_high > getMaxTimestamp()) {
      ld_debug("LogID %ju: WINDOW_END_REACHED at %s.",
               (uintmax_t)logid_.val(),
               toString(ts_window_high).c_str());
      // We reached the end of the timestamp window.
      nextTimestamp_.storeMax(ts_window_high);
    }
  }

  lastBatchStatus_ = status;
  curPosition_ = buffer_.begin();

  if (buffer_.empty()) {
    onBatchEnd();
    return;
    // `this` might be deleted here if we reached the end of the log.
  } else {
    startRecordRebuildingStores(rebuildingSettings_->max_records_in_flight);
  }
}

size_t LogRebuilding::getMaxBlockSize() {
  return Worker::settings().sticky_copysets_block_size;
}

size_t LogRebuilding::getLogRebuildingSizeThreshold() {
  return 1024 * 1024 * rebuildingSettings_->max_log_rebuilding_size_mb;
}

size_t LogRebuilding::getTotalLogRebuildingSize() {
  return (numRecordRebuildingAmendsInFlight_ *
          (sizeof(RecordRebuildingAmend) + sizeof(RecordRebuildingAmendState) +
           sizeof(RecordDurabilityState))) +
      (numRecordRebuildingStoresInFlight_ *
       (sizeof(RecordRebuildingStore) + sizeof(RecordDurabilityState))) +
      (sizeof(RecordDurabilityState) *
       (numRecordRebuildingPendingStoreDurable_ +
        numRecordRebuildingPendingAmendDurable_ +
        numRecordRebuildingPendingAmend_));
}

bool LogRebuilding::shouldCancelStallTimer() {
  if (!isStallTimerActive()) {
    return false;
  }
  return isLogRebuildingMakingProgress();
}

bool LogRebuilding::shouldNotifyRebuildingCoordinator() {
  return nonDurableRecordList_.empty() && !isStallTimerActive() &&
      (lastBatchStatus_ == E::WINDOW_END_REACHED ||
       lastBatchStatus_ == E::UNTIL_LSN_REACHED ||
       lastBatchStatus_ == E::CAUGHT_UP);
}

bool LogRebuilding::isLogRebuildingMakingProgress() {
  auto progressSizeThreshold =
      (1024 * 1024 * rebuildingSettings_->max_log_rebuilding_size_mb) / 2;
  return (nonDurableRecordList_.empty() ||
          getTotalLogRebuildingSize() < progressSizeThreshold);
}

int LogRebuilding::checkRecordForBlockChange(const RawRecord& rec) {
  copyset_size_t new_copyset_size;
  Payload payload;
  last_seen_copyset_candidate_buf_.resize(COPYSET_SIZE_MAX);
  int rv =
      LocalLogStoreRecordFormat::parse(rec.blob,
                                       nullptr,
                                       nullptr,
                                       nullptr,
                                       nullptr,
                                       &new_copyset_size,
                                       last_seen_copyset_candidate_buf_.data(),
                                       COPYSET_SIZE_MAX,
                                       nullptr,
                                       nullptr,
                                       &payload,
                                       getMyShardID().shard());
  if (rv != 0) {
    RATELIMIT_ERROR(std::chrono::seconds(1),
                    1,
                    "Cannot parse record at lsn %s of log %lu.",
                    lsn_to_string(rec.lsn).c_str(),
                    logid_.val_);
    ld_check(err == E::MALFORMED_RECORD);
    return -1;
  }
  last_seen_copyset_candidate_buf_.resize(new_copyset_size);
  bool copyset_changed =
      (last_seen_copyset_ != last_seen_copyset_candidate_buf_);
  bool byte_limit_exceeded = bytes_in_current_block_ > getMaxBlockSize();
  if (copyset_changed || byte_limit_exceeded) {
    // end of the block reached, bump block counter and save the new copyset
    ++current_block_id_;
    bytes_in_current_block_ = 0;
    std::swap(last_seen_copyset_, last_seen_copyset_candidate_buf_);
  }
  bytes_in_current_block_ += payload.size();
  return 0;
}

void LogRebuilding::startRecordRebuildingStores(
    int numRecordRebuildingToStart) {
  while (numRecordRebuildingToStart > 0 && curPosition_ != buffer_.end()) {
    ld_check((*curPosition_).blob.data);
    int rv = checkRecordForBlockChange(*curPosition_);
    if (rv != 0) {
      ld_check_eq(err, E::MALFORMED_RECORD);
      if (num_malformed_records_seen_ >=
          rebuildingSettings_->max_malformed_records_to_tolerate) {
        // Suspiciously many records are malformed. Escalate and stall.
        RATELIMIT_CRITICAL(
            std::chrono::seconds(10),
            10,
            "Rebuilding saw too many (%lu) malformed records for log %lu. "
            "Stopping rebuilding just in case. Please investigate.",
            num_malformed_records_seen_,
            logid_.val());
        return;
      }
      WORKER_STAT_INCR(rebuilding_malformed_records);
      ++num_malformed_records_seen_;

      // Don't create RecordRebuilding for bad records.
      curPosition_++;
      continue;
    }

    auto r = createRecordRebuildingStore(
        current_block_id_, std::move(*curPosition_), cur_replication_scheme_);

    {
      const epoch_t epoch = lsn_to_epoch(r->getRecord().lsn);
      const epoch_t expected_lo = epoch_t(curEpochRange_->first.lower());
      const epoch_t expected_hi = epoch_t(curEpochRange_->first.upper() - 1);
      ld_check(epoch >= expected_lo && epoch <= expected_hi);
    }

    auto lsn = r->getRecord().lsn;
    const bool ro =
        rebuildingSettings_->read_only == RebuildingReadOnlyOption::ON_DONOR;
    auto rds = std::make_unique<RecordDurabilityState>(lsn);
    rds->size = r->getRecord().blob.size;
    rds->rr = std::move(r);
    if (!recordDurabilityState_.count(lsn)) {
      nonDurableRecordList_.push_back(*(rds.get()));
      recordDurabilityState_.insert(std::make_pair(lsn, std::move(rds)));
      recordDurabilityState_[lsn]->rr->start(ro);
      ++numRecordRebuildingStoresInFlight_;
      --numRecordRebuildingToStart;
    } else {
      RATELIMIT_WARNING(
          std::chrono::seconds(10),
          2,
          "Rebuilding of lsn %s of log %lu is already in progress."
          "Possible duplicate delivered from local log store. Skipping",
          lsn_to_string(lsn).c_str(),
          logid_.val_);
      rds.reset();
    }
    curPosition_++;
  }

  if (curPosition_ == buffer_.end() &&
      numRecordRebuildingStoresInFlight_ == 0) {
    onBatchEnd();
    // `this` might be deleted here
    return;
  }

  restartLsn_ = nonDurableRecordList_.front().lsn;
  aggregateFilterStats();
}

void LogRebuilding::writeCheckpoint() {
  if (rebuildingPlan_.epochsToRead.empty()) {
    // Space optimization: do not store checkpoints for logs for which the
    // historical nodeset does not contain a nodeset of interest.  Storing a
    // checkpoint would help discard the log faster in case this donor node
    // crashes later, however by doing this we avoid storing one checkpoint per
    // log which is not advised if we want to scale the maximum number of logs
    // we can have in a tier.
    return;
  }

  const lsn_t next_lsn =
      restartLsn_ == LSN_INVALID ? readPointer_.lsn : restartLsn_;
  if (next_lsn == LSN_OLDEST) {
    // There is no point writting a checkpoint.
    return;
  }

  const lsn_t rebuilt_upto = next_lsn == LSN_MAX ? LSN_MAX : next_lsn - 1;

  ld_check(rebuilt_upto >= lastCheckpoint_);
  if (lastCheckpoint_ < rebuilt_upto) {
    writeCheckpoint(rebuilt_upto);
    lastCheckpoint_ = rebuilt_upto;
  }
}

void LogRebuilding::writeCheckpoint(lsn_t rebuilt_upto) {
  auto task = std::make_unique<WriteLogRebuildingCheckpointTask>(
      logid_, version_, rebuilt_upto);
  auto task_queue =
      ServerWorker::onThisThread()->getStorageTaskQueueForShard(shard_);
  task_queue->putTask(std::move(task));
}

void LogRebuilding::onAllStoresReceived(
    lsn_t lsn,
    std::unique_ptr<FlushTokenMap> flushTokenMap) {
  ld_check(numRecordRebuildingStoresInFlight_ > 0);
  numRecordRebuildingStoresInFlight_--;
  numRecordRebuildingPendingStoreDurable_++;

  ld_check(recordDurabilityState_.count(lsn));
  RecordDurabilityState* rds = recordDurabilityState_[lsn].get();

  rds->flushTokenMap = std::move(flushTokenMap);
  rds->rras = rds->rr->getRecordRebuildingAmendState();
  rds->status = RecordDurabilityState::Status::STORES_NON_DURABLE;
  // Destroys RecordRebuildingStore state machine
  rds->rr.reset();
  SteadyTimestamp time_to_store(SteadyTimestamp::now() - rds->start_time_);
  STAT_ADD(getStats(),
           rebuilding_donor_stored_ms,
           time_to_store.toMilliseconds().count());

  const size_t sz = recordDurabilityState_[lsn]->size;
  cur_batch_total_size_ += sz;
  cur_window_total_size_ += sz;

  ld_spew("All stores received for rebuilding of record with lsn:%s"
          "for the log:%lu",
          lsn_to_string(lsn).c_str(),
          logid_.val_);

  registerAndUpdateFlushStatus(rds);

  if (rds->status == RecordDurabilityState::Status::STORES_DURABLE) {
    onAllStoresDurable(lsn);
  }

  startRecordRebuildingStores();
}

void LogRebuilding::onAllStoresDurable(lsn_t lsn) {
  ld_assert(recordDurabilityState_.count(lsn));
  auto rds = recordDurabilityState_[lsn].get();
  ld_check(rds);
  ld_check(rds->status == RecordDurabilityState::Status::STORES_DURABLE);
  ld_check(rds->rras);
  ld_check(numRecordRebuildingPendingStoreDurable_ > 0);
  numRecordRebuildingPendingStoreDurable_--;
  numRecordRebuildingPendingAmend_++;
  ld_spew("All stores durable for rebuilding of record with lsn:%s"
          "for the log:%lu",
          lsn_to_string(lsn).c_str(),
          logid_.val_);
  lsnStoresDurable_.push(lsn);
  startRecordRebuildingAmends();
}

void LogRebuilding::onAllAmendsReceived(
    lsn_t lsn,
    std::unique_ptr<FlushTokenMap> flushTokenMap) {
  ld_assert(recordDurabilityState_.count(lsn));
  auto rds = recordDurabilityState_.at(lsn).get();
  ld_check(rds);
  rds->flushTokenMap = std::move(flushTokenMap);
  ld_check(numRecordRebuildingAmendsInFlight_ > 0);
  --numRecordRebuildingAmendsInFlight_;
  ++numRecordRebuildingPendingAmendDurable_;
  // Destroy the RecordRebuildingAmend state machine
  rds->rra.reset();

  registerAndUpdateFlushStatus(rds);
  ld_spew("All amends received for rebuilding of record with lsn:%s"
          "for the log:%lu",
          lsn_to_string(lsn).c_str(),
          logid_.val_);

  if (rds->status == RecordDurabilityState::Status::AMENDS_DURABLE) {
    onAllAmendsDurable(lsn);
  }

  startRecordRebuildingAmends();

  if (shouldCancelStallTimer()) {
    // If stall timer is active and with this flush, the amount of memory used
    // goes below the threshold, unstall and read a new batch. Activating with
    // zero timeout serves two purposes.
    // 1. Reads the new batch in next event loop iteration thus preventing the
    // worker thread from being held up for too long. (A memtable flush could
    // potentially result in numerous lsns being updated)
    // 2. Prevents a corner case where `this` could be destroyed while
    // processing a flushed message
    cancelStallTimer();
    activateReadNewBatchTimer();
  } else if (shouldNotifyRebuildingCoordinator()) {
    // All records processed so far are durable
    ld_check(numRecordRebuildingPendingStoreDurable_ == 0 &&
             numRecordRebuildingPendingAmendDurable_ == 0 &&
             numRecordRebuildingStoresInFlight_ == 0 &&
             numRecordRebuildingAmendsInFlight_ == 0 &&
             numRecordRebuildingPendingAmend_ == 0);
    ld_check(getTotalLogRebuildingSize() == 0);
    activateNotifyRebuildingCoordinatorTimer();
  }
}

void LogRebuilding::startRecordRebuildingAmends() {
  if (processingDurableStores_) {
    // prevent recursive calls
    return;
  }

  size_t max_amends_in_flight = rebuildingSettings_->max_amends_in_flight;

  if (max_amends_in_flight <= numRecordRebuildingAmendsInFlight_) {
    // max_amends_in_flight was probably decreased at runtime
    return;
  }

  SCOPE_EXIT {
    processingDurableStores_ = false;
  };

  processingDurableStores_ = true;

  while (numRecordRebuildingAmendsInFlight_ < max_amends_in_flight &&
         !lsnStoresDurable_.empty()) {
    lsn_t lsn = lsnStoresDurable_.front();
    lsnStoresDurable_.pop();
    ld_assert(recordDurabilityState_.count(lsn));
    auto rds = recordDurabilityState_[lsn].get();
    ld_check(rds);
    ld_check(rds->status == RecordDurabilityState::Status::STORES_DURABLE);
    rds->rra = createRecordRebuildingAmend(*(rds->rras.get()));
    rds->rras.reset();
    ld_check(rds->rra);
    rds->status = RecordDurabilityState::Status::AMENDS_NON_DURABLE;
    SteadyTimestamp time_to_amend(SteadyTimestamp::now() - rds->start_time_);
    STAT_ADD(getStats(),
             rebuilding_donor_amended_ms,
             time_to_amend.toMilliseconds().count());

    ld_check(numRecordRebuildingPendingAmend_ > 0);
    numRecordRebuildingPendingAmend_--;
    numRecordRebuildingAmendsInFlight_++;
    rds->rra->start();
  }
}

void LogRebuilding::onAllAmendsDurable(lsn_t lsn) {
  ld_check(!recordDurabilityState_.empty());
  ld_assert(recordDurabilityState_.at(lsn)->status ==
            RecordDurabilityState::Status::AMENDS_DURABLE);

  ++nRecordsReplicated_;
  ld_check(numRecordRebuildingPendingAmendDurable_ > 0);
  --numRecordRebuildingPendingAmendDurable_;
  size_t sz = recordDurabilityState_.at(lsn)->size;
  nBytesReplicated_ += sz;
  bytesRebuiltSinceCheckpoint_ += sz;
  recordDurabilityState_.erase(lsn);
  ld_spew("All amends durable for rebuilding of record with lsn:%s"
          "for the log:%lu",
          lsn_to_string(lsn).c_str(),
          logid_.val_);

  if (!nonDurableRecordList_.empty()) {
    restartLsn_ = nonDurableRecordList_.front().lsn;
  } else {
    restartLsn_ = readPointer_.lsn;
  }
}

void LogRebuilding::registerAndUpdateFlushStatus(RecordDurabilityState* rds) {
  ld_check(rds != nullptr);
  ld_check(rds->flushTokenMap);
  auto it = rds->flushTokenMap->begin();

  while (it != rds->flushTokenMap->end()) {
    if (!nodeMemtableFlushStatus_.count(it->first)) {
      nodeMemtableFlushStatus_.insert(
          std::make_pair(it->first, NodeMemtableFlushStatus()));
    }

    auto& flushStatus = nodeMemtableFlushStatus_[it->first];
    if (flushStatus.flushedUpto_ >= it->second) {
      // Memtable already flushed.
      it = rds->flushTokenMap->erase(it);
      continue;
    }

    // The max flush token this log rebuilding state machine is interested in
    // for the NodeID,ServerInstanceID pair
    flushStatus.maxFlushToken_ =
        std::max(flushStatus.maxFlushToken_, it->second);

    auto key = std::make_tuple(it->first.first, it->first.second, it->second);
    if (!LsnPendingFlush_.count(key)) {
      LsnPendingFlush_.insert(std::make_pair(key, std::vector<lsn_t>()));
    }

    auto& vec = LsnPendingFlush_[key];
    vec.push_back(rds->lsn);
    ld_spew("lsn:%lu%s waiting for FlushToken:%lu, from node:%d",
            logid_.val_,
            lsn_to_string(rds->lsn).c_str(),
            it->second,
            it->first.first);
    it++;
  }

  updateFlushStatus(rds);
}

void LogRebuilding::updateFlushStatus(RecordDurabilityState* rds) {
  ld_check(rds != nullptr);
  ld_check(rds->flushTokenMap);
  if (rds->flushTokenMap->empty()) {
    SteadyTimestamp time_to_persist(SteadyTimestamp::now() - rds->start_time_);
    switch (rds->status) {
      case RecordDurabilityState::Status::STORES_NON_DURABLE:
        STAT_ADD(getStats(),
                 rebuilding_donor_store_persisted_ms,
                 time_to_persist.toMilliseconds().count());
        rds->status = RecordDurabilityState::Status::STORES_DURABLE;
        break;
      case RecordDurabilityState::Status::AMENDS_NON_DURABLE:
        STAT_ADD(getStats(),
                 rebuilding_donor_amend_persisted_ms,
                 time_to_persist.toMilliseconds().count());
        rds->status = RecordDurabilityState::Status::AMENDS_DURABLE;
        break;
      default:
        ld_check(false);
        ld_error(
            "RecordRebuildingAmend's status invalid:%d", (uint8_t)rds->status);
        break;
    }
  }
}

void LogRebuilding::onMemtableFlushed(node_index_t node_index,
                                      ServerInstanceId server_instance_id,
                                      FlushToken flushedUpto) {
  ServerWorker* w = ServerWorker::onThisThread(false);

  if (w) {
    ld_spew("LogRebuilding for log:%lu received a memtable flush message from "
            "N%d, shardid:%d, server_instance:%lu, flushtoken:%lu",
            logid_.val_,
            node_index,
            shard_,
            server_instance_id,
            flushedUpto);
  }

  auto nodeIndexServerInstanceIdPair =
      std::make_pair(node_index, server_instance_id);

  if (!nodeMemtableFlushStatus_.count(nodeIndexServerInstanceIdPair)) {
    nodeMemtableFlushStatus_.insert(std::make_pair(
        nodeIndexServerInstanceIdPair,
        NodeMemtableFlushStatus(flushedUpto, FlushToken_INVALID)));
    return;
  }

  auto& flushStatus = nodeMemtableFlushStatus_[nodeIndexServerInstanceIdPair];

  if (flushStatus.maxFlushToken_ == FlushToken_INVALID) {
    // There are no outstanding Stores/Amends for this memtable
    // that we know of yet. update and return
    flushStatus.flushedUpto_ = flushedUpto;
    return;
  }

  if (flushStatus.flushedUpto_ > flushedUpto) {
    // We received a notification earlier for a memtable id greater
    // than the current notification. Possible late arriving notification
    // Safe to ignore since previous notification would have ensured
    // all records waiting on a lower memtable id were updated
    return;
  }

  // For all FlushTokens between prev flushedUpto+1 and max FlushToken we
  // are interested in, update the flush status for each record that was
  // written to those memtables.
  FlushToken memtable_id = flushStatus.flushedUpto_ + 1;
  FlushToken end = std::min(flushedUpto, flushStatus.maxFlushToken_);

  while (memtable_id <= end) {
    auto key = std::make_tuple(nodeIndexServerInstanceIdPair.first,
                               nodeIndexServerInstanceIdPair.second,
                               memtable_id);
    if (LsnPendingFlush_.count(key)) {
      auto it = LsnPendingFlush_[key].begin();
      while (it != LsnPendingFlush_[key].end()) {
        ld_assert(recordDurabilityState_.count(*it));
        auto rds = recordDurabilityState_[*it].get();
        rds->flushTokenMap->erase(nodeIndexServerInstanceIdPair);
        updateFlushStatus(rds);
        if (rds->status == RecordDurabilityState::Status::STORES_DURABLE) {
          onAllStoresDurable(*it);
        } else if (rds->status ==
                   RecordDurabilityState::Status::AMENDS_DURABLE) {
          onAllAmendsDurable(*it);
        }
        ++it;
      }
      // All lsn waiting on this flush have been updated
      LsnPendingFlush_.erase(key);
    }
    memtable_id++;
  }

  flushStatus.flushedUpto_ = flushedUpto;

  if (shouldCancelStallTimer()) {
    cancelStallTimer();
    activateReadNewBatchTimer();
  } else if (shouldNotifyRebuildingCoordinator()) {
    // All records processed so far are durable
    ld_check(numRecordRebuildingPendingStoreDurable_ == 0 &&
             numRecordRebuildingPendingAmendDurable_ == 0 &&
             numRecordRebuildingStoresInFlight_ == 0 &&
             numRecordRebuildingAmendsInFlight_ == 0 &&
             numRecordRebuildingPendingAmend_ == 0);
    ld_check(getTotalLogRebuildingSize() == 0);
    activateNotifyRebuildingCoordinatorTimer();
  }
}

void LogRebuilding::notifyRebuildingCoordinator() {
  if (!nonDurableRecordList_.empty()) {
    // We must have read something in response to a
    // window move after the timer was activated.
    // Don't sent any notification just yet.
    return;
  }

  // If we already sent a window end notification, send a new size update
  // to rebuilding coordinator.
  if (lastBatchStatus_ == E::WINDOW_END_REACHED) {
    notifyLogRebuildingSize();
    return;
  }

  if (lastBatchStatus_ == E::UNTIL_LSN_REACHED ||
      lastBatchStatus_ == E::CAUGHT_UP) {
    onComplete();
  }
}

void LogRebuilding::onGracefulShutdown(node_index_t node_index,
                                       ServerInstanceId server_instance_id) {
  auto nodeIndexServerInstanceIdPair =
      std::make_pair(node_index, server_instance_id);

  if (!nodeMemtableFlushStatus_.count(nodeIndexServerInstanceIdPair) ||
      nodeMemtableFlushStatus_[nodeIndexServerInstanceIdPair].maxFlushToken_ ==
          FlushToken_INVALID) {
    // We are not interested in this node's flush status.
    return;
  }

  // Since this is a graceful shutdown, assume all outstanding memtables
  // are flushed for this node.
  onMemtableFlushed(
      node_index,
      server_instance_id,
      nodeMemtableFlushStatus_[nodeIndexServerInstanceIdPair].maxFlushToken_);
}

void LogRebuilding::addCurWindowTotalSizeToHistogram() {
  ServerWorker* w = ServerWorker::onThisThread(false);
  if (w) {
    PER_SHARD_HISTOGRAM_ADD(Worker::stats(),
                            log_rebuilding_local_window_size,
                            shard_,
                            cur_window_total_size_);
    PER_SHARD_HISTOGRAM_ADD(Worker::stats(),
                            log_rebuilding_num_batches_per_window,
                            shard_,
                            cur_window_num_batches_);
  }
}

void LogRebuilding::addCurBatchTotalSizeToHistogram() {
  ServerWorker* w = ServerWorker::onThisThread(false);
  if (w) {
    PER_SHARD_HISTOGRAM_ADD(Worker::stats(),
                            log_rebuilding_process_batch,
                            shard_,
                            usec_since(batch_process_start_time_));
    PER_SHARD_HISTOGRAM_ADD(Worker::stats(),
                            log_rebuilding_batch_size,
                            shard_,
                            cur_batch_total_size_);
  }
}

void LogRebuilding::publishTrace(const std::string& status) {
  rebuilding_events_tracer_.traceLogRebuilding(
      startTimestamp_.time_since_epoch(),
      msec_since(creation_time_),
      logid_,
      *rebuildingSet_,
      version_,
      rebuildingPlan_.untilLSN,
      nBytesReplicated_,
      nRecordsReplicated_,
      numRecordRebuildingStoresInFlight_,
      status);
}

void LogRebuilding::onComplete(bool write_checkpoint) {
  ld_check(!storageTaskInFlight_);
  addCurWindowTotalSizeToHistogram();
  // If all record are durable, checkpoint and notify
  // rebuilding coordinator otherwise, just notify rc
  if (nonDurableRecordList_.empty()) {
    ld_check(numRecordRebuildingStoresInFlight_ == 0 &&
             numRecordRebuildingAmendsInFlight_ == 0 &&
             numRecordRebuildingPendingAmendDurable_ == 0 &&
             numRecordRebuildingPendingStoreDurable_ == 0 &&
             numRecordRebuildingPendingAmend_ == 0);
    // Advance the pointer to the very end so that we write LSN_MAX for the
    // checkpoint.
    readPointer_.lsn = LSN_MAX;
    restartLsn_ = LSN_MAX;
    if (write_checkpoint) {
      writeCheckpoint();
    }
    notifyComplete();
    deleteThis();
  } else {
    notifyReachedUntilLsn();
  }
}

void LogRebuilding::onCopysetInvalid(lsn_t lsn) {
  ld_assert(recordDurabilityState_.count(lsn));
  auto rds = recordDurabilityState_.at(lsn).get();
  ld_check(rds);
  ld_check(numRecordRebuildingAmendsInFlight_ > 0);
  --numRecordRebuildingAmendsInFlight_;
  // Destroy the RecordRebuildingAmend state machine
  rds->rra.reset();

  startRecordRebuildingAmends();

  if (shouldCancelStallTimer()) {
    cancelStallTimer();
    activateReadNewBatchTimer();
  }
}

void LogRebuilding::onReadTaskDone(RebuildingReadStorageTask& task) {
  if (iteratorCache_ && !iteratorCache_->valid(task.options) &&
      task.ownedIterator) {
    // Store the iterator that was used for reading in cache so it can be reused
    // for subsequent reads.
    iteratorCache_->set(task.options, std::move(task.ownedIterator));
  }
  if (task.ownedIterator) {
    // freeing the iterator if we didn't place it in the cache
    task.ownedIterator.reset();
  }

  ServerWorker* w = ServerWorker::onThisThread(false);
  if (w) {
    PER_SHARD_HISTOGRAM_ADD(Worker::stats(),
                            log_rebuilding_partitions_per_batch,
                            shard_,
                            task.readCtx.it_stats_.seen_logsdb_partitions);
  }

  onReadTaskDone(task.records,
                 task.status,
                 task.readCtx.read_ptr_,
                 RecordTimestamp(task.readCtx.ts_window_high_),
                 task.restartVersion);
}

void LogRebuilding::onReadTaskDone(
    RebuildingReadStorageTask::RecordContainer& records,
    Status status,
    ReadPointer& read_ptr,
    RecordTimestamp ts_window_high,
    lsn_t restart_version) {
  if (restart_version != restartVersion_) {
    // We rewound the state machine after issuing this task, discard this task.
    return;
  }

  ld_check(storageTaskInFlight_);
  storageTaskInFlight_ = false;

  if (status != E::FAILED && timer_) { // timer_ may be nullptr in tests
    timer_->reset();
  }

  for (auto& record : records) {
    buffer_.emplace_back(std::move(record));
  }
  records.clear();
  aggregateFilterStats();

  ld_spew("Task came back for log %lu with %lu records and status %s",
          logid_.val_,
          buffer_.size(),
          error_description(status));
  startProcessingBatch(status, read_ptr, ts_window_high);
  // `this` might be destroyed here.
}

void LogRebuilding::onBatchEnd() {
  addCurBatchTotalSizeToHistogram();
  cur_batch_total_size_ = 0;
  buffer_.clear();

  switch (lastBatchStatus_) {
    case E::BYTE_LIMIT_REACHED:
    case E::PARTIAL:
      // activate a zero timeout timer so that we read
      // a new batch in next event loop iteration
      activateReadNewBatchTimer();
      break;
    case E::WINDOW_END_REACHED:
      if (nextTimestamp_ > getMaxTimestamp()) {
        // We reached the end of the timestamp window.
        // Notify the ShardRebuildingV1 state machine. It will wake us up as
        // soon as it slides the local timestamp window.
        ld_debug("LogID %ju: End of window(%s). Next timestamp %s.",
                 (uintmax_t)logid_.val(),
                 toString(windowEnd_).c_str(),
                 toString(nextTimestamp_).c_str());
        addCurWindowTotalSizeToHistogram();
        publishTrace(RebuildingEventsTracer::WINDOW_END_REACHED);
        notifyWindowEnd(nextTimestamp_);
      } else {
        // We reached the end of the current epoch interval. Advance
        // readPointer_ to the beginning of the next interval.
        ++curEpochRange_;
        // There should be a new epoch interval to read otherwise the status
        // would have been E::UNTIL_LSN_REACHED.
        ld_check(curEpochRange_ != rebuildingPlan_.epochsToRead.end());
        cur_replication_scheme_ = createReplicationScheme(
            *curEpochRange_->second, rebuildingPlan_.sequencerNodeID);
        readPointer_.lsn =
            compose_lsn(epoch_t(curEpochRange_->first.lower()), ESN_MIN);
        ld_debug("LogID %ju: Setting lsn to 0x%jx.",
                 (uintmax_t)logid_.val(),
                 (uintmax_t)readPointer_.lsn);
        activateReadNewBatchTimer();
      }
      break;
    case E::UNTIL_LSN_REACHED:
    case E::CAUGHT_UP:
      publishTrace(RebuildingEventsTracer::COMPLETED);
      onComplete();
      // `this` is deleted.
      return;
    case E::FAILED:
      ld_check(timer_); // may be nullptr in tests but we don't test this path.
      ld_error("An error occurred while reading from the local log store. "
               "Will retry after %ldms",
               timer_->getNextDelay().count());
      publishTrace(RebuildingEventsTracer::FAILED_ON_LOGSTORE);
      timer_->activate();
      // Note: currently such an error is permanent so we will retry
      // indefinitely until the box is replaced. However once we implement only
      // disk repare this retry mechanism will prove useful.
      break;
    default:
      // The following status code returned by LocalLogStoreReader::read()
      // should not be received here:
      // - E::WOULDBLOCK: should not happen because we configured the iterator
      //                  for blocking io.
      // - E::ABORTED:    RebuildingReadStorageTask::processRecord() should
      //                  never fail.
      //
      ld_error("Unexpected status %s", error_description(lastBatchStatus_));
      publishTrace(RebuildingEventsTracer::UNEXPECTED_FAILURE);
      ld_check(false);
      break;
  }
}

void LogRebuilding::timerCallback() {
  ld_check(!storageTaskInFlight_);

  // `timer can be activated for 2 reasons:
  // - We failed to read the checkpoint (if any);
  // - We failed to issue a RebuildingReadStorageTask.

  if (readCheckpointState_ == ReadCheckpointState::DONE) {
    readNewBatch();
  } else {
    readCheckpointState_ = ReadCheckpointState::READING;
    readCheckpoint();
  }
}

void LogRebuilding::markNodesInRebuildingSetNotAvailable(
    ReplicationScheme& replication) {
  auto nc = getConfig()->getNodesConfiguration();

  for (auto it : rebuildingSet_->shards) {
    if (!nc->isNodeInServiceDiscoveryConfig(it.first.node())) {
      // This node is no longer in the config.
      continue;
    }
    if (!it.second.dc_dirty_ranges.empty()) {
      // Shard is only missing some time-ranged records. It should be
      // up and able to take stores.
      continue;
    }
    replication.nodeset_state->setNotAvailableUntil(
        it.first,
        std::chrono::steady_clock::time_point::max(),
        NodeSetState::NodeSetState::NotAvailableReason::STORE_DISABLED);
  }
}

void LogRebuilding::onReadTaskDropped(RebuildingReadStorageTask& task) {
  if (task.restartVersion != restartVersion_) {
    // We rewound the state machine after issuing this task, discard this task.
    return;
  }

  ld_check(storageTaskInFlight_);
  storageTaskInFlight_ = false;

  ld_check(timer_); // may be nullptr in tests but we don't test this path.
  ld_error("Could not read from local log store because read storage task was "
           "dropped. Will retry after %ldms",
           timer_->getNextDelay().count());
  timer_->activate();
}

void LogRebuilding::getDebugInfo(InfoRebuildingLogsTable& table) const {
  table.next()
      .set<0>(logid_)
      .set<1>(shard_)
      .set<2>(startTimestamp_.toMilliseconds())
      .set<3>(rebuildingSet_->describe(std::numeric_limits<size_t>::max()))
      .set<4>(version_)
      .set<5>(rebuildingPlan_.untilLSN)
      .set<6>(getMaxTimestamp().toMilliseconds())
      .set<7>(readPointer_.lsn)
      .set<8>(nRecordsReplicated_)
      .set<9>(nBytesReplicated_)
      .set<10>(numRecordRebuildingStoresInFlight_)
      .set<11>(numRecordRebuildingPendingStoreDurable_)
      .set<12>(numRecordRebuildingPendingAmend_)
      .set<13>(numRecordRebuildingAmendsInFlight_)
      .set<14>(numRecordRebuildingPendingAmendDurable_);

  if (!nonDurableRecordList_.empty()) {
    table.set<7>(nonDurableRecordList_.front().lsn);
  } else {
    table.set<7>(readPointer_.lsn);
  }

  if (lastBatchStatus_ != E::UNKNOWN) {
    table.set<15>(error_name(lastBatchStatus_));
  }
}

StatsHolder* LogRebuilding::getStats() {
  return Worker::stats();
}

const Settings& LogRebuilding::getSettings() const {
  return Worker::settings();
}

RecordRebuildingInterface* LogRebuilding::findRecordRebuilding(lsn_t lsn) {
  if (recordDurabilityState_.count(lsn)) {
    RecordDurabilityState* rds = recordDurabilityState_.at(lsn).get();
    if (rds->rr) {
      return rds->rr.get();
    }
    // Could return nullptr if rra has not been started yet
    return rds->rra.get();
  }

  return nullptr;
}

void LogRebuilding::readCheckpoint() {
  auto& map = ServerWorker::onThisThread()->processor_->getLogStorageStateMap();
  LogStorageState* log_state = map.insertOrGet(logid_, getMyShardID().shard());

  // Try to read the trim point immediately if it's cached. If we can't find it,
  // ReadLogRebuildingCheckpointTask will read it.
  bool need_trim_point = true;
  if (log_state) {
    folly::Optional<lsn_t> trim_point = log_state->getTrimPoint();
    if (trim_point.hasValue()) {
      readPointer_.lsn = std::max(readPointer_.lsn, trim_point.value() + 1);
      ld_debug("LogID %ju: Have trimpoint. Setting lsn to 0x%jx.",
               (uintmax_t)logid_.val(),
               (uintmax_t)readPointer_.lsn);
      need_trim_point = false;
    }
  }

  auto task = std::make_unique<ReadLogRebuildingCheckpointTask>(
      logid_, restartVersion_, need_trim_point);
  auto task_queue =
      ServerWorker::onThisThread()->getStorageTaskQueueForShard(shard_);
  task_queue->putTask(std::move(task));
}

void LogRebuilding::putReadStorageTask(
    const LocalLogStoreReader::ReadContext& ctx) {
  LocalLogStore::ReadOptions options("LogRebuilding::putRead", true);
  options.allow_blocking_io = true;
  options.tailing = true;
  options.fill_cache = rebuildingSettings_->use_rocksdb_cache;
  // Use the copyset index to skip some records
  options.allow_copyset_index = true;

  std::weak_ptr<LocalLogStore::ReadIterator> read_iterator;
  if (iteratorCache_ && iteratorCache_->valid(options)) {
    // Creating an iterator may be an expensive operation, so fetch it here only
    // if an existing one is found in the cache. Otherwise,
    // RebuildingReadStorageTask will create a new iterator, while
    // onReadTaskDone() will update iteratorCache_.
    read_iterator = iteratorCache_->createOrGet(options);
  }

  auto task =
      std::make_unique<RebuildingReadStorageTask>(restartVersion_,
                                                  seekToTimestamp_,
                                                  ctx,
                                                  options,
                                                  std::move(read_iterator));
  auto task_queue =
      ServerWorker::onThisThread()->getStorageTaskQueueForShard(shard_);
  task_queue->putTask(std::move(task));
}

ShardID LogRebuilding::getMyShardID() const {
  return ShardID(getMyNodeID().index(), shard_);
}

NodeID LogRebuilding::getMyNodeID() const {
  return Worker::onThisThread()->processor_->getMyNodeID();
}

ServerInstanceId LogRebuilding::getServerInstanceId() const {
  return Worker::onThisThread()->processor_->getServerInstanceId();
}

std::shared_ptr<UpdateableConfig> LogRebuilding::getConfig() const {
  return Worker::onThisThread()->getUpdateableConfig();
}

void LogRebuilding::invalidateIterators() {
  ld_check(iteratorCache_);
  auto now = std::chrono::steady_clock::now();
  auto ttl = Worker::settings().iterator_cache_ttl;
  iteratorCache_->invalidateIfUnused(now, ttl);
}

void LogRebuilding::notifyComplete() {
  size_t tot_skipped = nRecordsSCDFiltered_ + nRecordsNotDirtyFiltered_ +
      nRecordsTimestampFiltered_;

  RATELIMIT_INFO(
      std::chrono::seconds(60),
      1,
      "Completed LogRebuilding state machine for log %lu "
      "with rebuilding set: %s and plan %s. Rebuilt %lu records (%lu bytes). "
      "Seeks %lu. Skipped %lu records "
      "(SCD: %lu, ND: %lu, DRAINED: %lu, TS: %lu, LATE: %lu).",
      logid_.val_,
      rebuildingSet_->describe().c_str(),
      rebuildingPlan_.toString().c_str(),
      nRecordsReplicated_,
      nBytesReplicated_,
      nTimeBasedSeeks_,
      tot_skipped,
      nRecordsSCDFiltered_,
      nRecordsNotDirtyFiltered_,
      nRecordsDrainedFiltered_,
      nRecordsTimestampFiltered_,
      nRecordsLateFiltered_);

  owner_.postCallbackRequest([logid = logid_](ShardRebuildingV1* owner) {
    if (owner) {
      owner->onLogRebuildingComplete(logid);
    }
  });
}

void LogRebuilding::notifyWindowEnd(RecordTimestamp next) {
  owner_.postCallbackRequest(
      [logid = logid_, next, size = getTotalLogRebuildingSize()](
          ShardRebuildingV1* owner) {
        if (owner) {
          owner->onLogRebuildingWindowEnd(logid, next, size);
        }
      });
}

void LogRebuilding::notifyReachedUntilLsn() {
  owner_.postCallbackRequest(
      [logid = logid_,
       size = getTotalLogRebuildingSize()](ShardRebuildingV1* owner) {
        if (owner) {
          owner->onLogRebuildingReachedUntilLsn(logid, size);
        }
      });
}

void LogRebuilding::notifyLogRebuildingSize() {
  owner_.postCallbackRequest(
      [logid = logid_,
       size = getTotalLogRebuildingSize()](ShardRebuildingV1* owner) {
        if (owner) {
          owner->onLogRebuildingSizeUpdate(logid, size);
        }
      });
}

void LogRebuilding::deleteThis() {
  // Delete `this` by removing us from the Worker's map of LogRebuilding
  // objects.
  auto& rebuildings = Worker::onThisThread()->runningLogRebuildings();
  ld_check(rebuildings.find(logid_, shard_) == this);
  rebuildings.erase(logid_, shard_);
}

std::shared_ptr<ReplicationScheme>
LogRebuilding::createReplicationScheme(EpochMetaData metadata,
                                       NodeID sequencer_node_id) {
  auto cfg = Worker::getConfig();
  const auto& nc = Worker::onThisThread()->getNodesConfiguration();
  auto log_group = cfg->getLogGroupByIDShared(logid_);
  auto& rebuilding_shards = getRebuildingSet().shards;
  auto it = rebuilding_shards.find(getMyShardID());
  bool relocate_local_records = it != rebuilding_shards.end() &&
      it->second.mode == RebuildingMode::RELOCATE;
  auto scheme = std::make_shared<ReplicationScheme>(
      logid_,
      std::move(metadata),
      nc,
      getMyNodeID(),
      log_group ? &log_group->attrs() : nullptr,
      Worker::settings(),
      relocate_local_records,
      sequencer_node_id);
  markNodesInRebuildingSetNotAvailable(*scheme);
  return scheme;
}

std::unique_ptr<RecordRebuildingStore>
LogRebuilding::createRecordRebuildingStore(
    size_t block_id,
    RawRecord r,
    std::shared_ptr<ReplicationScheme> replication) {
  ld_check(replication);
  ld_check(!replication->epoch_metadata.shards.empty());
  return std::make_unique<RecordRebuildingStore>(
      block_id, shard_, std::move(r), this, std::move(replication));
}

std::unique_ptr<RecordRebuildingAmend>
LogRebuilding::createRecordRebuildingAmend(RecordRebuildingAmendState rras) {
  return std::make_unique<RecordRebuildingAmend>(rras.lsn_,
                                                 shard_,
                                                 this,
                                                 rras.replication_,
                                                 rras.storeHeader_,
                                                 rras.flags_,
                                                 rras.newCopyset_,
                                                 rras.amendRecipients_,
                                                 rras.rebuildingWave_);
}

std::unique_ptr<BackoffTimer>
LogRebuilding::createTimer(std::function<void()> callback) {
  auto timer =
      std::make_unique<ExponentialBackoffTimer>(std::move(callback),
                                                std::chrono::milliseconds(5),
                                                std::chrono::seconds(10));
  return std::move(timer);
}

std::unique_ptr<Timer> LogRebuilding::createIteratorTimer() {
  auto callback = [this] {
    invalidateIterators();
    ld_check(iteratorInvalidationTimer_);
    // keep the timer active until LogRebuilding is destroyed.
    iteratorInvalidationTimer_->activate(Worker::settings().iterator_cache_ttl);
  };

  auto timer = std::make_unique<Timer>(std::move(callback));
  timer->activate(Worker::settings().iterator_cache_ttl);
  return timer;
}

std::unique_ptr<Timer> LogRebuilding::createStallTimer() {
  auto callback = [this] {
    if (!nonDurableRecordList_.empty()) {
      STAT_INCR(getStats(), log_rebuilding_record_durability_timeout);
      restart();
    } else {
      readNewBatch();
    }
  };
  auto timer = std::make_unique<Timer>(std::move(callback));
  return timer;
}

std::unique_ptr<Timer> LogRebuilding::createNotifyRebuildingCoordinatorTimer() {
  auto timer =
      std::make_unique<Timer>([this] { notifyRebuildingCoordinator(); });
  return timer;
}

void LogRebuilding::activateNotifyRebuildingCoordinatorTimer() {
  notifyRebuildingCoordinatorTimer_->activate(std::chrono::microseconds(0));
}

void LogRebuilding::activateStallTimer() {
  ld_spew("Activating stall timer for log:%lu", logid_.val_);
  stallTimer_->activate(rebuildingSettings_->record_durability_timeout);
}

std::unique_ptr<Timer> LogRebuilding::createReadNewBatchTimer() {
  auto timer = std::make_unique<Timer>([this] { readNewBatch(); });
  return timer;
}

void LogRebuilding::activateReadNewBatchTimer() {
  ld_check(readNewBatchTimer_);
  readNewBatchTimer_->activate(std::chrono::microseconds(0));
}

void LogRebuilding::cancelStallTimer() {
  ld_check(stallTimer_->isActive());
  stallTimer_->cancel();
}

bool LogRebuilding::isStallTimerActive() {
  return stallTimer_->isActive();
}

void LogRebuilding::restart() {
  readPointer_.lsn = restartLsn_;
  seekToTimestamp_.clear();
  curDirtyTimeWindow_ = --rebuildingSet_->all_dirty_time_intervals.end();

  recordDurabilityState_.clear();
  LsnPendingFlush_.clear();
  nonDurableRecordList_.clear();
  lsnStoresDurable_ = std::queue<lsn_t>();
  nodeMemtableFlushStatus_.clear();
  numRecordRebuildingAmendsInFlight_ = 0;
  numRecordRebuildingStoresInFlight_ = 0;
  numRecordRebuildingPendingStoreDurable_ = 0;
  numRecordRebuildingPendingAmendDurable_ = 0;
  numRecordRebuildingPendingAmend_ = 0;
  id_ = log_rebuilding_id_t(next_id++);

  if (lastBatchStatus_ == E::UNTIL_LSN_REACHED ||
      lastBatchStatus_ == E::CAUGHT_UP) {
    // If we had reached the end of the log in the previous run,
    // set the window end to max to prevent returning WINDOW_END
    // if at all read storage task hits the read limit while reading
    // from local log store
    windowEnd_ = RecordTimestamp::max();
  }
  lastBatchStatus_ = E::UNKNOWN;

  buffer_.clear();
  cur_batch_total_size_ = 0;
  bytesRebuiltSinceCheckpoint_ = 0;
  if (isStallTimerActive()) {
    cancelStallTimer();
  }

  if (notifyRebuildingCoordinatorTimer_) {
    notifyRebuildingCoordinatorTimer_->cancel();
  }

  if (readNewBatchTimer_) {
    readNewBatchTimer_->cancel();
  }

  ld_debug("Restarting log rebuilding for log:%lu with "
           "maxTimestamp:%s, readPointer.lsn:%s",
           logid_.val_,
           format_time(getMaxTimestamp()).c_str(),
           lsn_to_string(readPointer_.lsn).c_str());

  startReading();
}

Request::Execution StartLogRebuildingRequest::execute() {
  if (!owner_) {
    // The ShardRebuildingV1 was destroyed.
    return Execution::COMPLETE;
  }

  auto& rebuildings = Worker::onThisThread()->runningLogRebuildings();

  auto existing = rebuildings.find(logid_, shard_);
  if (existing) {
    // There's already a LogRebuilding. It must be stale, belonging to a
    // ShardRebuildingV1 that has already been destroyed. Abort it.
    auto log_rebuilding = checked_downcast<LogRebuilding*>(existing);
    ld_check(!log_rebuilding->getOwner());
    ld_check(restartVersion_ > log_rebuilding->getRestartVersion());
    log_rebuilding->abort();
    ld_check(!rebuildings.find(logid_, shard_));
  }

  std::shared_ptr<IteratorCache> iterator_cache;
  if (rebuildingSettings_->use_iterator_cache) {
    iterator_cache = std::make_shared<IteratorCache>(
        &ServerWorker::onThisThread()
             ->processor_->sharded_storage_thread_pool_->getByIndex(shard_)
             .getLocalLogStore(),
        logid_,
        true /* created_for_rebuilding */);
  }

  auto log_rebuilding_ptr = std::make_unique<LogRebuilding>(
      owner_, logid_, shard_, rebuildingSettings_, iterator_cache);
  LogRebuilding* log_rebuilding = log_rebuilding_ptr.get();
  rebuildings.insert(logid_, shard_, std::move(log_rebuilding_ptr));

  log_rebuilding->start(rebuildingSet_,
                        std::move(rebuildingPlan_),
                        windowEnd_,
                        version_,
                        restartVersion_);

  return Execution::COMPLETE;
}

Request::Execution AbortLogRebuildingRequest::execute() {
  auto log = getLogRebuilding(logid_, shard_);

  if (!log) {
    // LogRebuilding state machine was aborted.
    return Execution::COMPLETE;
  }

  if (restartVersion_ != log->getRestartVersion()) {
    ld_check(false);
    return Execution::COMPLETE;
  }

  log->abort();
  return Execution::COMPLETE;
}

Request::Execution RestartLogRebuildingRequest::execute() {
  auto log = getLogRebuilding(logid_, shard_);

  if (!log) {
    // LogRebuilding state machine was aborted.
    return Execution::COMPLETE;
  }

  if (restartVersion_ != log->getRestartVersion()) {
    // Discard this Request, this is due to Request objects being re-ordered
    // (unlikely).
    return Execution::COMPLETE;
  }

  log->restart();
  return Execution::COMPLETE;
}

Request::Execution LogRebuildingMoveWindowRequest::execute() {
  auto log = getLogRebuilding(logid_, shard_);

  if (!log) {
    // LogRebuilding state machine was aborted.
    return Execution::COMPLETE;
  }

  if (restartVersion_ != log->getRestartVersion()) {
    ld_check(false);
    return Execution::COMPLETE;
  }

  log->window(windowEnd_);
  return Execution::COMPLETE;
}
}} // namespace facebook::logdevice
