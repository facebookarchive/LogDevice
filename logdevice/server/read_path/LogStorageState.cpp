/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#define __STDC_FORMAT_MACROS
#include "logdevice/server/read_path/LogStorageState.h"

#include <limits>

#include <folly/Memory.h>
#include <folly/ScopeGuard.h>

#include "logdevice/common/AdminCommandTable.h"
#include "logdevice/common/ExponentialBackoffTimer.h"
#include "logdevice/common/GetSeqStateRequest.h"
#include "logdevice/common/protocol/RELEASE_Message.h"
#include "logdevice/common/stats/Stats.h"
#include "logdevice/common/util.h"
#include "logdevice/server/RecordCache.h"
#include "logdevice/server/ReleaseRequest.h"
#include "logdevice/server/ServerProcessor.h"
#include "logdevice/server/ServerWorker.h"
#include "logdevice/server/locallogstore/LocalLogStore.h"
#include "logdevice/server/read_path/LogStorageStateMap.h"
#include "logdevice/server/storage_tasks/PerWorkerStorageTaskQueue.h"
#include "logdevice/server/storage_tasks/RecoverLogStateTask.h"
#include "logdevice/server/storage_tasks/RecoverSealTask.h"
#include "logdevice/server/storage_tasks/StorageTask.h"

namespace facebook { namespace logdevice {

// LSN_INVALID needs to be the smallest lsn_t value (0).  We use it as the
// initial last_released_lsn value and assume that any other value will
// overwrite it in updateLastReleasedLSN().
static_assert(LSN_INVALID == std::numeric_limits<lsn_t>::min(),
              "LSN_INVALID needs to be smallest possible lsn_t value");

static const std::chrono::milliseconds RETRY_RELEASE_INITIAL_DELAY(100);
static const std::chrono::milliseconds RETRY_RELEASE_MAX_DELAY(30000);

LogStorageState::LogStorageState(logid_t log_id,
                                 shard_index_t shard,
                                 LogStorageStateMap* owner,
                                 RecordCacheDependencies* cache_deps)
    : record_cache_(
          cache_deps != nullptr
              ? std::make_unique<RecordCache>(log_id, shard, cache_deps)
              : nullptr),
      log_id_(log_id),
      shard_(shard),
      owner_(owner) {
  retry_release_.timer_scheduled_ = false;
  retry_release_.force_ = false;
  if (owner_->getProcessor() != nullptr) { // may be null in tests
    purge_coordinator_ =
        owner_->getProcessor()->createPurgeCoordinator(log_id, shard_, this);
  }
}

LogStorageState::~LogStorageState() = default;

bool LogStorageState::notePermanentError(const char* context) {
  if (!permanent_error_.exchange(true)) {
    STAT_INCR(owner_->getStats(), logs_with_permanent_error);
    ld_error("%s failed for log:%lu. Setting permanent error flag in "
             "LogStorageState.",
             context,
             log_id_.val_);
    return true;
  }
  return false;
}

bool LogStorageState::hasPermanentError() {
  return permanent_error_.load();
}

int LogStorageState::updateLastReleasedLSN(lsn_t new_val,
                                           LastReleasedSource source) {
  SCOPE_EXIT {
    last_released_lsn_state_.fetch_or((int)source);
  };

  // Update the last per-epoch released LSN first, to ensure it is always at
  // least as high as the (global) last leleased LSN.
  updateLastPerEpochReleasedLSN(new_val);

  // If our LSN is larger than the one already in the map, update it.
  lsn_t prev = atomic_fetch_max(last_released_lsn_, new_val);
  if (prev >= new_val) {
    // last_released_lsn was already >= new_val
    err = E::UPTODATE;
    return -1;
  }
  return 0;
}

int LogStorageState::updateLastPerEpochReleasedLSN(lsn_t new_val) {
  lsn_t prev = atomic_fetch_max(last_per_epoch_released_lsn_, new_val);
  if (prev >= new_val) {
    err = E::UPTODATE;
    return -1;
  }
  return 0;
}

folly::Optional<lsn_t> LogStorageState::getTrimPoint() const {
  folly::Optional<lsn_t> result; // initially empty
  if (trim_point_.hasValue()) {
    result.assign(trim_point_.load());
  }
  return result;
}

folly::Optional<epoch_t>
LogStorageState::getPerEpochLogMetadataTrimPoint() const {
  folly::Optional<epoch_t> result; // initially empty
  if (per_epoch_metadata_trim_point_.hasValue()) {
    result.assign(epoch_t(per_epoch_metadata_trim_point_.load()));
  }
  return result;
}

folly::Optional<Seal>
LogStorageState::getSeal(LogStorageState::SealType type) const {
  ld_check(type < SealType::Count);
  folly::Optional<Seal> result; // initially empty
  if (seals_[static_cast<size_t>(type)].hasValue()) {
    result.assign(seals_[static_cast<size_t>(type)].load());
  }
  return result;
}

folly::Optional<epoch_t> LogStorageState::getLastCleanEpoch() const {
  folly::Optional<epoch_t> result; // initially empty
  if (last_clean_epoch_.hasValue()) {
    result.assign(epoch_t(last_clean_epoch_.load()));
  }
  return result;
}

const folly::Optional<std::pair<epoch_t, OffsetMap>>&
LogStorageState::getEpochOffsetMap() const {
  RWLock::ReadHolder read_guard(rw_lock_);
  return latest_epoch_offsets_;
}

void LogStorageState::updateLastCleanEpoch(epoch_t epoch) {
  last_clean_epoch_.fetchMax(epoch.val_);
}

void LogStorageState::updateEpochOffsetMap(
    std::pair<epoch_t, OffsetMap> epoch_offsets) {
  RWLock::WriteHolder write_guard(rw_lock_);
  if (latest_epoch_offsets_.hasValue() &&
      latest_epoch_offsets_.value().first < epoch_offsets.first) {
    // No updates needed for older epoch.
    return;
  }
  latest_epoch_offsets_.assign(
      std::make_pair(epoch_offsets.first, std::move(epoch_offsets.second)));
}

int LogStorageState::updateTrimPoint(lsn_t new_val) {
  lsn_t prev = trim_point_.fetchMax(new_val);
  if (prev >= new_val) {
    err = E::UPTODATE;
    return -1;
  }
  return 0;
}

int LogStorageState::updatePerEpochLogMetadataTrimPoint(epoch_t new_epoch) {
  epoch_t::raw_type prev =
      per_epoch_metadata_trim_point_.fetchMax(new_epoch.val_);
  if (prev >= new_epoch.val_) {
    err = E::UPTODATE;
    return -1;
  }
  return 0;
}

int LogStorageState::updateSeal(Seal new_seal, LogStorageState::SealType type) {
  ld_check(type < SealType::Count);
  ld_check(new_seal.validOrEmpty());
  Seal prev = seals_[static_cast<size_t>(type)].fetchMax(new_seal);
  if (new_seal < prev) {
    err = E::UPTODATE;
    return -1;
  }
  return 0;
}

void LogStorageState::retryRelease(worker_id_t id, bool force) {
  std::lock_guard<std::mutex> guard(retry_release_.mutex_);
  retry_release_.failed_workers_.set(id.val_);
  retry_release_.force_ |= force;
  if (!retry_release_.timer_scheduled_) {
    ExponentialBackoffTimerNode* node = Worker::onThisThread()->registerTimer(
        std::bind(
            &LogStorageState::onRetryReleaseTimer, this, std::placeholders::_1),
        RETRY_RELEASE_INITIAL_DELAY,
        RETRY_RELEASE_MAX_DELAY);
    node->timer->activate();
    retry_release_.timer_scheduled_ = true;
  } else {
    // There is a timer scheduled to fire or currently running.  Once it
    // finishes resending, it will check for any new failures and reactivate.
    // The lock we currently hold ensures that the timer won't miss this call.
  }
}

void LogStorageState::onRetryReleaseTimer(ExponentialBackoffTimerNode* node) {
  ld_spew("fired for log %lu", log_id_.val_);

  ld_check(retry_release_.timer_scheduled_);

  std::bitset<MAX_WORKERS> failed_workers;
  bool force;
  {
    std::lock_guard<std::mutex> guard(retry_release_.mutex_);
    failed_workers = retry_release_.failed_workers_;
    // Reset the public bitset.  Later we will check it to see if anyone has
    // added new bits while we were broadcasting, to see if we need to
    // reactivate the timer.
    retry_release_.failed_workers_.reset();
    force = retry_release_.force_;
  }

  ReleaseRequest::broadcastReleaseRequest(
      ServerWorker::onThisThread()->processor_,
      RecordID(last_released_lsn_, log_id_),
      shard_,
      [&](worker_id_t id) {
        return failed_workers.test(id.val_) && this->isWorkerSubscribed(id);
      },
      force);
  // If any workers failed again, they will have conveniently gotten readded
  // to the public bitset, by ReleaseRequest::broadcastReleaseRequest().

  {
    std::lock_guard<std::mutex> guard(retry_release_.mutex_);
    if (retry_release_.failed_workers_.any()) {
      // Some workers failed while retrying above, or failed on another thread
      // while we were retrying.  Reactivate the timer with a larger delay.
      node->timer->activate();
    } else {
      // The bitset is empty.  Clean up.
      delete node;
      retry_release_.timer_scheduled_ = false;
      retry_release_.force_ = false;
    }
  }
}

int LogStorageState::recover(std::chrono::microseconds interval,
                             LogStorageState::RecoverContext ctx,
                             bool force_ask_sequencer) {
  ServerWorker* w = ServerWorker::onThisThread();
  ld_check(w->processor_->sequencer_locator_ != nullptr);

  folly::Optional<lsn_t> trim_point = getTrimPoint();
  LogStorageState::LastReleasedLSN last_released = getLastReleasedLSN();

  // Do we need to ask the sequencer for any of the missing information?
  bool ask_sequencer = (!last_released.hasValue() ||
                        last_released.source() !=
                            LogStorageState::LastReleasedSource::RELEASE) ||
      force_ask_sequencer;

  // should we attempt to read it from the local log store?
  bool recover_from_store = !last_released.hasValue() ||
      !trim_point.hasValue() || !getLastCleanEpoch().hasValue();

  if (!ask_sequencer && !recover_from_store) {
    // nothing needs to be recovered
    return 0;
  }

  if (hasPermanentError()) {
    // There's not much hope of recovering this log state, so let's not try.
    err = E::FAILED;
    return -1;
  }

  std::chrono::microseconds prev = last_recovery_time_.load();
  std::chrono::microseconds current;

  do {
    current = std::chrono::duration_cast<std::chrono::microseconds>(
        std::chrono::steady_clock::now().time_since_epoch());

    if (current - prev < interval) {
      // not enough time has passed since the last time we tried to recover
      // the state
      return 0;
    }
  } while (!last_recovery_time_.compare_exchange_weak(prev, current));

  // If need to ask sequencer, create and start a GetSeqStateRequest.  Allow
  // only one request inflight at a time for this log -- once it completes,
  // it'll update the state for the log.  (We don't rely on
  // GetSeqStateRequest's internal coalescing mechanism to avoid accumulating
  // callbacks; this method may be called many times quickly on server
  // startup.)
  if (ask_sequencer && !get_seq_state_inflight_.exchange(true)) {
    GetSeqStateRequest::Options opts;
    opts.wait_for_recovery = true;
    opts.include_epoch_offset = true;
    opts.on_complete = [s = shard_](GetSeqStateRequest::Result result) {
      LogStorageState::getSeqStateRequestCallback(s, result);
    };
    std::unique_ptr<Request> rq = std::make_unique<GetSeqStateRequest>(
        log_id_, (GetSeqStateRequest::Context)ctx, opts);

    ld_spew("Posting a new GetSeqStateRequest(id:%" PRIu64 ") for log:%lu",
            (uint64_t)rq->id_,
            log_id_.val_);
    int rv = w->processor_->postWithRetrying(rq);
    if (rv) {
      ld_error("Failed to post request for log:%lu, err:%s, rv=%d",
               log_id_.val_,
               error_name(err),
               rv);
    }
  }

  // If we haven't yet tried to read from the local log store, try that.
  if (recover_from_store && !recover_log_state_task_in_flight_.exchange(true)) {
    std::unique_ptr<StorageTask> task =
        std::make_unique<RecoverLogStateTask>(log_id_);
    w->getStorageTaskQueueForShard(shard_)->putTask(std::move(task));
  }

  return 0;
}

int LogStorageState::recoverSeal(seal_callback_t callback) {
  folly::Optional<Seal> normal_seal = getSeal(SealType::NORMAL);
  folly::Optional<Seal> soft_seal = getSeal(SealType::SOFT);
  if (normal_seal.hasValue() && soft_seal.hasValue()) {
    // both normal and soft seals have values, no need to recover
    Seals seals;
    seals.setSeal(SealType::NORMAL, normal_seal.value());
    seals.setSeal(SealType::SOFT, soft_seal.value());
    callback(E::OK, seals);
    return 0;
  }

  if (hasPermanentError()) {
    err = E::FAILED;
    return -1;
  }

  std::unique_ptr<StorageTask> task =
      std::make_unique<RecoverSealTask>(log_id_, std::move(callback));
  ServerWorker::onThisThread()->getStorageTaskQueueForShard(shard_)->putTask(
      std::move(task));

  return 0;
}

void LogStorageState::getDebugInfo(InfoLogStorageStateTable& table) const {
  table.next();

  table.set<0>(log_id_);
  table.set<1>(shard_);

  LastReleasedLSN last_released = getLastReleasedLSN();
  if (last_released.hasValue()) {
    table.set<2>(last_released.value());
    const char* source = last_released.source() == LastReleasedSource::RELEASE
        ? "sequencer"
        : "local log store";
    table.set<3>(source);
  }

  folly::Optional<lsn_t> trim_point = getTrimPoint();
  if (trim_point.hasValue()) {
    table.set<4>(trim_point.value());
  }

  folly::Optional<epoch_t> epoch_trim_point = getPerEpochLogMetadataTrimPoint();
  if (epoch_trim_point.hasValue()) {
    table.set<5>(epoch_trim_point.value());
  }

  folly::Optional<Seal> seal = getSeal(SealType::NORMAL);
  if (seal.hasValue()) {
    table.set<6>(seal.value().epoch);
    table.set<7>(seal.value().seq_node.toString());
  }
  folly::Optional<Seal> soft_seal = getSeal(SealType::SOFT);
  if (seal.hasValue()) {
    table.set<8>(soft_seal.value().epoch);
    table.set<9>(soft_seal.value().seq_node.toString());
  }

  // Convert from steady clock epoch to absolute time.
  table.set<10>(std::chrono::duration_cast<std::chrono::microseconds>(
      std::chrono::system_clock::now().time_since_epoch() -
      (std::chrono::steady_clock::now().time_since_epoch() -
       last_recovery_time_.load())));

  table.set<11>(log_removal_time_.load());

  folly::Optional<epoch_t> last_clean = getLastCleanEpoch();
  if (last_clean.hasValue()) {
    table.set<12>(last_clean.value());
  }

  auto latest_epoch = getEpochOffsetMap();
  if (latest_epoch.hasValue()) {
    table.set<13>(latest_epoch->first);
    table.set<14>(latest_epoch->second.toString());
  }

  table.set<15>(permanent_error_.load());
}

void LogStorageState::getSeqStateRequestCallback(
    shard_index_t shard_idx,
    const GetSeqStateRequest::Result& result) {
  const logid_t log_id = result.log_id;
  ServerWorker* worker = ServerWorker::onThisThread();
  ld_check(worker->processor_->runningOnStorageNode());

  LogStorageState* log_state =
      worker->processor_->getLogStorageStateMap().insertOrGet(
          log_id, shard_idx);
  if (log_state == nullptr) {
    RATELIMIT_CRITICAL(std::chrono::seconds(10),
                       10,
                       "Failed to insert LogStorageState for log %lu: %s",
                       log_id.val_,
                       error_description(err));
    return;
  }

  // Epoch offset may be available if sequencer is not under recovery and
  // LogTailAttributes were requested by setting INCLUDE_EPOCH_OFFSET flag.
  if (result.last_released_lsn != LSN_INVALID) {
    log_state->purge_coordinator_->onReleaseMessage(
        result.last_released_lsn,
        result.last_seq,
        ReleaseType::GLOBAL,
        true /* do_release */,
        result.epoch_offsets.value_or(OffsetMap()));
  } else {
    // Sequencer may send LSN_INVALID if it's still not done recovering the
    // log -- ignore it in that case.
  }

  log_state->get_seq_state_inflight_.store(false);
}

}} // namespace facebook::logdevice
