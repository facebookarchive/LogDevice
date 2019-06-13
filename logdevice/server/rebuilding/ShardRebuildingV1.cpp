/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/server/rebuilding/ShardRebuildingV1.h"

#include "logdevice/common/AdminCommandTable.h"
#include "logdevice/common/Processor.h"
#include "logdevice/common/Worker.h"
#include "logdevice/common/configuration/UpdateableConfig.h"
#include "logdevice/server/locallogstore/PartitionedRocksDBStore.h"

namespace facebook { namespace logdevice {

ShardRebuildingV1::LogState::LogState(ShardRebuildingV1* owner,
                                      logid_t log_id,
                                      std::unique_ptr<RebuildingPlan> _plan)
    : owner(owner), logId(log_id), plan(std::move(_plan)) {
  ld_check(plan != nullptr);
  STAT_INCR(owner->getStats(), num_logs_rebuilding);
}

ShardRebuildingV1::LogState::~LogState() {
  if (plan != nullptr) { // this LogState wasn't emptied by move constructor
    STAT_DECR(owner->getStats(), num_logs_rebuilding);
  }
}

ShardRebuildingV1::ShardRebuildingV1(
    shard_index_t shard,
    lsn_t rebuilding_version,
    lsn_t restart_version,
    std::shared_ptr<const RebuildingSet> rebuilding_set,
    LocalLogStore* store,
    UpdateableSettings<RebuildingSettings> rebuilding_settings,
    std::shared_ptr<UpdateableConfig> config,
    Listener* listener)
    : rebuildingVersion_(rebuilding_version),
      restartVersion_(restart_version),
      shard_(shard),
      rebuildingSet_(rebuilding_set),
      store_(store),
      rebuildingSettings_(rebuilding_settings),
      config_(config),
      listener_(listener),
      callbackHelper_(this) {}

void ShardRebuildingV1::start(
    std::unordered_map<logid_t, std::unique_ptr<RebuildingPlan>> plan) {
  auto settings = rebuildingSettings_.get();

  // Init local window and partition slider.

  if (settings->local_window == RecordTimestamp::max().toMilliseconds()) {
    // The window has an infinite size.
    localWindowEnd_ = RecordTimestamp::max();
  }

  bool use_partitioned_slider =
      settings->local_window_uses_partition_boundary && isPartitionedStore();

  if (use_partitioned_slider) {
    localWindowSlider_ = std::make_unique<PartitionedLocalWindowSlider>(this);
  } else {
    localWindowSlider_ = std::make_unique<FixedSizeLocalWindowSlider>(
        this, settings->local_window);
  }

  // Init log states.

  for (auto& p : plan) {
    logid_t log = p.first;
    ld_check(p.second != nullptr);
    auto ins =
        activeLogs_.emplace(log, LogState(this, log, std::move(p.second)));
    ld_check(ins.second);
    LogState& log_state = ins.first->second;

    wakeupQueue_.push(log, log_state.nextTimestamp);
  }

  // Kick off some `LogRebuilding`s.
  someLogMadeProgress();
}

void ShardRebuildingV1::destroy() {
  for (auto& it : activeLogs_) {
    if (it.second.hasLogRebuilding) {
      sendAbortLogRebuildingRequest(it.second.workerId, it.first);
    }
  }
  activeLogs_.clear();
}

ShardRebuildingV1::~ShardRebuildingV1() {
  destroy();
}

void ShardRebuildingV1::advanceGlobalWindow(RecordTimestamp new_window_end) {
  globalWindowEnd_ = new_window_end;

  if (!activeLogs_.empty() &&         // haven't completed yet
      nRunningLogRebuildings_ == 0 && // done with current window
      !wakeupQueue_.empty()) {        // ready to move window
    trySlideLocalWindow();
  }
}

void ShardRebuildingV1::noteConfigurationChanged() {
  std::shared_ptr<Configuration> config = config_->get();

  std::vector<logid_t> logs_to_abort;
  for (auto& p : activeLogs_) {
    logid_t logid = p.first;
    if (MetaDataLog::isMetaDataLog(logid)) {
      // rebuilding metadata logs regardless of whether the data log exists
      // or not
      continue;
    }
    const std::shared_ptr<LogsConfig::LogGroupNode> l =
        config->getLogGroupByIDShared(logid);
    if (!l) {
      ld_info("Log:%lu marked for rebuilding was removed from config.",
              logid.val());
      logs_to_abort.push_back(logid);
    }
  }

  for (auto logid : logs_to_abort) {
    auto it = activeLogs_.find(logid);
    if (it == activeLogs_.end()) {
      continue;
    }

    LogState& log_state = it->second;
    log_state.abort = true;
    if (log_state.hasLogRebuilding) {
      sendAbortLogRebuildingRequest(log_state.workerId, logid);
    }
  }
}

void ShardRebuildingV1::someLogMadeProgress() {
  ld_check(!completed_);

  // Do not wake up logs if we have hit the memory limit
  // for the total size of logrebuilding state machines.
  size_t total_log_rebuilding_size_per_shard =
      rebuildingSettings_->total_log_rebuilding_size_per_shard_mb * 1024 * 1024;
  if (logRebuildingTotalSize_ >= total_log_rebuilding_size_per_shard) {
    // if logs are available for restart, schedule them else start timer
    if (restartLogRebuilding_.size() > 0) {
      wakeUpLogs();
    } else if (!isStallTimerActive()) {
      activateStallTimer();
    }
    return;
  }

  if (isStallTimerActive()) {
    cancelStallTimer();
  }

  wakeUpLogs();

  if (nRunningLogRebuildings_ == 0) {
    if (activeLogs_.empty()) {
      ld_check(nLogRebuildingRestartTimerActive_ == 0 &&
               restartLogRebuilding_.empty());
      completed_ = true;
      listener_->onShardRebuildingComplete(shard_);
    } else {
      // The wakeupQueue_ could be empty if all logs in activeLogs_ reached the
      // end but not all record are durable.
      if (wakeupQueue_.empty()) {
        ld_check(activeLogs_.size() ==
                 restartLogRebuilding_.size() +
                     nLogRebuildingRestartTimerActive_);
        return;
      }
      // If wakeUpQueue_ is not empty, logs should not be inside the window.
      ld_check(wakeupQueue_.sizeInsideWindow() == 0);

      RecordTimestamp next_ts = wakeupQueue_.nextTimestamp();
      listener_->notifyShardDonorProgress(
          shard_, next_ts, rebuildingVersion_, -1);
      trySlideLocalWindow();
    }
  }
}

void ShardRebuildingV1::getDebugInfo(InfoRebuildingShardsTable& table) const {
  table.set<4>(localWindowEnd_.toMilliseconds())
      .set<6>(nRunningLogRebuildings_)
      .set<7>(wakeupQueue_.sizeInsideWindow())
      .set<8>(restartLogRebuilding_.size())
      .set<9>(logRebuildingTotalSize_)
      .set<10>(stallTimer_ && stallTimer_->isActive())
      .set<11>(nLogRebuildingRestartTimerActive_)
      .set<12>(activeLogs_.size());
}

void ShardRebuildingV1::sendStartLogRebuildingRequest(
    int worker_id,
    logid_t logid,
    std::shared_ptr<const RebuildingSet> rebuilding_set,
    RebuildingPlan rebuilding_plan,
    RecordTimestamp next_timestamp,
    lsn_t version) {
  ld_check(!rebuildingSet_->all_dirty_time_intervals.empty());
  std::unique_ptr<Request> req =
      std::make_unique<StartLogRebuildingRequest>(callbackHelper_.ticket(),
                                                  worker_id,
                                                  logid,
                                                  shard_,
                                                  rebuildingSettings_,
                                                  rebuilding_set,
                                                  std::move(rebuilding_plan),
                                                  next_timestamp,
                                                  version,
                                                  restartVersion_);
  Worker::onThisThread()->processor_->postWithRetrying(req);
}

void ShardRebuildingV1::sendAbortLogRebuildingRequest(int worker_id,
                                                      logid_t logid) {
  std::unique_ptr<Request> req = std::make_unique<AbortLogRebuildingRequest>(
      worker_id, logid, shard_, restartVersion_);
  Worker::onThisThread()->processor_->postWithRetrying(req);
}

void ShardRebuildingV1::sendLogRebuildingMoveWindowRequest(
    int worker_id,
    logid_t logid,
    RecordTimestamp end) {
  std::unique_ptr<Request> req =
      std::make_unique<LogRebuildingMoveWindowRequest>(
          worker_id, logid, shard_, end, restartVersion_);
  Worker::onThisThread()->processor_->postWithRetrying(req);
}

void ShardRebuildingV1::sendRestartLogRebuildingRequest(int worker_id,
                                                        logid_t logid) {
  STAT_INCR(getStats(), log_rebuilding_restarted_by_rebuilding_coordinator);
  std::unique_ptr<Request> req = std::make_unique<RestartLogRebuildingRequest>(
      worker_id, logid, shard_, restartVersion_);
  Worker::onThisThread()->processor_->postWithRetrying(req);
}

void ShardRebuildingV1::activateStallTimer() {
  if (!stallTimer_) {
    stallTimer_ = std::make_unique<Timer>([this] { onStallTimerExpired(); });
  }

  ld_check(stallTimer_);
  // Setting the timeout to twice record_durability_timeout
  // to give LogRebuilding state machines more time to send updated status.
  // In steady state, we should never have to activate the timer unless
  // total_log_rebuilding_size_per_shard_mb is set too low
  stallTimer_->activate(2 * rebuildingSettings_->record_durability_timeout);
}

void ShardRebuildingV1::cancelStallTimer() {
  if (stallTimer_ && stallTimer_->isActive()) {
    stallTimer_->cancel();
  }
}

void ShardRebuildingV1::cancelRestartTimerForLog(logid_t logid) {
  auto it = activeLogs_.find(logid);
  if (it != activeLogs_.end() && it->second.restartTimer) {
    it->second.restartTimer->cancel();
  }
}

bool ShardRebuildingV1::isStallTimerActive() {
  return stallTimer_ && stallTimer_->isActive();
}

void ShardRebuildingV1::onStallTimerExpired() {
  // If the stall timer fired, schedule all logs whose memory size
  // is known to be non-zero for a restart.
  STAT_INCR(getStats(), log_rebuilding_stall_timer_expired);
  for (auto& it : activeLogs_) {
    auto& log_state = it.second;
    if (log_state.logRebuildingSize > 0 && !log_state.isLogRebuildingRunning) {
      restartLogRebuilding_.insert(log_state.logId);
      ld_check(logRebuildingTotalSize_ >= log_state.logRebuildingSize);
      logRebuildingTotalSize_ -= log_state.logRebuildingSize;
      log_state.logRebuildingSize = 0;
      // Cancel the log's restart timer if it is active.
      if (log_state.restartTimer && log_state.restartTimer->isActive()) {
        ld_check(nLogRebuildingRestartTimerActive_ > 0);
        nLogRebuildingRestartTimerActive_--;
        log_state.restartTimer->cancel();
      }
    }
  }
  wakeUpLogs();
}

void ShardRebuildingV1::onLogRebuildingComplete(logid_t logid) {
  auto it = activeLogs_.find(logid);
  if (it == activeLogs_.end()) {
    return;
  }

  ld_check(!completed_);

  auto& log_state = it->second;
  if (log_state.isLogRebuildingRunning) {
    ld_check(nRunningLogRebuildings_ > 0);
    --nRunningLogRebuildings_;
    log_state.isLogRebuildingRunning = false;
  }

  if (isRestartTimerActiveForLog(logid)) {
    ld_check(nLogRebuildingRestartTimerActive_ > 0);
    nLogRebuildingRestartTimerActive_--;
    cancelRestartTimerForLog(logid);
  }

  ld_check(logRebuildingTotalSize_ >= log_state.logRebuildingSize);
  logRebuildingTotalSize_ -= log_state.logRebuildingSize;
  log_state.logRebuildingSize = 0;
  activeLogs_.erase(it);
  restartLogRebuilding_.erase(logid);
  ld_info("LogRebuilding of %s log:%lu, version:%s completed durably",
          MetaDataLog::isMetaDataLog(logid) ? "metadata" : "data",
          logid.val_,
          lsn_to_string(rebuildingVersion_).c_str());

  someLogMadeProgress();
}

void ShardRebuildingV1::onLogRebuildingReachedUntilLsn(logid_t logid,
                                                       size_t size) {
  auto it = activeLogs_.find(logid);
  if (it == activeLogs_.end()) {
    return;
  }

  ld_check(!completed_);

  auto& log_state = it->second;
  ld_check(nRunningLogRebuildings_ > 0);
  --nRunningLogRebuildings_;
  ld_check(log_state.isLogRebuildingRunning);
  log_state.isLogRebuildingRunning = false;

  ld_check(logRebuildingTotalSize_ >= log_state.logRebuildingSize);
  logRebuildingTotalSize_ -= log_state.logRebuildingSize;
  log_state.logRebuildingSize = size;
  logRebuildingTotalSize_ += log_state.logRebuildingSize;

  // Log rebuilding has reached end but not all records are
  // durable yet. Start a restart timer
  activateRestartTimerForLog(logid);
  nLogRebuildingRestartTimerActive_++;
  someLogMadeProgress();
}

bool ShardRebuildingV1::isRestartTimerActiveForLog(logid_t logid) {
  auto it = activeLogs_.find(logid);
  return it != activeLogs_.end() && it->second.restartTimer &&
      it->second.restartTimer->isActive();
}

void ShardRebuildingV1::activateRestartTimerForLog(logid_t logid) {
  ld_assert(activeLogs_.count(logid));
  auto it = activeLogs_.find(logid);
  if (!it->second.restartTimer) {
    it->second.restartTimer = std::make_unique<Timer>(

        [this, logid] { onRestartTimerExpiredForLog(logid); });
  }
  it->second.restartTimer->activate(
      rebuildingSettings_->record_durability_timeout);
}

void ShardRebuildingV1::onRestartTimerExpiredForLog(logid_t logid) {
  ld_check(nLogRebuildingRestartTimerActive_ > 0);
  --nLogRebuildingRestartTimerActive_;
  restartLogRebuilding_.insert(logid);

  auto it = activeLogs_.find(logid);
  ld_check(it != activeLogs_.end());
  LogState& log = it->second;
  ld_check(logRebuildingTotalSize_ >= log.logRebuildingSize);
  logRebuildingTotalSize_ -= log.logRebuildingSize;
  log.logRebuildingSize = 0;

  wakeUpLogs();
}

bool ShardRebuildingV1::isLogRebuilding(logid_t logid) {
  auto it = activeLogs_.find(logid);
  if (it == activeLogs_.end()) {
    // LogRebuilding was aborted while in wakeupqueue
    return false;
  }
  LogState& log = it->second;

  if (log.abort) {
    if (!log.hasLogRebuilding) {
      ld_check(logRebuildingTotalSize_ >= log.logRebuildingSize);
      logRebuildingTotalSize_ -= log.logRebuildingSize;
      activeLogs_.erase(it);
    }
    return false;
  }
  return true;
}

void ShardRebuildingV1::wakeUpLogs() {
  size_t max_logs_in_flight = rebuildingSettings_->max_logs_in_flight;

  wakeupQueue_.advanceWindow(localWindowEnd_);

  if (max_logs_in_flight <= nRunningLogRebuildings_) {
    // Note: "max_logs_in_flight" can be decreased at runtime so
    //       "nRunningLogRebuildings_" can exceed this
    //       value while active logs drain.
    return;
  }

  // Number of logs we can wake up.
  const size_t n_logs = max_logs_in_flight - nRunningLogRebuildings_;
  size_t n_woken = 0;

  std::unordered_set<logid_t, logid_t::Hash> restartLogs;
  auto restartLog = restartLogRebuilding_.begin();
  while (n_woken < n_logs && restartLog != restartLogRebuilding_.end()) {
    if (isLogRebuilding(*restartLog)) {
      restartLogs.insert(*restartLog);
      auto it = activeLogs_.find(*restartLog);
      ld_check(it != activeLogs_.end());
      LogState& log = it->second;
      sendRestartLogRebuildingRequest(log.workerId, log.logId);
      ld_check(!log.isLogRebuildingRunning);
      log.isLogRebuildingRunning = true;
      ++nRunningLogRebuildings_;
      ++n_woken;
    }
    restartLog = restartLogRebuilding_.erase(restartLog);
  }

  // Remove the logs that we restarted from the queue
  wakeupQueue_.remove(std::move(restartLogs));

  // Loop again if all the logs we select have been aborted.
  while (wakeupQueue_.sizeInsideWindow() > 0 && n_woken < n_logs) {
    std::vector<logid_t> logs = wakeupQueue_.pop(n_logs - n_woken);

    for (logid_t logid : logs) {
      if (!isLogRebuilding(logid)) {
        continue;
      }
      auto it = activeLogs_.find(logid);
      ld_check(it != activeLogs_.end());
      LogState& log = it->second;
      if (log.hasLogRebuilding) {
        sendLogRebuildingMoveWindowRequest(
            log.workerId, log.logId, localWindowEnd_);
      } else {
        if (log.workerId == -1) {
          log.workerId = folly::Hash()(log.logId.val_) % getWorkerCount();
        }
        log.hasLogRebuilding = true;
        sendStartLogRebuildingRequest(log.workerId,
                                      log.logId,
                                      rebuildingSet_,
                                      *log.plan,
                                      localWindowEnd_,
                                      rebuildingVersion_);
      }

      ld_check(!log.isLogRebuildingRunning);
      log.isLogRebuildingRunning = true;
      ++nRunningLogRebuildings_;
      ++n_woken;
    }
  }
}

void ShardRebuildingV1::PartitionedLocalWindowSlider::getPartitionTimestamps() {
  partition_timestamps_.clear();

  ld_check(owner_);
  LocalLogStore* store = owner_->store_;
  ld_check(store);

  PartitionedRocksDBStore* pstore =
      dynamic_cast<PartitionedRocksDBStore*>(store);
  ld_check(pstore);

  auto list = pstore->getPartitionList();
  ld_check(list->size() > 0);

  partition_timestamps_.reserve(list->size());
  for (auto& p : *list) {
    auto partition_ts = p->starting_timestamp;
    // -1 here so that the check in PartitionedRocksDBStoreIterator doesn't
    // read this partition when we reach it
    partition_timestamps_.push_back(partition_ts.time_since_epoch().count() == 0
                                        ? partition_ts
                                        : --partition_ts);
  }
}

void ShardRebuildingV1::PartitionedLocalWindowSlider::resetTimestamps(
    RecordTimestamp next_ts) {
  getPartitionTimestamps();
  ld_check(partition_timestamps_.size() > 0);
  // We hope that starting timestamps of partitions are sorted
  auto it = std::lower_bound(
      partition_timestamps_.begin(), partition_timestamps_.end(), next_ts);
  cur_partition_ = std::distance(partition_timestamps_.begin(), it);
}

RecordTimestamp ShardRebuildingV1::LocalWindowSlider::getGlobalWindowEnd() {
  return owner_->globalWindowEnd_;
}

bool ShardRebuildingV1::PartitionedLocalWindowSlider::getNewLocalWindowEnd(
    RecordTimestamp next_ts,
    RecordTimestamp* out) {
  ld_check(out);

  if (partition_timestamps_.empty()) {
    resetTimestamps(next_ts);
  }

  while (cur_partition_ < partition_timestamps_.size()) {
    if (partition_timestamps_[cur_partition_] >= next_ts) {
      break;
    }
    ++cur_partition_;
  }

  if (cur_partition_ >= partition_timestamps_.size()) {
    ld_error("Can't slide local window of shard %u to ts %s, last partition "
             "at ts %s",
             owner_ ? owner_->shard_ : 42, // can be null in tests
             format_time(next_ts).c_str(),
             format_time(partition_timestamps_.size() > 0
                             ? partition_timestamps_.back()
                             : RecordTimestamp::min())
                 .c_str());
    return false;
  }

  // The result is not clamped to the global window end. This is because the
  // purpose of the partitioned window slider is to slide window to the end of
  // the partition regardless of the size of the global window. So the global
  // window is enforced by checking the beginning of the local window, not its
  // end, against the end of the global window. This is already done in
  // trySlideLocalWindow(), so here we don't do anything
  // about the global window.
  *out = partition_timestamps_[cur_partition_];
  return true;
}

bool ShardRebuildingV1::FixedSizeLocalWindowSlider::getNewLocalWindowEnd(
    RecordTimestamp next_ts,
    RecordTimestamp* out) {
  ld_check(out);
  if (slide_interval_ == RecordTimestamp::duration::max()) {
    // Treat max() as infinity.
    next_ts = RecordTimestamp::max();
  } else if (next_ts.toMilliseconds().count() > 0 &&
             slide_interval_ > RecordTimestamp::max() - next_ts) {
    // Addition would overflow, clamp to max().
    next_ts = RecordTimestamp::max();
  } else {
    next_ts += slide_interval_;
  }
  // Clamp to global window.
  *out = std::min(next_ts, getGlobalWindowEnd());
  return true;
}

void ShardRebuildingV1::trySlideLocalWindow() {
  const std::chrono::milliseconds local_window =
      rebuildingSettings_->local_window;

  // The caller ensures all that.
  ld_check(!activeLogs_.empty());
  ld_check(nRunningLogRebuildings_ == 0);
  ld_check(!wakeupQueue_.empty());
  ld_check(wakeupQueue_.sizeInsideWindow() == 0);

  SCOPE_EXIT {
    bool was_waiting_on_global_window =
        waitingOnGlobalWindowSince_ != SteadyTimestamp::min();
    // Checking whether we slid the window successfully to update status
    if (nRunningLogRebuildings_ == 0 && !was_waiting_on_global_window) {
      // no LogRebuildings woke up after attempting to slide the window - we
      // are waiting on the global window
      PER_SHARD_STAT_SET(
          getStats(), rebuilding_global_window_waiting_flag, shard_, 1);
      waitingOnGlobalWindowSince_ = SteadyTimestamp::now();
    } else if (nRunningLogRebuildings_ > 0 && was_waiting_on_global_window) {
      // Sliding was successful - not waiting on global window anymore
      PER_SHARD_STAT_SET(
          getStats(), rebuilding_global_window_waiting_flag, shard_, 0);
      auto waited =
          SteadyTimestamp(SteadyTimestamp::now() - waitingOnGlobalWindowSince_);
      PER_SHARD_STAT_ADD(getStats(),
                         rebuilding_global_window_waiting_total,
                         shard_,
                         waited.toMilliseconds().count());
      waitingOnGlobalWindowSince_ = SteadyTimestamp::min();
    }
  };

  RecordTimestamp next_ts = wakeupQueue_.nextTimestamp();
  // All log rebuildings are caught up, which means they all reported that the
  // next record to be read has a timestamp greater than the local window end.
  ld_check(next_ts > localWindowEnd_);

  if (next_ts > globalWindowEnd_ ||
      // Refuse to make effective local window too small
      // (smaller than 1/3 of the target size).
      globalWindowEnd_ - next_ts < local_window / 3) {
    // Reached end of global window, waiting for it to slide.
    return;
  }

  RecordTimestamp old_window_end = localWindowEnd_;
  bool success =
      localWindowSlider_->getNewLocalWindowEnd(next_ts, &localWindowEnd_);
  if (!success) {
    // if PartitionedLocalWindowSlider fails, revert to
    // FixedSizeLocalWindowSlider
    ld_assert(
        dynamic_cast<PartitionedLocalWindowSlider*>(localWindowSlider_.get()));
    localWindowSlider_.reset(
        new FixedSizeLocalWindowSlider(this, local_window));
    success =
        localWindowSlider_->getNewLocalWindowEnd(next_ts, &localWindowEnd_);
    ld_check(success);
  }

  ld_check(localWindowEnd_ >= old_window_end);
  if (localWindowEnd_ != old_window_end) {
    ld_info("Moving local window from %s to %s for shard %u and "
            "rebuilding set %s. WakeUpQueue's next timestamp is %s",
            format_time(old_window_end).c_str(),
            format_time(localWindowEnd_).c_str(),
            shard_,
            rebuildingSet_->describe().c_str(),
            format_time(next_ts).c_str());
    PER_SHARD_STAT_SET(getStats(),
                       rebuilding_local_window_end,
                       shard_,
                       localWindowEnd_.toMilliseconds().count());
    if (old_window_end != RecordTimestamp::min()) {
      PER_SHARD_STAT_ADD(
          getStats(), rebuilding_local_window_slide_num, shard_, 1);
      PER_SHARD_STAT_ADD(getStats(),
                         rebuilding_local_window_slide_total,
                         shard_,
                         RecordTimestamp(localWindowEnd_ - old_window_end)
                             .toMilliseconds()
                             .count());
    }

    wakeUpLogs();
  }
}

void ShardRebuildingV1::onLogRebuildingWindowEnd(logid_t logid,
                                                 RecordTimestamp ts,
                                                 size_t size) {
  ld_check(!completed_);
  auto it = activeLogs_.find(logid);
  ld_check(it != activeLogs_.end());
  auto& log_state = it->second;
  ld_check(log_state.isLogRebuildingRunning);
  ld_check(ts > localWindowEnd_);

  log_state.isLogRebuildingRunning = false;
  ld_check(nRunningLogRebuildings_ > 0);
  --nRunningLogRebuildings_;
  ld_check(logRebuildingTotalSize_ >= log_state.logRebuildingSize);
  logRebuildingTotalSize_ -= log_state.logRebuildingSize;
  log_state.logRebuildingSize = size;
  logRebuildingTotalSize_ += log_state.logRebuildingSize;
  restartLogRebuilding_.erase(logid);

  log_state.nextTimestamp = ts;
  wakeupQueue_.push(logid, log_state.nextTimestamp);

  someLogMadeProgress();
}

void ShardRebuildingV1::onLogRebuildingSizeUpdate(logid_t logid, size_t size) {
  auto it = activeLogs_.find(logid);
  ld_check(it != activeLogs_.end());
  auto& log_state = it->second;

  ld_check(logRebuildingTotalSize_ >= log_state.logRebuildingSize);
  size_t prev = logRebuildingTotalSize_;
  logRebuildingTotalSize_ -= log_state.logRebuildingSize;
  log_state.logRebuildingSize = size;
  logRebuildingTotalSize_ += log_state.logRebuildingSize;

  if (log_state.logRebuildingSize == 0) {
    // Remove the log from restart set
    restartLogRebuilding_.erase(logid);
  }

  if (logRebuildingTotalSize_ < prev) {
    someLogMadeProgress();
  }
}

StatsHolder* ShardRebuildingV1::getStats() {
  return Worker::stats();
}

bool ShardRebuildingV1::isPartitionedStore() {
  ld_check(store_);
  return dynamic_cast<PartitionedRocksDBStore*>(store_) != nullptr;
}

int ShardRebuildingV1::getWorkerCount() {
  return Worker::onThisThread()->processor_->getWorkerCount(
      WorkerType::GENERAL);
}

lsn_t ShardRebuildingV1::getRestartVersion() const {
  return restartVersion_;
}
RecordTimestamp ShardRebuildingV1::getLocalWindowEnd() const {
  return localWindowEnd_;
}

}} // namespace facebook::logdevice
