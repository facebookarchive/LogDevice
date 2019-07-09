/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/common/Sequencer.h"

#include <cinttypes>

#include "logdevice/common/AllSequencers.h"
#include "logdevice/common/Appender.h"
#include "logdevice/common/AppenderBuffer.h"
#include "logdevice/common/EpochMetaData.h"
#include "logdevice/common/EpochMetaDataMap.h"
#include "logdevice/common/EpochSequencer.h"
#include "logdevice/common/FireAndForgetRequest.h"
#include "logdevice/common/GetTrimPointRequest.h"
#include "logdevice/common/LogRecoveryRequest.h"
#include "logdevice/common/MetaDataLog.h"
#include "logdevice/common/MetaDataLogWriter.h"
#include "logdevice/common/NodeID.h"
#include "logdevice/common/NodeSetFinder.h"
#include "logdevice/common/PeriodicReleases.h"
#include "logdevice/common/Processor.h"
#include "logdevice/common/SequencerBackgroundActivator.h"
#include "logdevice/common/Socket.h"
#include "logdevice/common/Worker.h"
#include "logdevice/common/configuration/Configuration.h"
#include "logdevice/common/debug.h"
#include "logdevice/common/protocol/APPEND_Message.h"
#include "logdevice/common/protocol/RELEASE_Message.h"
#include "logdevice/common/util.h"

namespace facebook { namespace logdevice {

Sequencer::Sequencer(logid_t log_id,
                     UpdateableSettings<Settings> settings,
                     StatsHolder* stats,
                     AllSequencers* parent)
    : log_id_(log_id),
      settings_(settings),
      stats_(stats),
      parent_(parent),
      last_state_change_timestamp_(
          std::chrono::steady_clock::now().time_since_epoch()),
      reactivation_limiter_(settings_.get()->reactivation_limit),
      ml_manager_(this),
      periodic_releases_(std::make_shared<PeriodicReleases>(this)) {
  ld_check(log_id != LOGID_INVALID);
  setEpochSequencers(nullptr, nullptr);
  clearMetaDataMapImpl();
  if (!MetaDataLog::isMetaDataLog(log_id)) {
    // create metadata log writer for a data log
    metadata_writer_ = std::make_unique<MetaDataLogWriter>(this);
  }
}

std::shared_ptr<EpochSequencer> Sequencer::getCurrentEpochSequencer() const {
  ld_assert(epoch_seqs_.get() != nullptr);
  return epoch_seqs_.get()->current;
}

std::shared_ptr<EpochSequencer> Sequencer::getDrainingEpochSequencer() const {
  ld_assert(epoch_seqs_.get() != nullptr);
  return epoch_seqs_.get()->draining;
}

int Sequencer::startActivation(GetMetaDataFunc metadata_func,
                               ActivationPred pred) {
  {
    folly::SharedMutex::WriteHolder write_lock(state_mutex_);
    if (pred && !pred(*this)) {
      err = E::ABORTED;
      return -1;
    }
    // check the current state to see if it is eligible for activation
    switch (getState()) {
      case State::UNAVAILABLE:
      case State::ACTIVE:
      case State::PREEMPTED:
        break;
      // cannot activate in the following states:
      case State::ACTIVATING:
        // sequencer is already in the process of activation
        err = E::INPROGRESS;
        return -1;
      case State::PERMANENT_ERROR:
        err = E::SYSLIMIT;
        return -1;
    }

    if (!reactivation_limiter_.isAllowed()) {
      RATELIMIT_INFO(std::chrono::seconds(5),
                     5,
                     "Failed to initiate activation of sequencer of log %lu "
                     "because it has reached its activation rate limit.",
                     log_id_.val_);
      err = E::TOOMANY;
      return -1;
    }

    setState(State::ACTIVATING);
  }

  int rv = metadata_func(log_id_);
  if (rv != 0) {
    // move the sequencer to the original state
    onActivationFailed();
    err = E::FAILED;
    return -1;
  }

  return 0;
}

ActivateResult Sequencer::completeActivationWithMetaData(
    epoch_t epoch,
    const std::shared_ptr<Configuration>& cfg,
    std::unique_ptr<EpochMetaData> metadata) {
  ld_check(epoch != EPOCH_INVALID);
  ld_check(metadata && metadata->isValid());
  ld_check(epoch == metadata->h.epoch);

  ld_info(
      "Activating sequencer for log %lu epoch %u.", log_id_.val_, epoch.val_);

  std::shared_ptr<EpochSequencers> seqs_before;
  bool draining_started = false;
  std::pair<UpdateMetaDataMapResult, std::shared_ptr<const EpochMetaDataMap>>
      metadata_result;
  std::shared_ptr<const EpochMetaData> metadata_for_provisioning;

  {
    folly::SharedMutex::WriteHolder write_lock(state_mutex_);
    if (getState() != State::ACTIVATING) {
      // This is normal. For example, we will get here if we lose a race to
      // another activation attempt for this log whose EpochStore request
      // completes faster.
      RATELIMIT_INFO(std::chrono::seconds(5),
                     5,
                     "completeActivationWithMetaData() was called on a "
                     "sequencer in state %s. logid=%lu, epoch=%u. Ignoring.",
                     stateString(getState()),
                     log_id_.val_,
                     getCurrentEpoch().val_);
      return ActivateResult::FAILED;
    }

    // get the current epoch sequencers before activation
    seqs_before = epoch_seqs_.get();
    ld_check(seqs_before != nullptr);

    const epoch_t current_epoch = getCurrentEpoch();
    // current_epoch_ must be in sync with the current epoch sequencer with
    // the state_mutex_ held
    ld_check(seqs_before->current == nullptr ||
             seqs_before->current->getEpoch() == current_epoch);

    // check if we are actually advancing epoch
    if (current_epoch > epoch) {
      RATELIMIT_WARNING(std::chrono::seconds(5),
                        5,
                        "Attempt to activate a sequencer for log %lu and "
                        "epoch %u while the sequencer is already running a "
                        "greater epoch %u. This should be rare.",
                        log_id_.val_,
                        epoch.val_,
                        current_epoch.val_);
      return ActivateResult::FAILED;
    } else if (current_epoch == epoch) {
      RATELIMIT_ERROR(std::chrono::seconds(1),
                      2,
                      "INTERNAL ERROR: attempt to activate a sequencer for "
                      "log %lu and epoch %u while a sequencer for that same "
                      "epoch already exists",
                      log_id_.val_,
                      epoch.val_);
      return ActivateResult::FAILED;
    }

    // 1. create new epoch sequencer with epoch metadata provided,
    //    and replacing the sequencer pair in epochs_ so that the newly
    //    created epoch sequencer become the new `current' while the
    //    previous `current' become the new `draining'
    auto new_epoch_sequencer = createEpochSequencer(epoch, std::move(metadata));
    if (new_epoch_sequencer == nullptr) {
      // log_id_ was removed from the config
      RATELIMIT_ERROR(std::chrono::seconds(1),
                      2,
                      "Failed to activate a sequencer for log %lu and "
                      "epoch %u but the log is no longer in config.",
                      log_id_.val_,
                      epoch.val_);
      return ActivateResult::FAILED;
    }

    metadata_for_provisioning = new_epoch_sequencer->getMetaData();

    setEpochSequencers(
        /*current=*/std::move(new_epoch_sequencer),
        /*draining=*/seqs_before->current);

    // 2. attempt to update historical metadata information with the newly
    //    gotten metadata
    ld_assert(getCurrentEpochSequencer() != nullptr);
    metadata_result =
        updateMetaDataMap(epoch, *getCurrentEpochSequencer()->getMetaData());

    // 3. update current_epoch_, move the Sequencer to ACTIVE state
    auto prev_epoch = current_epoch_.exchange(epoch.val_);
    setState(State::ACTIVE);

    if (prev_epoch != epoch.val() - 1) {
      // If sequencer for previous epoch was on a different node, we don't know
      // what the append rate was up until now. Tell that to rate estimator.
      append_rate_estimator_.clear(SteadyTimestamp::now());
    }
  }

  // 4. at this point, new appends are likely to be routed to the newly created
  //    epoch. Abort draining the previos `draining' epoch and start draining
  //    the previous `current' epoch, if any.
  if (seqs_before->current != nullptr) {
    // Note: draining may complete synchronously with this call
    // (e.g., no running Appends when draining started), and
    // noteDrainingCompleted() might be called before startDraining()
    // returns.
    seqs_before->current->startDraining();
    draining_started = true;
    if (seqs_before->current->getState() != EpochSequencer::State::QUIESCENT) {
      // if draining is not finished, start a timer for the draining operation.
      startDrainingTimer(seqs_before->current->getEpoch());
    }
  }

  if (seqs_before->draining != nullptr) {
    // it is safe to abort draining since log recovery/graceful reactivation
    // completion will be scheduled with this activation.
    seqs_before->draining->startDestruction();
  }

  ld_check(metadata_for_provisioning);
  ml_manager_.considerWritingMetaDataLogRecord(metadata_for_provisioning, cfg);

  switch (metadata_result.first) {
    case UpdateMetaDataMapResult::UPDATED: {
      ld_check(metadata_result.second != nullptr);
      ld_info("Historical epoch metadata for log %lu: %s.",
              log_id_.val_,
              metadata_result.second->toString().c_str());
      onMetaDataMapUpdate(metadata_result.second);
    } break;
    case UpdateMetaDataMapResult::READ_METADATA_LOG:
      getHistoricalMetaData(GetHistoricalMetaDataMode::IMMEDIATE);
      break;
  }

  if (!MetaDataLog::isMetaDataLog(log_id_)) {
    // 5. start broadcasting releases to storage shards (see PeriodicReleases)
    startPeriodicReleasesBroadcast();

    // 6. start ansyc GetTrimPointRequest if neceessary
    startGetTrimPointRequest();

    // 7. start timer to periodically read and refresh the metadata map.
    getHistoricalMetaData(GetHistoricalMetaDataMode::PERIODIC);
  }

  return (draining_started ? ActivateResult::GRACEFUL_DRAINING
                           : ActivateResult::RECOVERY);
}

void Sequencer::startGetTrimPointRequest() {
  // ensure GetTrimPointRequest start only once
  if (!get_trim_point_running_.exchange(true)) {
    std::unique_ptr<Request> req = std::make_unique<GetTrimPointRequest>(
        log_id_, settings_.get()->get_trimpoint_interval);
    getProcessor()->postImportant(req);
  }
}

void Sequencer::updateTrimPoint(Status status, lsn_t tp) {
  if (status == E::OK) {
    trim_point_.fetchMax(tp);
  }
}

std::pair<Status, bool> Sequencer::isLogEmpty() {
  auto tail_record = getTailRecord();

  // Tell the client to try again in a bit if
  // 1) activation or recovery haven't finished,
  // 2) we're in some error state. Client should get routed elsewhere.
  if (!tail_record || !tail_record->isValid() || !getTrimPoint().hasValue()) {
    return {E::AGAIN, false};
  }

  return {E::OK, tail_record->header.lsn <= getTrimPoint().value()};
}

void Sequencer::noteDrainingCompleted(epoch_t epoch, Status drain_status) {
  DrainedAction action;
  std::shared_ptr<EpochSequencer> to_evict = nullptr;
  {
    folly::SharedMutex::WriteHolder write_lock(state_mutex_);

    auto current_seqs = epoch_seqs_.get();
    auto& current_draining = current_seqs->draining;

    ld_check(current_draining == nullptr ||
             current_draining->getEpoch() >= epoch);
    if (current_draining != nullptr && current_draining->getEpoch() == epoch) {
      // the current draining epoch has finished draining
      if (drain_status == E::OK) {
        // start graceful reactivation completion procedure
        // TODO: may check if epoch is exactly current_epoch-1, however don't
        //       bother to do it for now since the completion procedure is a
        //       full-fledged log recovery
        ld_debug("Sequencer of log %lu has successfully finished draining "
                 "its epoch %u.",
                 log_id_.val_,
                 epoch.val_);
        action = DrainedAction::GRACEFUL_COMPLETION;
      } else {
        RATELIMIT_INFO(std::chrono::seconds(5),
                       2,
                       "Sequencer of log %lu has finished draining its "
                       "epoch %u with an unsuccessful status %s.",
                       log_id_.val_,
                       epoch.val_,
                       error_description(drain_status));
        action = DrainedAction::RECOVERY;
      }

      // draining completes, evict the draining sequencer
      setEpochSequencers(
          /*current=*/current_seqs->current,
          /*draining=*/nullptr);

      to_evict = current_draining;
    } else {
      // two cases can apply here:
      // 1) the sequencer might have been reactivated to a higher epoch,
      //    and a new log recovery or graceful reactivation procedure must
      //    have been scheduled. Nothing to do for this stale epoch.
      // 2) draining for _epoch_ has already been aborted (e.g., draining timer
      //    has expired). Again, log recovery must have been scheduled at the
      //    time draining is aborted. Nothing to do for now.
      RATELIMIT_INFO(std::chrono::seconds(5),
                     2,
                     "Sequencer of log %lu finished draining its "
                     "epoch %u with an unsuccessful status %s, however the "
                     "current draining epoch is %u. Nothing to do.",
                     log_id_.val_,
                     epoch.val_,
                     error_description(drain_status),
                     (current_draining ? current_draining->getEpoch().val_
                                       : EPOCH_INVALID.val_));
      action = DrainedAction::NONE;
    }
  }

  if (to_evict != nullptr) {
    // Note: if draining has timed out, then the epoch sequencer to evict may
    //       still be in the DRAINING state with Appender running. To make sure
    //       all Appender of its epoch are eventually retired, start the
    //       destruction of the epoch sequencer (this is a no-op for epoch
    //       sequencer already in QUIESCENT state).
    to_evict->startDestruction();
  }

  finalizeDraining(action);
}

void Sequencer::startGracefulReactivationCompletion() {
  STAT_INCR(stats_, graceful_reactivation_completion_started);
  STAT_INCR(stats_, recovery_scheduled);
  startRecovery();
}

void Sequencer::finalizeDraining(DrainedAction action) {
  if (settings_.get()->bypass_recovery) {
    ld_warning("Bypassing recovery of log %lu next_epoch %u according "
               "to test options.",
               log_id_.val_,
               current_epoch_.load());
    return;
  }
  switch (action) {
    case DrainedAction::NONE:
      break;
    case DrainedAction::GRACEFUL_COMPLETION:
      startGracefulReactivationCompletion();
      break;
    case DrainedAction::RECOVERY:
      STAT_INCR(stats_, recovery_scheduled);
      startRecovery();
      break;
  }
}

namespace {

class StartDrainTimerRequest : public Request {
 public:
  explicit StartDrainTimerRequest(logid_t log_id, epoch_t draining_epoch)
      : Request(RequestType::START_SEQ_DRAIN_TIMER),
        log_id_(log_id),
        epoch_(draining_epoch) {}

  Execution execute() override {
    const std::chrono::milliseconds draining_timeout =
        Worker::settings().epoch_draining_timeout;
    auto* w = Worker::onThisThread();
    ExponentialBackoffTimerNode* node =
        w->registerTimer(std::bind(&Sequencer::onDrainingTimerExpired,
                                   log_id_,
                                   epoch_,
                                   std::placeholders::_1),
                         draining_timeout,
                         draining_timeout);
    node->timer->activate();
    return Execution::COMPLETE;
  }

 private:
  logid_t log_id_;
  epoch_t epoch_;
};

// An one-off request for getting historical metadata from metadata logs.
// The request will self-destruct after result is returned
class GetHistoricalMetaDataRequest : public FireAndForgetRequest {
 public:
  GetHistoricalMetaDataRequest(logid_t log_id,
                               epoch_t current_epoch,
                               Sequencer::GetHistoricalMetaDataMode mode)
      : FireAndForgetRequest(RequestType::GET_HISTORICAL_METADATA),
        log_id_(log_id),
        current_epoch_(current_epoch),
        mode_(mode) {
    ld_check(!MetaDataLog::isMetaDataLog(log_id_));
  }

  void executionBody() override {
    switch (mode_) {
      case Sequencer::GetHistoricalMetaDataMode::PERIODIC:
        startPeriodicUpdateMetadataMap();
        break;
      case Sequencer::GetHistoricalMetaDataMode::IMMEDIATE:
        getMetaData();
    }
  }

  void getMetaData() {
    nodeset_finder_ = std::make_unique<NodeSetFinder>(
        log_id_,
        Worker::settings().read_historical_metadata_timeout,
        [this](Status status) { onResult(status); },
        // Sequencer itself should get metadata from the metadata log
        NodeSetFinder::Source::METADATA_LOG);

    nodeset_finder_->start();
  }

  void onResult(Status status) {
    ld_check(nodeset_finder_ != nullptr);
    std::shared_ptr<Sequencer> seq =
        Worker::onThisThread()->processor_->allSequencers().findSequencer(
            log_id_);

    bool should_retry = false;
    if (seq) {
      std::shared_ptr<const EpochMetaDataMap::Map> result_map;
      if (status == E::OK) {
        auto result = nodeset_finder_->getResult();
        ld_check(result != nullptr);
        result_map = result->getMetaDataMap();
      }

      should_retry = seq->onHistoricalMetaData(
          status, current_epoch_, std::move(result_map));
    }

    if (should_retry) {
      if (!backoff_timer_) {
        backoff_timer_ = std::make_unique<ExponentialBackoffTimer>(

            [this]() { getMetaData(); },
            Worker::settings().sequencer_historical_metadata_retry_delay);
        backoff_timer_->randomize();
      }
      backoff_timer_->activate();
    } else if (mode_ == Sequencer::GetHistoricalMetaDataMode::IMMEDIATE) {
      // job done, destroy the request
      destroy();
      // `this` is destroyed
    }
  }

  void scheduleHistoricalMetaDataUpdate() {
    // not applicable to metadatalog
    ld_check(!MetaDataLog::isMetaDataLog(log_id_));
    ld_check(update_metadata_map_timer_ != nullptr);
    ld_check(!update_metadata_map_timer_->isActive());

    std::shared_ptr<Sequencer> seq =
        Worker::onThisThread()->processor_->allSequencers().findSequencer(
            log_id_);
    if (seq == nullptr || seq->getState() != Sequencer::State::ACTIVE ||
        seq->getCurrentEpoch() != current_epoch_) {
      // destroy the request and stop scheduling more work
      // when a new sequencer is activate or sequencer is re-activate, it
      // will start a new request
      destroy();
      // the request doesn't exist anymore
      return;
    }
    getMetaData();
    update_metadata_map_timer_->activate(
        Worker::settings().update_metadata_map_interval);
    ld_spew("Setting update_metadata_map_timer_ interval to %lu msecs",
            Worker::settings().update_metadata_map_interval.count());
  }

  void startPeriodicUpdateMetadataMap() {
    // not applicable to metadatalog
    ld_check(!MetaDataLog::isMetaDataLog(log_id_));

    if (update_metadata_map_timer_ == nullptr) {
      update_metadata_map_timer_ = std::make_unique<Timer>(
          [this] { scheduleHistoricalMetaDataUpdate(); });
    }
    ld_check(!update_metadata_map_timer_->isActive());

    update_metadata_map_timer_->activate(
        Worker::settings().update_metadata_map_interval);
    ld_spew("Setting update_metadata_map_timer_ interval to %lu msecs",
            Worker::settings().update_metadata_map_interval.count());
  }

 private:
  const logid_t log_id_;
  const epoch_t current_epoch_;
  Sequencer::GetHistoricalMetaDataMode mode_{
      Sequencer::GetHistoricalMetaDataMode::IMMEDIATE};
  std::unique_ptr<Timer> update_metadata_map_timer_{nullptr};
  std::unique_ptr<NodeSetFinder> nodeset_finder_;
  std::unique_ptr<ExponentialBackoffTimer> backoff_timer_;
};

} // namespace

void Sequencer::startDrainingTimer(epoch_t draining) {
  std::unique_ptr<Request> req =
      std::make_unique<StartDrainTimerRequest>(log_id_, draining);
  parent_->getProcessor()->postImportant(req);
}

/* static */
void Sequencer::onDrainingTimerExpired(logid_t log_id,
                                       epoch_t draining_epoch,
                                       ExponentialBackoffTimerNode* node) {
  SCOPE_EXIT {
    // always cancel the timer
    ld_check(node->list_hook.is_linked());
    delete node;
  };

  std::shared_ptr<Sequencer> seq = findSequencer(log_id);
  if (!seq || seq->getState() == Sequencer::State::PERMANENT_ERROR) {
    // sequencer not exist or in permanent error, cancel timer
    ld_error("Sequencer for log %lu does not exist or is in permanent error.",
             log_id.val_);
    return;
  }

  WORKER_STAT_INCR(sequencer_draining_timedout);
  // consider draining finished with E::TIMEDOUT
  seq->noteDrainingCompleted(draining_epoch, Status::TIMEDOUT);
}

std::pair<Sequencer::UpdateMetaDataMapResult,
          std::shared_ptr<const EpochMetaDataMap>>
Sequencer::updateMetaDataMap(epoch_t epoch, const EpochMetaData& metadata) {
  ld_check(metadata.isValid());
  ld_check(epoch == metadata.h.epoch);

  std::shared_ptr<const EpochMetaDataMap> existing_map = metadata_map_.get();
  std::shared_ptr<const EpochMetaDataMap> new_map;
  if (existing_map != nullptr) {
    auto last = existing_map->getLastEpochMetaData();
    ld_check(last != nullptr);
    const EpochMetaData& last_metadata_entry = *last;
    const epoch_t existing_effective_until = existing_map->getEffectiveUntil();

    if (last_metadata_entry.h.effective_since > metadata.h.effective_since) {
      RATELIMIT_CRITICAL(std::chrono::seconds(10),
                         10,
                         "Sequencer for log %lu has gotten metadata for "
                         "epoch %u which has a smaller effective since than "
                         "epoch metadata it previous gotten! new metadata %s, "
                         "existing %s.",
                         log_id_.val_,
                         epoch.val_,
                         metadata.toString().c_str(),
                         last_metadata_entry.toString().c_str());
      STAT_INCR(stats_, sequencer_got_inconsistent_metadata);
      // likely we have a metadata corruption, re-read the metadata log in the
      // hope of finding out a consistent result
      clearMetaDataMapImpl();
      return std::make_pair(
          UpdateMetaDataMapResult::READ_METADATA_LOG, nullptr);
    }

    if (last_metadata_entry.h.effective_since == metadata.h.effective_since) {
      // the new epoch is essentially using the same epoch metadata
      if (!last_metadata_entry.identicalInMetaDataLog(metadata)) {
        RATELIMIT_CRITICAL(std::chrono::seconds(10),
                           10,
                           "Sequencer for log %lu has gotten metadata for "
                           "epoch %u from the epoch store but that metadata "
                           "does not match an already gotten metadata with the "
                           "same effective since of %u! new metadata: %s, "
                           "existing %s.",
                           log_id_.val_,
                           epoch.val_,
                           metadata.h.effective_since.val_,
                           metadata.toString().c_str(),
                           last_metadata_entry.toString().c_str());
        STAT_INCR(stats_, sequencer_got_inconsistent_metadata);
        // likely we have a metadata corruption, re-read the metadata log in the
        // hope of finding out a consistent result
        clearMetaDataMapImpl();
        return std::make_pair(
            UpdateMetaDataMapResult::READ_METADATA_LOG, nullptr);
      }

      // we should call this exactly once for each epoch in ascending order
      ld_check(existing_effective_until < epoch);

      // extend the effective until of the existing metadata map to _epoch_
      new_map = existing_map->withNewEffectiveUntil(epoch);
      ld_check(new_map != nullptr);
      metadata_map_.update(new_map);
      return std::make_pair(UpdateMetaDataMapResult::UPDATED, new_map);
    }

    ld_check(last_metadata_entry.h.effective_since <
             metadata.h.effective_since);
    if (existing_effective_until >= metadata.h.effective_since) {
      RATELIMIT_CRITICAL(std::chrono::seconds(10),
                         10,
                         "Sequencer for log %lu has gotten metadata for "
                         "epoch %u with effective since %u but last historical "
                         "metadata has a larger effective until %u! "
                         "new metadata: %s, existing %s.",
                         log_id_.val_,
                         epoch.val_,
                         metadata.h.effective_since.val_,
                         existing_effective_until.val_,
                         metadata.toString().c_str(),
                         last_metadata_entry.toString().c_str());
      STAT_INCR(stats_, sequencer_got_inconsistent_metadata);
      // likely we have a metadata corruption, re-read the metadata log in the
      // hope of finding out a consistent result
      clearMetaDataMapImpl();
      return std::make_pair(
          UpdateMetaDataMapResult::READ_METADATA_LOG, nullptr);
    }

    if (existing_effective_until ==
        epoch_t(metadata.h.effective_since.val_ - 1)) {
      // another case for optimization, the previous metadata is effective until
      // epoch _e_ and the newly gotten metadata has an effective since of
      // _e_ + 1, we know that there is no new metadata entries in between so it
      // is safe to just extend the map with the new entry

      // set the epoch of metadata == effective_since to match the content of
      // metadata logs
      auto metadata_insert = metadata;
      metadata_insert.h.epoch = metadata_insert.h.effective_since;
      new_map = existing_map->withNewEntry(metadata_insert, epoch);
      ld_check(new_map != nullptr);
      metadata_map_.update(new_map);
      return std::make_pair(UpdateMetaDataMapResult::UPDATED, new_map);
    }
  } else {
    // exisiting_map == nullptr
    if (metadata.h.effective_since == EPOCH_MIN) {
      // in case there is no existing metadata but the new metadata we got has
      // effective since of EPOCH_MIN, there is no need to read metadata log
      // since the entry has to be the earliest

      // set the epoch of metadata == effective_since to match the content of
      // metadata logs
      auto metadata_insert = metadata;
      metadata_insert.h.epoch = metadata_insert.h.effective_since;
      EpochMetaDataMap::Map map{{metadata.h.effective_since, metadata_insert}};
      new_map = EpochMetaDataMap::create(
          std::make_shared<const EpochMetaDataMap::Map>(std::move(map)), epoch);
      ld_check(new_map != nullptr);
      metadata_map_.update(new_map);
      return std::make_pair(UpdateMetaDataMapResult::UPDATED, new_map);
    }
  }

  // for all other case, read the metadata log for historical metadata
  clearMetaDataMapImpl();
  return std::make_pair(UpdateMetaDataMapResult::READ_METADATA_LOG, nullptr);
}

void Sequencer::getHistoricalMetaData(GetHistoricalMetaDataMode mode) {
  // this should never be called on a metadata logid. metadata logs always
  // have their epoch metadata with effective since == EPOCH_MIN.
  ld_check(!MetaDataLog::isMetaDataLog(log_id_));
  std::unique_ptr<Request> rq = std::make_unique<GetHistoricalMetaDataRequest>(
      log_id_, getCurrentEpoch(), mode);
  Worker::onThisThread()->processor_->postWithRetrying(rq);
}

bool Sequencer::onHistoricalMetaData(
    Status status,
    epoch_t request_epoch,
    std::shared_ptr<const EpochMetaDataMap::Map> historical_metadata) {
  if (getCurrentEpoch() > request_epoch) {
    // the epoch has advanced since we made the request, this is a stale result,
    // nothing to do
    return false;
  }

  std::shared_ptr<const EpochMetaDataMap> new_map;
  {
    folly::SharedMutex::WriteHolder write_lock(state_mutex_);
    auto current = getCurrentEpochSequencer();
    if (!current || current->getEpoch() > request_epoch) {
      // recheck staleness again inside the lock
      return false;
    }

    if (getState() == State::PERMANENT_ERROR ||
        getState() == State::UNAVAILABLE) {
      // sequencer is not in working state, do not update its historical
      // metadata
      RATELIMIT_INFO(std::chrono::seconds(10),
                     10,
                     "Not updating historical metadata for log %lu because "
                     "sequencer is in %s state.",
                     log_id_.val_,
                     stateString(getState()));
      return false;
    }

    const epoch_t current_epoch = current->getEpoch();
    // epoch of the sequencer must always advance
    ld_check(current_epoch == request_epoch);

    if (status != E::OK) {
      // time out occured or there is metadata corruption / data loss, retry
      // reading the metadata log content
      RATELIMIT_ERROR(std::chrono::seconds(10),
                      10,
                      "Error on getting historical metadata for log %lu "
                      "requested at epoch %u: %s. Will retry.",
                      log_id_.val_,
                      request_epoch.val_,
                      error_description(status));
      // need to schedule a retry
      return true;
    } else {
      ld_check(historical_metadata != nullptr);
      EpochMetaDataMap::Map map = *historical_metadata;
      ld_check(!map.empty());
      if (map.crbegin()->first > current_epoch) {
        // got a metadata log record whose effective since > current epoch,
        // the current sequencer might be stale. currently there is no way to
        // detect such preemption so just log a message here
        RATELIMIT_WARNING(std::chrono::seconds(10),
                          10,
                          "Got historical metadata for log %lu with an "
                          "effective since epoch %u larger than sequencer's "
                          "current epoch %u. The sequencer might be stale.",
                          log_id_.val_,
                          map.crbegin()->first.val_,
                          current_epoch.val_);
      }
      ld_check(current->getMetaData() != nullptr);
      auto current_metadata = *current->getMetaData();
      current_metadata.h.epoch = current_metadata.h.effective_since;

      // remove all entries >= current effective_since
      map.erase(map.lower_bound(current_metadata.h.effective_since), map.end());
      // insert the current metadata
      auto result = map.insert(
          std::make_pair(current_metadata.h.effective_since, current_metadata));
      ld_check(result.second);

      new_map = EpochMetaDataMap::create(
          std::make_shared<const EpochMetaDataMap::Map>(std::move(map)),
          /*effective_until=*/current_epoch);
      ld_check(new_map != nullptr);
      metadata_map_.update(new_map);
    }
  }

  ld_check(new_map != nullptr);
  ld_debug("Historical epoch metadata for log %lu: %s.",
           log_id_.val_,
           new_map->toString().c_str());
  onMetaDataMapUpdate(new_map);
  return false;
}

bool Sequencer::setNodeSetParamsInCurrentEpoch(
    epoch_t epoch,
    EpochMetaData::NodeSetParams params) {
  auto current = getCurrentEpochSequencer();
  if (!current || current->getEpoch() != epoch) {
    return false;
  }
  return current->setNodeSetParams(params);
}

void Sequencer::clearMetaDataMapImpl() {
  metadata_map_.update(nullptr);
}

void Sequencer::onPermanentError() {
  State old_state;
  std::shared_ptr<EpochSequencers> seqs_before;
  {
    folly::SharedMutex::WriteHolder write_lock(state_mutex_);
    old_state = getState();
    seqs_before = epoch_seqs_.get();
    ld_check(seqs_before != nullptr);
    if (old_state != State::PERMANENT_ERROR) {
      // 1) set current_epoch_ to EPOCH_INVALID;
      // 2) evict all managed epoch sequencers
      current_epoch_.store(EPOCH_INVALID.val_);
      setEpochSequencers(/*current=*/nullptr, /*draining=*/nullptr);
      clearMetaDataMapImpl();
      setState(State::PERMANENT_ERROR);
    }
  }

  if (old_state != State::PERMANENT_ERROR) {
    ld_info("Sequencer for log %lu transitioned from %s to %s.",
            log_id_.val_,
            stateString(old_state),
            stateString(State::PERMANENT_ERROR));

    // we are moving into PERMANENT_ERROR state, we should abort
    // all Appenders in this Sequencer's sliding window. This can be done
    // by broadcasting a special type of request to all Workers that will
    // make them abort all Appenders for this log_id_ from their
    // activeAppenders() maps.
    if (seqs_before->draining != nullptr) {
      seqs_before->draining->startDestruction();
    }
    if (seqs_before->current != nullptr) {
      seqs_before->current->startDestruction();
    }
  } else {
    // Sequencer is already in permanent error state, there should be
    // no valid EpochSequencers
    ld_check(seqs_before->draining == nullptr);
    ld_check(seqs_before->current == nullptr);
  }
}

void Sequencer::onActivationFailed() {
  State next;
  folly::SharedMutex::WriteHolder write_lock(state_mutex_);
  if (getState() != State::ACTIVATING) {
    // the sequencer has already been moved out of ACTIVATING state,
    // nothing to do.
    return;
  }

  if (isPreempted()) {
    // the sequencer can get preempted without a current epoch sequencer
    // (i.e., lost the activation race with a different node during first
    // time activation). Still move sequencer to PREEMPTED to indicat the
    // preemption.
    next = State::PREEMPTED;
  } else if (getCurrentEpochSequencer() == nullptr) {
    next = State::UNAVAILABLE;
  } else {
    // if the Sequencer has a valid epoch and is not preempted, move
    // it back to ACTIVE state. It is possible that the Sequencer can
    // still not take new appends because it has run out-of available ESNs.
    // In such case, the next Appender will still trigger reactivation of the
    // Sequencer.
    next = State::ACTIVE;
  }

  setState(next);
}

////////////////////////// Appender ////////////////////////////////

RunAppenderStatus Sequencer::runAppender(Appender* appender) {
  if (!appender || appender->started()) {
    ld_critical("INTERNAL ERROR: Appender %p not valid or already started for "
                "log %lu.",
                appender,
                getLogID().val_);
    ld_check(false);
    err = E::INVALID_PARAM;
    return RunAppenderStatus::ERROR_DELETE;
  }

  // note that we do not check the preemption status here: it is checked
  // in AppenderPrep before calling this function
  std::shared_ptr<EpochSequencer> current = getCurrentEpochSequencer();
  if (current == nullptr) {
    const bool activating = (getState() == State::ACTIVATING);
    err = (activating ? E::INPROGRESS : E::NOSEQUENCER);
    return RunAppenderStatus::ERROR_DELETE;
  }

  // we are going to run the Appender through `current'
  const epoch_t epoch = current->getEpoch();
  SteadyTimestamp now = SteadyTimestamp::now();
  atomic_fetch_max(last_append_, now.toMilliseconds());

  size_t payload_size = appender->getPayload()->size();

  ld_check(epoch != EPOCH_INVALID);
  if (appender->isStale(epoch)) {
    // processing this appender will break ordering as a client has already seen
    // a record in the log with the higher epoch
    err = E::STALE;
    return RunAppenderStatus::ERROR_DELETE;
  }

  if (appender->maxAppendersHardLimitReached()) {
    err = E::TEMPLIMIT;
    return RunAppenderStatus::ERROR_DELETE;
  }

  // Was this append previously sent to another sequencer, which gave it an LSN
  // and sent it to some storage noes, at least one of which returned
  // E::PREEMPTED?  To avoid silent dups, we hold it until after recovery,
  // and if the earlier version is recovered, don't write it now.
  if (appender->getLSNBeforeRedirect() != LSN_INVALID) {
    if (auto status = handleAppendWithLSNBeforeRedirect(appender)) {
      return status.value();
    }
  }

  // TEST: simulate bad hardware flipping bits in payload of STORE messages
  // See Settings::test_sequencer_corrupt_stores
  if (settings_->test_sequencer_corrupt_stores) {
    appender->TEST_corruptPayload();
  }

  RunAppenderStatus res = current->runAppender(appender);

  // Count the record's size towards log throughput estimate only if we're going
  // to actually run the appender.
  if (res == RunAppenderStatus::SUCCESS_KEEP) {
    append_rate_estimator_.addValue(
        payload_size, getRateEstimatorWindowSize(), now);
  }

  return res;
}

size_t Sequencer::getNumAppendsInFlight() const {
  auto current = getCurrentEpochSequencer();
  return current == nullptr ? 0 : current->getNumAppendsInFlight();
}

size_t Sequencer::getMaxWindowSize() const {
  auto current = getCurrentEpochSequencer();
  return current == nullptr ? 0 : current->getMaxWindowSize();
}

folly::Optional<EpochSequencerImmutableOptions>
Sequencer::getEpochSequencerOptions() const {
  auto current = getCurrentEpochSequencer();
  if (current == nullptr) {
    return folly::none;
  } else {
    return current->getImmutableOptions();
  }
}

std::chrono::milliseconds Sequencer::getTimeSinceLastAppend() const {
  auto t = last_append_.load();
  if (t.count() == 0) {
    return std::chrono::milliseconds::max();
  }
  return std::chrono::duration_cast<std::chrono::milliseconds>(
             std::chrono::steady_clock::now().time_since_epoch()) -
      t;
}

///////////////////// Last Released ///////////////////////

bool Sequencer::noteAppenderReaped(lsn_t reaped_lsn,
                                   epoch_t* last_released_epoch_out) {
  ld_check(last_released_epoch_out != nullptr);

  // do not advance last released if the epoch of the reaped appender is
  // known to be preempted
  if (lsn_to_epoch(reaped_lsn) <= preempted_epoch_.load().epoch) {
    err = E::PREEMPTED;
    return false;
  }

  lsn_t last_released_now = last_released_.load();
  do {
    *last_released_epoch_out = lsn_to_epoch(last_released_now);
    if (*last_released_epoch_out != lsn_to_epoch(reaped_lsn)) {
      err = E::STALE;
      return false;
    }
    // At this point the epochs of last_released_ and reaped_lsn_ are
    // known to be the same. Assert that within the same epoch last_released_
    // never allows releasing records that have not been fully replicated (and
    // their Appenders reaped). We do allow last_released_ to be ahead of
    // reaped if last_released_ is in a higher epoch than the last reaped lsn.
    // This can happen after log recovery finishes for the old epoch and
    // last_released_ is advanced to the next epoch. The epoch recovery
    // procedure guarantees safety in that case.
    ld_check(last_released_now <= reaped_lsn);

    // NOTE: we do not enforce last_released to be reaped_lsn - 1 in the
    // compare exchange statement. The reason is that in log recovery last
    // released is set to LNG of the current epoch but the operation cannot be
    // atomically synced with the this operation. Here we rely on the caller
    // - EpochSequencer::noteAppenderReaped() - to call this
    // function _only_ when reaping of this Appender increased the last known
    // good esn of the epoch, which means that all previous Appenders are
    // fully replicated. On the other hand, if LNG is not increased meaning
    // there were Appenders aborted for the epoch, this function won't be called
    // and last released won't get increased as Appenders are reaped for the
    // epoch.
  } while (
      !last_released_.compare_exchange_strong(last_released_now, reaped_lsn));
  return true;
}

void Sequencer::onNodeIsolated() {
  if (getState() == State::ACTIVE) {
    setUnavailable(UnavailabilityReason::ISOLATED);
  }
}

///////////////////// Preemption /////////////////////////

void Sequencer::notePreempted(epoch_t epoch, NodeID preempted_by) {
  ld_spew("PREEMPTED by %s for epoch %u of log %lu",
          preempted_by.toString().c_str(),
          epoch.val(),
          log_id_.val());
  Seal prev = atomic_fetch_max(preempted_epoch_, Seal(epoch, preempted_by));
  if (epoch >= getCurrentEpoch() && prev.epoch <= epoch) {
    // The seal epoch is larger equal than the current epoch, and we are
    // actually advancing preempted_epoch_. Move the sequencer into PREEMPTED
    // state
    folly::SharedMutex::WriteHolder write_lock(state_mutex_);
    // we need to check again under the state mutex since the sequencer may
    // get reactivated.
    // Only allow the transition from ACTIVE -> PREEMPTED. Specifically,
    // do not set the preempt state if seqeuncer is activating since it will
    // soon get a new epoch which is unlikely to be preempted
    if (epoch >= getCurrentEpoch() && getState() == State::ACTIVE) {
      setState(State::PREEMPTED);
    }
  }
}

NodeID Sequencer::checkIfPreempted(epoch_t epoch) const {
  Seal preempted = preempted_epoch_.load();
  if (epoch <= preempted.epoch) {
    return preempted.seq_node;
  }
  return NodeID(); // invalid NodeID
}

bool Sequencer::isPreempted() const {
  return checkIfPreempted(getCurrentEpoch()).isNodeID();
}

Seal Sequencer::getSealRecord() const {
  return preempted_epoch_.load();
}

////////// Recovery /////////////////

int Sequencer::startRecovery(std::chrono::milliseconds delay) {
  auto current_epoch = getCurrentEpochSequencer();
  if (current_epoch == nullptr) {
    ld_warning("Not starting log recovery for log %lu because the log is "
               "removed from config or sequencer in permanent error. "
               "Current sequencer state: %s",
               log_id_.val_,
               stateString(getState()));
    STAT_INCR(stats_, recovery_completed);
    return 0;
  }

  epoch_t epoch = current_epoch->getEpoch();
  std::shared_ptr<const EpochMetaData> metadata = current_epoch->getMetaData();
  ld_check(metadata);
  ld_check(epoch == metadata->h.epoch);
  if (epoch > EPOCH_MIN) {
    auto recovery_rq =
        new LogRecoveryRequest(log_id_, epoch, std::move(metadata), delay);
    ld_check(recovery_rq);
    worker_id_t target_worker = recoveryWorkerThread();
    if (target_worker.val_ >= 0) {
      recovery_rq->setTargetWorker(target_worker);
    }

    std::unique_ptr<Request> rq(recovery_rq);
    return Worker::onThisThread()->processor_->postWithRetrying(rq);
  } else {
    // there's nothing to recover since we are at epoch 1.
    TailRecord tail_empty_log{
        TailRecordHeader{log_id_,
                         LSN_INVALID,                       // tail LSN 0
                         0,                                 // timestamp
                         {BYTE_OFFSET_INVALID},             // deprecated
                         TailRecordHeader::CHECKSUM_PARITY, // flags
                         {}},
        OffsetMap::fromLegacy(0),
        std::shared_ptr<PayloadHolder>()};

    onRecoveryCompleted(E::OK,
                        epoch,
                        std::move(tail_empty_log),
                        std::unique_ptr<RecoveredLSNs>());
  }
  return 0;
}

bool Sequencer::isRecoveryComplete() const {
  auto last_released = last_released_.load();
  return (last_released_ != LSN_INVALID) &&
      (lsn_to_epoch(last_released) == getCurrentEpoch());
}

void Sequencer::onRecoveryCompleted(
    Status status,
    epoch_t epoch,
    TailRecord previous_epoch_tail,
    std::unique_ptr<const RecoveredLSNs> recovered_lsns) {
  int rv;
  lsn_t lng;

  ld_check(epoch != EPOCH_INVALID);
  STAT_INCR(stats_, recovery_completed);
  // This lock is used to block other writers, but not other readers.  So the
  // order of assignments below is very important, and we rely on the default
  // memory order for std::atomic oprations being sequentially consistent
  // ordering.  In particular:
  //
  // recovered_lsns_ must be udpated BEFORE calling advanceLastReleased().
  //
  // advanceLastReleased() must be called BEFORE processRedirectedRecords().
  {
    folly::SharedMutex::WriteHolder write_lock(state_mutex_);
    if (getState() == State::PERMANENT_ERROR ||
        getState() == State::UNAVAILABLE) {
      ld_info("Not advancing last released for log %lu because sequencer "
              "is in %s state.",
              log_id_.val_,
              stateString(getState()));
      return;
    }

    std::shared_ptr<EpochSequencer> current = getCurrentEpochSequencer();
    if (current == nullptr) {
      // proceed only if we have a valid current sequencer. It is possilbe
      // that sequencer became unavailable and then gets reactivated, leaving
      // the sequencer in ACTIVATING state but w/o a current valid epoch
      // sequencer
      RATELIMIT_INFO(std::chrono::seconds(10),
                     10,
                     "No valid current epoch sequencer for log %lu when "
                     "recovery with epoch %u completes. Sequencer might become "
                     "unavaialble before.",
                     log_id_.val_,
                     epoch.val_);
      return;
    }

    ld_check(current->getEpoch() == getCurrentEpoch());
    epoch_t current_epoch = current->getEpoch();
    ld_check(epoch <= current_epoch);

    if (epoch < current_epoch) {
      ld_warning("Log recovery initiated for next_epoch %u completed after "
                 "the Sequencer was re-activated into a higher epoch %u. This "
                 "is benign and should be rare.",
                 epoch.val_,
                 current_epoch.val_);
      return;
    }

    // advance last released lsn to the LNG of the current epoch
    if (status == E::OK) {
      ld_check(epoch == current_epoch);
      STAT_INCR(stats_, recovery_success);
      lng = current->getLastKnownGood();

      // the equality below is guaranteed by the fact that lng_ changes
      // epochs only under state_mutex_ that we got before we read
      // current_epoch and continue to hold.
      ld_check(lsn_to_epoch(lng) == current_epoch);

      // This assignment must happen BEFORE calling advanceLastReleased().
      if (recovered_lsns) {
        recovered_lsns_.update(std::move(recovered_lsns));
      } else {
        recovered_lsns_.update(std::make_shared<RecoveredLSNs>());
      }

      // advance last released must happen BEFORE processRedirectedRecords().
      if (lsn_to_epoch(last_released_.load()) < lsn_to_epoch(lng)) {
        last_released_.store(lng);
      } else {
        ld_critical("INTERNAL ERROR: Last released %s is already larger or "
                    "equal than the epoch of the current LNG: %s for log %lu.",
                    lsn_to_string(last_released_.load()).c_str(),
                    lsn_to_string(lng).c_str(),
                    log_id_.val_);
        ld_check(false);
        return;
      }

      // On recovery success, tail record must be valid and contains cumulative
      // byte offset instead of offset within epoch
      ld_check(previous_epoch_tail.isValid());
      ld_check(!previous_epoch_tail.containOffsetWithinEpoch());

      tail_record_previous_epoch_.update(
          std::make_shared<TailRecord>(std::move(previous_epoch_tail)));
    }
  }

  // perform other completion jobs outside of the state_mutex_
  switch (status) {
    case E::OK:
      // Recovery is done and all records belonging to smaller epochs,
      // as well as all fully stored records in the current epoch have
      // been released. Make sure that all storage nodes get the memo.
      sendReleases();
      processRedirectedRecords();
      break;

    case E::PREEMPTED:
      STAT_INCR(stats_, recovery_preempted);
      break;

    case E::NOTINCONFIG:
      // this log is no longer in config
      setUnavailable(UnavailabilityReason::LOG_REMOVED);
      STAT_INCR(stats_, recovery_failed);
      break;

    default:
      // for all other errors, retry after a delay
      STAT_INCR(stats_, recovery_failed);
      RATELIMIT_ERROR(std::chrono::seconds(1),
                      10,
                      "Log recovery failed for log %lu: %s. Will retry.",
                      log_id_.val_,
                      error_description(status));

      STAT_INCR(stats_, recovery_scheduled);
      rv = startRecovery(std::chrono::seconds(1));
      if (rv != 0) {
        ld_check(err == E::SHUTDOWN);
        ld_error("Failed to restart recovery for log %lu: %s",
                 log_id_.val_,
                 error_description(err));
        // we are shutting down, leave recovery to our successor
      }
      break;
  }
}

////////////////////// Misc and Utilities //////////////////

lsn_t Sequencer::getNextLSN() const {
  auto current = getCurrentEpochSequencer();
  return current != nullptr ? current->getNextLSN() : LSN_INVALID;
}

std::shared_ptr<const EpochMetaData> Sequencer::getCurrentMetaData() const {
  auto current = getCurrentEpochSequencer();
  return current != nullptr ? current->getMetaData() : nullptr;
}

std::shared_ptr<const EpochMetaDataMap> Sequencer::getMetaDataMap() const {
  return metadata_map_.get();
}

bool Sequencer::hasAvailableLsns() const {
  auto current = getCurrentEpochSequencer();
  return current != nullptr ? current->hasAvailableLsns() : false;
}

size_t Sequencer::getLastKnownGood() const {
  auto current = getCurrentEpochSequencer();
  return current != nullptr ? current->getLastKnownGood() : LSN_INVALID;
}

std::shared_ptr<CopySetManager> Sequencer::getCurrentCopySetManager() const {
  auto current = getCurrentEpochSequencer();
  return current != nullptr ? current->getCopySetManager() : nullptr;
}

epoch_t Sequencer::getDrainingEpoch() const {
  auto draining = getDrainingEpochSequencer();
  return draining != nullptr ? draining->getEpoch() : EPOCH_INVALID;
}

const char* Sequencer::stateString(State st) {
  switch (st) {
    case State::UNAVAILABLE:
      return "UNAVAILABLE";
    case State::ACTIVATING:
      return "ACTIVATING";
    case State::ACTIVE:
      return "ACTIVE";
    case State::PREEMPTED:
      return "PREEMPTED";
    case State::PERMANENT_ERROR:
      return "PERMANENT_ERROR";
  }

  return "UNKNOWN";
}

void Sequencer::setState(State state) {
  last_state_change_timestamp_ =
      std::chrono::steady_clock::now().time_since_epoch();
  state_.store(state);
}

void Sequencer::setEpochSequencers(std::shared_ptr<EpochSequencer> current,
                                   std::shared_ptr<EpochSequencer> draining) {
  epoch_seqs_.update(std::make_shared<EpochSequencers>(
      EpochSequencers{std::move(current), std::move(draining)}));
}

std::shared_ptr<EpochSequencer>
Sequencer::createEpochSequencer(epoch_t epoch,
                                std::unique_ptr<EpochMetaData> metadata) {
  auto cfg = getClusterConfig();
  const std::shared_ptr<LogsConfig::LogGroupNode> logcfg =
      cfg->getLogGroupByIDShared(log_id_);
  if (!logcfg) {
    RATELIMIT_WARNING(std::chrono::seconds(5),
                      5,
                      "Failed to create epoch sequencer of log %lu epoch %u "
                      "because the log has been removed from the config.",
                      log_id_.val_,
                      epoch.val_);
    return nullptr;
  }

  auto local_settings = settings_.get();
  EpochSequencerImmutableOptions immutable_options(
      logcfg->attrs(), *local_settings);

  auto epoch_seq = std::make_shared<EpochSequencer>(
      log_id_, epoch, std::move(metadata), immutable_options, this);

  // initialize copyset manager for the epoch
  epoch_seq->createOrUpdateCopySetManager(
      cfg, getNodesConfiguration(), *local_settings);
  ld_check(epoch_seq->getCopySetManager() != nullptr);
  return epoch_seq;
}

std::shared_ptr<const configuration::nodes::NodesConfiguration>
Sequencer::getNodesConfiguration() const {
  return Worker::onThisThread()->getNodesConfiguration();
}

std::shared_ptr<Configuration> Sequencer::getClusterConfig() const {
  return Worker::getConfig();
}

Processor* Sequencer::getProcessor() const {
  return (nullptr != parent_) ? parent_->getProcessor()
                              : Worker::onThisThread()->processor_;
}

std::pair<std::shared_ptr<EpochSequencer>, std::shared_ptr<EpochSequencer>>
Sequencer::getEpochSequencers() const {
  auto seqs = epoch_seqs_.get();
  return std::make_pair(seqs->current, seqs->draining);
}

void Sequencer::deactivateSequencer() {
  setUnavailable(UnavailabilityReason::DEACTIVATED_BY_ADMIN);
}

bool Sequencer::checkNoRedirectUntil() const {
  return std::chrono::steady_clock::now().time_since_epoch() <
      no_redirect_until_.load();
}

void Sequencer::setNoRedirectUntil(
    std::chrono::steady_clock::duration duration) {
  auto until =
      (duration == std::chrono::steady_clock::duration::max()
           ? duration
           : std::chrono::steady_clock::now().time_since_epoch() + duration);
  atomic_fetch_max(no_redirect_until_, until);
}

void Sequencer::clearNoRedirectUntil() {
  no_redirect_until_ = std::chrono::steady_clock::duration::min();
}

///////////////////////////////////////////////

/* static */
std::shared_ptr<Sequencer> Sequencer::findSequencer(logid_t log_id) {
  Worker* worker = Worker::onThisThread();
  return worker->processor_->allSequencers().findSequencer(log_id);
}

MetaDataLogWriter* Sequencer::getMetaDataLogWriter() const {
  return metadata_writer_.get();
  ;
}

/* static */
const char* Sequencer::activateResultToString(ActivateResult result) {
  switch (result) {
    case ActivateResult::RECOVERY:
      return "RECOVERY";
    case ActivateResult::GRACEFUL_DRAINING:
      return "GRACEFUL_DRAINING";
    case ActivateResult::FAILED:
      return "FAILED";
  }
  ld_check(false);
  return "UNKNOWN";
}

Sequencer::~Sequencer() {}

////////////// Releases and Periodical Releases //////////////

void Sequencer::schedulePeriodicReleases() {
  periodic_releases_->schedule();
}

void Sequencer::startPeriodicReleasesBroadcast() {
  periodic_releases_->startBroadcasting();
}

void Sequencer::onStorageNodeDisconnect(node_index_t node_idx) {
  periodic_releases_->invalidateLastLsnsOfNode(node_idx);
  schedulePeriodicReleases();
}

void Sequencer::onMetaDataMapUpdate(
    const std::shared_ptr<const EpochMetaDataMap>& updated_metadata) {
  periodic_releases_->onMetaDataMapUpdate(updated_metadata);
  // schedule periodical release as there might be new storage shards added
  // to the union of historical storage sets
  schedulePeriodicReleases();
}

int Sequencer::sendReleases(lsn_t lsn,
                            ReleaseType release_type,
                            const SendReleasesPred& pred) {
  // caller should ensure not passing an INVALID LSN
  ld_check(lsn != LSN_INVALID);
  ld_check(epoch_valid(lsn_to_epoch(lsn)));
  epoch_t epoch = lsn_to_epoch(lsn);
  const RecordID rid{lsn_to_esn(lsn), epoch, getLogID()};

  ld_spew("Sending %s RELEASE messages for record %s",
          release_type_to_string(release_type).c_str(),
          rid.toString().c_str());

  if (release_type == ReleaseType::PER_EPOCH &&
      !epochMetaDataAvailable(epoch)) {
    ld_debug("Trying to send per-epoch RELEASE messages, but epoch metadata "
             "not yet available for record %s",
             rid.toString().c_str());
    err = E::AGAIN;
    return -1;
  }

  auto metadata_map = getMetaDataMap();
  if (metadata_map == nullptr) {
    RATELIMIT_INFO(std::chrono::seconds(10),
                   2,
                   "Can't send %s RELEASE messages for record %s "
                   "since the sequencer does not yet have the historical "
                   "epoch metadata map.",
                   release_type_to_string(release_type).c_str(),
                   rid.toString().c_str());
    err = E::AGAIN;
    return -1;
  }

  Worker* w = Worker::onThisThread();
  const auto& nodes_configuration = w->getNodesConfiguration();

  // this also filters out non-storage nodes and nodes not in config
  auto all_shards = metadata_map->getUnionStorageSet(*nodes_configuration);

  ld_check(all_shards != nullptr);
  Sender& sender = w->sender();

  // Header is the same for all messages we send below.
  const RELEASE_Header header{rid, release_type};

  int rv = 0;
  for (const auto& shard : *all_shards) {
    // Skip shards that fail the predicate, if any.
    if (!pred || pred(lsn, release_type, shard)) {
      auto h = header;
      h.shard = shard.shard();
      if (sender.sendMessage(
              std::make_unique<RELEASE_Message>(h), shard.asNodeID()) != 0) {
        RATELIMIT_DEBUG(
            std::chrono::seconds(1),
            1,
            "Failed to send a RELEASE message for record %s to %s: %s",
            rid.toString().c_str(),
            shard.toString().c_str(),
            error_description(err));
        rv = -1;
      }
    }
  }
  return rv;
}

void Sequencer::sendReleases() {
  // Load lng_ first to avoid case where lng_ and last_released_ are
  // concurrently updated to the same lsn, but we could see
  // last_released_ < lng_ if last_released_ was loaded first. It is okay to
  // see last_released_ > lng_. In this case, we can safely assume there has
  // been a concurrent update, and we will want to send a single, global
  // release message.
  lsn_t lng = getLastKnownGood();
  lsn_t last_released = last_released_.load();

  // First send global release message, then possibly send per-epoch release
  // message. No need to send per-epoch release message if ESN is invalid (no
  // released records in epoch yet), or global last_released is caught up to
  // per-epoch lng.
  bool send_failed = false;
  if (last_released != LSN_INVALID) {
    send_failed = (sendReleases(last_released, ReleaseType::GLOBAL) != 0);
  }

  if (lsn_to_esn(lng) != ESN_INVALID && lng > last_released) {
    send_failed |= sendReleases(lng, ReleaseType::PER_EPOCH) != 0;
  }

  if (send_failed) {
    // At least one send failed. Use PeriodicReleases to resend the message(s).
    schedulePeriodicReleases();
  }
}

void Sequencer::onSequencerMetaDataRead(epoch_t effective_since) {
  epoch_t::raw_type effective_since_prev =
      atomic_fetch_max(max_effective_since_read_, effective_since.val_);
  ld_debug("Sequencer metadata read, effective_since=%" PRIu32 " "
           "effective_since_prev=%" PRIu32,
           effective_since.val_,
           effective_since_prev);
}

bool Sequencer::epochMetaDataAvailable(epoch_t epoch) const {
  // Atomically get EpochMetaData for the sequencer's current epoch.
  auto metadata = getCurrentMetaData();
  if (metadata == nullptr) {
    return false;
  }

  // *metadata is immutable, so we can safely read from it, even if there are
  // concurrent calls to active(). The effective_until is given by the
  // effective_since of the *next* metadata record, which we do not know. But
  // we can use the sequencer's current epoch as a conservative lower estimate.
  // It was stored in metadata_->h.epoch during creation of the EpochMetaData.
  epoch_t metadata_effective_until = metadata->h.epoch;
  epoch_t metadata_effective_since = metadata->h.effective_since;
  ld_check(metadata_effective_since <= metadata_effective_until);

  // An epoch is safe to read from (and it is thus safe to send per-epoch
  // RELEASE message for this epoch), if its metadata has been written and the
  // metadata is available for reads. The "metadata of epoch e" is the unique
  // metadata such that e falls in between the metadata's effective_since and
  // its effective_until.
  //
  // In our case, if metadata_effective_since is less-than or equal
  // max_effective_since_read, we know that metadata_ has been written and is
  // readable, because all metadata is written strictly and synchronously in
  // order of effective_since.
  //
  // The metadata is valid from metadata_effective_since to (at least)
  // metadata_effective_until, so any epoch that falls in between that range
  // (inclusive) is covered by that metadata. Also, since metadata is written
  // strictly in order, we know that there is metadata available for reads for
  // any epoch below metadata_effective_since. It follows that any epoch
  // less-than or equal metadata_effective_until is covered.
  return epoch <= metadata_effective_until &&
      metadata_effective_since.val_ <= max_effective_since_read_.load();
}

folly::Optional<RunAppenderStatus>
Sequencer::handleAppendWithLSNBeforeRedirect(Appender* appender) {
  // MetaSequencer objects can be destroyed, and probably will be before the
  // Request is run.  However, MetaSequencer records are never redirected so
  // we don't need to worry about them. Non-MetaSequencer Sequencer objects
  // are never destroyed.
  ld_check(!MetaDataLog::isMetaDataLog(log_id_));
  if (MetaDataLog::isMetaDataLog(log_id_)) {
    RATELIMIT_ERROR(std::chrono::seconds(1),
                    1,
                    "Got a pre-empted metadata append on log %lu, LSN before "
                    "redirect %s.  Processing it anyway.",
                    log_id_.val_,
                    lsn_to_string(appender->getLSNBeforeRedirect()).c_str());
    return folly::none;
  }

  if (isRecoveryComplete()) {
    ld_assert(recovered_lsns_.get());
    switch (recovered_lsns_.get()->recoveryStatus(
        appender->getLSNBeforeRedirect())) {
      case RecoveredLSNs::RecoveryStatus::REPLICATED: {
        STAT_INCR(stats_, append_redirected_previously_stored);
        // Don't store anything, reply to the append with the previous LSN.
        // Since we don't know the timestamp for the stored LSN, make this
        // obvious to the client by returning a zero timestamp.
        appender->overrideTimestamp(RecordTimestamp::zero());
        appender->sendReply(appender->getLSNBeforeRedirect(), Status::OK);
        return RunAppenderStatus::SUCCESS_DELETE;
      }
      case RecoveredLSNs::RecoveryStatus::UNKNOWN: {
        STAT_INCR(stats_, append_redirected_maybe_stored);
        // Can we send a CANCELLED?
        std::unique_ptr<SocketProxy> socket_proxy{
            appender->getClientSocketProxy()};
        if (socket_proxy && !socket_proxy->isClosed()) {
          err = E::CANCELLED;
          return RunAppenderStatus::ERROR_DELETE;
        } else {
          // We don't know whether the record was recovered, and we can't tell
          // the user we don't know.  So just fall back on the old behavior:
          // append, ensuring we get at least one copy but risking a silent
          // duplicate.
          break;
        }
      }
      case RecoveredLSNs::RecoveryStatus::HOLE:
        STAT_INCR(stats_, append_redirected_newly_stored);
        // The record wasn't recovered, so fall through to the normal append
        // path, as if the previous attempt never happened.
        break;
    }
  } else {
    // Save append until recovery is complete.
    std::unique_ptr<Appender> appender_ptr(appender);
    bool success =
        Worker::onThisThread()->previouslyRedirectedAppends().bufferAppender(
            log_id_, appender_ptr);
    if (success) {
      return RunAppenderStatus::SUCCESS_KEEP;
    }

    // If the buffer is full, just append anyway.  Better silent dups than
    // dropping records.
    RATELIMIT_ERROR(
        std::chrono::seconds(1),
        1,
        "Might get silent duplicate on log %lu, first LSN %s: wanted to hold "
        "pre-empted append until recovery was complete, to see if earlier "
        "version of it will be recovered, but buffer was full, so appending "
        "it now.",
        log_id_.val_,
        lsn_to_string(appender->getLSNBeforeRedirect()).c_str());
    appender_ptr.release();
  }

  return folly::none;
}

void Sequencer::processRedirectedRecords(Status, logid_t logid) {
  Worker* worker = Worker::onThisThread();

  ld_check(logid == log_id_);

  worker->previouslyRedirectedAppends().processQueue(
      logid, AppenderBuffer::processBufferedAppender);
}

using ProcessRedirectedRecordsRequest =
    CompletionRequestBase<std::function, logid_t>;

void Sequencer::processRedirectedRecords() {
  Processor* processor = Worker::onThisThread()->processor_;

  // MetaSequencer objects can be destroyed, and probably will be before the
  // Request is run.  However, MetaSequencer records are never redirected so we
  // don't need to worry about them. Non-MetaSequencer Sequencer objects are
  // never destroyed.
  if (MetaDataLog::isMetaDataLog(log_id_)) {
    return;
  }

  // Post a ProcessRedirectedRecordsRequest to all workers
  for (worker_id_t worker_idx{0};
       worker_idx.val_ < processor->getWorkerCount(WorkerType::GENERAL);
       ++worker_idx.val_) {
    std::unique_ptr<Request> rq =
        std::make_unique<ProcessRedirectedRecordsRequest>(
            [](Status st, logid_t logid) {
              Worker* worker = Worker::onThisThread();
              const auto sequencer =
                  worker->processor_->allSequencers().findSequencer(
                      logid); // Mon-MetaSequencer Sequencer objects are never
                              // taken out of AllSequencers, they exist until
                              // all workers have stopped and the processor
                              // object is being destroyed.
              ld_check(sequencer);

              sequencer->processRedirectedRecords(st, logid);
            },
            worker_id_t{worker_idx},
            E::OK,
            log_id_);

    int rv = processor->postWithRetrying(rq);
    if (rv != 0 && err != E::SHUTDOWN) {
      ld_error("Got unexpected err %s for Processor::postWithRetrying() "
               "with log %lu",
               error_name(err),
               log_id_.val_);
      ld_check(false);
    }
  }
}

void Sequencer::noteConfigurationChanged(
    std::shared_ptr<Configuration> cfg,
    std::shared_ptr<const configuration::nodes::NodesConfiguration>
        nodes_configuration,
    bool is_sequencer_node) {
  // Note: nodeset and replication factor are updated upon sequencer
  // activation, with the value from the epochstore. Here we only update
  // properties that do not require an epoch bump.
  const std::shared_ptr<LogsConfig::LogGroupNode> log =
      cfg->getLogGroupByIDShared(log_id_);
  if (!log) {
    // Handle deletion of logs. Drain/abort all epoches and put the Sequencer
    // into UNAVAILABLE state.
    setUnavailable(UnavailabilityReason::LOG_REMOVED);
    return;
  }

  if (!is_sequencer_node) {
    // This node is not a sequencer node anymore
    setUnavailable(UnavailabilityReason::NOT_A_SEQUENCER_NODE);
  }

  // If nodes were added or removed in config, update nodeset and
  // copyset selector for both current and draining epochs (if any)
  auto seqs = epoch_seqs_.get();
  auto local_settings = settings_.get();
  if (seqs->current) {
    seqs->current->noteConfigurationChanged(
        cfg, nodes_configuration, *local_settings);
  }
  if (seqs->draining) {
    seqs->draining->noteConfigurationChanged(
        cfg, nodes_configuration, *local_settings);
  }
}

void Sequencer::setUnavailable(UnavailabilityReason r) {
  // only drain if the node is not a sequencer node anymore; for other cases,
  // abort all appenders
  bool drain = (r == UnavailabilityReason::NOT_A_SEQUENCER_NODE ||
                        r == UnavailabilityReason::DEACTIVATED_BY_ADMIN
                    ? true
                    : false);
  State old_state;
  std::shared_ptr<EpochSequencers> seqs_before;
  {
    folly::SharedMutex::WriteHolder write_lock(state_mutex_);
    old_state = getState();
    seqs_before = epoch_seqs_.get();
    ld_check(seqs_before != nullptr);
    if (old_state == State::UNAVAILABLE ||
        old_state == State::PERMANENT_ERROR) {
      ld_assert(current_epoch_.load() == EPOCH_INVALID.val_);
      return;
    }
    current_epoch_.store(EPOCH_INVALID.val_);
    clearMetaDataMapImpl();
    // evict all epochs
    if (drain) {
      setEpochSequencers(
          /*current=*/nullptr, /*draining=*/seqs_before->current);
    } else {
      setEpochSequencers(/*current=*/nullptr, /*draining=*/nullptr);
    }
    setState(State::UNAVAILABLE);
  }

  switch (r) {
    case UnavailabilityReason::NOT_A_SEQUENCER_NODE:
      RATELIMIT_WARNING(std::chrono::seconds(1),
                        10,
                        "Transitioning sequencer for log %lu into UNAVAILABLE "
                        "state because this nodes is not a sequencer node "
                        "anymore.",
                        log_id_.val_);
      STAT_INCR(stats_, sequencer_unavailable_not_sequencer_node);
      break;
    case UnavailabilityReason::LOG_REMOVED:
      RATELIMIT_WARNING(std::chrono::seconds(1),
                        10,
                        "Transitioning sequencer into UNAVAILABLE state "
                        "because log %lu is no longer in the config file.",
                        log_id_.val_);
      STAT_INCR(stats_, sequencer_unavailable_log_removed_from_config);
      break;
    case UnavailabilityReason::ISOLATED:
      RATELIMIT_WARNING(std::chrono::seconds(1),
                        10,
                        "Transitioning sequencer into UNAVAILABLE state "
                        "because this node was isolated for %lu seconds.",
                        settings_->isolated_sequencer_ttl.count());
      STAT_INCR(stats_, sequencer_unavailable_node_isolated);
      break;
    case UnavailabilityReason::DEACTIVATED_BY_ADMIN:
      RATELIMIT_WARNING(
          std::chrono::seconds(1),
          10,
          "Transitioning sequencer into UNAVAILABLE state "
          "because admin command invoked deactivation of log %lu.",
          log_id_.val_);
      STAT_INCR(stats_, sequencer_unavailable_admin_deactivated);
      break;
    case UnavailabilityReason::SHUTDOWN:
      break;
  }
  // destroy all epoch sequencers evicted, start draining the current epoch if
  // necessary
  if (seqs_before->draining != nullptr) {
    seqs_before->draining->startDestruction();
  }
  if (seqs_before->current != nullptr) {
    if (drain) {
      seqs_before->current->startDraining();
      if (seqs_before->current->getState() !=
          EpochSequencer::State::QUIESCENT) {
        // if draining is not finished, start a timer for the draining
        // operation.
        startDrainingTimer(seqs_before->current->getEpoch());
      }
    } else {
      seqs_before->current->startDestruction();
    }
  }

  // Tell sequencer activator that this sequencer is not activating anymore.
  if (parent_ != nullptr) {
    SequencerBackgroundActivator::requestNotifyCompletion(
        parent_->getProcessor(), log_id_, E::ABORTED);
  }
}

void Sequencer::shutdown() {
  setUnavailable(UnavailabilityReason::SHUTDOWN);
}

void Sequencer::noteReleaseSuccessful(ShardID shard,
                                      lsn_t released_lsn,
                                      ReleaseType release_type) {
  periodic_releases_->noteReleaseSuccessful(shard, released_lsn, release_type);
}

bool Sequencer::checkNodeSet() const {
  auto current_epoch = getCurrentEpochSequencer();
  if (current_epoch == nullptr) {
    // sequencer is still activating and does not have the initial
    // nodeset fetched from epoch store
    err = E::INPROGRESS;
    return false;
  }

  return current_epoch->checkNodeSet();
}

std::shared_ptr<const TailRecord> Sequencer::getTailRecord() const {
  // the read lock is needed to prevent the race that sequencer
  // activates between getting previous epoch offset and in-epoch offset
  // of the current epoch
  folly::SharedMutex::ReadHolder read_lock(state_mutex_);

  if (getState() == State::PERMANENT_ERROR ||
      getState() == State::UNAVAILABLE) {
    return nullptr;
  }

  auto current_epoch = getCurrentEpochSequencer();
  if (current_epoch == nullptr) {
    return nullptr;
  }

  if (!isRecoveryComplete()) {
    return nullptr;
  }

  auto previous_tail = tail_record_previous_epoch_.get();
  // must have a valid previous tail after log recovery is compeleted
  ld_check(previous_tail != nullptr);
  ld_check(!previous_tail->containOffsetWithinEpoch());
  ld_check(lsn_to_epoch(previous_tail->header.lsn) < current_epoch->getEpoch());

  auto current_epoch_tail = current_epoch->getTailRecord();
  if (current_epoch_tail == nullptr) {
    // the current epoch has no released record yet, the log tail is still
    // at the previous epoch
    return previous_tail;
  }

  // The tail is at the current epoch, but we need to compute the accumulative
  // byteoffset and replace the one in the per-epoch tail
  auto ret_tail = std::make_shared<TailRecord>(*current_epoch_tail);
  ld_check(ret_tail->containOffsetWithinEpoch());

  ret_tail->header.flags &= ~TailRecordHeader::OFFSET_WITHIN_EPOCH;
  if (!ret_tail->offsets_map_.isValid() ||
      !previous_tail->offsets_map_.isValid()) {
    ret_tail->offsets_map_.clear();
  } else {
    ret_tail->offsets_map_ = OffsetMap::mergeOffsets(
        previous_tail->offsets_map_, current_epoch_tail->offsets_map_);
  }

  ld_check(!ret_tail->containOffsetWithinEpoch());
  return ret_tail;
}

OffsetMap Sequencer::getEpochOffsetMap() const {
  if (!isRecoveryComplete()) {
    return OffsetMap();
  }

  auto previous_tail = tail_record_previous_epoch_.get();
  if (previous_tail == nullptr) {
    return OffsetMap();
  }

  ld_check(!previous_tail->containOffsetWithinEpoch());
  return previous_tail->offsets_map_;
}

std::pair<int64_t, std::chrono::milliseconds>
Sequencer::appendRateEstimate() const {
  return append_rate_estimator_.getRate(
      getRateEstimatorWindowSize(), SteadyTimestamp::now());
}

std::chrono::milliseconds Sequencer::getRateEstimatorWindowSize() const {
  std::chrono::milliseconds window = settings_->nodeset_adjustment_period;
  if (window.count() <= 0) {
    window = std::chrono::hours(1);
  }
  return window;
}

}} // namespace facebook::logdevice
