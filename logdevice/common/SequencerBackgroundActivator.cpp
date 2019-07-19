/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/common/SequencerBackgroundActivator.h"

#include "logdevice/common/AllSequencers.h"
#include "logdevice/common/EpochMetaDataUpdater.h"
#include "logdevice/common/EpochSequencer.h"
#include "logdevice/common/MetaDataLog.h"
#include "logdevice/common/MetaDataLogWriter.h"
#include "logdevice/common/NodeSetSelectorFactory.h"
#include "logdevice/common/Processor.h"
#include "logdevice/common/Worker.h"

namespace facebook { namespace logdevice {
using UpdateResult = EpochMetaData::UpdateResult;
using ReactivationDecision = SequencerBackgroundActivator::ReactivationDecision;

SequencerBackgroundActivator::SequencerBackgroundActivator()
    : nodeset_adjustment_period_(Worker::settings().nodeset_adjustment_period),
      unconditional_nodeset_randomization_enabled_(
          Worker::settings().nodeset_size_adjustment_min_factor == 0),
      nodeset_max_randomizations_(
          Worker::settings().nodeset_max_randomizations) {}

void SequencerBackgroundActivator::checkWorkerAsserts() {
  Worker* w = Worker::onThisThread();
  ld_check(w);
  ld_check(w->worker_type_ == getWorkerType(w->processor_));
  ld_check(w->idx_.val() ==
           getThreadAffinity(w->processor_->getWorkerCount(w->worker_type_)));
  ld_check(w->sequencerBackgroundActivator());
  ld_check(w->sequencerBackgroundActivator().get() == this);
}

void SequencerBackgroundActivator::schedule(std::vector<logid_t> log_ids,
                                            bool queued_by_alarm_callback) {
  checkWorkerAsserts();
  uint64_t num_scheduled = 0;
  for (auto& log_id : log_ids) {
    // metadata log sequencers don't interact via EpochStore, hence using this
    // state machine to activate them won't work
    ld_check(!MetaDataLog::isMetaDataLog(log_id));
    LogState& state = logs_[log_id];
    activateNodesetAdjustmentTimerIfNeeded(log_id, state);

    // We can skip over logs already in the queue. Note that we can't skip over
    // logs that have an active alarm since they may be getting queued again for
    // a more urgent reason. In that case they will be added once more to the
    // queue, even though they already have an active alarm, and then
    // re-evaluated.
    // If we have delayed once already and the alarm has fired then we want to
    // record that even if we are already on the queue for another change.
    if (queued_by_alarm_callback) {
      state.queued_by_alarm_callback = queued_by_alarm_callback;
    }
    if (state.in_queue) {
      continue;
    }
    state.in_queue = true;
    queue_.push(log_id);
    ++num_scheduled;
  }
  bumpScheduledStat(num_scheduled);
  maybeProcessQueue();
}

SequencerBackgroundActivator::ProcessLogDecision
SequencerBackgroundActivator::processOneLog(logid_t log_id,
                                            LogState& state,
                                            ResourceBudget::Token& token) {
  Worker* worker = Worker::onThisThread();
  auto config = worker->getConfig();
  const auto& nodes_configuration = worker->getNodesConfiguration();

  auto& all_seq = worker->processor_->allSequencers();
  auto seq = all_seq.findSequencer(log_id);

  if (!seq) {
    // No sequencer for that log, nothing to do.

    // Assert that we didn't have any work in flight for this log.
    // It should be impossible becase we never start work if sequencer doesn't
    // exist, and sequencers are never removed from AllSequencers.
    ld_check(!state.token.valid());
    state.token.release();

    return ProcessLogDecision::SUCCESS;
  }

  if (state.token.valid()) {
    // Something's already in flight for this log. Don't do anything for now.
    // We'll be notified and run the check again when it completes.
    return ProcessLogDecision::SUCCESS;
  }

  const auto my_node_id = worker->processor_->getMyNodeID();
  const bool is_sequencer_node =
      nodes_configuration->getSequencerMembership()->isSequencingEnabled(
          my_node_id.index());

  seq->noteConfigurationChanged(config, nodes_configuration, is_sequencer_node);

  if (!is_sequencer_node) {
    // no need to check for reactivation and such. The sequencer should've been
    // deactivated by the call to noteConfigurationChanged() above
    return ProcessLogDecision::SUCCESS;
  }

  auto epoch_metadata = seq->getCurrentMetaData();
  ProcessLogDecision dec =
      reprovisionOrReactivateIfNeeded(log_id, state, seq, epoch_metadata);
  if (dec == ProcessLogDecision::NOOP) {
    // No updates needed to nodeset parameters, we're in steady state now.
    // Schedule next nodeset randomization if needed. Distinct from handling for
    // SUCCESS where a provisioning is in flight.
    recalculateNodesetRandomizationTime(log_id, state, epoch_metadata);
    return ProcessLogDecision::SUCCESS;
  } else if (dec == ProcessLogDecision::POSTPONED) {
    return dec;
  } else if (dec == ProcessLogDecision::FAILED) {
    // Reprovisioning could not be started, but may still be necessary.
    bool should_retry = err == E::FAILED || err == E::NOBUFS ||
        err == E::TOOMANY || err == E::NOTCONN || err == E::ACCESS ||
        err == E::ISOLATED;
    if (err != E::INPROGRESS && err != E::NOSEQUENCER) {
      RATELIMIT_INFO(std::chrono::seconds(10),
                     2,
                     "Got %s when checking if log %lu needs a metadata "
                     "update. Will%s try again later.",
                     error_name(err),
                     log_id.val(),
                     should_retry ? "" : " not");
    }
    return should_retry ? ProcessLogDecision::FAILED
                        : ProcessLogDecision::SUCCESS;
  }

  // Reprovisioning in flight, moving token into the sequencer.
  ld_check(!state.token.valid());
  state.token = std::move(token);
  ld_check(state.token.valid());
  return ProcessLogDecision::SUCCESS;
}

/* Some events trigger a sequencer reactivation and metadata log
 * update but they can be performed slowly by delaying them. This
 * allows us to stagger sequencer reactivation related recoveries
 * from small changes to the cluster.
 */
SequencerBackgroundActivator::ProcessLogDecision
SequencerBackgroundActivator::postponeSequencerReactivation(logid_t logid) {
  // process this log picking a random time over a window specified in the
  // settings.
  LogState& state = logs_[logid];
  ld_check(!queue_.empty());
  ld_check(queue_.front() == logid);
  ld_check(state.in_queue);
  ld_check(!state.token.valid());
  queue_.pop();
  state.token.release();
  state.in_queue = false;
  bumpCompletedStat();

  // If we already have an active alarm for this log
  // then we can piggy-back on it if we have non-urgent
  // changes to the config.
  if (state.reactivation_delay_timer.isActive()) {
    WORKER_STAT_INCR(sequencer_reactivations_delay_timer_reused);
    return ProcessLogDecision::POSTPONED;
  }

  // compute random delay
  auto min = Worker::settings().sequencer_reactivation_delay_secs.lo.count();
  auto max = Worker::settings().sequencer_reactivation_delay_secs.hi.count();
  std::chrono::seconds delay_secs(folly::Random::rand32(min, max));
  auto cb = [self = this, log_id = logid]() {
    WORKER_STAT_INCR(sequencer_reactivations_delay_completed);
    LogState& cur_state = self->logs_[log_id];
    cur_state.reactivation_delay_timer.cancel();
    self->schedule({log_id}, true /* queued_by_alarm_callback */);
  };
  RecordTimestamp delayTS = RecordTimestamp::now() + delay_secs;
  RATELIMIT_INFO(std::chrono::seconds(10),
                 10,
                 "Delaying reactivation of log %ld for %ld secs (until %s)",
                 logid.val(),
                 delay_secs.count(),
                 delayTS.toString().c_str());

  state.reactivation_delay_timer.assign(cb);
  state.reactivation_delay_timer.activate(delay_secs);
  WORKER_STAT_INCR(sequencer_reactivations_delayed);
  return ProcessLogDecision::POSTPONED;
}

SequencerBackgroundActivator::ProcessLogDecision
SequencerBackgroundActivator::updateEpochMetadata(
    logid_t logid,
    std::shared_ptr<Sequencer> seq,
    std::unique_ptr<EpochMetaData>& new_metadata,
    const epoch_t& current_epoch) {
  // Update the nodeset params in epoch store without reactivating sequencer.
  WORKER_STAT_INCR(metadata_updates_without_sequencer_reactivation);
  ld_check(new_metadata != nullptr);
  auto& all_seq = Worker::onThisThread()->processor_->allSequencers();
  auto callback =
      [&all_seq, seq, current_epoch, new_params = new_metadata->nodeset_params](
          Status st,
          logid_t log_id,
          std::unique_ptr<const EpochMetaData> info,
          std::unique_ptr<EpochStore::MetaProperties> meta_props) {
        if (st == E::OK || st == E::UPTODATE) {
          if (!seq->setNodeSetParamsInCurrentEpoch(current_epoch, new_params)) {
            RATELIMIT_INFO(std::chrono::seconds(10),
                           2,
                           "Lost the race when updating nodeset params for "
                           "log %lu epoch %u to %s. This should be rare.",
                           log_id.val(),
                           current_epoch.val(),
                           new_params.toString().c_str());
          }
        }

        if (st == E::ABORTED) {
          // Epoch didn't match. Our sequencer is preempted.
          ld_check(info != nullptr);
          ld_check(info->h.epoch != EPOCH_INVALID);
          all_seq.notePreemption(log_id,
                                 epoch_t(info->h.epoch.val() - 1),
                                 meta_props.get(),
                                 seq.get(),
                                 "updating nodeset params");
        }

        if (st != E::SHUTDOWN && st != E::FAILED) {
          SequencerBackgroundActivator::requestNotifyCompletion(
              Worker::onThisThread()->processor_, log_id, st);
        }
      };
  MetaDataTracer tracer(Worker::onThisThread()->processor_->getTraceLogger(),
                        logid,
                        MetaDataTracer::Action::UPDATE_NODESET_PARAMS);
  int rv = all_seq.getEpochStore().createOrUpdateMetaData(
      logid,
      std::make_shared<EpochMetaDataUpdateNodeSetParams>(
          epoch_t(current_epoch.val() + 1), new_metadata->nodeset_params),
      callback,
      std::move(tracer),
      EpochStore::WriteNodeID::KEEP_LAST);
  if (rv != 0) {
    RATELIMIT_ERROR(
        std::chrono::seconds(10),
        2,
        "Failed to update nodeset params for log %lu in epoch store '%s': %s",
        logid.val(),
        all_seq.getEpochStore().identify().c_str(),
        error_name(err));
    ld_check_in(err,
                ({E::INTERNAL,
                  E::NOTCONN,
                  E::ACCESS,
                  E::SYSLIMIT,
                  E::NOTFOUND,
                  E::FAILED}));
    return ProcessLogDecision::FAILED;
  }
  return ProcessLogDecision::SUCCESS;
}

ReactivationDecision SequencerBackgroundActivator::processMetadataChanges(
    logid_t logid,
    std::shared_ptr<const EpochMetaData>& current_metadata,
    std::unique_ptr<EpochMetaData>& new_metadata) {
  auto config = Worker::onThisThread()->getConfig();
  if (!config->serverConfig()->sequencersProvisionEpochStore()) {
    return ReactivationDecision::NOOP;
  }

  const auto& nodes_configuration =
      Worker::onThisThread()->getNodesConfiguration();
  const std::shared_ptr<LogsConfig::LogGroupNode> logcfg =
      config->getLogGroupByIDShared(logid);

  LogState& state = logs_[logid];

  // See if we need to change nodeset size or seed.
  folly::Optional<nodeset_size_t> target_nodeset_size;
  folly::Optional<uint64_t> nodeset_seed;
  if (nodeset_adjustment_period_.count() <= 0) {
    // Nodeset adjustments are disabled.
    // Revert nodeset size to its unadjusted value.
    target_nodeset_size =
        logcfg->attrs().nodeSetSize().value().value_or(NODESET_SIZE_MAX);
    nodeset_seed.clear();
  } else if (state.pending_adjustment.hasValue()) {
    if (state.pending_adjustment->epoch == current_metadata->h.epoch) {
      // An adjustment is pending. Try to apply it.
      target_nodeset_size = state.pending_adjustment->new_size;
      nodeset_seed = state.pending_adjustment->new_seed;
    } else {
      // A scheduled adjustment is outdated.
      state.pending_adjustment.clear();
    }
  }
  bool use_new_storage_set_format =
      Worker::settings().epoch_metadata_use_new_storage_set_format;

  // Use the same logic for updating metadata as during sequencer activation.
  UpdateResult result = updateMetaDataIfNeeded(logid,
                                               new_metadata,
                                               *config,
                                               *nodes_configuration,
                                               target_nodeset_size,
                                               nodeset_seed,
                                               /* nodeset_selector */ nullptr,
                                               use_new_storage_set_format,
                                               /* provision_if_empty */ false,
                                               /* update_if_exists */ true,
                                               /* force_update */ false);
  if (result == UpdateResult::FAILED) {
    RATELIMIT_ERROR(
        std::chrono::seconds(10),
        2,
        "Failed to consider updating epoch metadata for log %lu: %s",
        logid.val(),
        error_name(err));
    new_metadata = nullptr;
    // This is unexpected. Don't update metadata and don't retry.
    return ReactivationDecision::NOOP;
  }

  if (result == UpdateResult::UNCHANGED) {
    // No update needed.
    new_metadata = nullptr;
    return ReactivationDecision::NOOP;
  }
  ld_check(new_metadata != nullptr);

  // Assert that the nodeset selector is satisfied with the new nodeset and
  // won't want to change it again right away. Otherwise we may get into an
  // infinite loop of nodeset updates.
  {
    auto another_metadata = std::make_unique<EpochMetaData>(*new_metadata);
    auto another_res = updateMetaDataIfNeeded(logid,
                                              another_metadata,
                                              *config,
                                              *nodes_configuration,
                                              target_nodeset_size,
                                              nodeset_seed,
                                              /* nodeset_selector */ nullptr,
                                              use_new_storage_set_format,
                                              false,
                                              true,
                                              false);

    // The first check is redundant but provides a better error message.
    if (!ld_catch(another_res != UpdateResult::FAILED,
                  "updateMetaDataIfNeeded() succeeded, then failed when called "
                  "again. This should be impossible. Log: %lu, epoch: %u, old "
                  "metadata: %s, new metadata: %s",
                  logid.val(),
                  current_metadata->h.epoch.val(),
                  current_metadata->toString().c_str(),
                  new_metadata->toString().c_str()) ||
        !ld_catch(another_res == UpdateResult::UNCHANGED,
                  "updateMetaDataIfNeeded() wants to update metadata twice "
                  "in a row. This should be impossible. Log: %lu, epoch: %u, "
                  "old metadata: %s, new metadata: %s, yet another metadata: "
                  "%s, first result: %d, second result: %d",
                  logid.val(),
                  current_metadata->h.epoch.val(),
                  current_metadata->toString().c_str(),
                  new_metadata->toString().c_str(),
                  another_metadata->toString().c_str(),
                  (int)result,
                  (int)another_res)) {
      // Cancel the update and return success to prevent retrying.
      return ReactivationDecision::NOOP;
    }
  }

  ld_check(result != UpdateResult::FAILED);
  ld_check(result != UpdateResult::UNCHANGED);

  if (result == UpdateResult::ONLY_NODESET_PARAMS_CHANGED) {
    RATELIMIT_INFO(std::chrono::seconds(10),
                   10,
                   "Updating nodeset params in epoch store for log %lu epoch "
                   "%u from %s to %s without changing nodeset.",
                   logid.val(),
                   current_metadata->h.epoch.val(),
                   current_metadata->nodeset_params.toString().c_str(),
                   new_metadata->nodeset_params.toString().c_str());
    return ReactivationDecision::UPDATE_METADATA;
  } else if (result == UpdateResult::NONSUBSTANTIAL_RECONFIGURATION) {
    RATELIMIT_INFO(
        std::chrono::seconds(10),
        10,
        "Reactivating sequencer with delay for log %lu epoch %u to update "
        "epoch metadata from %s to %s",
        logid.val(),
        current_metadata->h.epoch.val(),
        current_metadata->toString().c_str(),
        new_metadata->toString().c_str());
    return ReactivationDecision::POSTPONE;
  } else {
    RATELIMIT_INFO(std::chrono::seconds(10),
                   10,
                   "Reactivating sequencer for log %lu epoch %u to update "
                   "epoch metadata from %s to %s",
                   logid.val(),
                   current_metadata->h.epoch.val(),
                   current_metadata->toString().c_str(),
                   new_metadata->toString().c_str());
    return ReactivationDecision::REACTIVATE;
  }
}

SequencerBackgroundActivator::ProcessLogDecision
SequencerBackgroundActivator::reprovisionOrReactivateIfNeeded(
    logid_t logid,
    LogState& state,
    std::shared_ptr<Sequencer> seq,
    std::shared_ptr<const EpochMetaData> current_metadata) {
  ld_check(!MetaDataLog::isMetaDataLog(logid));
  ld_check(seq != nullptr);

  // Only do anything if the sequencer is active.
  // If sequencer is inactive (e.g. preempted or error), it'll reprovision
  // metadata on next activation.
  // If sequencer activation is in progress, the sequencer will trigger another
  // call to this method when activation completes.
  // Also check that sequencer has epoch metadata; it may seem redundant because
  // sequencer always has epoch metadata if it's active, but there's a race
  // condition - maybe we grabbed metadata, then sequencer reactivated, then we
  // grabbed the state.
  auto seq_state = seq->getState();
  if (seq_state != Sequencer::State::ACTIVE || current_metadata == nullptr) {
    err = seq_state == Sequencer::State::ACTIVATING ? E::INPROGRESS
                                                    : E::NOSEQUENCER;
    return ProcessLogDecision::FAILED;
  }
  if (current_metadata->isEmpty() || current_metadata->disabled()) {
    // Sequencer wouldn't agree to activate with such metadata.
    ld_check(false);
    err = E::INTERNAL;
    return ProcessLogDecision::FAILED;
  }

  auto config = Worker::onThisThread()->getConfig();

  const std::shared_ptr<LogsConfig::LogGroupNode> logcfg =
      config->getLogGroupByIDShared(logid);
  if (!logcfg) {
    // logid no longer in config
    err = E::NOTFOUND;
    return ProcessLogDecision::FAILED;
  }

  epoch_t current_epoch = current_metadata->h.epoch;
  ld_check(current_epoch != EPOCH_INVALID);

  if (current_epoch.val() >= EPOCH_MAX.val() - 2) {
    // Ran out of epoch numbers, can't reactivate.
    err = E::TOOBIG;
    return ProcessLogDecision::FAILED;
  }

  ld_check(current_metadata);
  if (!current_metadata->writtenInMetaDataLog()) {
    // We can't reprovision metadata before it's written into the metadata
    // log. After it's written, SequencerMetaDataLogManager will re-check
    // whether reprovisioning is needed.
    err = E::INPROGRESS;
    return ProcessLogDecision::FAILED;
  }

  folly::Optional<EpochSequencerImmutableOptions> current_options =
      seq->getEpochSequencerOptions();
  if (!current_options.hasValue()) {
    err = E::NOSEQUENCER;
    return ProcessLogDecision::FAILED;
  }

  EpochSequencerImmutableOptions new_options(
      logcfg->attrs(), Worker::settings());

  // First process if the sequencer options have changed.
  ReactivationDecision optionsDecision = ReactivationDecision::NOOP;
  if (new_options != current_options.value()) {
    RATELIMIT_INFO(std::chrono::seconds(10),
                   10,
                   "Reactivating sequencer for log %lu epoch %u "
                   "because options changed from %s to %s.",
                   logid.val(),
                   current_epoch.val(),
                   current_options.value().toString().c_str(),
                   new_options.toString().c_str());
    optionsDecision = ReactivationDecision::REACTIVATE;
  }

  // Copy sequencer's metadata and increment epoch. The result should be
  // equal to the metadata in epoch store (unless this sequencer is preempted,
  // which we will notice thanks to acceptable_activation_epoch).
  std::unique_ptr<EpochMetaData> new_metadata =
      std::make_unique<EpochMetaData>(*current_metadata);
  ld_check(current_metadata->h.epoch < EPOCH_MAX);
  ++new_metadata->h.epoch.val_;
  new_metadata->setEpochIncrementAt();

  // Then process the extent to which config params and nodeset
  // have changed.
  ReactivationDecision metadataDecision =
      processMetadataChanges(logid, current_metadata, new_metadata);

  // Now we can reconcile the option changes and metadata changes into a single
  // decision reflecting the bigger of the two changes. Note that optionsDecisio
  // can only be NOOP or REACTIVATE. So the comparison based upgrade below is
  // safe because we will never upgrade ReactivationDecision::UPDATE_METADATA to
  // ReactivationDecision::POSTPONE.
  ld_check(optionsDecision == ReactivationDecision::NOOP ||
           optionsDecision == ReactivationDecision::REACTIVATE);
  ReactivationDecision decision =
      optionsDecision > metadataDecision ? optionsDecision : metadataDecision;

  switch (decision) {
    case ReactivationDecision::NOOP:
      // No changes. Nothing to do.
      WORKER_STAT_INCR(sequencer_reactivations_noop);
      err = E::UPTODATE;
      return ProcessLogDecision::NOOP;
    case ReactivationDecision::UPDATE_METADATA:
      // If just the config params changed then update the epoch store and
      // return.
      return updateEpochMetadata(logid, seq, new_metadata, current_epoch);
    case ReactivationDecision::POSTPONE:
      // We need reactivation. If it can be postponed then do that. Otherwise
      // fallthrough to immediate reactivation below.
      if (Worker::settings().sequencer_reactivation_delay_secs.lo.count() &&
          Worker::settings().sequencer_reactivation_delay_secs.hi.count() &&
          !state.queued_by_alarm_callback) {
        return postponeSequencerReactivation(logid);
      }
      // Switch off this flag so that the next enqueue doesn't go through
      // immediately.
      state.queued_by_alarm_callback = false;
      FOLLY_FALLTHROUGH;
    case ReactivationDecision::REACTIVATE:
      // We need reactivation to be performed now. Either because we have an
      // important change to the options or metadata or because it's a change
      // that can be delayed but it has already been delayed once.
      ProcessLogDecision dec = ProcessLogDecision::SUCCESS;
      WORKER_STAT_INCR(sequencer_reactivations_for_metadata_update);
      auto& all_seq = Worker::onThisThread()->processor_->allSequencers();
      int rv = all_seq.activateSequencer(
          logid,
          "background reconfiguration",
          [](const Sequencer&) { return true; },
          /* acceptable_activation_epoch */ epoch_t(current_epoch.val() + 1),
          /* check_metadata_log_before_provisioning */ false,
          std::shared_ptr<EpochMetaData>(std::move(new_metadata)));
      if (rv != 0) {
        ld_check_in(err,
                    ({E::NOTFOUND,
                      E::NOBUFS,
                      E::INPROGRESS,
                      E::FAILED,
                      E::TOOMANY,
                      E::SYSLIMIT,
                      E::ISOLATED}));

        dec = ProcessLogDecision::FAILED;
      }
      return dec;
  }
  // Control never reaches here.
  return ProcessLogDecision::NOOP;
}

void SequencerBackgroundActivator::notifyCompletion(logid_t logid,
                                                    Status /* st */) {
  checkWorkerAsserts();
  if (MetaDataLog::isMetaDataLog(logid)) {
    // We don't reactivate metadata logs.
    return;
  }
  LogState& state = logs_[logid];
  activateNodesetAdjustmentTimerIfNeeded(logid, state);

  // If the operation that just completed was triggered by us, reclaim the
  // in-flight slot we assigned to it.
  bool had_token = state.token.valid();
  state.token.release();

  // Schedule a re-check for the log, in case config was updated while sequencer
  // activation was in flight. Re-checking is cheap when no changes are needed.
  bool inserted = !state.in_queue;
  if (inserted) {
    state.in_queue = true;
    queue_.push(logid);
  }

  if (had_token && !inserted) {
    bumpCompletedStat();
  }
  if (!had_token && inserted) {
    bumpScheduledStat();
  }

  maybeProcessQueue();
}

void SequencerBackgroundActivator::maybeProcessQueue() {
  checkWorkerAsserts();
  deactivateQueueProcessingTimer();
  size_t limit =
      Worker::settings().max_sequencer_background_activations_in_flight;
  if (!budget_) {
    budget_ = std::make_unique<ResourceBudget>(limit);
  } else if (budget_->getLimit() != limit) {
    // in case the setting changed after initialization
    budget_->setLimit(limit);
  }

  std::chrono::steady_clock::time_point start_time =
      std::chrono::steady_clock::now();
  bool made_progress = false;

  while (!queue_.empty() && budget_->available() > 0) {
    // Limiting this loop to 2ms
    if (made_progress && usec_since(start_time) > 2000) {
      // This is taking a while, let's yield for a few milliseconds.
      activateQueueProcessingTimer(std::chrono::microseconds(5000));
      break;
    }
    made_progress = true;

    ld_check(!queue_.empty());
    logid_t log_id = queue_.front();
    LogState& state = logs_.at(log_id); // LogState must exist
    ld_check(state.in_queue);

    auto token = budget_->acquireToken();
    ld_check(token.valid());
    ProcessLogDecision decision = processOneLog(log_id, state, token);
    if (decision != ProcessLogDecision::POSTPONED) {
      // In the postponed case the logId is directly dequeued and scehduled for
      // processing.
      ld_check(!queue_.empty());
      ld_check(queue_.front() == log_id);
      ld_check(state.in_queue);
      queue_.pop();
    }

    switch (decision) {
      case ProcessLogDecision::NOOP:
        break;
      case ProcessLogDecision::SUCCESS:
        state.in_queue = false;

        if (token) {
          // The token hasn't been moved into the LogState, presumably because
          // nothing needs to be done for this log.
          token.release();
          // Since we're releasing the token, we have to bump the stat.
          bumpCompletedStat();
        }
        break;
      case ProcessLogDecision::POSTPONED:
        break;
      case ProcessLogDecision::FAILED:
        // Failed processing a sequencer.
        // Move the failed log to the back of the queue.
        queue_.push(log_id);
        // There's no point in retrying immediately, so instead do it on a
        // timer.
        activateQueueProcessingTimer(folly::none);
        return;
    }
  }
}

void SequencerBackgroundActivator::activateQueueProcessingTimer(
    folly::Optional<std::chrono::microseconds> timeout) {
  Worker* w = Worker::onThisThread();
  ld_check(w);
  if (!retry_timer_.isAssigned()) {
    retry_timer_.assign([this] { maybeProcessQueue(); });
  }
  if (!timeout.hasValue()) {
    timeout = Worker::settings().sequencer_background_activation_retry_interval;
  }
  retry_timer_.activate(timeout.value());
}

void SequencerBackgroundActivator::deactivateQueueProcessingTimer() {
  retry_timer_.cancel();
}

void SequencerBackgroundActivator::bumpScheduledStat(uint64_t val) {
  WORKER_STAT_ADD(background_sequencer_reactivation_checks_scheduled, val);
}

void SequencerBackgroundActivator::bumpCompletedStat(uint64_t val) {
  WORKER_STAT_ADD(background_sequencer_reactivation_checks_completed, val);
}

namespace {

class SequencerBackgroundActivatorRequest : public Request {
 public:
  explicit SequencerBackgroundActivatorRequest(
      Processor* processor,
      std::function<void(SequencerBackgroundActivator&)> func)
      : Request(RequestType::SEQUENCER_BACKGROUND_ACTIVATOR),
        workerType_(SequencerBackgroundActivator::getWorkerType(processor)),
        func_(func) {}

  WorkerType getWorkerTypeAffinity() override {
    return workerType_;
  }
  int getThreadAffinity(int nthreads) override {
    return SequencerBackgroundActivator::getThreadAffinity(nthreads);
  }

  Execution execute() override {
    auto& act = Worker::onThisThread()->sequencerBackgroundActivator();
    if (!act) {
      act = std::make_unique<SequencerBackgroundActivator>();
    }
    func_(*act);
    return Execution::COMPLETE;
  }

  WorkerType workerType_;
  std::function<void(SequencerBackgroundActivator&)> func_;
};

} // namespace

void SequencerBackgroundActivator::activateNodesetAdjustmentTimerIfNeeded(
    logid_t log_id,
    LogState& state) {
  if (nodeset_adjustment_period_.count() <= 0 ||
      state.nodeset_adjustment_timer.isActive()) {
    return;
  }
  if (!state.nodeset_adjustment_timer.isAssigned()) {
    state.nodeset_adjustment_timer.assign([this, log_id, &state] {
      maybeAdjustNodesetSize(log_id, state);
      ld_check(nodeset_adjustment_period_.count() > 0);
      state.next_nodeset_adjustment_time =
          std::chrono::steady_clock::now() + nodeset_adjustment_period_;
      state.nodeset_adjustment_timer.activate(nodeset_adjustment_period_);
    });
  }

  // Randomly stagger logs so their timers don't fire all at the same time.
  auto first_delay =
      to_usec(folly::Random::randDouble01() * nodeset_adjustment_period_);
  state.next_nodeset_adjustment_time =
      std::chrono::steady_clock::now() + first_delay;
  state.nodeset_adjustment_timer.activate(first_delay);
}

void SequencerBackgroundActivator::onSettingsUpdated() {
  // Set val = new_val and return true if the original val was != new_val.
  auto upd = [](auto& val, auto new_val) {
    if (val != new_val) {
      val = new_val;
      return true;
    }
    return false;
  };

  bool adjustment_period_changed = upd(
      nodeset_adjustment_period_, Worker::settings().nodeset_adjustment_period);
  // Important to use '|' instead of the short-circuiting '||'!
  bool randomization_settings_changed =
      upd(unconditional_nodeset_randomization_enabled_,
          Worker::settings().nodeset_size_adjustment_min_factor == 0) |
      upd(nodeset_max_randomizations_,
          Worker::settings().nodeset_max_randomizations);

  if (!adjustment_period_changed && !randomization_settings_changed) {
    return;
  }

  for (auto& kv : logs_) {
    bool scheduled = false;

    if (adjustment_period_changed) {
      kv.second.next_nodeset_adjustment_time =
          std::chrono::steady_clock::time_point::max();
      kv.second.nodeset_adjustment_timer.cancel();
      if (nodeset_adjustment_period_.count() <= 0) {
        // Nodeset adjusting was disabled.
        // May need to revert nodeset size back to its unadjusted value.
        kv.second.pending_adjustment.clear();
        schedule({kv.first});
        scheduled = true;
      } else {
        // Activate timer using the new period.
        activateNodesetAdjustmentTimerIfNeeded(kv.first, kv.second);
      }
    }

    if (randomization_settings_changed && !scheduled) {
      // Need to recalculate next randomization time.
      schedule({kv.first});
    }
  }
}

void SequencerBackgroundActivator::maybeAdjustNodesetSize(logid_t log_id,
                                                          LogState& state) {
  auto& all_seq = Worker::onThisThread()->processor_->allSequencers();
  auto seq = all_seq.findSequencer(log_id);
  if (!seq || seq->getState() != Sequencer::State::ACTIVE) {
    // Sequencer not active.
    return;
  }
  auto epoch_metadata = seq->getCurrentMetaData();
  if (!epoch_metadata) {
    return;
  }

  std::shared_ptr<Configuration> config = Worker::onThisThread()->getConfig();
  const std::shared_ptr<LogsConfig::LogGroupNode> logcfg =
      config->getLogGroupByIDShared(log_id);
  if (!logcfg) {
    // Log no longer in config.
    return;
  }

  NodesetAdjustment adj;
  adj.epoch = epoch_metadata->h.epoch;

  double min_factor = Worker::settings().nodeset_size_adjustment_min_factor;

  do { // while (false)
    auto est = seq->appendRateEstimate();
    if (est.second < Worker::settings().nodeset_adjustment_min_window) {
      // The sequencer hasn't collected enough stats yet.
      break;
    }

    folly::Optional<std::chrono::seconds> backlog =
        logcfg->attrs().backlogDuration().value();
    if (!backlog.hasValue()) {
      // The log has infinite retention, don't resize its nodeset.
      break;
    }

    // Don't make nodeset smaller than the nodeSetSize log attribute.
    nodeset_size_t min_nodeset_size =
        logcfg->attrs().nodeSetSize().value().value_or(NODESET_SIZE_MAX);
    min_nodeset_size =
        std::max(min_nodeset_size, static_cast<nodeset_size_t>(1));

    // Estimate how much space this log uses on all storage nodes combined.
    double append_rate = 1. * est.first / to_sec_double(est.second);
    double total_data_size = append_rate * to_sec_double(backlog.value()) *
        ReplicationProperty::fromLogAttributes(logcfg->attrs())
            .getReplicationFactor();

    // Nodeset size we would like to use based only on log's throughput.
    size_t unclamped_new_size = std::lround(
        total_data_size /
        Worker::settings().nodeset_adjustment_target_bytes_per_shard);
    // Clamp nodeset size to range [min_nodeset_size, nodeset_size_t::max()].
    nodeset_size_t new_size = static_cast<nodeset_size_t>(std::min(
        unclamped_new_size,
        static_cast<size_t>(std::numeric_limits<nodeset_size_t>::max())));
    new_size = std::max(new_size, min_nodeset_size);
    ld_check(new_size > 0);

    nodeset_size_t old_size =
        epoch_metadata->nodeset_params.target_nodeset_size;
    // How different the new nodeset size is from the current one.
    // If not different enough, don't bother adjusting the nodeset.
    double factor = 1. * std::max(new_size, old_size) /
        std::max(static_cast<nodeset_size_t>(1), std::min(new_size, old_size));
    // old_size=0 means nodeset size adjustment used to be disabled.
    // In this case update epoch metadata to make sure it contains the right
    // target nodeset size. (If size of the actual nodeset is already right,
    // consistent hashing will usually generate the same nodeset anyway, and
    // we won't have to reactivate sequencer.)
    if ((factor >= min_factor || old_size == 0) && new_size != old_size) {
      adj.new_size = new_size;

      ld_info("Adjusting target nodeset size of log %lu epoch %u from %d "
              "(actual %lu) to %d. Throughput: %.3f KB/s in %.3f hours, "
              "estimated data size: %.3f GB.",
              log_id.val(),
              adj.epoch.val(),
              (int)old_size,
              epoch_metadata->shards.size(),
              (int)new_size,
              append_rate / 1e3,
              to_sec_double(est.second) / 3600,
              total_data_size / 1e9);
    }
  } while (false);

  if (adj.new_size.hasValue()) {
    // Instead of applying the adjustment right here, put it in the LogState
    // and enqueue, to make sure it goes through the ResourceBudget.
    state.pending_adjustment = adj;
    schedule({log_id});
    WORKER_STAT_INCR(nodeset_adjustments_done);
  } else {
    WORKER_STAT_INCR(nodeset_adjustments_skipped);
  }
}

void SequencerBackgroundActivator::randomizeNodeset(logid_t log_id,
                                                    LogState& state) {
  auto& all_seq = Worker::onThisThread()->processor_->allSequencers();
  auto seq = all_seq.findSequencer(log_id);
  if (!seq || seq->getState() != Sequencer::State::ACTIVE) {
    // Sequencer not active.
    return;
  }
  auto epoch_metadata = seq->getCurrentMetaData();
  if (!epoch_metadata) {
    return;
  }

  std::shared_ptr<Configuration> config = Worker::onThisThread()->getConfig();
  const std::shared_ptr<LogsConfig::LogGroupNode> logcfg =
      config->getLogGroupByIDShared(log_id);
  if (!logcfg) {
    // Log no longer in config.
    return;
  }

  epoch_t epoch = epoch_metadata->h.epoch;
  uint64_t new_seed = folly::Random::rand64();

  if (state.pending_adjustment.hasValue() &&
      state.pending_adjustment->epoch >= epoch) {
    state.pending_adjustment->new_seed = new_seed;
  } else {
    NodesetAdjustment adj;
    adj.epoch = epoch;
    adj.new_seed = new_seed;
    state.pending_adjustment = adj;
  }

  ld_info(
      "Randomizing nodeset of log %lu epoch %u (every %lds). New seed: %lu.",
      log_id.val(),
      epoch.val(),
      to_sec(state.nodeset_randomization_period).count(),
      new_seed);

  schedule({log_id});
  WORKER_STAT_INCR(nodeset_randomizations_done);
}

void SequencerBackgroundActivator::recalculateNodesetRandomizationTime(
    logid_t logid,
    LogState& state,
    std::shared_ptr<const EpochMetaData> epoch_metadata) {
  ld_check(epoch_metadata->isValid());

  // Decide how often to randomize.
  std::chrono::milliseconds period = std::chrono::milliseconds::max();
  do { // while (false)
    if (unconditional_nodeset_randomization_enabled_) {
      // Unconditionally randomize nodesets every nodeset_adjustment_period.
      period = nodeset_adjustment_period_;
      if (period.count() == 0) {
        period = std::chrono::milliseconds::max();
      }
      break;
    }

    // If target nodeset size is x times greater than max allowed, randomize
    // the nodeset x times per retention.
    size_t target_size = epoch_metadata->nodeset_params.target_nodeset_size;
    size_t actual_size = epoch_metadata->shards.size();
    ld_check(actual_size != 0);
    double randomizations_per_retention = std::min(
        1. * target_size / actual_size, 1. * nodeset_max_randomizations_);
    // Only do randomization if it's significantly more often than once per
    // retention. In particular, if actual size matches the target, randomizing
    // would be weird. The 1.5 threshold is arbitrary.
    if (randomizations_per_retention < 1.5) {
      break;
    }

    auto config = Worker::onThisThread()->getConfig();
    const std::shared_ptr<LogsConfig::LogGroupNode> logcfg =
        config->getLogGroupByIDShared(logid);
    if (!logcfg) {
      // logid no longer in config
      break;
    }
    folly::Optional<std::chrono::seconds> backlog =
        logcfg->attrs().backlogDuration().value();
    if (!backlog.hasValue()) {
      // The log has infinite retention.
      break;
    }

    period = std::chrono::milliseconds(static_cast<int64_t>(
        to_sec_double(backlog.value()) / randomizations_per_retention * 1000));
  } while (false);

  if (period < std::chrono::seconds(1)) {
    RATELIMIT_ERROR(std::chrono::seconds(10),
                    2,
                    "Suspiciously high rate of nodeset randomization for log "
                    "%lu: %.3fs. Disabling randomization just in case.",
                    logid.val(),
                    period.count() / 1000.);
    period = std::chrono::milliseconds::max();
  }

  // All randomization-related fields of LogState should agree on whether
  // randomization is enabled for the log.
  ld_check_eq(
      state.nodeset_randomization_period != std::chrono::milliseconds::max(),
      state.next_nodeset_randomization_time !=
          std::chrono::steady_clock::time_point::max());
  ld_check_eq(
      state.nodeset_randomization_period != std::chrono::milliseconds::max(),
      state.nodeset_randomization_timer.isActive());

  if (period == state.nodeset_randomization_period) {
    // Period didn't change, nothing to do here.
    return;
  }

  if (period == std::chrono::milliseconds::max()) {
    // Disable randomization for this log.
    state.next_nodeset_randomization_time =
        std::chrono::steady_clock::time_point::max();
    state.nodeset_randomization_timer.cancel();
    state.nodeset_randomization_period = std::chrono::milliseconds::max();
    return;
  }

  if (!state.nodeset_randomization_timer.isAssigned()) {
    state.nodeset_randomization_timer.assign([this, logid, &state] {
      // Timer is canceled whenever nodeset_randomization_period is set to
      // max(), so we should only be here if it's not max().
      ld_check(state.nodeset_randomization_period !=
               std::chrono::milliseconds::max());
      // Schedule next run.
      state.next_nodeset_randomization_time =
          std::chrono::steady_clock::now() + state.nodeset_randomization_period;
      state.nodeset_randomization_timer.activate(
          state.nodeset_randomization_period);

      randomizeNodeset(logid, state);
    });
  }

  auto now = std::chrono::steady_clock::now();

  // Decide what the first delay should be.
  double first_delay_fraction;
  if (state.nodeset_randomization_timer.isActive()) {
    // Randomization period changed while we were in the middle of waiting for
    // the previous period to elapse. If we were x% of the way through the old
    // period, start the timer for (100-x)% of the new period. This way small
    // changes of the period won't have large effect.
    first_delay_fraction =
        to_sec_double(state.next_nodeset_randomization_time - now) /
        to_sec_double(state.nodeset_randomization_period);
    first_delay_fraction = std::max(0., std::min(1., first_delay_fraction));
  } else {
    // We're activating nodeset randomization timer for the first time,
    // or switching randomization from disabled to enabled for this log.
    // Randomly stagger the timer activation. This prevents a thundering
    // herd and makes randomizations work correctly even if server gets
    // restarted more often than randomization period.
    first_delay_fraction = folly::Random::randDouble01();
  }

  std::chrono::milliseconds delay =
      std::chrono::duration_cast<std::chrono::milliseconds>(
          period * first_delay_fraction);
  state.nodeset_randomization_period = period;
  state.next_nodeset_randomization_time = now + delay;
  state.nodeset_randomization_timer.activate(delay);
}

std::vector<SequencerBackgroundActivator::LogDebugInfo>
SequencerBackgroundActivator::getLogsDebugInfo(
    const std::vector<logid_t>& logs) {
  std::vector<LogDebugInfo> out;
  out.reserve(logs.size());
  for (logid_t log : logs) {
    out.emplace_back();
    LogDebugInfo& info = out.back();
    auto it = logs_.find(log);
    if (it == logs_.end()) {
      // If log is not found, leave a default LogDebugInfo.
      continue;
    }

    const LogState& state = it->second;
    if (!state.token.valid() && state.nodeset_adjustment_timer.isActive()) {
      info.next_nodeset_adjustment_time = state.next_nodeset_adjustment_time;
    }
  }
  return out;
}

void SequencerBackgroundActivator::requestSchedule(Processor* processor,
                                                   std::vector<logid_t> logs) {
  ld_check(!logs.empty());
  std::unique_ptr<Request> rq =
      std::make_unique<SequencerBackgroundActivatorRequest>(
          processor,
          [logs = std::move(logs)](SequencerBackgroundActivator& act) mutable {
            act.schedule(std::move(logs));
          });
  int rv = processor->postImportant(rq);
  ld_check(rv == 0 || err == E::SHUTDOWN);
}

void SequencerBackgroundActivator::requestNotifyCompletion(Processor* processor,
                                                           logid_t log,
                                                           Status st) {
  std::unique_ptr<Request> rq =
      std::make_unique<SequencerBackgroundActivatorRequest>(
          processor, [log, st](SequencerBackgroundActivator& act) {
            act.notifyCompletion(log, st);
          });
  int rv = processor->postImportant(rq);
  ld_check(rv == 0 || err == E::SHUTDOWN);
}

std::vector<SequencerBackgroundActivator::LogDebugInfo>
SequencerBackgroundActivator::requestGetLogsDebugInfo(
    Processor* processor,
    const std::vector<logid_t>& logs) {
  std::vector<LogDebugInfo> out;
  std::unique_ptr<Request> rq =
      std::make_unique<SequencerBackgroundActivatorRequest>(
          processor, [&logs, &out](SequencerBackgroundActivator& act) {
            out = act.getLogsDebugInfo(logs);
          });
  int rv = processor->blockingRequestImportant(rq);
  ld_check(rv == 0 || err == E::SHUTDOWN);
  return out;
}

}} // namespace facebook::logdevice
