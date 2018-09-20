/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "SequencerBackgroundActivator.h"

#include "logdevice/common/AllSequencers.h"
#include "logdevice/common/MetaDataLog.h"
#include "logdevice/common/MetaDataLogWriter.h"
#include "logdevice/common/Processor.h"
#include "logdevice/common/Worker.h"

namespace facebook { namespace logdevice {

void SequencerBackgroundActivator::checkWorkerAsserts() {
  Worker* w = Worker::onThisThread();
  ld_check(w);
  ld_assert(
      w->idx_.val() ==
      getThreadAffinity(w->processor_->getWorkerCount(WorkerType::GENERAL)));
  ld_assert(w->sequencerBackgroundActivator());
  ld_check(w->sequencerBackgroundActivator().get() == this);
}

void SequencerBackgroundActivator::schedule(std::vector<logid_t> log_ids) {
  checkWorkerAsserts();
  uint64_t num_scheduled = 0;
  for (auto& log_id : log_ids) {
    // metadata log sequencers don't interact via EpochStore, hence using this
    // state machine to activate them won't work
    ld_check(!MetaDataLog::isMetaDataLog(log_id));
    auto res = queue_.insert(log_id.val());
    if (res.second) {
      // new element inserted
      ++num_scheduled;
    }
  }
  bumpScheduledStat(num_scheduled);
  maybeProcessQueue();
}

bool SequencerBackgroundActivator::processOneLog(ResourceBudget::Token token) {
  ld_check(!queue_.empty());
  auto it = queue_.begin();
  auto log_id = logid_t(*it);
  auto config = Worker::onThisThread()->getConfig();

  auto& all_seq = Worker::onThisThread()->processor_->allSequencers();
  auto seq = all_seq.findSequencer(log_id);

  if (!seq) {
    // No sequencer for that log, we're done with this one
    bumpCompletedStat();
    return true;
  }
  const auto* node_cfg =
      config->serverConfig()->getNode(config->serverConfig()->getMyNodeID());
  assert(node_cfg);
  bool is_sequencer_node = node_cfg->isSequencingEnabled();

  seq->noteConfigurationChanged(config, is_sequencer_node);

  if (!is_sequencer_node) {
    // no need to check for reactivation and such. The sequencer should've been
    // deactivated by the call to noteConfigurationChanged() above
    bumpCompletedStat();
    return true;
  }

  int rv =
      all_seq.reactivateIf(log_id,
                           std::bind(AllSequencers::sequencerShouldReactivate,
                                     std::placeholders::_1,
                                     config),
                           true /* only_consecutive_epoch */);
  if (rv == -1) {
    bool should_reprovision =
        seq ? AllSequencers::sequencerShouldReactivate(*seq, config) : false;
    if (should_reprovision) {
      // reprovisioning could not be started, but is still necessary
      return false;
    }
    // reprovisioning not necessary anymore
    queue_.erase(it);
    bumpCompletedStat();
    return true;
  }
  // reprovisioning in flight, moving token into the sequencer
  ld_check(seq);
  if (seq->background_activation_token_.valid()) {
    // This activation is already in flight, bumping the stat here since only
    // one completion is going to follow.
    token.release();
    bumpCompletedStat();
  } else {
    seq->background_activation_token_ = std::move(token);
  }
  queue_.erase(it);
  return true;
}

void SequencerBackgroundActivator::notifyCompletion(logid_t logid,
                                                    Status /* st */) {
  checkWorkerAsserts();
  auto seq =
      Worker::onThisThread()->processor_->allSequencers().findSequencer(logid);
  if (!seq || !seq->background_activation_token_.valid()) {
    // we don't care about this activation
    return;
  }
  bool should_reprovision =
      AllSequencers::sequencerShouldReactivate(*seq, Worker::getConfig());
  seq->background_activation_token_.release();
  if (should_reprovision) {
    auto res = queue_.insert(logid.val());
    if (!res.second) {
      // this log_id is in queue_ already. And it was in flight, as we have the
      // token. It's now deduped, so we should bump the completed stat for one
      // of the copies
      bumpCompletedStat();
    }
  } else {
    // we are done!
    bumpCompletedStat();
  }
  maybeProcessQueue();
}

void SequencerBackgroundActivator::maybeProcessQueue() {
  checkWorkerAsserts();
  deactivateQueueProcessingTimer();
  size_t limit = maxRequestsInFlight();
  if (!budget_) {
    budget_.reset(new ResourceBudget(limit));
  } else if (budget_->getLimit() != limit) {
    // in case the setting changed after initialization
    budget_->setLimit(limit);
  }

  bool ran_out_of_time = false;
  std::chrono::steady_clock::time_point start_time =
      std::chrono::steady_clock::now();

  while (!queue_.empty() && budget_->available() > 0) {
    auto token = budget_->acquireToken();
    ld_check(token.valid());
    if (!processOneLog(std::move(token))) {
      // failed processing a sequencer. There's no point in retrying
      // immediately, so instead do it on a timer
      activateQueueProcessingTimer();
      break;
    }

    // Limiting this loop to 1ms
    if (usec_since(start_time) > 1000) {
      ran_out_of_time = true;
      break;
    }
  }
  if (queue_.empty() && budget_->available() == budget_->getLimit()) {
    // destroy this instance of SequencerBackgroundActivator, as it has nothing
    // left to do
    Worker::onThisThread()->sequencerBackgroundActivator().reset();
  } else if (ran_out_of_time) {
    // schedule retry
    activateQueueProcessingTimer();
  }
}

size_t SequencerBackgroundActivator::maxRequestsInFlight() {
  return Worker::settings().max_sequencer_background_activations_in_flight;
}

void SequencerBackgroundActivator::activateQueueProcessingTimer() {
  Worker* w = Worker::onThisThread();
  ld_check(w);
  if (!retry_timer_.isAssigned()) {
    retry_timer_.assign(w->getEventBase(), [this] { maybeProcessQueue(); });
  }
  auto timeout =
      Worker::settings().sequencer_background_activation_retry_interval;
  retry_timer_.activate(timeout, &w->commonTimeouts());
}

void SequencerBackgroundActivator::deactivateQueueProcessingTimer() {
  retry_timer_.cancel();
}

void SequencerBackgroundActivator::bumpCompletedStat(uint64_t val) {
  WORKER_STAT_ADD(background_sequencer_reactivations_completed, val);
}

void SequencerBackgroundActivator::bumpScheduledStat(uint64_t val) {
  WORKER_STAT_ADD(background_sequencer_reactivations_scheduled, val);
}

}} // namespace facebook::logdevice
