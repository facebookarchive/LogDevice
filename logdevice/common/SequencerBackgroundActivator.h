/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <memory>
#include <unordered_set>

#include "logdevice/common/Processor.h"
#include "logdevice/common/ResourceBudget.h"
#include "logdevice/common/Timer.h"
#include "logdevice/include/Err.h"
#include "logdevice/include/types.h"

namespace facebook { namespace logdevice {

/**
 * @file State machine that triggers sequencer reactivations when nodesets or
 *       window size need to be changed.
 */

class AllSequencers;
class Processor;
class Sequencer;

class SequencerBackgroundActivator {
 public:
  SequencerBackgroundActivator() {}

  static WorkerType getWorkerType(Processor* processor) {
    // This returns either WorkerType::BACKGROUND or WorkerType::GENERAL based
    // on whether we have background workers.
    if (processor->getWorkerCount(WorkerType::BACKGROUND) > 0) {
      return WorkerType::BACKGROUND;
    }
    return WorkerType::GENERAL;
  }
  static constexpr int getThreadAffinity(int nthreads) {
    // Assigning to one specific worker. We already have a bunch of stuff on
    // W0, so picking a different worker for sequencer reactivations
    return 7 % nthreads;
  }

  // Schedules the given logs for checking whether recativation is needed.
  // If no updates are needed, this should be cheap.
  void schedule(std::vector<logid_t> logs);

  // Called when a sequencer activation completes, successfully or not.
  void notifyCompletion(logid_t log, Status st);

  // Posts a request to call schedule() from the correct thread.
  static void requestSchedule(Processor* processor, std::vector<logid_t> logs);

  // Posts a request to call notifyCompletion() from the correct thread.
  static void requestNotifyCompletion(Processor* processor,
                                      logid_t log,
                                      Status st);

 private:
  // internal method that checks that SequencerBackgroundActivator methods are
  // running on the right thread
  void checkWorkerAsserts();

  // Processes the given log. Returns false if it failed and needs to be
  // retried.
  bool processOneLog(logid_t log_id, ResourceBudget::Token& token);

  // Does the actual useful work.
  // Checks if the current sequencer's epoch metadata (nodeset, replication
  // factor etc) and settings (window size) matches the config. If not, starts
  // either sequencer reactivation or an epoch store update.
  //
  // If the sequencer is not active or not ready for updating metadata (e.g.
  // current metadata is not written to metadata log yet), returns an error.
  // When sequencer becomes ready for metadata updates, it'll notify us
  // through notifyCompletion(), and we'll schedule another call to this check.
  //
  // @return 0 if an update was started. Otherwise returns -1 and assigns err.
  // If update was started, notifyCompletion() will be called when the update is
  // finished.
  // If no update is needed, err is set to UPTODATE.
  // The caller should retry later in case of the following errors:
  // FAILED, NOBUFS, TOOMANY, NOTCONN, ACCESS.
  // The caller shouldn't retry on the following errors:
  // NOSEQUENCER, INPROGRESS, NOTFOUND, SYSLIMIT, INTERNAL, TOOBIG.
  int reprovisionOrReactivateIfNeeded(logid_t logid,
                                      std::shared_ptr<Sequencer> seq);

  // processes the queue if there are pending requests and the number of
  // requests currently in flight does not exceed maxRequestsInFlight()
  void maybeProcessQueue();

  // Activates the timer for queue processing. If timeout is not provided,
  // uses sequencer_background_activation_retry_interval setting.
  void activateQueueProcessingTimer(
      folly::Optional<std::chrono::microseconds> timeout);

  // deactivates the timer for queue processing
  void deactivateQueueProcessingTimer();

  // Called when a log is added to queue_.
  void bumpScheduledStat(uint64_t val = 1);

  // Called when a Token is released. One token is acquired for every log
  // removed from queue_, so the calls to bumpScheduledStat() and
  // bumpCompletedStat() should balance out.
  void bumpCompletedStat(uint64_t val = 1);

  // Queue of log_ids to process. Avoiding use of an std::queue here for
  // deduplication purposes
  std::unordered_set<logid_t::raw_type> queue_;

  // timer for retrying processing the queue later in case of failures
  Timer retry_timer_;

  // limiter on the number of activations
  std::unique_ptr<ResourceBudget> budget_;
};

}} // namespace facebook::logdevice
