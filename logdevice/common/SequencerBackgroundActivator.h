/**
 * Copyright (c) 2017-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <memory>
#include <unordered_set>

#include "logdevice/include/Err.h"
#include "logdevice/include/types.h"
#include "logdevice/common/LibeventTimer.h"
#include "logdevice/common/ResourceBudget.h"

namespace facebook { namespace logdevice {

/**
 * @file State machine that notifies sequencers of config changes and
 *       reactivates them to reprovision metadata if needed
 */

class SequencerBackgroundActivator {
 public:
  SequencerBackgroundActivator() {}

  static constexpr int getThreadAffinity(int nthreads) {
    // assigning to one specific worker. We already have a bunch of stuff on
    // W0, so picking a different worker for sequencer reactivations
    return 7 % nthreads;
  }

  // schedules sequencers for the given logs for reactivation
  void schedule(std::vector<logid_t>);

  // Method called by the worker whenever it is notified about a completed
  // (successfully or not) sequencer reactivation
  void notifyCompletion(logid_t logid, Status);

 private:
  // internal method that checks that SequencerBackgroundActivator methods are
  // running on the right thread
  void checkWorkerAsserts();

  // processes the log at the top of the queue
  bool processOneLog(ResourceBudget::Token token);

  // processes the queue if there are pending requests and the number of
  // requests currently in flight does not exceed maxRequestsInFlight()
  void maybeProcessQueue();

  // activates the timer for queue processing
  void activateQueueProcessingTimer();

  // deactivates the timer for queue processing
  void deactivateQueueProcessingTimer();

  // max requests in flight
  size_t maxRequestsInFlight();

  // called whenever a reactivation (or several) has completed successfully
  void bumpCompletedStat(uint64_t val = 1);

  // called when background reactivations are scheduled
  void bumpScheduledStat(uint64_t val = 1);

  // Queue of log_ids to process. Avoiding use of an std::queue here for
  // deduplication purposes
  std::unordered_set<logid_t::raw_type> queue_;

  // timer for retrying processing the queue later in case of failures
  LibeventTimer retry_timer_;

  // limiter on the number of activations
  std::unique_ptr<ResourceBudget> budget_;
};

}} // namespace facebook::logdevice
