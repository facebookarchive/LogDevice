/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/common/replicated_state_machine/TrimRSMRetryHandler.h"

#include <folly/Memory.h>

#include "logdevice/common/BackoffTimer.h"
#include "logdevice/common/Processor.h"
#include "logdevice/common/Request.h"
#include "logdevice/common/Worker.h"
#include "logdevice/common/debug.h"
#include "logdevice/common/replicated_state_machine/ReplicatedStateMachine.h"
#include "logdevice/common/replicated_state_machine/TrimRSMRequest.h"

namespace facebook { namespace logdevice {

TrimRSMRetryHandler::TrimRSMRetryHandler(logid_t delta_log_id,
                                         logid_t snapshot_log_id,
                                         RSMType rsm_type)
    : delta_log_id_(delta_log_id),
      snapshot_log_id_(snapshot_log_id),
      rsm_type_(rsm_type),
      ref_holder_(this) {
  retry_timer_ = std::make_unique<ExponentialBackoffTimer>(

      std::bind(&TrimRSMRetryHandler::trimImpl, this),
      std::chrono::seconds{5},
      std::chrono::seconds{300});
}

void TrimRSMRetryHandler::trim(std::chrono::milliseconds retention) {
  // Change this unconditionally so that even if we are already in a retry loop
  // we take the new retention into account.
  retention_ = retention;

  if (!in_flight_retention_.hasValue()) {
    trimImpl();
  }
}

void TrimRSMRetryHandler::trimImpl() {
  ld_check(!retry_timer_->isActive());

  auto cur_timestamp = std::chrono::duration_cast<std::chrono::milliseconds>(
      std::chrono::high_resolution_clock::now().time_since_epoch());
  const auto trim_and_findtime_timeout = std::chrono::seconds{20};

  WeakRef<TrimRSMRetryHandler> ref = ref_holder_.ref();
  auto cb = [this, ref](Status st) {
    if (!ref) {
      // `this` is gone.
      return;
    }

    if (st == E::OK || st == E::PARTIAL) {
      // TODO(T11022734): there are known improvements that need to be made for
      // findTime() and trim() so they do not return E::PARTIAL in the context
      // of some nodes being in the config but staying in repair for a long
      // time. We can more aggresively retry when E::PARTIAL is considered a
      // transient failure.
      ld_check(in_flight_retention_.hasValue());
      if (in_flight_retention_.value() == retention_) {
        in_flight_retention_.clear();
        return;
      }

      rsm_info(rsm_type_,
               "trim() was called while another trim was in flight. Trimming "
               "again.");
      trimImpl();
    }

    rsm_error(rsm_type_,
              "Could not trim the event log state machine: %s. Will retry in "
              "%lums",
              error_name(st),
              retry_timer_->getNextDelay().count());

    ld_check(!retry_timer_->isActive());
    retry_timer_->activate();
  };

  in_flight_retention_ = retention_;
  std::unique_ptr<Request> rq =
      std::make_unique<TrimRSMRequest>(delta_log_id_,
                                       snapshot_log_id_,
                                       cur_timestamp - retention_,
                                       cb,
                                       Worker::onThisThread()->idx_,
                                       Worker::onThisThread()->worker_type_,
                                       rsm_type_,
                                       false, /* don't trim everything */
                                       trim_and_findtime_timeout,
                                       trim_and_findtime_timeout);
  Worker::onThisThread()->processor_->postWithRetrying(rq);
}

}} // namespace facebook::logdevice
