/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <chrono>

#include <folly/Optional.h>

#include "logdevice/common/WeakRefHolder.h"
#include "logdevice/common/replicated_state_machine/ReplicatedStateMachine-enum.h"
#include "logdevice/common/types_internal.h"
#include "logdevice/include/Err.h"
#include "logdevice/include/types.h"

/**
 * A utility for requesting trimming of a RSM, and which retries on the
 * background.
 */

namespace facebook { namespace logdevice {

class ExponentialBackoffTimer;

class TrimRSMRetryHandler {
 public:
  /**
   * Create a TrimRSMRetryHandler.
   *
   * @param delta_log_id    must not be LOGID_INVALID;
   * @param snapshot_log_id must not be LOGID_INVALID.
   * @param rsm_type        defines the RSM type for logging purposes.
   */
  TrimRSMRetryHandler(logid_t delta_log_id,
                      logid_t snapshot_log_id,
                      RSMType rsm_type);

  /**
   * Request trimming of the RSM.
   *
   * This will issue a TrimRSMRequest in the background and retry indefinitely
   * (with exponential backoff) if it fails. If called while this object is
   * already in a retry loop, the new trim will be done on the next retry.
   *
   * @param retention Request to keep historical data for at least this amount
   *                  of time.
   */
  void trim(std::chrono::milliseconds retention);

 private:
  logid_t delta_log_id_;
  logid_t snapshot_log_id_;
  RSMType rsm_type_;
  WeakRefHolder<TrimRSMRetryHandler> ref_holder_;
  std::chrono::milliseconds retention_;
  std::unique_ptr<ExponentialBackoffTimer> retry_timer_;

  // If set, either a TrimRSMRequest is in flight for this retention, or the
  // retry timer is active for retry.
  folly::Optional<std::chrono::milliseconds> in_flight_retention_;

  void trimImpl();
};

}} // namespace facebook::logdevice
