/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/common/ConnectThrottle.h"

#include <algorithm>

#include "logdevice/common/debug.h"

namespace facebook { namespace logdevice {

/**
 * @return tp value in milliseconds since tp's epoch
 */
static unsigned long to_ms(std::chrono::steady_clock::time_point tp) {
  return std::chrono::duration_cast<std::chrono::milliseconds>(
             tp.time_since_epoch())
      .count();
}

void ConnectThrottle::connectSucceeded() {
  current_delay_ = std::chrono::milliseconds::zero();
  down_until_ = std::chrono::steady_clock::time_point::min();

  ld_debug(
      "at %ld, resetting down_until_ to %ld", to_ms(now()), to_ms(down_until_));
}

void ConnectThrottle::connectFailed() {
  current_delay_ = current_delay_.count() == 0
      ? backoff_settings_.initial_delay
      : current_delay_ * backoff_settings_.multiplier;
  current_delay_ = std::min(current_delay_, backoff_settings_.max_delay);

  auto time_now = now();

  down_until_ = time_now + current_delay_;

  ld_debug("at %ld. set down_until_ to %lu, current_delay_ to %ld",
           to_ms(time_now),
           to_ms(down_until_),
           current_delay_.count());
}

bool ConnectThrottle::mayConnect() const {
  return now() > down_until_;
}
}} // namespace facebook::logdevice
