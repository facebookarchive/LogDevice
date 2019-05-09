/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/common/ExponentialBackoffTimer.h"

#include <algorithm>

#include <folly/Random.h>

#include "logdevice/common/TimeoutMap.h"
#include "logdevice/common/Worker.h"

namespace facebook { namespace logdevice {

ExponentialBackoffTimer::~ExponentialBackoffTimer() {
  cancel();
}

void ExponentialBackoffTimer::assign(
    std::function<void()> callback,
    const chrono_expbackoff_t<Duration>& settings) {
  // Sanity checks
  updateSettings(settings);

  // (Re-)initialize members
  setCallback(std::move(callback));
  calculateNextEffectiveDelay();
}

void ExponentialBackoffTimer::updateSettings(
    const chrono_expbackoff_t<Duration>& settings) {
  ld_check(settings.initial_delay >= Duration::zero());
  ld_check(settings.max_delay >= settings.initial_delay);
  ld_check(settings.multiplier >= 0);
  settings_ = settings;
  next_delay_ = settings_.initial_delay;
}

void ExponentialBackoffTimer::activate() {
  if (isActive()) {
    return;
  }

  current_delay_ = next_effective_delay_;
  timer_.activate(next_effective_delay_, timeout_map_);

  // Exponential backoff: next time we try to reconnect, multiply the delay
  if (next_delay_.count() == 0) {
    // If initial delay is zero but max delay is positive, arbitrarily start
    // at 100ms after the first firing.
    next_delay_ = std::min(std::chrono::milliseconds(100), settings_.max_delay);
  } else {
    next_delay_ =
        std::min(settings_.multiplier * next_delay_, settings_.max_delay);
  }
  calculateNextEffectiveDelay();
}

void ExponentialBackoffTimer::fire() {
  timer_.activate(std::chrono::microseconds(0));
  next_delay_ = settings_.initial_delay;
  calculateNextEffectiveDelay();
}

void ExponentialBackoffTimer::setCallback(std::function<void()> callback) {
  if (callback) {
    timer_.setCallback([cb = std::move(callback)] {
      ld_check(cb);
      cb();
    });
  } else {
    timer_.setCallback(nullptr);
  }
}

void ExponentialBackoffTimer::reset() {
  cancel();
  next_delay_ = settings_.initial_delay;
  calculateNextEffectiveDelay();
}

bool ExponentialBackoffTimer::isActive() const {
  return timer_.isActive();
}

void ExponentialBackoffTimer::cancel() {
  // no-op if timer is not active
  timer_.cancel();
}

void ExponentialBackoffTimer::calculateNextEffectiveDelay() {
  using namespace std::chrono;
  if (randomization_ == 0) {
    next_effective_delay_ = next_delay_;
  } else {
    ld_check(timeout_map_ == nullptr);
    auto next_delay_us = duration_cast<microseconds>(next_delay_).count();
    std::uniform_int_distribution<int64_t> dist_us(
        next_delay_us * (1 - randomization_),
        next_delay_us * (1 + randomization_));
    folly::ThreadLocalPRNG rng;
    next_effective_delay_ = duration_cast<Duration>(microseconds(dist_us(rng)));
  }
}

}} // namespace facebook::logdevice
