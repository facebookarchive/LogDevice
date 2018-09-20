/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "logdevice/common/ExponentialBackoffAdaptiveVariable.h"

#include <folly/Random.h>

namespace facebook { namespace logdevice {

ExponentialBackoffAdaptiveVariable::ExponentialBackoffAdaptiveVariable(
    double min,
    double initial,
    double max,
    double multiplier,
    double decrease_rate,
    double fuzz_factor)
    : min_(min),
      max_(max),
      multiplier_(multiplier),
      decrease_rate_(decrease_rate),
      fuzz_factor_(fuzz_factor) {
  setCurrentValue(initial);
}

double ExponentialBackoffAdaptiveVariable::getCurrentValue() const {
  return current_;
}

void ExponentialBackoffAdaptiveVariable::setCurrentValue(double val) {
  current_ = val;
  if (current_ > max_) {
    current_ = max_;
  } else if (current_ < min_) {
    current_ = min_;
  }
}

void ExponentialBackoffAdaptiveVariable::negativeFeedback() {
  setCurrentValue(fuzz(current_ * multiplier_));
  last_positive_feedback_.reset();
}

void ExponentialBackoffAdaptiveVariable::positiveFeedback(
    ExponentialBackoffAdaptiveVariable::TS::TimePoint now) {
  if (last_positive_feedback_.hasValue()) {
    const auto delta = std::chrono::duration_cast<std::chrono::milliseconds>(
        now - last_positive_feedback_.value());
    const double decrease_value = fuzz(delta.count() * decrease_rate_ / 1000);

    if (current_ < decrease_value) {
      current_ = min_;
    } else {
      setCurrentValue(current_ - decrease_value);
    }
  }

  last_positive_feedback_ = now;
}

double ExponentialBackoffAdaptiveVariable::fuzz(double val) {
  return val + val * folly::Random::randDouble(-fuzz_factor_, fuzz_factor_);
}

}} // namespace facebook::logdevice
