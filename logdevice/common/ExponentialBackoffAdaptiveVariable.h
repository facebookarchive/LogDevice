/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <chrono>

#include "folly/Optional.h"
#include "logdevice/common/Timestamp.h"

/**
 * @file Implementation of a variable value that grows multiplicatively upon
 *        receiving negative feedback and decreases additively upon receiving
 *        positive feedback.
 *
 * This can for example be used to implement an adaptive failure detection
 * algorithm with a "sensitivity" adaptive variable:
 *
 * - Upon receiving positive feedback (the failure detection is useful), the
 *   sensitivity is decreased at a constant rate additively;
 *
 * - Upon receiving negative feedback (the failure detection is too aggressive),
 *   the sensitivity gets increased multiplicatively.
 */

namespace facebook { namespace logdevice {

class ExponentialBackoffAdaptiveVariable {
 public:
  using TS = Timestamp<std::chrono::steady_clock,
                       detail::Holder,
                       std::chrono::milliseconds>;
  /**
   * @param min           Minimum cap on the value;
   * @param initial       Initial value;
   * @param max           Maximum cap on the value;
   * @param multiplier    Multiplier to apply when receiving negative feedback;
   * @param decrease_rate Decrease rate when receiving positive feedback, in
   *                      units per second.
   * @param fuzz_factor   Add some fuziness to the value. This helps randomize
   *                      the value that many variables may have when the all
   *                      react to the same positive/negative feedback, which
   *                      may be desirable in some scenarios. Expressed as a
   *                      percentage, for example 0.1 means the value will be
   *                      randomized by +/-10%.
   */
  ExponentialBackoffAdaptiveVariable(double min,
                                     double initial,
                                     double max,
                                     double multiplier,
                                     double decrease_rate,
                                     double fuzz_factor);

  /**
   * @return the current value.
   */
  double getCurrentValue() const;

  /**
   * @param val New value to set. Will be capped by min and max.
   */
  void setCurrentValue(double val);

  /**
   * Increase the variable multiplicatively.
   */
  void negativeFeedback();

  /**
   * Decrease the variable additively.
   *
   * This utility is meant to decrease the adaptive variable upon receiving
   * positive feedback according to a configured and fixed rate. As such calling
   * this function once a minute or once a second will not change the rate at
   * which the variable decreases assuming no negative signal in between. The
   * rate at which we call this function will however affect the granularity of
   * the changes.d
   *
   * @param now Current timestamp. Used to calculate by how much the variable
   * should be decreased.
   */
  void positiveFeedback(TS::TimePoint now);

 private:
  double min_;
  double max_;
  double multiplier_;
  double decrease_rate_;
  double fuzz_factor_;

  // Current value.
  double current_;

  // Timestamp of the last time we decreased the variable
  folly::Optional<TS::TimePoint> last_positive_feedback_;

  // Randomize a value given the "fuzz factor".
  double fuzz(double val);
};

/**
 * ChronoExponentialBackoffAdaptiveVariable is a thin wrapper around
 * ExponentialBackoffAdaptiveVariable to expose the variable as a chrono value.
 *
 * Because the underlying implementation uses floating points, we can still call
 * `positiveFeedback` at an interval smaller than the granularity of `Duration`
 * (say every milliseconds if Duration=seconds) and expect the value to decrease
 * across multiple calls.
 */

template <typename Duration>
class ChronoExponentialBackoffAdaptiveVariable {
 public:
  using duration_t = Duration;
  using rep_t = typename Duration::rep;
  ChronoExponentialBackoffAdaptiveVariable(duration_t min,
                                           duration_t initial,
                                           duration_t max,
                                           double multiplier,
                                           double decrease_rate,
                                           double fuzz_factor)
      : impl_(min.count(),
              initial.count(),
              max.count(),
              multiplier,
              decrease_rate,
              fuzz_factor) {}

  duration_t getCurrentValue() const {
    return duration_t(static_cast<rep_t>(std::floor(impl_.getCurrentValue())));
  }

  void setCurrentValue(duration_t val) {
    impl_.setCurrentValue(static_cast<double>(val.count()));
  }

  void negativeFeedback() {
    impl_.negativeFeedback();
  }

  void positiveFeedback(ExponentialBackoffAdaptiveVariable::TS::TimePoint now) {
    impl_.positiveFeedback(now);
  }

 private:
  ExponentialBackoffAdaptiveVariable impl_;
};

}} // namespace facebook::logdevice
