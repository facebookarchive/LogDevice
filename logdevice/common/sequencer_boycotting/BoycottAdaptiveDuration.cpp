/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "logdevice/common/sequencer_boycotting/BoycottAdaptiveDuration.h"

#include <chrono>

#include "logdevice/common/NodeID.h"

namespace facebook { namespace logdevice {

BoycottAdaptiveDuration::BoycottAdaptiveDuration(
    node_index_t node_index,
    std::chrono::milliseconds min_duration,
    std::chrono::milliseconds max_duration,
    std::chrono::milliseconds decrease_rate,
    std::chrono::milliseconds decrease_time_step,
    int increase_factor,
    std::chrono::milliseconds current_value,
    TS value_timestamp,
    std::chrono::milliseconds last_boycott_duration)
    : node_index_{node_index},
      min_duration_{min_duration},
      max_duration_{max_duration},
      decrease_rate_{decrease_rate},
      decrease_time_step_{decrease_time_step},
      increase_factor_{increase_factor},
      current_value_{current_value},
      value_timestamp_{value_timestamp},
      last_boycott_duration_{last_boycott_duration} {}

std::chrono::milliseconds
BoycottAdaptiveDuration::getEffectiveDuration(TS now) {
  // Don't apply positive feedback until the boycott duration is done.
  if (now - value_timestamp_ <= last_boycott_duration_) {
    return current_value_;
  }

  return std::max(current_value_ -
                      decrease_rate_ *
                          ((now - (value_timestamp_ + last_boycott_duration_)) /
                           decrease_time_step_),
                  min_duration_);
}

void BoycottAdaptiveDuration::negativeFeedback(
    std::chrono::milliseconds duration,
    TS now) {
  current_value_ = getEffectiveDuration(now) * increase_factor_;
  current_value_ = std::min(max_duration_, current_value_);
  last_boycott_duration_ = duration;
  value_timestamp_ = now;
}

void BoycottAdaptiveDuration::resetIssued(std::chrono::milliseconds duration,
                                          TS now) {
  current_value_ = duration;
  current_value_ = std::min(max_duration_, current_value_);
  current_value_ = std::max(min_duration_, current_value_);
  last_boycott_duration_ = std::chrono::milliseconds(0);
  value_timestamp_ = now;
}

}} // namespace facebook::logdevice
