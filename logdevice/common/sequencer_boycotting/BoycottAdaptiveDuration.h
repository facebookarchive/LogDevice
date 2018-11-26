/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <chrono>

#include "logdevice/common/NodeID.h"
#include "logdevice/common/protocol/ProtocolReader.h"
#include "logdevice/common/protocol/ProtocolWriter.h"
/**
 * @file An implementation to the multiplicative increase additive decrease
 * algorithm for the boycotting adaptive duration.
 * The value starts with some initial duration, negative feedback increases
 * the duration `increase_factor_` times. Positive feedback is calculated
 * on the fly based on the time of the last negative feedback.
 * The effective duration gets decreased `decrease_rate_` every
 * `decrease_time_step_` milliseconds since `value_timestamp_`.
 */

namespace facebook { namespace logdevice {
class BoycottAdaptiveDuration {
 public:
  using TS = std::chrono::time_point<std::chrono::system_clock,
                                     std::chrono::nanoseconds>;

  BoycottAdaptiveDuration() {}
  BoycottAdaptiveDuration(node_index_t node_index,
                          std::chrono::milliseconds min_duration,
                          std::chrono::milliseconds max_duration,
                          std::chrono::milliseconds decrease_rate,
                          std::chrono::milliseconds decrease_time_step,
                          int increase_factor,
                          std::chrono::milliseconds current_value,
                          TS value_timestamp,
                          std::chrono::milliseconds last_boycott_duration =
                              std::chrono::milliseconds(0));

  // Get the current effective duration after applying all the positive and
  // negative feedbacks
  std::chrono::milliseconds getEffectiveDuration(TS now) const;

  // Apply a negative feedback on the current duration. This is called whenever
  // the node gets boycotted passing the duration and the time of
  // the boycotting
  void negativeFeedback(std::chrono::milliseconds duration, TS now);

  // The node is no longer boycotted, start applying a positive feedback
  // immediately
  void resetIssued(std::chrono::milliseconds duration, TS now);

  node_index_t getNodeIndex() const {
    return node_index_;
  }

  TS getValueTimestamp() const {
    return value_timestamp_;
  }

  // Returns true if the current effective value is equal to the minimum value.
  // This means that this node has the default duration and can be removed.
  bool isDefault(TS now) const;

  void serialize(ProtocolWriter& writer) const;
  void deserialize(ProtocolReader& reader);

  bool operator==(const BoycottAdaptiveDuration& other) const;

 private:
  node_index_t node_index_;

  // The minimum duration any node can get using positive feedback.
  std::chrono::milliseconds min_duration_;
  // The maximum duration any node can be punished.
  std::chrono::milliseconds max_duration_;
  std::chrono::milliseconds decrease_rate_;
  std::chrono::milliseconds decrease_time_step_;
  int increase_factor_;

  // The current duration which is either set by the initial value or by
  // the negative feedback. It doesn't get changed on positive feedback.
  std::chrono::milliseconds current_value_;

  // The last modification time of the current_value_ (initial time or time of
  // last negative feedback).
  TS value_timestamp_;

  std::chrono::milliseconds last_boycott_duration_;
};
}} // namespace facebook::logdevice
