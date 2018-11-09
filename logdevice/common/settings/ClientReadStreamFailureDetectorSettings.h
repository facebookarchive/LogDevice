/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <chrono>

#include "logdevice/common/util.h"

namespace facebook { namespace logdevice {

struct ClientReadStreamFailureDetectorSettings {
  // Duration to use for the moving average.
  std::chrono::seconds moving_avg_duration;
  // Define the required margin of the outlier detection algorithm.  For
  // instance a required marginn of 5 means that a shard will be considered
  // outlier only if it's 500% slower than the average of the other shards.
  // This parameter changes adaptively in order to avoid too many rewinds.
  float required_margin;
  // Rate at which we decrease the required margin as we complete windows
  // without a rewind. Defined in terms of units / 1s. For instance a value of
  // 0.25 means we will substract 0.25 to the required margin for every second
  // spent reading.
  float required_margin_decrease_rate;
  // Define how long a shard should remain in the outlier list after it has
  // been detected slow. This duration is adaptive.
  chrono_expbackoff_t<std::chrono::seconds> outlier_duration;
  // Rate at which we decrease the time after which we'll try to reinstate an
  // outlier in the read set. This rate gets applied when a shard is not
  // detected an outlier.
  float outlier_duration_decrease_rate;
};
}} // namespace facebook::logdevice
