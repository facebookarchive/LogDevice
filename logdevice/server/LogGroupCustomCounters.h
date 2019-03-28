/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <folly/experimental/StringKeyedUnorderedMap.h>
#include <folly/small_vector.h>
#include <folly/stats/MultiLevelTimeSeries.h>

#include "logdevice/common/stats/Stats.h"

namespace facebook { namespace logdevice {

class LogsConfig;

using GroupResults = std::unordered_map<uint16_t, int64_t>;
using Duration = CustomCountersTimeSeries::TimeSeries::Duration;
using TimePoint = CustomCountersTimeSeries::TimeSeries::TimePoint;
using CustomCountersAggregateMap = folly::StringKeyedUnorderedMap<GroupResults>;

Duration getMaxInterval(StatsHolder* stats_holder, std::string time_series);

CustomCountersAggregateMap doAggregateCustomCounters(StatsHolder* stats,
                                                     const Duration& intervals);
}} // namespace facebook::logdevice
