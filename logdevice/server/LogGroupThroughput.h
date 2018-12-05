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

// Data type for rate calculations
using RateType = double;

// For each log group, this will hold the results for each interval in
// query_intervals_
using OneGroupResults = folly::small_vector<RateType, 4>;
using Duration = PerLogTimeSeries::TimeSeries::Duration;
using TimePoint = PerLogTimeSeries::TimeSeries::TimePoint;

using AggregateMap = folly::StringKeyedUnorderedMap<OneGroupResults>;

AggregateMap doAggregate(StatsHolder* stats,
                         std::string time_series_,
                         const std::vector<Duration>& intervals,
                         std::shared_ptr<LogsConfig> logs_config);

Duration getMaxInterval(StatsHolder* stats_holder, std::string time_series);

// checks that no requested intervals are higher than getMaxInterval()
bool verifyIntervals(StatsHolder* stats_holder,
                     std::string time_series,
                     std::vector<Duration> query_intervals,
                     std::string& err);

}} // namespace facebook::logdevice
