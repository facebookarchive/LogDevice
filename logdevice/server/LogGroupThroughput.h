/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <folly/stats/MultiLevelTimeSeries.h>
#include <folly/experimental/StringKeyedUnorderedMap.h>
#include <folly/small_vector.h>

#include "logdevice/common/stats/Stats.h"
#include "logdevice/server/Server.h"

namespace facebook { namespace logdevice {

// Data type for rate calculations
using RateType = double;

// For each log group, this will hold the results for each interval in
// query_intervals_
using OneGroupResults = folly::small_vector<RateType, 4>;
using Duration = PerLogTimeSeries::TimeSeries::Duration;
using TimePoint = PerLogTimeSeries::TimeSeries::TimePoint;

struct StringPieceHash {
  size_t operator()(folly::StringPiece s) const {
    return folly::hash::SpookyHashV2::Hash64(s.data(), s.size(), 0);
  }
};

using AggregateMap =
    folly::StringKeyedUnorderedMap<OneGroupResults, StringPieceHash>;

AggregateMap doAggregate(StatsHolder* stats,
                         std::string time_series_,
                         const std::vector<Duration>& intervals,
                         std::shared_ptr<LogsConfig> logs_config);

Duration getMaxInterval(Server* server_, std::string time_series);

// checks that no requested intervals are higher than getMaxInterval()
bool verifyIntervals(Server* server,
                     std::string time_series,
                     std::vector<Duration> query_intervals,
                     std::string& err);

}} // namespace facebook::logdevice
