/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/server/LogGroupThroughput.h"

#include <chrono>
#include <vector>

#include <boost/algorithm/string.hpp>
#include <boost/format.hpp>

#include "logdevice/common/commandline_util_chrono.h"
#include "logdevice/common/configuration/LogsConfig.h"

namespace facebook { namespace logdevice {

// Accesses thread-local per-log-group stats and aggregates them all into a
// big map
AggregateMap doAggregate(StatsHolder* stats,
                         std::string time_series_,
                         const std::vector<Duration>& intervals,
                         std::shared_ptr<LogsConfig> logs_config) {
  // Okay...  For each thread, for each log group in the thread-local
  // PerLogStats, for each query interval, calculate the rate in B/s and
  // aggregate.  Output is a map (log group, query interval) -> (sum of
  // rates collected from different threads).
  AggregateMap output;

  std::string delimiter = logs_config->getNamespaceDelimiter();

  std::shared_ptr<PerLogTimeSeries> facebook::logdevice::PerLogStats::*
      member_ptr = nullptr;
#define TIME_SERIES_DEFINE(name, strings, _, __)            \
  for (const std::string& str : strings) {                  \
    if (str == time_series_) {                              \
      member_ptr = &facebook::logdevice::PerLogStats::name; \
      break;                                                \
    }                                                       \
  }
#include "logdevice/common/stats/per_log_time_series.inc" // nolint
  ld_check(member_ptr != nullptr);

  stats->runForEach([&](facebook::logdevice::Stats& s) {
    // Use synchronizedCopy() so we do not have to hold a read lock on
    // per_log_stats map while we iterate over individual entries.
    for (auto& entry :
         s.synchronizedCopy(&facebook::logdevice::Stats::per_log_stats)) {
      std::lock_guard<std::mutex> guard(entry.second->mutex);
      std::string& clean_name = entry.first;

      auto stat = entry.second.get()->*member_ptr;
      if (!stat) {
        continue;
      }

      auto& time_series = *stat->timeSeries_;
      // NOTE: It might be tempting to pull `now' out of the loops but
      // folly::MultiLevelTimeSeries barfs if we ask it for data that is
      // too old.  Keep it under the lock for now, optimize if necessary.
      //
      // TODO: Constructing the TimePoint is slightly awkward at the moment
      // as the folly stats code is being cleaned up to better support real
      // clock types.  appendBytesTimeSeries_ should simply be changed to
      // use std::steady_clock as it's clock type.  I'll do that in a
      // separate diff for now, though.
      const TimePoint now{std::chrono::duration_cast<Duration>(
          std::chrono::steady_clock::now().time_since_epoch())};
      // Flush any cached updates and discard any stale data
      time_series.update(now);

      auto& aggregate_vector = output[clean_name];
      aggregate_vector.resize(intervals.size());
      // For each query interval, make a MultiLevelTimeSeries::rate() call
      // to find the approximate rate over that interval
      for (int i = 0; i < intervals.size(); ++i) {
        auto rate_per_time_type =
            time_series.rate<RateType>(now - intervals[i], now);
        // Duration may not be seconds, convert to seconds
        aggregate_vector[i] +=
            rate_per_time_type * Duration::period::den / Duration::period::num;
      }
    }
  });

  return output;
}

Duration getMaxInterval(StatsHolder* stats_holder, std::string time_series) {
#define TIME_SERIES_DEFINE(name, strings, _, __)                        \
  for (const std::string& str : strings) {                              \
    if (str == time_series) {                                           \
      return stats_holder->params_.get()->time_intervals_##name.back(); \
    }                                                                   \
  }
#include "logdevice/common/stats/per_log_time_series.inc" // nolint
  ld_check(false);
  return Duration{};
}

bool verifyIntervals(StatsHolder* stats_holder,
                     std::string time_series,
                     std::vector<Duration> query_intervals,
                     std::string& err) {
  Duration max_interval = getMaxInterval(stats_holder, time_series);
  using namespace std::chrono;
  for (auto interval : query_intervals) {
    if (interval > max_interval) {
      err = (boost::format("requested interval %s is larger than the max %s") %
             chrono_string(duration_cast<seconds>(interval)).c_str() %
             chrono_string(duration_cast<seconds>(max_interval)).c_str())
                .str();
      return false;
    }
  }
  return true;
}

}} // namespace facebook::logdevice
