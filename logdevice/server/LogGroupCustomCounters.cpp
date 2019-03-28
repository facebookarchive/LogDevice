/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/server/LogGroupCustomCounters.h"

#include <chrono>
#include <vector>

#include <boost/algorithm/string.hpp>
#include <boost/format.hpp>

#include "logdevice/common/commandline_util_chrono.h"
#include "logdevice/common/configuration/LogsConfig.h"

namespace facebook { namespace logdevice {

Duration getCustomCountersMaxInterval() {
  return CustomCountersTimeSeries::getCustomCounterIntervals().back();
}

CustomCountersAggregateMap doAggregateCustomCounters(StatsHolder* stats,
                                                     const Duration& interval) {
  using namespace std::chrono;

  CustomCountersAggregateMap output;

  stats->runForEach([&](facebook::logdevice::Stats& s) {
    // Use synchronizedCopy() so we do not have to hold a read lock on
    // per_log_append_bytes map while we iterate over individual entries.
    for (auto& entry :
         s.synchronizedCopy(&facebook::logdevice::Stats::per_log_stats)) {
      std::lock_guard<std::mutex> guard(entry.second->mutex);
      std::string& clean_name = entry.first;

      auto& custom_counters = entry.second->custom_counters;
      if (!custom_counters) {
        continue;
      }
      auto& time_series_map = *custom_counters->customCountersTimeSeries_;
      // NOTE: It might be tempting to pull `now' out of the loops but
      // folly::MultiLevelTimeSeries barfs if we ask it for data that is
      // too old.  Keep it under the lock for now, optimize if necessary.
      const TimePoint now{
          duration_cast<Duration>(steady_clock::now().time_since_epoch())};
      const TimePoint start{now - interval};
      std::vector<uint8_t> keys_to_remove;
      auto& aggregate_map = output[clean_name];
      for (auto& time_series : time_series_map) {
        // Flush any cached updates and discard any stale data
        time_series.second.update(now);
        if (time_series.second.count(getCustomCountersMaxInterval()) == 0) {
          keys_to_remove.emplace_back(time_series.first);
          continue;
        }
        aggregate_map[time_series.first] += time_series.second.sum(start, now);
      }

      for (auto key : keys_to_remove) {
        time_series_map.erase(key);
      }
    }
  });

  return output;
}
}} // namespace facebook::logdevice
