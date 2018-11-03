/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/server/admincommands/CustomCounters.h"

#include <algorithm>
#include <chrono>
#include <cinttypes>
#include <sstream>
#include <unordered_map>
#include <vector>

#include <boost/algorithm/string.hpp>
#include <boost/program_options/errors.hpp>
#include <folly/experimental/StringKeyedUnorderedMap.h>

#include "logdevice/common/commandline_util_chrono.h"

namespace facebook { namespace logdevice { namespace commands {

using OneGroupResults = std::unordered_map<uint8_t, int64_t>;

using Duration = StatsCustomCounters::Duration;

using TimePoint = CustomCountersTimeSeries::TimeSeries::TimePoint;

using AggregateMap = folly::StringKeyedUnorderedMap<OneGroupResults>;

static Duration getCustomCountersMaxInterval() {
  return CustomCountersTimeSeries::getCustomCounterIntervals().back();
}

// Accesses thread-local per-log-group stats and aggregates them all into a
// big map
static AggregateMap doAggregate(StatsHolder* stats,
                                const Duration& interval,
                                std::shared_ptr<LogsConfig> logs_config) {
  using namespace std::chrono;

  AggregateMap output;

  std::string delimiter = logs_config->getNamespaceDelimiter();

  stats->runForEach([&](facebook::logdevice::Stats& s) {
    // Use synchronizedCopy() so we do not have to hold a read lock on
    // per_log_append_bytes map while we iterate over individual entries.
    for (auto& entry :
         s.synchronizedCopy(&facebook::logdevice::Stats::per_log_stats)) {
      std::lock_guard<std::mutex> guard(entry.second->mutex);
      std::string& clean_name = entry.first;

      // The log group name now contains the the delimiter of the namespace
      // as the first character, this breaks LogProvisioner, for now we will
      // trim this and this should be removed as of t13899839
      boost::trim_if(clean_name, boost::is_any_of(delimiter));

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

static void printOne(EvbufferTextOutput& out,
                     folly::StringPiece group_name,
                     const OneGroupResults& results,
                     const std::vector<uint16_t>& keys_filter) {
  std::stringstream group_output;
  group_output << "STAT ";
  group_output.write(group_name.data(), group_name.size());
  group_output << "\r\n";
  bool output = false;
  if (keys_filter.empty()) {
    for (auto result : results) {
      group_output << "\t\t" << static_cast<uint16_t>(result.first) << " "
                   << result.second << "\r\n";
      output = true;
    }
  } else {
    for (uint8_t key : keys_filter) {
      auto result_it = results.find(key);
      if (result_it == results.end()) {
        continue;
      }
      output = true;
      group_output << "\t\t" << static_cast<uint16_t>(result_it->first) << " "
                   << result_it->second << "\r\n";
    }
  }
  if (output) {
    out.write(group_output.str());
  }
}

static void verifyInterval(const Duration& interval) {
  const auto& intervals = CustomCountersTimeSeries::getCustomCounterIntervals();
  auto res = std::find_if(
      intervals.begin(),
      intervals.end(),
      [interval](std::chrono::milliseconds i) { return interval < i; });
  if (res == intervals.end()) {
    using namespace std::chrono;
    std::stringstream error_str;
    error_str << "requested interval "
              << chrono_string(duration_cast<seconds>(interval))
              << "is not less than any of supported intervals [ ";
    for (auto supported : intervals) {
      error_str << chrono_string(duration_cast<seconds>(supported)) << ", ";
    }
    error_str << "]";
    throw boost::program_options::error(error_str.str());
  }
}

std::string StatsCustomCounters::getUsage() {
  using namespace std::chrono;

  return "Reports aggregation of custom counters per-log-group over the "
         "specified "
         "time period."
         "This server can provide aggregation for intervals up to " +
      chrono_string(duration_cast<seconds>(getCustomCountersMaxInterval())) +
      ". "
      "If no interval is specified, maximum interval is used  "
      "Example usage: stats custom counters --interval=1min 1 3 "
      "Reports aggregation for keys 1 and 3 over the 1min interval";
}

void StatsCustomCounters::getOptions(
    boost::program_options::options_description& opts) {
  using namespace boost::program_options;
  namespace arg = std::placeholders;

  // clang-format off
  opts.add_options()
    ("interval",
     value<Duration>(&interval_)
       ->notifier(&verifyInterval),
     "aggregate counters for given time period (e.g. \"1min\")"
    )
    ("keys",
     value<std::vector<uint16_t>>(&keys_)->notifier(
       [](const std::vector<uint16_t>& values) {
         for (auto value : values) {
           if (value > std::numeric_limits<uint8_t>::max()) {
             std::stringstream error;
             error << "requested key " << value <<
               " is larger than the max allowed " <<
               size_t(std::numeric_limits<uint8_t>::max());
             throw boost::program_options::error(error.str());
           }
         }
       }),
     "filter results by keys provided"
    );
  // clang-format on
}

void StatsCustomCounters::getPositionalOptions(
    boost::program_options::positional_options_description& out_options) {
  out_options.add("keys", -1);
}

void StatsCustomCounters::run() {
  StatsHolder* stats = server_->getParameters()->getStats();
  if (stats) {
    AggregateMap agg = doAggregate(
        stats,
        interval_,
        server_->getParameters()->getUpdateableConfig()->getLogsConfig());

    for (const auto& group_result : agg) {
      printOne(out_, group_result.first, group_result.second, keys_);
    }
  }
}

}}} // namespace facebook::logdevice::commands
