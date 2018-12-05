/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/server/admincommands/StatsThroughput.h"

#include <chrono>
#include <vector>

#include "logdevice/common/commandline_util_chrono.h"
#include "logdevice/server/LogGroupThroughput.h"

namespace facebook { namespace logdevice { namespace commands {

static bool groupPassesThreshold(const OneGroupResults& results,
                                 int64_t threshold) {
  ld_check(!results.empty());
  return std::any_of(results.begin(),
                     results.end(),
                     [threshold](RateType rate) { return rate >= threshold; });
}

static void printOne(EvbufferTextOutput& out,
                     folly::StringPiece group_name,
                     const OneGroupResults& results) {
  out.printf("STAT ");
  out.write(group_name.data(), group_name.size());
  for (auto result : results) {
    out.printf(" %ld", int64_t(result));
  }
  out.printf("\r\n");
}

static void reportAll(EvbufferTextOutput& out,
                      const AggregateMap& agg,
                      int64_t threshold) {
  for (const auto& entry : agg) {
    const OneGroupResults& results = entry.second;
    if (groupPassesThreshold(results, threshold)) {
      printOne(out, entry.first, results);
    }
  }
}

static void reportTop(EvbufferTextOutput& out,
                      const AggregateMap& agg,
                      int64_t threshold,
                      uint32_t top) {
  using SchwartzPair = std::pair<int64_t, folly::StringPiece>;
  std::vector<SchwartzPair> vec;
  for (const auto& entry : agg) {
    const OneGroupResults& results = entry.second;
    if (groupPassesThreshold(results, threshold)) {
      vec.emplace_back(
          *std::max_element(results.begin(), results.end()), entry.first);
    }
  }

  auto cmp = [](const SchwartzPair& p1, const SchwartzPair& p2) {
    // Higher throughput first, otherwise sort by name
    if (p1.first != p2.first) {
      return p1.first > p2.first;
    }
    return p1.second < p2.second;
  };

  if (vec.empty()) {
    return;
  }

  auto count = std::min<size_t>(top, vec.size());

  std::partial_sort(vec.begin(), vec.begin() + count, vec.end(), cmp);
  for (size_t i = 0; i < count; ++i) {
    printOne(out, vec[i].second, agg.find(vec[i].second)->second);
  }
}

static std::set<std::string>& allowedStats() {
  static std::set<std::string> res;
  if (res.empty()) {
#define TIME_SERIES_DEFINE(_, strings, __, ___) \
  for (const std::string& str : strings) {      \
    auto rv = res.insert(str);                  \
    ld_check(rv.second);                        \
  }
#include "logdevice/common/stats/per_log_time_series.inc" // nolint
  }
  return res;
}

std::string StatsThroughput::getUsage() {
  using namespace std::chrono;

  return "Reports estimated per-log-group append/read throughput over the "
         "specified "
         "time periods.  Result unit is B/s.  "
         "If no intervals are specified, 1-minute throughputs are given. "
         "Supported "
         "time series: '" +
      folly::join("', '", allowedStats()) +
      "'. "
      "Example usage: stats throughput appends_out 1min 10min";
}

void StatsThroughput::getOptions(
    boost::program_options::options_description& opts) {
  using namespace boost::program_options;
  namespace arg = std::placeholders;

  opts.add_options()(
      "timeseries",
      value<std::string>(&time_series_)
          ->required()
          ->notifier([](const std::string& str) {
            if (allowedStats().find(str) == allowedStats().end()) {
              throw boost::program_options::error(
                  "The only supported time series "
                  "are '" +
                  folly::join("', '", allowedStats()) + "'");
            }
          }),
      "'appends_in' (same as 'appends'), 'appends_out' or 'reads'")(
      "threshold",
      value<int64_t>(&threshold_)->default_value(threshold_),
      "only include log groups for which the rate is at "
      "least this much (using max of provided intervals)")(
      "top",
      value<uint32_t>(&top_),
      "show the top N log groups (using max of provided intervals)")(
      "intervals",
      value<std::vector<Duration>>(&query_intervals_),
      "calculate throughput for given time periods (e.g. \"1min\")");
}

void StatsThroughput::getPositionalOptions(
    boost::program_options::positional_options_description& out_options) {
  out_options.add("timeseries", 1);
  out_options.add("intervals", -1);
}

void StatsThroughput::run() {
  std::string msg;
  StatsHolder* stats = server_->getParameters()->getStats();
  if (!verifyIntervals(stats, time_series_, query_intervals_, msg)) {
    if (!msg.empty()) {
      out_.printf("%s\n", msg.c_str());
    }
    return;
  }
  if (stats) {
    AggregateMap agg = doAggregate(
        stats,
        time_series_,
        query_intervals_,
        server_->getParameters()->getUpdateableConfig()->getLogsConfig());
    if (top_ > 0) {
      reportTop(out_, agg, threshold_, top_);
    } else {
      reportAll(out_, agg, threshold_);
    }
  }
}

}}} // namespace facebook::logdevice::commands
