/**
 * Copyright (c) 2017-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "logdevice/common/WorkerTimeoutStats.h"

#include <algorithm>
#include <tuple>

#include <folly/stats/TimeseriesHistogram-defs.h>

#include "logdevice/common/Worker.h"
#include "logdevice/common/debug.h"

using namespace std::literals::chrono_literals;
using namespace std::chrono;

constexpr auto kMaxNumberOfOutgoingMessages = 10000;
constexpr auto kBucketSize = 1;
constexpr auto kMinBucketsLevel = 0;
constexpr auto kMaxBucketsLevel = 20;
constexpr auto kTimeBucketsNum = 10;
constexpr std::initializer_list<std::chrono::steady_clock::duration> kLevels = {
    10s,
};

namespace facebook { namespace logdevice {

WorkerTimeoutStats::WorkerTimeoutStats()
    : overall_(kBucketSize,
               kMinBucketsLevel,
               kMaxBucketsLevel,
               HistogramContainer(kTimeBucketsNum, kLevels)) {}

void WorkerTimeoutStats::onCopySent(Status status,
                                    const ShardID& to,
                                    const STORE_Header& header,
                                    Clock::time_point now) {
  if (status != Status::OK) {
    return;
  }

  cleanup();
  const auto key = std::make_tuple(to.node(), header.rid, header.wave);
  outgoing_messages_.emplace_back(key, now);
  lookup_table_[key] = std::prev(outgoing_messages_.end());
}

void WorkerTimeoutStats::onReply(const ShardID& from,
                                 const STORE_Header& header,
                                 Clock::time_point now) {
  const auto key = std::make_tuple(from.node(), header.rid, header.wave);
  auto it = lookup_table_.find(key);
  if (it == lookup_table_.end()) {
    return;
  }

  const auto message_sent_timestamp = it->second->second;
  outgoing_messages_.erase(it->second);
  lookup_table_.erase(it);

  const int64_t round_trip_time = to_msec(now - message_sent_timestamp).count();
  ld_check(round_trip_time >= 0);
  const Latency round_trip_time_log = std::log2(std::max(round_trip_time, 1L));
  auto histogram_iterator = histograms_.emplace(
      std::piecewise_construct,
      std::forward_as_tuple(std::get<0>(key)),
      std::forward_as_tuple(kBucketSize,
                            kMinBucketsLevel,
                            kMaxBucketsLevel,
                            HistogramContainer(kTimeBucketsNum, kLevels)));

  auto& histogram = histogram_iterator.first->second;
  histogram.addValue(now, round_trip_time_log);
  overall_.addValue(now, round_trip_time_log);
}

void WorkerTimeoutStats::cleanup() {
  const size_t size = outgoing_messages_.size();
  if (size <= kMaxNumberOfOutgoingMessages) {
    return;
  }
  ld_check(size == kMaxNumberOfOutgoingMessages + 1);
  const auto it = outgoing_messages_.begin();
  lookup_table_.erase(it->first);
  outgoing_messages_.erase(it);
}

folly::Optional<std::array<WorkerTimeoutStats::Latency,
                           WorkerTimeoutStats::kQuantiles.size()>>
WorkerTimeoutStats::getEstimations(Levels level,
                                   int node,
                                   Clock::time_point now) {
  Histogram* current_histogram = nullptr;
  if (node == -1) {
    current_histogram = &overall_;
  } else {
    auto it = histograms_.find(node);
    if (it == histograms_.end()) {
      return {};
    }
    current_histogram = &(it->second);
  }
  current_histogram->update(now);

  if (current_histogram->count(level) < getMinSamplesPerBucket()) {
    return folly::none;
  }

  std::array<Latency, kQuantiles.size()> result;
  for (int i = 0; i < kQuantiles.size(); ++i) {
    result[i] = std::pow(
        2, current_histogram->getPercentileEstimate(kQuantiles[i], level));
  }
  return result;
}

void WorkerTimeoutStats::clear() {
  histograms_.clear();
  overall_.clear();
  outgoing_messages_.clear();
  lookup_table_.clear();
}

uint64_t WorkerTimeoutStats::getMinSamplesPerBucket() const {
  return Worker::settings().store_histogram_min_samples_per_bucket;
}

constexpr std::array<double, 6> WorkerTimeoutStats::kQuantiles;

}} // namespace facebook::logdevice
