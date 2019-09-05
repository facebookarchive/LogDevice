/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "logdevice/server/sequencer_boycotting/BoycottingStats.h"

namespace facebook { namespace logdevice {

using ClientNodeTimeSeriesMap =
    TimeSeriesMap<PerClientNodeTimeSeriesStats::Key,
                  PerClientNodeTimeSeriesStats::Value,
                  PerClientNodeTimeSeriesStats::Key::Hash>;
PerClientNodeTimeSeriesStats::PerClientNodeTimeSeriesStats(
    std::chrono::milliseconds retention_time)
    : retention_time_(retention_time),
      timeseries_(std::make_unique<TimeSeries>(30, retention_time)) {}

PerClientNodeTimeSeriesStats::TimeSeries*
PerClientNodeTimeSeriesStats::timeseries() const {
  return timeseries_.get();
}

void PerClientNodeTimeSeriesStats::append(ClientID client,
                                          NodeID node,
                                          uint32_t successes,
                                          uint32_t failures,
                                          TimePoint time) {
  timeseries_->addValue(
      time,
      ClientNodeTimeSeriesMap(
          PerClientNodeTimeSeriesStats::Key{client, node},
          PerClientNodeTimeSeriesStats::Value{successes, failures}));
}

void PerClientNodeTimeSeriesStats::reset() {
  timeseries_->clear();
}

bool operator==(const PerClientNodeTimeSeriesStats::Value& lhs,
                const PerClientNodeTimeSeriesStats::Value& rhs) {
  return lhs.successes == rhs.successes && lhs.failures == rhs.failures;
}

bool operator==(const PerClientNodeTimeSeriesStats::Key& a,
                const PerClientNodeTimeSeriesStats::Key& b) {
  return a.client_id == b.client_id && a.node_id == b.node_id;
}

bool operator!=(const PerClientNodeTimeSeriesStats::Key& a,
                const PerClientNodeTimeSeriesStats::Key& b) {
  return a.client_id != b.client_id || a.node_id != b.node_id;
}

using ClientNodeValue = PerClientNodeTimeSeriesStats::ClientNodeValue;
std::vector<ClientNodeValue>
PerClientNodeTimeSeriesStats::sum(TimePoint from, TimePoint to) const {
  return processStats(timeseries_->sum(from, to));
}

std::vector<ClientNodeValue> PerClientNodeTimeSeriesStats::sum() const {
  return processStats(timeseries_->sum());
}

std::vector<ClientNodeValue> PerClientNodeTimeSeriesStats::processStats(
    const ClientNodeTimeSeriesMap& total) const {
  const auto& map = total.data();
  std::vector<ClientNodeValue> result{};
  for (const auto& p : map) {
    result.emplace_back(
        ClientNodeValue{p.first.client_id, p.first.node_id, p.second});
  }

  return result;
}

void PerClientNodeTimeSeriesStats::updateCurrentTime(TimePoint current_time) {
  timeseries_->update(current_time);
}

// won't be an exact copy because of the approximate nature of
// BucketedTimeSeries, but it should be close enough not to matter
template <class T, class V>
static folly::BucketedTimeSeries<T, V> newTimeSeriesWithUpdatedRetentionTime(
    folly::BucketedTimeSeries<T, V>& time_series,
    std::chrono::milliseconds retention_time,
    std::chrono::steady_clock::time_point current_time) {
  const auto bucket_count = time_series.numBuckets();
  const auto bucket_duration = retention_time / bucket_count;

  time_series.update(current_time);

  folly::BucketedTimeSeries<T, V> time_series_new(bucket_count, retention_time);
  for (int i = 0; i < bucket_count; ++i) {
    const auto start_time =
        (current_time - retention_time) + i * bucket_duration;
    const auto end_time = start_time + bucket_duration;
    const auto sum = time_series.sum(start_time, end_time);
    const auto count = time_series.count(start_time, end_time);

    time_series_new.addValueAggregated(start_time, sum, count);
  }

  return time_series_new;
}

std::chrono::milliseconds PerClientNodeTimeSeriesStats::retentionTime() const {
  return retention_time_;
}

}} // namespace facebook::logdevice
