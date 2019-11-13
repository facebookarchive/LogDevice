/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "logdevice/server/sequencer_boycotting/MovingAverageAppendOutlierDetector.h"

#include <cmath>

#include "logdevice/common/OutlierDetection.h"
#include "logdevice/common/Timestamp.h"
#include "logdevice/common/Worker.h"

namespace facebook { namespace logdevice {

namespace {
double successRatio(uint32_t successes, uint32_t fails) {
  if (successes + fails == 0) {
    return 1.0;
  }

  return static_cast<double>(successes) / (successes + fails);
}

template <typename T>
T mean(std::vector<T> vec) {
  T sum{};
  for (const T& val : vec) {
    sum += val;
  }

  return sum / vec.size();
}
} // namespace

std::vector<node_index_t>
MovingAverageAppendOutlierDetector::detectOutliers(TimePoint now) {
  for (auto& aggregated : aggregated_stats_) {
    aggregated.append_successes.update(now);
    aggregated.append_fails.update(now);
  }

  if (shouldUpdateTimeSeriesSettings()) {
    updateRetentionSettingsOnTimeSeries();
  }

  updatePotentialOutliersUsingRMSD(now);

  ld_debug("Potential outliers: %s", toString(potential_outliers_).c_str());

  std::vector<PotentialOutlier> outliers;
  const auto grace_period = getGracePeriod();
  for (auto& entry : potential_outliers_) {
    if (now - entry.second.outlier_since >= grace_period) {
      outliers.emplace_back(entry.second);
    }
  }

  std::sort(outliers.begin(),
            outliers.end(),
            [](PotentialOutlier& lhs, PotentialOutlier& rhs) {
              // sort by success ratio, and then by node index
              auto lhs_s = successRatio(lhs.successes, lhs.fails);
              auto rhs_s = successRatio(rhs.successes, rhs.fails);

              return std::tie(lhs_s, lhs.node_index) <
                  std::tie(rhs_s, rhs.node_index);
            });

  std::vector<node_index_t> outlier_nodes;
  outlier_nodes.reserve(outliers.size());
  std::transform(
      outliers.cbegin(),
      outliers.cend(),
      std::back_inserter(outlier_nodes),
      [](const PotentialOutlier& outlier) { return outlier.node_index; });

  return outlier_nodes;
}

void MovingAverageAppendOutlierDetector::addStats(node_index_t node_index,
                                                  NodeStats stats,
                                                  TimePoint now) {
  if (aggregated_stats_.size() <= node_index) {
    aggregated_stats_.resize(
        node_index + 1,
        AggregatedNodeStats(getBucketCount(getStatsRetentionDuration(),
                                           getStatsCollectionPeriod()),
                            getStatsRetentionDuration()));
  }

  aggregated_stats_[node_index].append_successes.addValue(
      now, stats.append_successes);
  aggregated_stats_[node_index].append_fails.addValue(now, stats.append_fails);

  // if old values are given, we have to re-check the potential outliers since
  // then
  check_outlier_since_ = std::min(now, check_outlier_since_);
}

void MovingAverageAppendOutlierDetector::updatePotentialOutliersUsingRMSD(
    TimePoint now) {
  if (aggregated_stats_.empty()) {
    ld_spew("No stats");
    return;
  }

  // A sample for outlier detection. Contains the node index as the key and a
  // success ratio as the value.
  using Sample = std::pair<node_index_t, double>;
  using Samples = std::vector<Sample>;

  const auto collection_period = getStatsCollectionPeriod();
  const auto ts_start = now - collection_period;
  const auto node_count = aggregated_stats_.size();
  auto sensitivity = getSensitivity();
  auto later_now = now + std::chrono::nanoseconds{1};

  // Build a list of samples to do statistical analysis on.
  Samples samples;
  for (size_t node_index = 0; node_index < node_count; ++node_index) {
    auto& stats = aggregated_stats_[node_index];
    size_t successes = stats.append_successes.sum(ts_start, later_now);
    size_t fails = stats.append_fails.sum(ts_start, later_now);
    if (successes + fails != 0) {
      double ratio = 1 - successRatio(successes, fails);
      samples.push_back(Sample{node_index, ratio});
    }
  }

  // Only allow the outlier detection algorithm to detect a node as outlier if
  // the failure ratio is above the sensitivity threshold.
  std::function<bool(const Sample& s)> outlier_filter =
      [&](const Sample& s) -> bool { return s.second > sensitivity; };

  auto res = OutlierDetection::findOutliers(OutlierDetection::Method::RMSD,
                                            std::move(samples),
                                            getRequiredStdFromMean(),
                                            getMaxBoycottCount(),
                                            getRelativeMargin(),
                                            outlier_filter);

  // Update `potential_outliers_`.
  decltype(potential_outliers_) new_map;
  for (const Sample& outlier : res.outliers) {
    auto it = potential_outliers_.find(outlier.first);
    if (it == potential_outliers_.end()) {
      PotentialOutlier potential_outlier;
      potential_outlier.node_index = outlier.first;
      potential_outlier.outlier_since = now;
      it = new_map.emplace(outlier.first, std::move(potential_outlier)).first;
    } else {
      it = new_map.emplace(outlier.first, std::move(it->second)).first;
    }
    auto& stats = aggregated_stats_[outlier.first];
    it->second.successes += stats.append_successes.sum(ts_start, later_now);
    it->second.fails += stats.append_fails.sum(ts_start, later_now);
  }
  potential_outliers_ = std::move(new_map);
}

std::chrono::milliseconds
MovingAverageAppendOutlierDetector::getGracePeriod() const {
  return Worker::settings()
      .sequencer_boycotting.node_stats_boycott_grace_period;
}

float MovingAverageAppendOutlierDetector::getSensitivity() const {
  return Worker::settings().sequencer_boycotting.node_stats_boycott_sensitivity;
}

std::chrono::milliseconds
MovingAverageAppendOutlierDetector::getStatsCollectionPeriod() const {
  return Worker::settings()
      .sequencer_boycotting.node_stats_controller_aggregation_period;
}

std::chrono::milliseconds
MovingAverageAppendOutlierDetector::getStatsRetentionDuration() const {
  return Worker::settings().sequencer_boycotting.node_stats_retention_on_nodes;
}

void MovingAverageAppendOutlierDetector::updateRetentionSettingsOnTimeSeries() {
  auto retention_time = getStatsRetentionDuration();
  auto bucket_count =
      getBucketCount(getStatsRetentionDuration(), getStatsCollectionPeriod());

  auto updateTimeSeries = [bucket_count, retention_time](
                              AggregatedNodeStats::TimeSeries& time_series) {
    AggregatedNodeStats::TimeSeries time_series_new(
        bucket_count, retention_time);

    const auto bucket_duration = retention_time / bucket_count;
    auto start_time = time_series.getLatestTime() - retention_time;

    for (int i = 0; i < bucket_count; ++i) {
      const auto end_time =
          start_time + bucket_duration + std::chrono::nanoseconds(1);
      const auto sum = time_series.sum(start_time, end_time);
      const auto count = time_series.count(start_time, end_time);

      time_series_new.addValueAggregated(start_time, sum, count);

      start_time = end_time;
    }

    time_series = std::move(time_series_new);
  };

  for (auto& aggregated : aggregated_stats_) {
    updateTimeSeries(aggregated.append_successes);
    updateTimeSeries(aggregated.append_fails);
  }
}

bool MovingAverageAppendOutlierDetector::shouldUpdateTimeSeriesSettings()
    const {
  return !aggregated_stats_.empty() &&
      (aggregated_stats_[0].append_successes.duration() !=
           getStatsRetentionDuration() ||
       aggregated_stats_[0].append_successes.numBuckets() !=
           getBucketCount(
               getStatsRetentionDuration(), getStatsCollectionPeriod()));
}

int MovingAverageAppendOutlierDetector::getBucketCount(
    std::chrono::milliseconds retention_duration,
    std::chrono::milliseconds collection_period) const {
  // have two buckets per collection period to increase granularity a bit
  // But have an upper limit to ensure that not too many buckets are used
  return std::fmin(100, (retention_duration / collection_period) * 2);
}

double MovingAverageAppendOutlierDetector::getRequiredStdFromMean() const {
  return Worker::settings()
      .sequencer_boycotting.node_stats_boycott_required_std_from_mean;
}

unsigned int MovingAverageAppendOutlierDetector::getMaxBoycottCount() const {
  return Worker::settings().sequencer_boycotting.node_stats_max_boycott_count;
}

double MovingAverageAppendOutlierDetector::getRelativeMargin() const {
  return Worker::settings()
      .sequencer_boycotting.node_stats_boycott_relative_margin;
}

std::string
MovingAverageAppendOutlierDetector::PotentialOutlier::toString() const {
  return folly::sformat("(s:{}, f:{}, since {})",
                        successes,
                        fails,
                        logdevice::toString(outlier_since));
}

}} // namespace facebook::logdevice
