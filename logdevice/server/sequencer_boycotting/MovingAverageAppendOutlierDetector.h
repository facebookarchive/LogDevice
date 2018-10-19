/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <unordered_map>

#include <folly/stats/BucketedTimeSeries-defs.h>
#include <folly/stats/BucketedTimeSeries.h>

#include "logdevice/common/settings/Settings.h"
#include "logdevice/common/settings/UpdateableSettings.h"
#include "logdevice/server/sequencer_boycotting/AppendOutlierDetector.h"

/**
 * @file Detect outliers using the average of append success ratio for
 * each node, compared to the moving average of the success ratio for the entire
 * cluster
 */

namespace facebook { namespace logdevice {
class MovingAverageAppendOutlierDetector : public AppendOutlierDetector {
 protected:
  struct AggregatedNodeStats {
    AggregatedNodeStats(int bucket_count, std::chrono::milliseconds duration)
        : append_successes(bucket_count, duration),
          append_fails(bucket_count, duration) {}
    using TimeSeries =
        folly::BucketedTimeSeries<double, std::chrono::steady_clock>;
    TimeSeries append_successes;
    TimeSeries append_fails;
  };

  struct PotentialOutlier {
    node_index_t node_index{-1};
    // at what time was this node considered an outlier
    std::chrono::steady_clock::time_point outlier_since{};
    // will be accumulated each time it's considered an outlier
    uint32_t successes{0};
    uint32_t fails{0};
  };

 public:
  virtual ~MovingAverageAppendOutlierDetector() = default;

  std::vector<node_index_t> detectOutliers(TimePoint now) override;

  void addStats(node_index_t node_index,
                NodeStats stats,
                TimePoint now) override;

 protected:
  /**
   * @returns The setting that defines for how long a node should be an outlier
   * before boycotting should be performed
   */
  virtual std::chrono::milliseconds getGracePeriod() const;

  /**
   * @returns The setting that defines how sensitive outlier detection should be
   * compared to 100% success. If getSensitivity returns 0.05, that means that a
   * node will not be considered an outlier if it has a success ratio of 95% or
   * above.
   */
  virtual float getSensitivity() const;

  /**
   * @returns The setting that defines how often stats are collected by the
   * controller nodes
   */
  virtual std::chrono::milliseconds getStatsCollectionPeriod() const;

  /**
   * @returns The settings that defines how long stats are kept on nodes
   */
  virtual std::chrono::milliseconds getStatsRetentionDuration() const;

  virtual double getRequiredStdFromMean() const;

  virtual unsigned int getMaxBoycottCount() const;
  virtual double getRelativeMargin() const;
  virtual bool useRMSD() const;

 private:
  // Look for outliers with the legacy method that uses standard deviation.
  void updatePotentialOutliersUsingStdDev(TimePoint now);

  // Look for outliers with the new method that uses RMSD (@see
  // common/OutlierDetection.h)
  void updatePotentialOutliersUsingRMSD(TimePoint now);

  void updateRetentionSettingsOnTimeSeries();

  /**
   * If either the node_stats_retention_on_nodes or
   * node_stats_controller_aggregation_period setting has been updated, we have
   * to update the AggregatedNodeStats
   */
  bool shouldUpdateTimeSeriesSettings() const;

  /**
   * Calculates the amount of buckets should exist for the time series in the
   * AggregatedNodeStats, given the retention_duration and collection_period
   */
  int getBucketCount(std::chrono::milliseconds retention_duration,
                     std::chrono::milliseconds collection_period) const;

  /**
   * If stats are added at time t, then we have to check if there exists an
   * outlier since that time. If later another value is added, but at t-1, then
   * this is the time from which we have to check if there exists an outlier.
   * This is needed because stats are not ensured to arrive in-order
   */
  TimePoint check_outlier_since_{std::chrono::nanoseconds::max()};

  std::unordered_map<node_index_t, PotentialOutlier> potential_outliers_;
  std::vector<AggregatedNodeStats> aggregated_stats_;
};
}} // namespace facebook::logdevice
