/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once
#include <cmath>
#include <functional>
#include <memory>

#include <folly/Optional.h>
#include <folly/Random.h>

#include "logdevice/common/TraceLogger.h"
#include "logdevice/common/debug.h"

namespace facebook { namespace logdevice {

/**
 * A mix-in to tracers to handle sampling and to pick the samples to be
 * published. This also handles publishing and hands off the sample memory
 * ownership to the underlying TraceLogger implementation for delivery.
 */
class TraceSample;

class SampledTracer {
 public:
  explicit SampledTracer(std::shared_ptr<TraceLogger> logger)
      : logger_(std::move(logger)) {}

  virtual ~SampledTracer(){};

 protected:
  // @param force
  //   If true, force publishing the sample regardless of the sampling rate.
  // @param weight_multiplier
  //   Relative probability of logging this sample. Use it to log more
  //   important events more often.
  //   Should be >= 1 most of the time. E.g. duration in ms is ok, but duration
  //   in seconds is too small. At the same time, avoid making it bigger than
  //   necessary, because sampling is, potentially confusingly, insensitive to
  //   sample percentage changes above 1/weight_multiplier; see comment inside.
  //
  //   Example usages:
  //    * If a sample represents a potentially slow operation, you may want to
  //      set weight_multiplier to operation's execution time in milliseconds.
  //      This would make the exported data representative of how most of the
  //      time is spent rather than what the most frequent operations look like.
  //    * Set weight_multiplier to record's size in kilobytes.
  //      This would log more samples for bigger records, which may be useful
  //      e.g. if you're interested in space usage and most of the space is
  //      used by rare big records.
  template <typename BuilderFn /*should return a std::unique_ptr<TraceSample>*/>
  bool publish(const char* table,
               BuilderFn&& builder,
               bool force = false,
               double weight_multiplier = 1) {
    if (logger_) {
      // flipping the coin
      // Clamp multiplier to make sure we don't reject any samples when
      // percentage() is set to 100.
      weight_multiplier = std::max(1.0, weight_multiplier);
      // Probability of logging this sample.
      // Note that weight_multiplier may make it greater than 100%, which makes
      // the responce to percentage() nonlinear; e.g. if weight_multiplier is
      // record's size in bytes, and most records are ~1 KB in size,
      // decreasing percentage from 100% to 0.1% will have almost no effect on
      // the actual logging rate, but decreasing it below 0.1% will decrease
      // the logging rate almost proportionally.
      double pct = std::min(100.0, percentage(table) * weight_multiplier);
      if (force) {
        pct = 100;
      }
      bool should_log = folly::Random::randDouble(0, 100) < pct;
      if (should_log) {
        std::unique_ptr<TraceSample> sample = builder();
        uint32_t sample_rate = (uint32_t)std::lround(std::min(
            (double)std::numeric_limits<uint32_t>::max(), 100.0 / pct));
        logger_->pushSample(table, sample_rate, std::move(sample));
      }
      return should_log;
    }
    return false;
  }

  const std::shared_ptr<TraceLogger> logger_;

 private:
  double percentage(const char* table) const {
    // if there's no logger, it makes no sense to sample
    if (!logger_) {
      return 0;
    }

    // Precedence:
    // 1. Explicit tracer override
    // 2. Local default
    // 3. Global tracer default
    return logger_->getSamplePercentageForTracer(table).value_or(
        getDefaultSamplePercentage().value_or(
            logger_->getDefaultSamplePercentage()));
  }

  /**
   * Returns an a value when it wants to override the global trace
   * percentage, configured in TraceLoggerConfig. Individual trace overrides
   * will always take priority over the value defined below
   */
  virtual folly::Optional<double> getDefaultSamplePercentage() const {
    return folly::none;
  }
};
}} // namespace facebook::logdevice
