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
  template <typename BuilderFn /*should return a std::unique_ptr<TraceSample>*/>
  bool publish(const char* table, BuilderFn&& builder) {
    if (logger_) {
      // flipping the coin
      bool isLogging = shouldLog(table);
      if (isLogging) {
        std::unique_ptr<TraceSample> sample = builder();
        logger_->pushSample(table, sampleRate(table), std::move(sample));
      }
      return isLogging;
    }
    return false;
  }

  const std::shared_ptr<TraceLogger> logger_;

 private:
  inline bool shouldLog(const char* table) {
    return folly::Random::randDouble(0, 100) < percentage(table);
  }

  double sampleRate(const char* table) const {
    return std::round(1 / (percentage(table) / 100.0));
  }

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
