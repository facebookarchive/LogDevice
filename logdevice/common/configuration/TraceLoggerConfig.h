/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <unordered_map>

#include <folly/Optional.h>

namespace folly {
struct dynamic;
}

namespace facebook { namespace logdevice { namespace configuration {

// Sample Rate for all sampled Tracers
#define DEFAULT_SAMPLE_PERCENTAGE 0.1 // 0.1% 1 every 1000

struct TraceLoggerConfig {
  TraceLoggerConfig() {}
  double default_sampling = DEFAULT_SAMPLE_PERCENTAGE;
  std::unordered_map<std::string, double> percentages;

  /**
   * Looks up the sampling percentage for a certain tracer in the config
   *
   * @return Returns the percentage if an override has been found
   */
  folly::Optional<double> getSamplePercentage(const std::string& tracer) const;

  /*
   * Gets the global default sampling percentage
   * either configured in the config or the default value
   *
   * @return Returns global sampling percentage
   */
  double getDefaultSamplePercentage() const;

  folly::dynamic toFollyDynamic() const;
};

}}} // namespace facebook::logdevice::configuration
