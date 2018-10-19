/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/common/configuration/TraceLoggerConfig.h"

#include <iostream>

#include "logdevice/common/ClientReadTracer.h"

namespace facebook { namespace logdevice { namespace configuration {

folly::Optional<double>
TraceLoggerConfig::getSamplePercentage(const std::string& tracer) const {
  auto iter = percentages.find(tracer);
  if (iter != percentages.end()) {
    return iter->second;
  }

  return folly::none;
}

double TraceLoggerConfig::getDefaultSamplePercentage() const {
  return default_sampling;
}

folly::dynamic TraceLoggerConfig::toFollyDynamic() const {
  folly::dynamic res = folly::dynamic::object;
  if (default_sampling != DEFAULT_SAMPLE_PERCENTAGE) {
    res["default-sampling-percentage"] = default_sampling;
  }
  folly::dynamic tracers = folly::dynamic::object;
  for (const auto& kv : percentages) {
    tracers[kv.first] = kv.second;
  }
  res["tracers"] = std::move(tracers);
  return res;
}

}}} // namespace facebook::logdevice::configuration
