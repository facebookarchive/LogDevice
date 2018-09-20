/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/common/BoycottTracer.h"

#include "logdevice/common/Timestamp.h"
#include "logdevice/common/stats/Stats.h"

namespace facebook { namespace logdevice {

BoycottTracer::BoycottTracer(std::shared_ptr<TraceLogger> logger)
    : ThrottledTracer(std::move(logger),
                      BOYCOTT_TRACER,
                      std::chrono::minutes(1),
                      100) {}

void BoycottTracer::traceBoycott(
    NodeID boycotted_node,
    std::chrono::system_clock::time_point start_time,
    std::chrono::milliseconds boycott_duration,
    folly::dynamic controller_state) {
  auto sample_builder = [&]() -> std::unique_ptr<TraceSample> {
    auto sample = std::make_unique<TraceSample>();
    sample->addNormalValue("boycotted_node_id", boycotted_node.toString());
    sample->addIntValue("start_time", start_time.time_since_epoch().count());
    sample->addIntValue("duration_ms", boycott_duration.count());
    sample->addNormalValue("controller_state_json", toString(controller_state));
    return sample;
  };
  publish(sample_builder);
  ld_info("Boycotted %s since %s for %lums, controller state: %s",
          boycotted_node.toString().c_str(),
          format_time(SystemTimestamp(start_time)).c_str(),
          boycott_duration.count(),
          toString(controller_state).c_str());
}

}} // namespace facebook::logdevice
