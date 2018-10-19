/**
 * Copyright (c) 2018-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <memory>
#include <string>

#include "logdevice/common/ThrottledTracer.h"

namespace facebook { namespace logdevice {

class TraceLogger;

constexpr auto BOYCOTT_TRACER = "boycott_tracer";

class BoycottTracer : public ThrottledTracer {
 public:
  explicit BoycottTracer(std::shared_ptr<TraceLogger> logger);

  void traceBoycott(NodeID boycotted_node,
                    std::chrono::system_clock::time_point boycott_start_time,
                    std::chrono::milliseconds boycott_duration,
                    folly::dynamic controller_state);
};

}} // namespace facebook::logdevice
