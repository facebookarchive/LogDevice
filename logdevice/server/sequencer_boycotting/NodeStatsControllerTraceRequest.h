/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include "logdevice/common/Request.h"
#include "logdevice/server/sequencer_boycotting/NodeStatsController.h"

namespace facebook { namespace logdevice {

class NodeStatsControllerTraceRequest : public Request {
 public:
  NodeStatsControllerTraceRequest(
      NodeID boycotted_node,
      std::chrono::system_clock::time_point boycott_start_time,
      std::chrono::milliseconds boycott_duration)
      : Request(RequestType::NODE_STATS_CONTROLLER_TRACE),
        boycotted_node_(boycotted_node),
        start_time_(boycott_start_time),
        duration_(boycott_duration) {}

  int getThreadAffinity(int nthreads) override {
    return NodeStatsController::getThreadAffinity(nthreads);
  }

  Request::Execution execute() override;

 private:
  NodeID boycotted_node_;
  std::chrono::system_clock::time_point start_time_;
  std::chrono::milliseconds duration_;
};

}} // namespace facebook::logdevice
