/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <cstdint>
#include <vector>

#include "logdevice/common/AdminCommandTable-fwd.h"
#include "logdevice/common/NodeID.h"

/**
 * @file StatsControllerCallback is used in the messaging layer to communicate
 * with the NodeStatsController
 */

namespace facebook { namespace logdevice {

// in common/sequencer_boycotting/PerClientNodeStatsAggregator.h
struct BucketedNodeStats;

class NodeStatsControllerCallback {
 public:
  using msg_id_t = uint32_t;

  virtual ~NodeStatsControllerCallback() = default;

  virtual void onStatsReceived(msg_id_t msg_id,
                               NodeID from,
                               BucketedNodeStats stats) = 0;

  virtual void getDebugInfo(InfoAppendOutliersTable* table) = 0;

  virtual void
  traceBoycott(NodeID boycotted_node,
               std::chrono::system_clock::time_point boycott_start_time,
               std::chrono::milliseconds boycott_duration) = 0;
};
}} // namespace facebook::logdevice
