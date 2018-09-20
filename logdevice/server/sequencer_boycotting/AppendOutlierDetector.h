/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <chrono>

#include "logdevice/common/NodeID.h"

/**
 * @file The AppendOutlierDetector is an interface used to be able to easily use
 * different methods of outlier detection used to take decisions of boycotts. It
 * tracks the received stats and is able to take a decision if a node is an
 * outlier or not.
 */

namespace facebook { namespace logdevice {
class AppendOutlierDetector {
 public:
  struct NodeStats {
    uint32_t append_successes{0};
    uint32_t append_fails{0};
  };

  using TimePoint = std::chrono::steady_clock::time_point;

  virtual ~AppendOutlierDetector() = default;

  /**
   * @param now Check if there are any outliers at this time
   * @returns   A vector of outliers, sorted by worst to best. Empty if there
   *            are no outliers
   */
  virtual std::vector<node_index_t> detectOutliers(TimePoint now) = 0;

  /**
   * Lets the outlier detector track the stats sent from the nodes to later take
   * a decision if a node is an outlier or not
   *
   * @param node_index The index of the node which the stats belong to
   * @param stats      The append counts for the given node
   * @param now        The time at which the stats were received
   */
  virtual void addStats(node_index_t node_index,
                        NodeStats stats,
                        TimePoint now) = 0;
};
}} // namespace facebook::logdevice
