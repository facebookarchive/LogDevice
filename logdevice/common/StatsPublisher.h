/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <chrono>
#include <string>
#include <vector>

namespace facebook { namespace logdevice {

/**
 * @file Trivial interface for publishing stats to other systems.
 */

struct Stats;

class StatsPublisher {
 public:
  /**
   * Called periodically by StatsCollectionThread. 'current' and 'previous' are
   * vectors of stats sets for different things like client, server, ldbench
   * worker, and so on. 'current' is snapshots of stats collected just before
   * the call. `previous' is the previous snapshots and `elapsed' is the amount
   * of time between the snapshots, which can be used to calculate rates over
   * the time interval.
   */
  virtual void publish(const std::vector<const Stats*>& current,
                       const std::vector<const Stats*>& previous,
                       std::chrono::milliseconds elapsed) = 0;

  /**
   * Additionally aggregate stats against the given entity, for ALL stats sets.
   *
   * For example, all clients of a particular LogDevice tier could
   * aggregate their individual stats under an entity derived from the
   * LogDevice cluster name.
   */
  virtual void addRollupEntity(std::string entity) = 0;

  virtual ~StatsPublisher() {}
};

}} // namespace facebook::logdevice
