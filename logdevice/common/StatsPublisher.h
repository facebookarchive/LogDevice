/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <chrono>

namespace facebook { namespace logdevice {

/**
 * @file Trivial interface for publishing stats to other systems.
 */

struct Stats;

enum class StatsPublisherScope { SERVER, CLIENT };

class StatsPublisher {
 public:
  /**
   * Called periodically by StatsCollectionThread.  `current' gives a snapshot
   * of stats collected just before the call.  `previous' is the previous
   * snapshot and `elapsed' the amount of time between the two snapshots,
   * which can be used to calculate rates over the time interval.
   */
  virtual void publish(const Stats& current,
                       const Stats& previous,
                       std::chrono::milliseconds elapsed) = 0;

  /**
   * Additionally aggregate stats against the given entity.
   *
   * For example, all clients of a particular LogDevice tier could
   * aggregate their individual stats under an entity derived from the
   * LogDevice cluster name.
   */
  virtual void addRollupEntity(std::string entity) = 0;

  virtual ~StatsPublisher() {}
};

}} // namespace facebook::logdevice
