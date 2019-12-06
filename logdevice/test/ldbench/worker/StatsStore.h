/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <folly/dynamic.h>

namespace facebook { namespace logdevice { namespace ldbench {
/**
 * Base class for target of storing stats
 * StatsStore is not thread-safe and should only be invoked from a single
 * thread. Users could use a stats collection thread that collects stats
 * from all relevant threads and write the results to the StatsStore.
 */

class StatsStore {
 public:
  virtual ~StatsStore() {}
  /**
   * Return True if StatsStore is ready to receive stats
   */
  virtual bool isReady() = 0;
  /**
   * Persist the current stats into StatsStore.
   *
   * @param stats_obj
   *   A folly::dynamic object (a container of key-value pairs) of timeseries
   *   name to a numeric value. The values are the current value of the
   *   timeseries--aggregation happens downstream of StatsStore.
   *   E.g.,
   *     folly::dynamic::object("timestamp", 123)("succeed", 1000)("failed",
   *     200);
   */
  virtual void writeCurrentStats(const folly::dynamic& stats_obj) = 0;
};

}}} // namespace facebook::logdevice::ldbench
