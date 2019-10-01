/**
 * Copyright (c) 2019-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <folly/futures/Future.h>

namespace facebook { namespace logdevice {

class HealthMonitor {
 public:
  HealthMonitor() {}
  virtual ~HealthMonitor() {}

  virtual void startUp() {}
  virtual folly::SemiFuture<folly::Unit> shutdown() {
    return folly::makeSemiFuture();
  }

  // reporter methods
  virtual void reportWatchdogHealth(bool delayed) {}
  virtual void reportStalledWorkers(int num_stalled) {}
  virtual void reportWorkerStall(int idx, std::chrono::milliseconds duration) {}
  virtual void reportWorkerQueueHealth(int idx, bool delayed) {}
};

}} // namespace facebook::logdevice
