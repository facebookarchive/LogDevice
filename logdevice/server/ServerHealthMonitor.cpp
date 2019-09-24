/**
 * Copyright (c) 2019-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "logdevice/server/ServerHealthMonitor.h"

#include "logdevice/common/chrono_util.h"

namespace facebook { namespace logdevice {

ServerHealthMonitor::ServerHealthMonitor(folly::Executor& executor,
                                         std::chrono::milliseconds sleep_period)
    : executor_(executor), sleep_period_(sleep_period) {}

void ServerHealthMonitor::startUp() {
  monitorLoop();
}

void ServerHealthMonitor::monitorLoop() {
  folly::futures::sleep(sleep_period_)
      .via(&executor_)
      .then([this](folly::Try<folly::Unit>) mutable {
        if (shutdown_.load(std::memory_order::memory_order_relaxed)) {
          shutdown_promise_.setValue();
          return;
        }
        processReports();
        monitorLoop();
      });
}

folly::SemiFuture<folly::Unit> ServerHealthMonitor::shutdown() {
  shutdown_.exchange(true, std::memory_order::memory_order_relaxed);
  return shutdown_promise_.getSemiFuture();
}

}} // namespace facebook::logdevice
