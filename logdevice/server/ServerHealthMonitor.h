/**
 * Copyright (c) 2019-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <atomic>
#include <chrono>
#include <vector>

#include <folly/Executor.h>
#include <folly/futures/Promise.h>
#include <logdevice/common/HealthMonitor.h>

namespace facebook { namespace logdevice {

class ServerHealthMonitor : public HealthMonitor {
 public:
  ServerHealthMonitor(folly::Executor& executor,
                      std::chrono::milliseconds sleep_period);
  ~ServerHealthMonitor() override {}

  void startUp() override;
  folly::SemiFuture<folly::Unit> shutdown() override;

 protected:
  void monitorLoop();
  // void resetInternalState(); // to be added later if needed.
  void processReports() {}

 private:
  folly::Executor& executor_;
  std::chrono::milliseconds sleep_period_;
  std::atomic_bool shutdown_{false};
  folly::Promise<folly::Unit> shutdown_promise_;
};

}} // namespace facebook::logdevice
