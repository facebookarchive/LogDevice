/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <memory>

#include "logdevice/common/SampledTracer.h"
#include "logdevice/common/StorageTaskDebugInfo.h"
#include "logdevice/include/Err.h"
#include "logdevice/include/types.h"

namespace facebook { namespace logdevice {

class TraceLogger;

constexpr auto SLOW_STORAGE_TASKS_TRACER = "slow_storage_tasks";

class SlowStorageTasksTracer : SampledTracer {
 public:
  explicit SlowStorageTasksTracer(std::shared_ptr<TraceLogger> logger);

  void traceStorageTask(std::function<StorageTaskDebugInfo()> builder,
                        double execution_time_ms);

  folly::Optional<double> getDefaultSamplePercentage() const override {
    // By default send a sample every 100 seconds of execution time.
    return .001; // 100% / 100e3
  }
};

}} // namespace facebook::logdevice
