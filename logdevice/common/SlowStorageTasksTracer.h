/**
 * Copyright (c) 2017-present, Facebook, Inc.
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

  void traceStorageTask(const StorageTaskDebugInfo&);
};

}} // namespace facebook::logdevice
