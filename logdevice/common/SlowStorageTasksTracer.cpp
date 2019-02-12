/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/common/SlowStorageTasksTracer.h"

#include <algorithm>

#include "logdevice/include/LogAttributes.h"

namespace facebook { namespace logdevice {

SlowStorageTasksTracer::SlowStorageTasksTracer(
    std::shared_ptr<TraceLogger> logger)
    : SampledTracer(std::move(logger)) {}

void SlowStorageTasksTracer::traceStorageTask(
    std::function<StorageTaskDebugInfo()> builder,
    double execution_time_ms) {
  auto sample_builder = [&]() {
    auto info = builder();
    auto sample = std::make_unique<TraceSample>();

    sample->addIntValue("shard_id", info.shard_id);
    sample->addNormalValue("task_priority", info.priority);
    sample->addNormalValue("thread_type", info.thread_type);
    sample->addNormalValue("task_type", info.task_type);
    sample->addIntValue("enqueue_time", info.enqueue_time.count());
    sample->addNormalValue("durability", info.durability);

    if (info.log_id) {
      sample->addNormalValue(
          "log_id", folly::to<std::string>(info.log_id.value().val()));
    }
    if (info.lsn) {
      sample->addNormalValue("lsn", folly::to<std::string>(info.lsn.value()));
    }
    if (info.client_address) {
      sample->addNormalValue(
          "client_address", info.client_address.value().toStringNoPort());
    }
    if (info.execution_start_time) {
      sample->addIntValue(
          "execution_start_time", info.execution_start_time.value().count());
    }
    if (info.execution_end_time) {
      sample->addIntValue(
          "execution_end_time", info.execution_end_time.value().count());
    }
    if (info.client_id) {
      sample->addNormalValue("client_id", info.client_id.value().toString());
    }
    if (info.node_id) {
      sample->addNormalValue("node_id", info.node_id.value().toString());
    }
    if (info.extra_info) {
      sample->addNormalValue("extra_info", info.extra_info.value());
    }

    return sample;
  };

  publish(SLOW_STORAGE_TASKS_TRACER,
          sample_builder,
          /* force */ false,
          execution_time_ms);
}

}} // namespace facebook::logdevice
