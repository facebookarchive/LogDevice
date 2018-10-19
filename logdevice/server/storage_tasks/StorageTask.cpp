/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/server/storage_tasks/StorageTask.h"

#include "logdevice/common/AdminCommandTable.h"
#include "logdevice/common/Timestamp.h"
#include "logdevice/server/storage_tasks/StorageThreadPool.h"

namespace facebook { namespace logdevice {

StorageTaskDebugInfo StorageTask::getDebugInfo() const {
  StorageTaskDebugInfo info(storageThreadPool_->getShardIdx(),
                            toString(getPriority()),
                            storageTaskThreadTypeName(getThreadType()),
                            toString(getType()),
                            toSystemTimestamp(enqueue_time_).toMilliseconds(),
                            durability_to_string(durability()));
  // Fill optional fields
  if (execution_start_time_) {
    info.execution_start_time =
        toSystemTimestamp(execution_start_time_.value()).toMilliseconds();
  }
  if (execution_end_time_) {
    info.execution_end_time =
        toSystemTimestamp(execution_end_time_.value()).toMilliseconds();
  }

  // Fill type-specific fields
  getDebugInfoDetailed(info);

  return info;
}

}} // namespace facebook::logdevice
