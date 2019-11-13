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
                            SteadyTimestamp(enqueue_time_)
                                .approximateSystemTimestamp()
                                .toMilliseconds(),
                            durability_to_string(durability()));
  // Fill optional fields
  if (execution_start_time_) {
    info.execution_start_time = SteadyTimestamp(execution_start_time_.value())
                                    .approximateSystemTimestamp()
                                    .toMilliseconds();
  }
  if (execution_end_time_) {
    info.execution_end_time = SteadyTimestamp(execution_end_time_.value())
                                  .approximateSystemTimestamp()
                                  .toMilliseconds();
  }

  // Fill type-specific fields
  getDebugInfoDetailed(info);

  return info;
}

}} // namespace facebook::logdevice
