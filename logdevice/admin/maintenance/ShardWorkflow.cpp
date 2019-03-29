/**
 *  Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 *  All rights reserved.
 *
 *  This source code is licensed under the BSD-style license found in the
 *  LICENSE file in the root directory of this source tree.
 **/

#include "logdevice/admin/maintenance/ShardWorkflow.h"

#include <folly/MoveWrapper.h>

namespace facebook { namespace logdevice { namespace maintenance {

folly::SemiFuture<MaintenanceStatus>
ShardWorkflow::run(membership::StorageState storage_state,
                   ShardDataHealth data_health,
                   RebuildingMode rebuilding_mode) {
  folly::Promise<MaintenanceStatus> p;
  return p.getFuture();
}

void ShardWorkflow::writeToEventLog(
    std::unique_ptr<EventLogRecord> event,
    std::function<void(Status st, lsn_t lsn, const std::string& str)> cb) {}

void ShardWorkflow::computeMaintenanceStatus() {
  return;
}

ShardID ShardWorkflow::getShardID() const {
  return shard_;
}

void ShardWorkflow::computeMaintenanceStatusForDrain() {}

void ShardWorkflow::computeMaintenanceStatusForMayDisappear() {}

void ShardWorkflow::computeMaintenanceStatusForEnable() {}

void ShardWorkflow::createAbortEventIfRequired() {}

}}} // namespace facebook::logdevice::maintenance
