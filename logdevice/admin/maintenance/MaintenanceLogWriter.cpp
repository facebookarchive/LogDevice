/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "logdevice/admin/maintenance/MaintenanceLogWriter.h"

#include "logdevice/common/ThriftCodec.h"

namespace facebook { namespace logdevice { namespace maintenance {

void MaintenanceLogWriter::writeDelta(
    std::unique_ptr<MaintenanceDelta> delta,
    std::function<
        void(Status st, lsn_t version, const std::string& failure_reason)> cb,
    ClusterMaintenanceStateMachine::WriteMode mode,
    folly::Optional<lsn_t> base_version) {
  ld_check(delta);
  std::string serializedData =
      ThriftCodec::serialize<apache::thrift::BinarySerializer>(*delta);
  std::unique_ptr<Request> req =
      std::make_unique<MaintenanceLogWriteDeltaRequest>(
          ClusterMaintenanceStateMachine::workerType(processor_),
          std::move(serializedData),
          std::move(cb),
          std::move(mode),
          std::move(base_version));
  processor_->postWithRetrying(req);
}
}}} // namespace facebook::logdevice::maintenance
