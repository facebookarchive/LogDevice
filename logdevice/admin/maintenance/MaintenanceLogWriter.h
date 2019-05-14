/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include "logdevice/admin/maintenance/ClusterMaintenanceStateMachine.h"
#include "logdevice/admin/maintenance/gen-cpp2/MaintenanceDelta_types.h"

namespace facebook { namespace logdevice { namespace maintenance {

/**
 * A simple wrapper class that posts a MaintenanceLogWriteDeltaRequest
 */

class MaintenanceLogWriter {
 public:
  explicit MaintenanceLogWriter(Processor* processor) : processor_(processor) {}

  virtual ~MaintenanceLogWriter() {}

  virtual void writeDelta(
      std::unique_ptr<MaintenanceDelta> delta,
      std::function<
          void(Status st, lsn_t version, const std::string& failure_reason)> cb,
      ClusterMaintenanceStateMachine::WriteMode mode =
          ClusterMaintenanceStateMachine::WriteMode::CONFIRM_APPLIED,
      folly::Optional<lsn_t> base_version = folly::none);

  /**
   * Returns RemoveMaintenancesRequest for removing an internal maintenance
   * for the given shard by setting appropriate fields in the request
   */
  static thrift::RemoveMaintenancesRequest
  buildRemoveMaintenancesRequest(ShardID shard, std::string reason);
  /**
   * Returns MaintenanceDefinition for rebuilding a given shard internally with
   * all appropirate fields set
   */
  static thrift::MaintenanceDefinition
  buildMaintenanceDefinitionForRebuilding(ShardID shard, std::string reason);

 private:
  Processor* processor_;
};

}}} // namespace facebook::logdevice::maintenance
