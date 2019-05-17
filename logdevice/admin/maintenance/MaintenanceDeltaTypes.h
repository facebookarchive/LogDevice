/**
 * Copyright (c) 2018-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include "logdevice/admin/maintenance/gen-cpp2/MaintenanceDelta_types.h"

namespace facebook { namespace logdevice { namespace maintenance {

class MaintenanceDeltaTypes {
 public:
  /**
   * Validates the given definitions and if validation succeeds, adds it to the
   * given state or otherwise sets the failure reason and leaves state
   * unmodified
   */
  static int
  applyMaintenances(const std::vector<thrift::MaintenanceDefinition>& def,
                    thrift::ClusterMaintenanceState& state,
                    std::string& failure_reason);

  /**
   * Removes the maintenances from state that match the filters specified in the
   * RemoveMaintenancesRequest
   */
  static int removeMaintenances(const thrift::RemoveMaintenancesRequest& req,
                                thrift::ClusterMaintenanceState& state,
                                std::string& failure_reason);

  /**
   * Returns true if the given MaintenanceDefinition is valid
   */
  static bool isValid(const thrift::MaintenanceDefinition& def);
};
}}} // namespace facebook::logdevice::maintenance
