/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "logdevice/admin/MaintenanceAPIHandler.h"

#include "logdevice/admin/maintenance/APIUtils.h"
#include "logdevice/admin/maintenance/MaintenanceManager.h"

using namespace facebook::logdevice::thrift;
using namespace facebook::logdevice::maintenance;

namespace facebook { namespace logdevice {
// get maintenances
folly::SemiFuture<std::unique_ptr<MaintenanceDefinitionResponse>>
MaintenanceAPIHandler::semifuture_getMaintenances(
    std::unique_ptr<MaintenancesFilter> /*request*/) {
  auto failed = failIfMMDisabled();
  if (failed) {
    return *failed;
  }
  // TODO Implement.
  ld_check(maintenance_manager_);
  auto v = std::make_unique<MaintenanceDefinitionResponse>();
  return folly::makeSemiFuture<std::unique_ptr<MaintenanceDefinitionResponse>>(
      std::move(v));
} // namespace logdevice

// apply a new maintenance
folly::SemiFuture<std::unique_ptr<MaintenanceDefinitionResponse>>
MaintenanceAPIHandler::semifuture_applyMaintenance(
    std::unique_ptr<MaintenanceDefinition> definition) {
  auto failed = failIfMMDisabled();
  if (failed) {
    return *failed;
  }
  if (definition == nullptr) {
    return InvalidRequest("MaintenanceDefinition is required");
  }
  ld_check(maintenance_manager_);
  /**
   * Steps of creating a maintenance.
   *   1- Perform basic input validation on the supplied maintenance
   *   definition
   *   2- Expand the maintenance into N maintenances in a list and for
   * each maintenance ensure that all the required fields that we expect
   * per maintenance are set.
   *     - All nodes _must_ have the node_index set. This is the only
   * field needed in NodeID and the rest will be unset.
   *     - Shards with shard_index = -1 are expanded into a ShardSet
   * according to the number of shards per node as configured at the time
   * of the request.
   *     - If group is True, a single maintenance definition is created.
   *     - If group is False, we will create a list of maintenance
   * definitions where each has all the shards and sequencers for every
   * given node that matches. The group in this case will try to achieve
   * all operations (storage and sequencer) on a single node in one go.
   * Each group gets its own ID. 3- We submit the list of groups as a
   * single Delta to the state machine and wait to hear back to respond to
   * the user.
   */
  auto validation = APIUtils::validateDefinition(*definition);
  if (validation) {
    return *validation;
  }
  auto nodes_config = processor_->getNodesConfiguration();
  folly::Expected<std::vector<MaintenanceDefinition>, InvalidRequest>
      expanded_maintenances =
          APIUtils::expandMaintenances(*definition, nodes_config);
  if (expanded_maintenances.hasError()) {
    return expanded_maintenances.error();
  }
  auto v = std::make_unique<MaintenanceDefinitionResponse>();
  return folly::makeSemiFuture<std::unique_ptr<MaintenanceDefinitionResponse>>(
      std::move(v));
}

// remove a maintenance
folly::SemiFuture<std::unique_ptr<RemoveMaintenancesResponse>>
MaintenanceAPIHandler::semifuture_removeMaintenances(
    std::unique_ptr<RemoveMaintenancesRequest> /*request*/) {
  auto failed = failIfMMDisabled();
  if (failed) {
    return *failed;
  }
  // TODO Implement.
  ld_check(maintenance_manager_);
  auto v = std::make_unique<RemoveMaintenancesResponse>();
  return folly::makeSemiFuture<std::unique_ptr<RemoveMaintenancesResponse>>(
      std::move(v));
}

// unblock rebuilding (marks shards unrecoverable)
folly::SemiFuture<std::unique_ptr<UnblockRebuildingResponse>>
MaintenanceAPIHandler::semifuture_unblockRebuilding(
    std::unique_ptr<UnblockRebuildingRequest> /*request*/) {
  // This does not depend on maintenance manager
  return folly::makeSemiFuture<std::unique_ptr<UnblockRebuildingResponse>>(
      NotSupported("Not Implemented!"));
}
}} // namespace facebook::logdevice
