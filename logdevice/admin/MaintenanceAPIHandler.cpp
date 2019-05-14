/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "logdevice/admin/MaintenanceAPIHandler.h"

#include "logdevice/admin/maintenance/MaintenanceManager.h"

using namespace facebook::logdevice::thrift;

namespace facebook { namespace logdevice {
// get maintenances
folly::SemiFuture<std::unique_ptr<MaintenanceDefinitionResponse>>
MaintenanceAPIHandler::semifuture_getMaintenances(
    std::unique_ptr<MaintenancesFilter> /*request*/) {
  return failIfMMDisabled().toUnsafeFuture().thenValue([=](auto&&) {
    // TODO Implement.
    ld_check(maintenance_manager_);
    auto v = std::make_unique<MaintenanceDefinitionResponse>();
    return folly::makeSemiFuture<
        std::unique_ptr<MaintenanceDefinitionResponse>>(std::move(v));
  });
}

// apply a new maintenance
folly::SemiFuture<std::unique_ptr<MaintenanceDefinitionResponse>>
MaintenanceAPIHandler::semifuture_applyMaintenance(
    std::unique_ptr<MaintenanceDefinition> /*request*/) {
  return failIfMMDisabled().toUnsafeFuture().thenValue([=](auto&&) {
    // TODO Implement.
    ld_check(maintenance_manager_);
    auto v = std::make_unique<MaintenanceDefinitionResponse>();
    return folly::makeSemiFuture<
        std::unique_ptr<MaintenanceDefinitionResponse>>(std::move(v));
  });
}

// remove a maintenance
folly::SemiFuture<std::unique_ptr<RemoveMaintenancesResponse>>
MaintenanceAPIHandler::semifuture_removeMaintenances(
    std::unique_ptr<RemoveMaintenancesRequest> /*request*/) {
  return failIfMMDisabled().toUnsafeFuture().thenValue([=](auto&&) {
    // TODO Implement.
    ld_check(maintenance_manager_);
    auto v = std::make_unique<RemoveMaintenancesResponse>();
    return folly::makeSemiFuture<std::unique_ptr<RemoveMaintenancesResponse>>(
        std::move(v));
  });
}

// unblock rebuilding (marks shards unrecoverable)
folly::SemiFuture<std::unique_ptr<UnblockRebuildingResponse>>
MaintenanceAPIHandler::semifuture_unblockRebuilding(
    std::unique_ptr<UnblockRebuildingRequest> /*request*/) {
  // This does not depend on maintenance manager
  return folly::makeSemiFuture<std::unique_ptr<UnblockRebuildingResponse>>(
      thrift::NotSupported("Not Implemented!"));
}
}} // namespace facebook::logdevice
