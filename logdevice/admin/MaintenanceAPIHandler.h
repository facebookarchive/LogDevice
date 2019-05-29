/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include "logdevice/admin/AdminAPIHandlerBase.h"
#include "logdevice/admin/maintenance/types.h"

namespace facebook { namespace logdevice {
class MaintenanceAPIHandler : public virtual AdminAPIHandlerBase {
 public:
  // get a list of definitions
  virtual folly::SemiFuture<
      std::unique_ptr<thrift::MaintenanceDefinitionResponse>>
  semifuture_getMaintenances(
      std::unique_ptr<thrift::MaintenancesFilter> request) override;

  // apply a new maintenance
  virtual folly::SemiFuture<
      std::unique_ptr<thrift::MaintenanceDefinitionResponse>>
  semifuture_applyMaintenance(
      std::unique_ptr<thrift::MaintenanceDefinition> request) override;

  // remove a maintenance
  virtual folly::SemiFuture<std::unique_ptr<thrift::RemoveMaintenancesResponse>>
  semifuture_removeMaintenances(
      std::unique_ptr<thrift::RemoveMaintenancesRequest> request) override;

  // unblock rebuilding (marks shards unrecoverable)
  virtual folly::SemiFuture<std::unique_ptr<thrift::UnblockRebuildingResponse>>
  semifuture_unblockRebuilding(
      std::unique_ptr<thrift::UnblockRebuildingRequest> request) override;

 private:
  // throws NotSupported exception if MM is disabled.
  folly::Optional<thrift::NotSupported> failIfMMDisabled() {
    if (!updateable_admin_server_settings_->enable_maintenance_manager ||
        maintenance_manager_ == nullptr) {
      return thrift::NotSupported("MaintenanceManager is not enabled!");
    }
    return folly::none;
  }
};
}} // namespace facebook::logdevice
