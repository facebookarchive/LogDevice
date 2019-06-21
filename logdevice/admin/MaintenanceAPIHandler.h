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
  using ListMaintenanceDefs =
      folly::Expected<std::vector<thrift::MaintenanceDefinition>,
                      maintenance::MaintenanceError>;
  /**
   * This takes a list of definitions, get the latest state from the RSM and
   * find which maintenances need to be created vs. the ones that already exist
   * and can be returned.
   *
   * The returned MaintenanceDefinitions will not contain any information from
   * the maintenance manager.
   */
  folly::SemiFuture<ListMaintenanceDefs>
  applyAndGetMaintenances(std::vector<thrift::MaintenanceDefinition> defs);

  /**
   * Takes a list of new maintenances, and existing maintenance. Then it will
   * apply the new maintenance to the RSM, if everything is successful, it will
   * merge the results into one vector.
   *
   * This will then pass that list to MM if MM is running to augment the
   * maintenances. and fulfills the promise with the value.
   *
   */
  folly::SemiFuture<ListMaintenanceDefs>
  applyAndMerge(std::vector<thrift::MaintenanceDefinition> new_defs,
                std::vector<thrift::MaintenanceDefinition> existing);
};
}} // namespace facebook::logdevice
