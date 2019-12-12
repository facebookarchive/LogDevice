/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include "logdevice/admin/maintenance/types.h"
#include "logdevice/common/ShardID.h"
#include "logdevice/common/event_log/EventLogRebuildingSet.h"
#include "logdevice/include/Client.h"

namespace facebook { namespace logdevice { namespace maintenance {

/**
 * This function goes over a sequence of steps in order to migrate
 * the existing maintenances (both internal and external) to the new
 * maintenance manager.
 *
 * 1.   Verify that NCM is turned on for the cluster
 * 2.   Start EventLogStateMachine and wait till replay is complete
 * 3.   For all Shards that are rebuilding in Restore mode (without Drain flag)
 *      build an internal maintenance record
 * 4.   For all Shards that are rebuilding in Restore mode (with Drain flag)
 *      build an external maintenance record
 * 5.   For all shards that are rebuilding in Relocate mode
 *      build an external maintenance record
 * 6.   Update NCM to set Storage State to DATA_MIGRATION forcefully
 *
 * Note: This migration method is racy as RebuildingSupervisor could add new
 * shards to rebuilding set after this method has read the event log. One can
 * rerun the script until the rebuilding set and storage state in NCM converges.
 * In practice this may not need to run this more than couple of times
 */
Status migrateToMaintenanceManager(Client& client);

using MaintenanceDefMap =
    std::unordered_map<ShardID,
                       std::vector<thrift::MaintenanceDefinition>,
                       ShardID::Hash>;

constexpr auto MIGRATION_USER{"_migration_"};

/**
 * Generates a new MaintenanceDefinition for each node,shard pair in the
 * RebuildingSet. If MaintenanceDefinitions already exist in the maintenance
 * log, the vector will be empty
 */
MaintenanceDefMap
generateMaintenanceDefinition(const EventLogRebuildingSet& set,
                              const thrift::ClusterMaintenanceState& state);

thrift::MaintenanceDefinition
buildMaintenanceDefinitionForExternalMaintenance(ShardID shard,
                                                 std::string reason);

// Fetches the ClusterMaintenanceState by reading the maintenance log
int getClusterMaintenanceState(Client& client,
                               thrift::ClusterMaintenanceState& cms);

bool isDefinitionExists(const ShardID& shard,
                        RebuildingMode mode,
                        const thrift::ClusterMaintenanceState& state);

}}} // namespace facebook::logdevice::maintenance
