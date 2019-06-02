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
#include "logdevice/common/configuration/Configuration.h"

namespace facebook { namespace logdevice { namespace maintenance {
// Helper functions for Maintenance API requests and responses
namespace APIUtils {
/**
 * Validates that a user-supplied maintenance definition has all the required
 * fields and is well formed. Also updates the maintenance definition to
 * sanitize some of the input values. This includes removing leading or trailing
 * whitespaces from the user.
 */
folly::Optional<thrift::InvalidRequest>
validateDefinition(const MaintenanceDefinition& definition);

/**
 * Takes a single definition and if group is set to False, it will expand this
 * definition into multiple.
 *
 * Regardless of whether this is a single definition of more, each definition
 * will have its shardset expanded based on the nodes configuration. If nodes
 * or shards do not exist in the nodes config, we will return
 * thrift::InvalidRequest accordingly.
 *
 * Note: The generated list contains newly constructed objects that we manually
 * copy the values we need from the original definitions into. This is
 * implemented this way to reduce the chances that a new attribute is
 * added to the public thrift interface without the proper validation
 * leaking into the server RSM execution.
 *
 * If you added new field that you expect the user to set in
 * MaintenanceDefinition. Make sure that this function is updated.
 */
folly::Expected<std::vector<MaintenanceDefinition>, thrift::InvalidRequest>
expandMaintenances(
    const MaintenanceDefinition& definition,
    const std::shared_ptr<const NodesConfiguration>& nodes_config);

/**
 * Groups the given shards by node_index.
 * Every shard object **must** have the node_index set.
 */
folly::F14FastMap<int32_t, thrift::ShardSet>
groupShardsByNode(const ShardSet& shards);
/**
 * Goes through the supplied `definitions` and set the system generated values
 * for each of them based on the definition in input.
 *
 * This will also give each definition a randomly generated unique group id.
 */
void fillSystemGeneratedAttributes(
    const MaintenanceDefinition& input,
    std::vector<MaintenanceDefinition>& definitions);
/**
 * Compares two maintenances if they are requested from the same user and with
 * the same set of targets even if they have different group ids.
 */
bool areMaintenancesEquivalent(const MaintenanceDefinition& def1,
                               const MaintenanceDefinition& def2);
/**
 * Tries to find in the `defs` argument whether we can find an equivalent
 * maintenance to `target`. Returns the group-id of the match.
 */
folly::Optional<MaintenanceDefinition>
findEquivalentMaintenance(const std::vector<MaintenanceDefinition>& defs,
                          const MaintenanceDefinition& target);

/**
 * Finds a maintenance by group-id in the supplied list.
 */
folly::Optional<MaintenanceDefinition>
findMaintenanceByID(const std::string& group_id,
                    const std::vector<MaintenanceDefinition>& defs);

/**
 * Generates a random string for maintenance groups
 */
std::string generateGroupID(size_t length);

/**
 * Returns true if the filter is set (has at least one of the filters defined
 * and not empty)
 */
bool isMaintenancesFilterSet(const thrift::MaintenancesFilter& filter);
/**
 * Returns a vector of maintenance definitions that match the filter. Note that
 * if the filter is unset (no group-ids nor user is set) this will match all
 * maintenances.
 */
std::vector<MaintenanceDefinition>
filterMaintenances(const thrift::MaintenancesFilter& filter,
                   const std::vector<MaintenanceDefinition>& maintenances);
/**
 * Filter out the list of maintenances based on the given filter and calls the
 * functor on each matching definition.
 */
void matchMaintenances(const thrift::MaintenancesFilter& filter,
                       const std::vector<MaintenanceDefinition>& maintenances,
                       folly::Function<void(const MaintenanceDefinition&)>);

/**
 * Checks if a given maintenance matches the input filter. It's important to
 * note that this will match ANY maintenance if the filter is competely unset
 * (no user nor group_ids are set). If the higher-level code require the filter
 * to be set, it's the responsibility of that higher level code to verify that.
 */
bool doesMaintenanceMatchFilter(const thrift::MaintenancesFilter& filter,
                                const MaintenanceDefinition& def);

} // namespace APIUtils
}}} // namespace facebook::logdevice::maintenance
