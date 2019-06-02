/**
 * Copyright (c) 2018-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/admin/maintenance/MaintenanceDeltaTypes.h"

#include <folly/Format.h>
#include <thrift/lib/cpp/util/EnumUtils.h>

#include "logdevice/admin/maintenance/APIUtils.h"
#include "logdevice/admin/maintenance/types.h"
#include "logdevice/common/ShardID.h"
#include "logdevice/common/debug.h"
#include "logdevice/include/Err.h"

namespace facebook { namespace logdevice { namespace maintenance {

using apache::thrift::util::enumName;

int MaintenanceDeltaTypes::applyMaintenances(
    const std::vector<thrift::MaintenanceDefinition>& defs,
    thrift::ClusterMaintenanceState& state,
    std::string& failure_reason) {
  for (const auto& def : defs) {
    for (auto& existing_def : state.get_maintenances()) {
      ld_check(existing_def.group_id_ref().has_value());
      ld_check(def.group_id_ref().has_value());
      if (existing_def.group_id_ref().value() == def.group_id_ref().value()) {
        err = E::MAINTENANCE_CLASH;
        failure_reason = folly::sformat(
            "Group ID:{} already exists", existing_def.group_id_ref().value());
        return -1;
      }

      if (existing_def.get_user() != def.get_user()) {
        continue;
      }

      auto is_shard_in_existing_def = [&](thrift::ShardID s) {
        return std::find(existing_def.get_shards().begin(),
                         existing_def.get_shards().end(),
                         s) != existing_def.get_shards().end();
      };
      // The search is n^2. but the ShardSets are expected to be small
      // Reconsider if this turns out to be a pref problem
      auto shard_it = std::find_if(def.get_shards().begin(),
                                   def.get_shards().end(),
                                   is_shard_in_existing_def);
      if (shard_it != def.get_shards().end()) {
        err = E::MAINTENANCE_CLASH;
        failure_reason = folly::sformat(
            "Another maintenance ({}) was already created by user {} "
            "that set a target for shard {}",
            existing_def.group_id_ref().value(),
            def.get_user(),
            toString(ShardID(shard_it->get_node().node_index_ref().value(),
                             shard_it->get_shard_index())));
        return -1;
      }

      auto is_node_in_existing_def = [&](thrift::NodeID n) {
        return std::find(existing_def.get_sequencer_nodes().begin(),
                         existing_def.get_sequencer_nodes().end(),
                         n) != existing_def.get_sequencer_nodes().end();
      };

      auto node_it = std::find_if(def.get_sequencer_nodes().begin(),
                                  def.get_sequencer_nodes().end(),
                                  is_node_in_existing_def);
      if (node_it != def.get_sequencer_nodes().end()) {
        err = E::MAINTENANCE_CLASH;
        failure_reason = folly::sformat(
            "Another maintenance ({}) was already created by user {} "
            "that set a target for sequencer node {}",
            existing_def.group_id_ref().value(),
            def.get_user(),
            node_it->node_index_ref().value());
        return -1;
      }
    }
  }

  auto modified_defs = std::move(state).get_maintenances();
  for (const auto& def : defs) {
    modified_defs.push_back(def);
  }
  state.set_maintenances(std::move(modified_defs));
  return 0;
}

int MaintenanceDeltaTypes::removeMaintenances(
    const thrift::RemoveMaintenancesRequest& req,
    thrift::ClusterMaintenanceState& state,
    std::string& failure_reason) {
  const auto& filter = req.get_filter();
  if (!APIUtils::isMaintenancesFilterSet(filter)) {
    // We don't accept unset filters in removals, that would result in removing
    // all maintenances. That's something we don't want to support for safety.
    // Either groups to remove or user should be specified. Both cannot be empty
    err = E::INVALID_PARAM;
    failure_reason = "Cannot remove maintenances with an-unset filter";
    return -1;
  }

  // We remove all maintenances that match the filter. It's okay if some of the
  // group-ids supplied in the filter didn't match maintenance in state. Check
  // admin.thrift for the spec.
  std::vector<std::vector<MaintenanceDefinition>::iterator> pos_to_remove;
  int rv = 0;

  auto modified_defs = std::move(state).get_maintenances();
  auto it = modified_defs.begin();
  while (it != modified_defs.end()) {
    if (APIUtils::doesMaintenanceMatchFilter(filter, *it)) {
      pos_to_remove.push_back(it);
    }
    it++;
  }

  if (pos_to_remove.empty()) {
    // It's critical that we move the maintenances back.
    state.set_maintenances(std::move(modified_defs));
    // No matches. Let's not bump the version, we will return -1 and set the err
    // to E::NOTFOUND
    err = E::NOTFOUND;
    failure_reason = "The filter did not match any maintenances";
    return -1;
  }

  // It is important that we start from the end of pos_to_remove
  // vector. If we start from beginning, all subsequent iterator
  // references in the vector will be invalidated
  auto pos_it = pos_to_remove.rbegin();
  while (pos_it != pos_to_remove.rend()) {
    ld_info("Removing Group: %s", (*pos_it)->group_id_ref().value().c_str());
    modified_defs.erase(*pos_it);
    pos_it++;
  }

  state.set_maintenances(std::move(modified_defs));
  return rv;
}

}}} // namespace facebook::logdevice::maintenance
