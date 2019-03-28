/**
 *  Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 *  All rights reserved.
 *
 *  This source code is licensed under the BSD-style license found in the
 *  LICENSE file in the root directory of this source tree.
 **/
#include "logdevice/admin/maintenance/ClusterMaintenanceWrapper.h"

#include "logdevice/admin/AdminAPIUtils.h"
#include "logdevice/common/debug.h"

using facebook::logdevice::configuration::nodes::NodesConfiguration;

namespace facebook { namespace logdevice { namespace maintenance {

ClusterMaintenanceWrapper::ClusterMaintenanceWrapper(
    std::unique_ptr<thrift::ClusterMaintenanceState> state,
    std::shared_ptr<const NodesConfiguration> nodes_config)
    : state_(std::move(state)), nodes_config_(std::move(nodes_config)) {
  indexDefinitions();
}

void ClusterMaintenanceWrapper::clear() {
  groups_.clear();
  shards_to_groups_.clear();
  shards_to_targets_.clear();
  nodes_to_groups_.clear();
  nodes_to_targets_.clear();
}

void ClusterMaintenanceWrapper::indexDefinitions() {
  ld_assert(state_);
  for (const auto& definition : state_->get_definitions()) {
    // At this point we assume that all definitions are legal, they have been
    // validated by the RSM and the Admin API layer.
    //
    // We are guaranteed to see unique group-ids.
    ld_assert(definition.group_id_ref().has_value());
    GroupID group_id = definition.group_id_ref().value();
    groups_[group_id] = &definition;
    // Is that maintenance definition applying a state to shards?
    auto shards = definition.get_shards();
    if (shards.size() > 0) {
      // ignore_missing = true. We ignore nodes that we cannot find in the
      // configuration.
      ShardSet expanded_shards = expandShardSet(shards, *nodes_config_, true);
      for (auto shard : expanded_shards) {
        shards_to_groups_[shard].insert(group_id);
        shards_to_targets_[shard].insert(definition.get_shard_target_state());
      }
    }

    // Is that maintenance definition applying a state to sequencers?
    auto sequencers = definition.get_sequencer_nodes();
    if (sequencers.size() > 0) {
      for (const auto& node : sequencers) {
        folly::Optional<node_index_t> found_node_idx =
            findNodeIndex(node, *nodes_config_);
        if (!found_node_idx) {
          // Ignore nodes that we couldn't find in configuration.
          ld_info("Maintenance (group=%s) references to a node that doesn't "
                  "exist in the configuration anymore. Can happen if nodes "
                  "have been removed!",
                  group_id.c_str());
          continue;
        }
        node_index_t node_id = *found_node_idx;
        nodes_to_groups_[node_id].insert(group_id);
        nodes_to_targets_[node_id] = definition.get_sequencer_target_state();
      }
    }
  }
}

const MaintenanceDefinition*
ClusterMaintenanceWrapper::getMaintenanceByGroupID(GroupID group) const {
  const auto it = groups_.find(group);
  if (it != groups_.end()) {
    return it->second;
  }
  return nullptr;
}

std::set<GroupID>
ClusterMaintenanceWrapper::getGroupsForShard(ShardID shard) const {
  ld_assert(shard.isValid());
  const auto it = shards_to_groups_.find(shard);
  if (it != shards_to_groups_.end()) {
    return it->second;
  }
  return std::set<GroupID>();
}

std::set<GroupID>
ClusterMaintenanceWrapper::getGroupsForSequencer(node_index_t node) const {
  const auto it = nodes_to_groups_.find(node);
  if (it != nodes_to_groups_.end()) {
    return it->second;
  }
  return std::set<GroupID>();
}

std::set<ShardOperationalState>
ClusterMaintenanceWrapper::getShardTargetStates(ShardID shard) const {
  ld_assert(shard.isValid());
  const auto it = shards_to_targets_.find(shard);
  if (it != shards_to_targets_.end()) {
    return it->second;
  }
  return std::set<ShardOperationalState>{ShardOperationalState::ENABLED};
}

SequencingState ClusterMaintenanceWrapper::getSequencerTargetState(
    node_index_t node_index) const {
  const auto it = nodes_to_targets_.find(node_index);
  if (it != nodes_to_targets_.end()) {
    return it->second;
  }
  return SequencingState::ENABLED;
}

bool ClusterMaintenanceWrapper::shouldSkipSafetyCheck(ShardID shard) const {
  std::set<GroupID> groups = getGroupsForShard(shard);
  bool skip_safety_check = false;

  for (const auto& group : groups) {
    // we use [] operator as we are sure that group exists.
    ld_assert(groups_.count(group) > 0);
    auto* definition = groups_.at(group);
    skip_safety_check |= definition->get_skip_safety_checks();
  }
  return skip_safety_check;
}

bool ClusterMaintenanceWrapper::shouldForceRestoreRebuilding(
    ShardID shard) const {
  std::set<GroupID> groups = getGroupsForShard(shard);
  bool force_restore_mode = false;

  for (const auto& group : groups) {
    // we use [] operator as we are sure that group exists.
    ld_assert(groups_.count(group) > 0);
    auto* definition = groups_.at(group);
    force_restore_mode |= definition->get_force_restore_rebuilding();
  }
  return force_restore_mode;
}

bool ClusterMaintenanceWrapper::shouldSkipSafetyCheck(
    node_index_t node_id) const {
  std::set<GroupID> groups = getGroupsForSequencer(node_id);
  bool skip_safety_check = false;

  for (const auto& group : groups) {
    // we use [] operator as we are sure that group exists.
    ld_assert(groups_.count(group) > 0);
    auto* definition = groups_.at(group);
    skip_safety_check |= definition->get_skip_safety_checks();
  }
  return skip_safety_check;
}

bool ClusterMaintenanceWrapper::isPassiveDrainAllowed(ShardID shard) const {
  std::set<GroupID> groups = getGroupsForShard(shard);
  bool passive_drain_allowed = false;

  for (const auto& group : groups) {
    // we use [] operator as we are sure that group exists.
    ld_assert(groups_.count(group) > 0);
    auto* definition = groups_.at(group);
    passive_drain_allowed |= definition->get_allow_passive_drains();
  }
  return passive_drain_allowed;
}
}}} // namespace facebook::logdevice::maintenance
