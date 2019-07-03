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
    const std::shared_ptr<const NodesConfiguration>& nodes_config)
    : state_(std::move(state)), nodes_config_(nodes_config) {
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
  ld_assert(nodes_config_ != nullptr);
  ld_assert(state_);
  for (const auto& definition : state_->get_maintenances()) {
    // At this point we assume that all definitions are legal, they have been
    // validated by the RSM and the Admin API layer.
    //
    // We are guaranteed to see unique group-ids.
    ld_assert(definition.group_id_ref().has_value());
    GroupID group_id = definition.group_id_ref().value();
    groups_[group_id] = &definition;
    for (auto shard : getShardsFromDefinition(groups_[group_id])) {
      shards_to_groups_[shard].insert(group_id);
      shards_to_targets_[shard].insert(definition.get_shard_target_state());
    }

    // Is that maintenance definition applying a state to sequencers?
    for (auto node_id : getSequencersFromDefinition(groups_[group_id])) {
      nodes_to_groups_[node_id].insert(group_id);
      nodes_to_targets_[node_id] = definition.get_sequencer_target_state();
    }
  }
}

const MaintenanceDefinition*
ClusterMaintenanceWrapper::getMaintenanceByGroupID(const GroupID& group) const {
  const auto it = groups_.find(group);
  if (it != groups_.end()) {
    return it->second;
  }
  return nullptr;
}

ShardSet
ClusterMaintenanceWrapper::getShardsForGroup(const GroupID& group) const {
  return getShardsFromDefinition(getMaintenanceByGroupID(group));
}

ShardSet ClusterMaintenanceWrapper::getShardsFromDefinition(
    const MaintenanceDefinition* def) const {
  // ignore_missing = true. We ignore nodes that we cannot find in the
  // configuration.
  return def && def->get_shards().size() > 0
      ? expandShardSet(def->get_shards(), *nodes_config_, true)
      : ShardSet{};
}

folly::F14FastSet<node_index_t>
ClusterMaintenanceWrapper::getSequencersForGroup(const GroupID& group) const {
  return getSequencersFromDefinition(getMaintenanceByGroupID(group));
}

folly::F14FastSet<node_index_t>
ClusterMaintenanceWrapper::getSequencersFromDefinition(
    const MaintenanceDefinition* def) const {
  folly::F14FastSet<node_index_t> result;
  if (def == nullptr) {
    return result;
  }

  auto& sequencers = def->get_sequencer_nodes();
  if (sequencers.size() > 0) {
    for (const auto& node : sequencers) {
      folly::Optional<node_index_t> found_node_idx =
          findNodeIndex(node, *nodes_config_);
      if (!found_node_idx) {
        // Ignore nodes that we couldn't find in configuration.
        ld_info("Maintenance (group=%s) references to a node that doesn't "
                "exist in the configuration anymore. Can happen if nodes "
                "have been removed!",
                def->group_id_ref().value().c_str());
        continue;
      }
      result.insert(*found_node_idx);
    }
  }
  return result;
}

const folly::F14FastSet<GroupID>&
ClusterMaintenanceWrapper::getGroupsForShard(const ShardID& shard) const {
  ld_assert(shard.isValid());
  static const folly::F14FastSet<GroupID> default_state{};
  const auto it = shards_to_groups_.find(shard);
  if (it != shards_to_groups_.end()) {
    return it->second;
  }
  return default_state;
}

const folly::F14FastSet<GroupID>&
ClusterMaintenanceWrapper::getGroupsForSequencer(node_index_t node) const {
  static const folly::F14FastSet<GroupID> default_state{};
  const auto it = nodes_to_groups_.find(node);
  if (it != nodes_to_groups_.end()) {
    return it->second;
  }
  return default_state;
}

folly::F14FastMap<GroupID, ShardSet>
ClusterMaintenanceWrapper::groupShardsByGroupID(
    const std::vector<ShardID>& shards) const {
  folly::F14FastMap<GroupID, ShardSet> output;
  for (const auto& shard : shards) {
    // Let's find it in out groups index
    auto groups = getGroupsForShard(shard);
    for (const GroupID& group : groups) {
      output[group].insert(shard);
    }
  }
  return output;
}

folly::F14FastMap<GroupID, folly::F14FastSet<node_index_t>>
ClusterMaintenanceWrapper::groupSequencersByGroupID(
    const std::vector<node_index_t>& nodes) const {
  folly::F14FastMap<GroupID, folly::F14FastSet<node_index_t>> output;
  for (const auto& node : nodes) {
    // Let's find it in out groups index
    auto groups = getGroupsForSequencer(node);
    for (const GroupID& group : groups) {
      output[group].insert(node);
    }
  }
  return output;
}

const folly::F14FastSet<ShardOperationalState>&
ClusterMaintenanceWrapper::getShardTargetStates(const ShardID& shard) const {
  static const folly::F14FastSet<ShardOperationalState> default_state{
      ShardOperationalState::ENABLED};
  ld_assert(shard.isValid());
  const auto it = shards_to_targets_.find(shard);
  if (it != shards_to_targets_.end()) {
    return it->second;
  }
  return default_state;
}

SequencingState ClusterMaintenanceWrapper::getSequencerTargetState(
    node_index_t node_index) const {
  const auto it = nodes_to_targets_.find(node_index);
  if (it != nodes_to_targets_.end()) {
    return it->second;
  }
  return SequencingState::ENABLED;
}

bool ClusterMaintenanceWrapper::shouldSkipSafetyCheck(
    const ShardID& shard) const {
  const auto& groups = getGroupsForShard(shard);
  bool skip_safety_check = false;

  for (const GroupID& group : groups) {
    // we use [] operator as we are sure that group exists.
    ld_assert(groups_.count(group) > 0);
    auto* definition = groups_.at(group);
    skip_safety_check |= definition->get_skip_safety_checks();
  }
  return skip_safety_check;
}

bool ClusterMaintenanceWrapper::shouldForceRestoreRebuilding(
    const ShardID& shard) const {
  const auto& groups = getGroupsForShard(shard);
  bool force_restore_mode = false;

  for (const GroupID& group : groups) {
    ld_assert(groups_.count(group) > 0);
    auto* definition = groups_.at(group);
    force_restore_mode |= definition->get_force_restore_rebuilding();
  }
  return force_restore_mode;
}

bool ClusterMaintenanceWrapper::shouldSkipSafetyCheck(
    node_index_t node_id) const {
  const auto& groups = getGroupsForSequencer(node_id);
  bool skip_safety_check = false;

  for (const GroupID& group : groups) {
    ld_assert(groups_.count(group) > 0);
    auto* definition = groups_.at(group);
    skip_safety_check |= definition->get_skip_safety_checks();
  }
  return skip_safety_check;
}

bool ClusterMaintenanceWrapper::isPassiveDrainAllowed(
    const ShardID& shard) const {
  const auto& groups = getGroupsForShard(shard);
  bool passive_drain_allowed = false;

  for (const GroupID& group : groups) {
    ld_assert(groups_.count(group) > 0);
    auto* definition = groups_.at(group);
    passive_drain_allowed |= definition->get_allow_passive_drains();
  }
  return passive_drain_allowed;
}
}}} // namespace facebook::logdevice::maintenance
