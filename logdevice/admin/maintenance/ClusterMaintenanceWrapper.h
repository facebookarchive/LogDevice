/**
 *  Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 *  All rights reserved.
 *
 *  This source code is licensed under the BSD-style license found in the
 *  LICENSE file in the root directory of this source tree.
 **/
#pragma once

#include <folly/Optional.h>
#include <folly/container/F14Map.h>
#include <folly/container/F14Set.h>

#include "logdevice/admin/maintenance/types.h"
#include "logdevice/common/ShardID.h"
#include "logdevice/common/configuration/nodes/NodesConfiguration.h"

namespace facebook { namespace logdevice { namespace maintenance {

/**
 * A wrapper class around thrift's ClusterMaintenanceState. This wrapper offers
 * easy accessors to find the target states for a given shard or a sequencer
 * node.
 */
class ClusterMaintenanceWrapper {
 public:
  ClusterMaintenanceWrapper(
      std::unique_ptr<thrift::ClusterMaintenanceState> state,
      const std::shared_ptr<const configuration::nodes::NodesConfiguration>&
          nodes_config);

  /**
   * Queries the groups by ID and returns a pointer if found. The
   * MaintenanceDefinition lifetime is bound by this wrapper object.
   */
  const MaintenanceDefinition*
  getMaintenanceByGroupID(const GroupID& group) const;

  /**
   * Returns a normalized list of shards from the given GroupID. Looks up
   * the given group id in current ClusterMaintenanceState and returns a
   * normalized set of shards from the corresponding MaintenanceDefinition
   */
  ShardSet getShardsForGroup(const GroupID& group) const;

  /**
   * Returns the list of sequencer nodes from the given GroupID. Looks up
   * the given group id in current ClusterMaintenanceState and returns a
   * set of nodes from the corresponding MaintenanceDefinition
   */
  folly::F14FastSet<node_index_t>
  getSequencersForGroup(const GroupID& group) const;

  /**
   * This doesn't look into the node role, if there is no maintenance applied
   * for this particular shard it will return {ShardOperationalState::ENABLED}
   * It's the responsibility of the caller to verify whether this is a storage
   * node or not.
   */
  const folly::F14FastSet<ShardOperationalState>&
  getShardTargetStates(const ShardID& shard) const;

  /**
   * This doesn't look into the node role, if there is no maintenance applied
   * for this particular node index, this will return SequencingState::ENABLED.
   * It's the responsibility of the caller to verify whether this is a
   * sequencer node or not.
   *
   * This also will return ENABLED for nodes that are not even member of the
   * cluster.
   */
  SequencingState getSequencerTargetState(node_index_t node_index) const;

  /**
   * Returns a reference to a reference to an empty set if no maintenances
   * applied on this shard.
   */
  const folly::F14FastSet<GroupID>&
  getGroupsForShard(const ShardID& shard) const;

  /**
   * Returns a a reference to an empty set if no maintenances applied on this
   * node.
   */
  const folly::F14FastSet<GroupID>&
  getGroupsForSequencer(node_index_t node) const;

  /**
   * Accepts a list of shard ids and return them grouped by group-ids. Note that
   * the shard may appear in multiple groups.
   */
  folly::F14FastMap<GroupID, ShardSet>
  groupShardsByGroupID(const std::vector<ShardID>& shards) const;

  /**
   * Accepts a list of node ids and return them grouped by group-ids. Note that
   * the node may appear in multiple groups.
   */
  folly::F14FastMap<GroupID, folly::F14FastSet<node_index_t>>
  groupSequencersByGroupID(const std::vector<node_index_t>& shards) const;

  /**
   * Does the maintenances applied on a shard want us to skip safety checks?
   */
  bool shouldSkipSafetyCheck(const ShardID& shard) const;
  /**
   * Does the maintenances applied on a sequencer want us to skip safety checks?
   */
  bool shouldSkipSafetyCheck(node_index_t nodex) const;
  /**
   * Do we allow passive drains to happen automatically?
   */
  bool isPassiveDrainAllowed(const ShardID& shard) const;
  /**
   * This is usually set by the RebuildingSupervisor to indicate that we should
   * run rebuilding in RESTORE mode.
   */
  bool shouldForceRestoreRebuilding(const ShardID& shard) const;
  /**
   * Update the underlying nodes configuration used by this structure to resolve
   * nodes and shards. This will trigger a recreation of the internal indexes.
   */
  void updateNodesConfiguration(
      const std::shared_ptr<const configuration::nodes::NodesConfiguration>&
          nodes_config) {
    ld_assert(nodes_config != nullptr);
    if (nodes_config_->getVersion() == nodes_config->getVersion()) {
      ld_debug("NodesConfig is up-to-date(version:%lu). Not regenerating index "
               "for definitions",
               nodes_config_->getVersion().val_);
      return;
    }

    nodes_config_ = nodes_config;
    clear();
    indexDefinitions();
    ld_debug("Regenerated index for definitions using NodesConfiguration "
             "version:%lu",
             nodes_config_->getVersion().val_);
  }

  uint64_t getVersion() {
    return static_cast<uint64_t>(state_->get_version());
  }

 private:
  void clear();
  void indexDefinitions();

  std::unique_ptr<thrift::ClusterMaintenanceState> state_;
  std::shared_ptr<const configuration::nodes::NodesConfiguration> nodes_config_;
  folly::F14NodeMap<GroupID, const MaintenanceDefinition*> groups_;

  folly::F14NodeMap<ShardID, folly::F14FastSet<GroupID>> shards_to_groups_;
  folly::F14NodeMap<ShardID, folly::F14FastSet<ShardOperationalState>>
      shards_to_targets_;

  folly::F14NodeMap<node_index_t, folly::F14FastSet<GroupID>> nodes_to_groups_;
  folly::F14NodeMap<node_index_t, SequencingState> nodes_to_targets_;

  ShardSet getShardsFromDefinition(const MaintenanceDefinition* def) const;
  folly::F14FastSet<node_index_t>
  getSequencersFromDefinition(const MaintenanceDefinition* def) const;
};

}}} // namespace facebook::logdevice::maintenance
