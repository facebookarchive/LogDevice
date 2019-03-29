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
      std::shared_ptr<const configuration::nodes::NodesConfiguration>
          nodes_config);

  /**
   * Queries the groups by ID and returns a pointer if found. The
   * MaintenanceDefinition lifetime is bound by this wrapper object.
   */
  const MaintenanceDefinition* getMaintenanceByGroupID(GroupID group) const;

  /**
   * This doesn't look into the node role, if there is no maintenance applied
   * for this particular shard, this will return
   * {ShardOperationalState::ENABLED}. It's the responsibility of the caller to
   * verify whether this is a storage node or not.
   *
   * This also will return ENABLED for shards that are not even member of the
   * cluster.
   */
  std::unordered_set<ShardOperationalState>
  getShardTargetStates(ShardID shard) const;
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
   * Returns an empty vector if no maintenances applied on this shard.
   */
  std::unordered_set<GroupID> getGroupsForShard(ShardID shard) const;
  /**
   * Returns an empty vector if no maintenances applied on this node.
   */
  std::unordered_set<GroupID> getGroupsForSequencer(node_index_t node) const;

  /**
   * Does the maintenances applied on a shard want us to skip safety checks?
   */
  bool shouldSkipSafetyCheck(ShardID shard) const;
  /**
   * Does the maintenances applied on a sequencer want us to skip safety checks?
   */
  bool shouldSkipSafetyCheck(node_index_t nodex) const;
  /**
   * Do we allow passive drains to happen automatically?
   */
  bool isPassiveDrainAllowed(ShardID shard) const;
  /**
   * This is usually set by the RebuildingSupervisor to indicate that we should
   * run rebuilding in RESTORE mode.
   */
  bool shouldForceRestoreRebuilding(ShardID shard) const;
  /**
   * Update the underlying nodes configuration used by this structure to resolve
   * nodes and shards. This will trigger a recreation of the internal indexes.
   */
  void updateNodesConfiguration(
      std::shared_ptr<const configuration::nodes::NodesConfiguration>
          nodes_config) {
    clear();
    indexDefinitions();
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

  folly::F14NodeMap<ShardID, std::unordered_set<GroupID>> shards_to_groups_;
  folly::F14NodeMap<ShardID, std::unordered_set<ShardOperationalState>>
      shards_to_targets_;

  folly::F14NodeMap<node_index_t, std::unordered_set<GroupID>> nodes_to_groups_;
  folly::F14NodeMap<node_index_t, SequencingState> nodes_to_targets_;
};

}}} // namespace facebook::logdevice::maintenance
