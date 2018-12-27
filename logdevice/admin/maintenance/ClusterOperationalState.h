/**
 *  Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 *  All rights reserved.
 *
 *  This source code is licensed under the BSD-style license found in the
 *  LICENSE file in the root directory of this source tree.
 **/
#pragma once

#include <memory>

#include "logdevice/admin/maintenance/ClusterMaintenanceStateMachine.h"
#include "logdevice/admin/maintenance/ClusterMaintenanceState_generated.h"
#include "logdevice/admin/maintenance/ShardDataHealth.h"
#include "logdevice/common/Timestamp.h"
#include "logdevice/common/configuration/nodes/NodesConfiguration.h"
#include "logdevice/common/membership/types.h"

/**
 * @file ClusterOperationalState class defines the current state of nodes and
 * shards in the cluster. ClusterMaintenanceManager overlays the
 * ClusterMaintenanceState with shard data health and the cluster membership
 * to determine the maintenances that should be active and also keeps track of
 * any pending maintenances.
 */

namespace facebook { namespace logdevice {

class ClusterOperationalState {
 public:
  ClusterOperationalState(
      const configuration::nodes::NodesConfiguration& config,
      const ShardAuthoritativeStatusMap& map) {
    initialize(config, map);
  }

  ~ClusterOperationalState();

  using ShardOperationalState =
      cluster_maintenance_state::fbuffers::ShardOperationalState;
  using ShardMaintenance = ClusterMaintenanceState::ShardMaintenance;
  using SequencerMaintenance = ClusterMaintenanceState::SequencerMaintenance;
  using PendingSequencerMaintenanceSet =
      ClusterMaintenanceState::PendingSequencerMaintenanceSet;
  using PendingShardMaintenanceSet =
      ClusterMaintenanceState::PendingShardMaintenanceSet;

  struct SequencerState {
    // Current Sequencing state. Initialized based config
    // when ClusterOperationalState is created and updated
    // based on active maintenances
    SequencingState state{SequencingState::UNKNOWN};
    SystemTimestamp sequencer_state_last_updated;
    // Pending maintenances updated based on the last
    // update from the ClusterMaintenanceStateMachine
    PendingSequencerMaintenanceSet pending_maintenances;
  };

  struct ShardState {
    // Current shard data health. Updated based on
    // the AuthoritativeStatus for the shard
    ShardDataHealth data_health{ShardDataHealth::UNKNOWN};
    // Current Operational state. Updated based on
    // active maintenances
    ShardOperationalState operational_state{ShardOperationalState::UNKNOWN};
    SystemTimestamp operational_state_last_updated;
    // Pending maintenances updated based on the last
    // update from CLusterMaintenanceStateMachine
    PendingShardMaintenanceSet pending_maintenances;
  };

  struct NodeState {
    SequencerState sequencer_state;
    std::map<shard_index_t, ShardState> shards_state;
  };

  /**
   * Set the storage_state to state for a given shard
   *
   * @param shard   ShardID for which the state has to be set
   * @param state   value of storage_state in ShardState to set to
   *
   * @return  -1 Node/Shard does not exist
   *           0 ShardOperationalState is updated successfully
   */
  int setShardOperationalState(ShardID shard, ShardOperationalState state);
  /**
   * Set the SequencingState for a given node
   *
   * @param node  Index of the node for which SequencingState has to be set
   * @param state value of SequencingState to set to
   *
   * @return  -1 Node does not exist in
   *           0 SequencingState is updated Successfully
   */
  int setSequencingState(node_index_t node, SequencingState state);

  /**
   * Update ShardDataHealth based on the Authoritative status
   *
   * @param map ShardAuthoritativeStatusMap used to update data_health
   */
  void applyShardStatus(const ShardAuthoritativeStatusMap& map);
  ShardOperationalState getShardOperationalState(ShardID shard) const;
  ShardDataHealth getShardDataHealth(ShardID shard) const;
  SequencingState getSequencingState(node_index_t node) const;
  bool isSequencingEnabledInMembership(node_index_t node) const;

 private:
  void initialize(const configuration::nodes::NodesConfiguration& config,
                  const ShardAuthoritativeStatusMap& map);
  // The version of the last membership update this state received
  membership::MembershipVersion::Type membership_version_;
  // Version of the last ShardAuthoritativeStatus map this state is aware of
  lsn_t shard_authoritative_status_map_version_{LSN_INVALID};
  // version of the ClusterMaintenanceState that was last evaluated
  lsn_t cluster_maintenance_state_version_{LSN_INVALID};
  std::unordered_map<node_index_t, NodeState> cluster_state_;
  // Translates the AuthoritativeStatus to corresponding ShardDataHealth
  // value
  ShardDataHealth
  translateShardAuthoritativeStatus(AuthoritativeStatus status,
                                    bool is_time_range_rebuilding);

}; // end ClusterOperationalState

}} // namespace facebook::logdevice
