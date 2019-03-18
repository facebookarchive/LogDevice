/**
 * Copyright (c) 2018-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

include "common.thrift"
include "nodes.thrift"
include "safety.thrift"

namespace cpp2 facebook.logdevice.thrift
namespace py3 logdevice.admin
namespace php LogDevice
namespace wiki LogDevice.Maintenance

struct ShardMaintenance {
  /*
   * if ShardID's shard_index == -1 this maintenance targets the entire node.
   * Accepted values are [MAY_DISAPPEAR, DRAINED]
   */
  1: common.ShardID shard,
  2: nodes.ShardOperationalState target_state,
}

struct SequencerMaintenance {
  1: common.NodeID node,
  /*
   * Accepted value is DISABLED only.
   */
  2: nodes.SequencerState target_state,
}

struct ApplyMaintenanceRequest {
  1: list<ShardMaintenance> shards_maintenance,
  2: list<SequencerMaintenance> sequencers_maintenance,
  3: string user,
  4: string reason,
  /*
   * Dangerous and should only be used for emergency situations.
   */
  5: optional bool skip_safety_checks = false,
}

struct ApplyMaintenanceResponse {
  1: list<ShardMaintenance> shards_maintenance,
  2: list<SequencerMaintenance> sequencers_maintenance,
}

/*
 * Defines which maintenances we are matching, typically used for removing
 * applied maintenances.
 */
struct RemoveMaintenanceRequest {
  /*
   * This has to be supplied. If the user is not set, you will get an
   * InvalidRequest exception.
   */
  1: string user,
  /*
   * The list of shards that we want to remove maintenances from. If empty, this
   * filter will match all shards. If you want to remove a maintenance on
   * sequencer node or all shards of a node, pass -1 to shard_index.
   */
  2: list<common.ShardID> shards,
  /*
   * The list of nodes we want to remove sequencer maintenance applied by this
   * user.
   */
  3: list<common.NodeID> sequencers,
  /*
   * Optional: The reason of removing the maintenance, this is used for
   * maintenance auditing and logging.
   */
  4: optional string reason,
}

/*
 * Left empty for future use.
 */
struct RemoveMaintenanceResponse {}

/*
 * High-level maintenance status progress
 */
enum MaintenanceTransitionStatus {
  /*
   * The MaintenanceManager has not started this maintenance transition yet.
   */
  NOT_STARTED = 0,
  STARTED = 1,
  /*
   * MaintenanceManager is waiting for a response from the
   * NodesConfigurationManager after requesting to apply changes
   */
  AWAITING_STORAGE_STATE_CHANGES = 2,
  /*
   * MaintenanceManager is performing a safety verification to ensure that the
   * operation is safe.
   */
  AWAITING_SAFETY_CHECK_RESULTS = 3,
  /*
   * The internal safety checker deemed this maintenance operation unsafe. The
   * maintenance will remain to be blocked until the next retry of safety check
   * succeeds.
   */
  BLOCKED_UNTIL_SAFE = 4,
  /*
   * MaintenanceManager is waiting for data migration/rebuilding to complete.
   * operation is safe.
   */
  AWAITING_DATA_REBUILDING = 5,
  /*
   * Data migration is blocked because it would lead to data loss if unblocked.
   * If this is required, use the unblockRebuilding to skip lost records and
   * and unblock readers waiting for the permanently lost records to be
   * recovered.
   */
  REBUILDING_IS_BLOCKED = 6,
  /*
   * Maintenance is expecting the node to join the cluster so we can finialize
   * this maintenance transition. MaintenanceManager will keep waiting until the
   * node becomes alive.
   */
  AWAITING_NODE_TO_JOIN = 7,
}

struct MaintenanceProgress {
  1: MaintenanceTransitionStatus status,
  2: optional common.Timestamp last_updated_at,
  /*
   * If the operation is blocked, the maintenance manager will attempt to retry
   * the blocking operation approximately at this timestamp.
   */
  3: optional common.Timestamp next_retry_at,
  /*
   * This will be set only if the operation is deemed unsafe
   * (MaintenanceTransitionStatus::BLOCKED_UNTIL_SAFE), this returns the
   * result of the _last_ run of safety checker.
   */
  4: optional safety.CheckImpactResponse safety_check_response,
}

/*
 * A data structure that encapsulates a maintenance on a specific shard. The
 * maintenance be either active (or pending). In case it's active and the
 * maintenance manager is actually performing an operation the progress object
 * will be set.
 */
struct ShardMaintenanceState {
  1: bool is_active,
  2: nodes.ShardOperationalState target_state,
  /*
   * This is set if the maintenance is driven by an internal operation. This
   * can be because RebuildingSupervisor has detected loss of nodes or we are
   * performing a transition because a maintenance has been removed.
   * In this case the user will be a special string
   * "LogDeviceInternal"
   */
  3: bool internal,
  4: string user,
  5: string reason,
  6: common.Timestamp created_at,
  /*
   * Only set if is_active == true.
   */
  7: optional MaintenanceProgress progress,
}

struct SequencerMaintenanceState {
  1: bool is_active,
  2: nodes.SequencingState target_state,
  3: string user,
  4: string reason,
  5: common.Timestamp created_at,
  6: optional MaintenanceProgress progress,
}


/*
 * A data structure that holds a list of all maintenances for a specific node,
 * whether these are sequencing maintenances and/or shard maintenances.
 */
struct NodeMaintenanceState {
  1: common.NodeID node,
  /*
   * The list of maintenances applied to every shard in this node, the active
   * maintenance will have is_active set to true.
   */
  2: map<common.ShardIndex, list<ShardMaintenanceState>> shards_maintenance,
  3: list<SequencerMaintenanceState> sequencers_maintenance,
}

struct GetMaintenancesResult {
  1: list<NodeMaintenanceState> nodes,
}

/*
 * See unblockRebuilding() for documentation.
 */
struct UnblockRebuildingRequest {
  /*
   * This information will be used for auditing purposes
   */
  1: string user,
  2: string reason,
}

// TODO: TBD
struct UnblockRebuildingResponse { }
