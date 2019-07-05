/**
 * Copyright (c) 2018-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

include "logdevice/admin/if/common.thrift"
include "logdevice/admin/if/nodes.thrift"
include "logdevice/admin/if/safety.thrift"

namespace cpp2 facebook.logdevice.thrift
namespace py3 logdevice.admin
namespace php LogDevice
namespace wiki Thriftdoc.LogDevice.Maintenance

/**
 * What is the progress for a particular maintenance definition.
 */
enum MaintenanceProgress {
  /**
   * The MaintenanceManager has not started this maintenance transition yet.
   */
  UNKNOWN = 0,
  /**
   * A maintenance is considered BLOCKED_UNTIL_SAFE if _any_ of the shards or
   * sequencers didn't reach the target and has failed the safety check to do
   * so.
   */
  BLOCKED_UNTIL_SAFE = 1,
  IN_PROGRESS = 2,
  /**
   * A maintenance is considered complete if all shards and sequencers in the
   * maintenance have both reached (at least) the target, and the maintenance
   * has been loaded by the maintenance manager.
   */
  COMPLETED = 3,
}

struct MaintenanceDefinition {
  /**
   * if ShardID's shard_index == -1 this maintenance targets the entire node.
   * Accepted values are [MAY_DISAPPEAR, DRAINED]
   */
  1: common.ShardSet shards,
  /**
   * Must be set to either MAY_DISAPPEAR or DRAINED iff shards is non-empty.
   */
  2: nodes.ShardOperationalState shard_target_state,
  3: list<common.NodeID> sequencer_nodes,
  /**
   * Must be set to DISABLED iff sequencer_nodes is non-empty.
   */
  4: nodes.SequencingState sequencer_target_state,
  /**
   * The user associated with this maintenance request. The same user cannot
   * request multiple maintenances with different targets for the same shards.
   * This will trigger MaintenanceClash exception (see exceptions.thrift)
   *
   * `user` cannot contain whitespaces. Otherwise, an InvalidRequest exception
   * will be thrown.
   */
  5: string user,
  /**
   * A string representation of why this maintenance was requested. This is
   * purely for logging and tooling.
   */
  6: string reason,
  /**
   * Extra attributes to be associated with this maintenance. This is up to the
   * user. Can be used to store metadata about external tooling triggering the
   * maintenance.
   */
  7: map<string, string> extras,
  /**
   * Dangerous and should only be used for emergency situations.
   */
  8: bool skip_safety_checks = false,
  /**
   * Should NEVER be set by the user. Admin API will reject requests setting
   * this to true. This can only be set by internal maintenances requested by
   * the RebuildingSupervisor.
   *
   * If this is set to true, this means that we don't expect the shards to be
   * donors in the rebuilding process. aka. shards are inaccessible!
   */
  9: bool force_restore_rebuilding = false,
  /**
   * Attempt to group the maintenances in this request. This will give a hint to
   * the internal maintenance scheduler that the user wishes for these
   * maintenance targets to happen as a single unit.
   *
   * From the API perspective, the user should not treat the partial results are
   * reliable as the system might need to revert some of them. The target state
   * for this group is only guaranteed if all the shards in the group have
   * reached their targets. However, the system will not revert states
   * unnecessarily.
   */
  10: bool group = true,
  /**
   * This is the time in seconds that we want to keep this maintenance applied.
   * The countdown starts as you apply the maintenance, you need to take into
   * account how long does it take the system reach the target state. The ttl
   * will be refreshed every time the user tries to re-apply the same
   * maintenance.
   *
   * Example:
   * The user `foo` requests a DRAINED maintenance for Node(1). The ttl is set
   * to 1 hour (ttl_seconds=3600). Onces the server responds to your request.
   * The Maintenance Manager starts a count down that the maintenance will
   * expire in (now + 3600 seconds).
   * If the user wishes to extend the TTL, they should call the same
   * applyMaintenance() call with same arguments (user, shards, shard_target_state,
   * sequencer_nodes, sequencer_target_state, are what matter). Or simply
   * filling the (user, group-id) field instead. If the request didn't match all
   * the shards in the group, we will fail the request with MaintenanceMatchError
   * exception.
   *
   * The Maintenance Manager will add another ttl_seconds to the current time.
   *
   * The requested refresh might include a different TTL. When the TTL expire,
   * the maintenance is "removed".
   * TTL = 0 means that this will never expire, it has to be removed manually.
   *
   * Note that refreshing the ttl of a shard in the group means updating the ttl
   * for the entire group.
   *
   * TTL can NOT be negative values, InvalidRequest exception will be thrown in
   * this case.
   *
   */
  11: i32 ttl_seconds = 0,
  /**
   * This instructs the system on whether it will automatically attempt to
   * fallback into a passive drain (if the requested target is DRAINED) if the
   * active drain is not possible. This can happen in cases where the historical
   * storage sets for some logs do not have enough shards to rebuild the data
   * to.
   * Passive drains means that we stop accepting writes on these shards and we
   * will continue waiting until the data is trimmed naturally by retention.
   * This will not be possible if some of these logs have infinite retention.
   */
  12: bool allow_passive_drains = false,
  /**
   * Only set by the server once the group is created. If the argument group is
   * set to False, The system will generate a group_id for each of the shards
   * and sequencers in the request. The maintenance is going to be treated as N
   * independent maintenances.
   */
  13: optional common.MaintenanceGroupID group_id,
  /**
   * If this particular maintenance is blocked on safety checker, the result
   * will be returned in this object to help the user understand why. The
   * information here is a cached version of the last time the maintenance
   * manager has performed a safety check and once safety check passes this will
   * be unset.
   */
  14: optional safety.CheckImpactResponse last_check_impact_result,
  /**
   * This field is populated by the server, it's the timestamp that this
   * maintenance will expire based on server time.
   */
  15: optional common.Timestamp expires_on,
  /**
   * Timestamp at which the maintenance was first requested (in milliseconds)
   */
  16: optional common.Timestamp created_on,
  /**
   * What is the current stage of progress for that maintenance, this is can be
   * used to know whether this particular maintenance has completed or not.
   */
  17: MaintenanceProgress progress,
}

/**
 * a list of maintenance definition objects given a specific version of the
 * internal state machine.
 */
struct ClusterMaintenanceState {
  /**
   * The list of maintenances requested by the user
   */
  1: list<MaintenanceDefinition> maintenances,
  /**
   * The version of the state.
   */
  2: common.unsigned64 version,
}

struct MaintenanceDefinitionResponse {
  /**
   * The maintenance that was either created or returned, it will be updated
   * with the group-id in this case.
   *
   * The reason this returns a list is that you may have created a single
   * request but `group=false`. This request is going to be exploded into
   * multiple maintenance definitions with their own group-ids.
   */
  1: list<MaintenanceDefinition> maintenances,
}

/**
 * A matching filter object on one or more maintenances
 */
struct MaintenancesFilter {
  /**
   * If empty, will get all maintenances unless another filter is specified
   */
  1: list<common.MaintenanceGroupID> group_ids,
  /**
   * If set, gets maintenances created by this user
   */
  2: optional string user,
}

/**
 * Defines which maintenances we are matching, Used for removing
 * applied maintenances. This request works as a search filter to decide which
 * maintenance are going to be removed. The different values set in this request
 * will further narrow the match.
 */
struct RemoveMaintenancesRequest {
  // Maintenances matching this filter will be removed. You cannot supply an
  // empty filter (throws InvalidRequest).
  1: MaintenancesFilter filter,
  /**
   * Optional: The user doing the removal operation, this is used for
   * maintenance auditing and logging.
   */
  2: string user,
  /**
   * Optional: The reason of removing the maintenance, this is used for
   * maintenance auditing and logging.
   */
  3: string reason,
}

/**
 * Left empty for future use.
 */
struct RemoveMaintenancesResponse {
  // The list of maintenances removed successfully.
  1: list<MaintenanceDefinition> maintenances,
}

/**
 * See unblockRebuilding() for documentation.
 */
struct UnblockRebuildingRequest {
  /**
   * This information will be used for auditing purposes
   */
  1: string user,
  2: string reason,
}

// TODO: TBD
struct UnblockRebuildingResponse { }
