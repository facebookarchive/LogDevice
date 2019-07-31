/**
 * Copyright (c) 2018-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

include "logdevice/admin/if/common.thrift"

include "logdevice/common/membership/Membership.thrift"

namespace cpp2 facebook.logdevice.thrift
namespace py3 logdevice.admin
namespace php LogDevice
namespace wiki Thriftdoc.LogDevice.Nodes


struct SequencerConfig {
  /**
   * A relative number that describes how much this sequencer node should run
   * sequencer objects compared to other sequencer nodes on the cluster.
   * This cannot be non-positive if the sequencer field is set in NodeConfig.
   */
  1: double weight = 1,
}

struct StorageConfig {
  /**
   * This is a positive value indicating how much storage traffic it will get
   * relative to other nodes in the cluster. This cannot be zero.
   */
  1: double weight = 1,
  /**
   * How many shards does this storage node has. Each shard will have to map to
   * a directory called.
   */
  2: i32 num_shards = 1,
}

/**
 * The object the defines the properties of a node in the cluster. These
 * attributes are not meant to be changed frequently. They are defining
 * properties of what the role of the node is and its physical properties.
 *
 * For its operational state (e.g. disabled or enabled) Check the `NodeState`
 * instead.
 */
struct NodeConfig {
  /**
   * If this set is empty, the node is neither a storage node nor a sequencer.
   */
  1: common.NodeIndex node_index,
  2: common.SocketAddress data_address,
  3: set<common.Role> roles;
  4: optional common.Addresses other_addresses;
  /**
   * A string representing the physical location of the node, this has to use
   * the notation of "dc.row.cluster.rack", a dot-separated location string that
   * is used to describe the failure domain hierarchy of the cluster. length of
   * this is arbitrary.
   */
  5: optional string location;
  /**
   * This is only set if `roles` contain SEQUENCER
   */
  6: optional SequencerConfig sequencer;
  /**
   * This is only set if `roles` contain STORAGE
   */
  7: optional StorageConfig storage;
  /**
   * A convenience structure for location
   */
  8: common.Location location_per_scope;
  /*
   * A unique name for the node in the cluster.
   */
  9: string name;
}

/**
 * High-level maintenance status progress
 */
enum MaintenanceStatus {
  /**
   * The MaintenanceManager has not started this maintenance transition yet.
   */
  NOT_STARTED = 0,
  STARTED = 1,
  /**
   * MaintenanceManager is waiting for a response from the
   * NodesConfigurationManager after requesting to apply changes
   */
  AWAITING_NODES_CONFIG_CHANGES = 2,
  /**
   * MaintenanceManager is performing a safety verification to ensure that the
   * operation is safe.
   */
  AWAITING_SAFETY_CHECK = 3,
  /**
   * The internal safety checker deemed this maintenance operation unsafe. The
   * maintenance will remain to be blocked until the next retry of safety check
   * succeeds.
   */
  BLOCKED_UNTIL_SAFE = 4,
  /**
   * MaintenanceManager is waiting for data migration/rebuilding to complete.
   * operation is safe.
   */
  AWAITING_DATA_REBUILDING = 5,
  /**
   * Data migration is blocked because it would lead to data loss if unblocked.
   * If this is required, use the unblockRebuilding to skip lost records and
   * and unblock readers waiting for the permanently lost records to be
   * recovered.
   */
  REBUILDING_IS_BLOCKED = 6,
  COMPLETED = 7,
  /**
   * Maintenance cannot proceed because of an internal error error.
   * (ex: Event couldn't be written to event log). That will be retried
   * automatically by the Maintenance Manager.
   */
  RETRY = 8,
  /**
   * MaintenanceManager is waiting for the NodesConfiguration to finish a
   * transition.
   */
  AWAITING_NODES_CONFIG_TRANSITION = 9,

  /**
   * MaintenanceManager observed a new node that needs to be enabled, but the
   * Node is still in the PROVISIONING state, so the MaintenanceManager is
   * waiting for the node to mark itself as PROVISIONED.
   */
  AWAITING_NODE_PROVISIONING = 10,
}

/**
 * A data structure that holds progress information about maintenances applied
 * to a given shard in a node.
 */
struct ShardMaintenanceProgress {
  1: MaintenanceStatus status,
  2: set<ShardOperationalState> target_states,
  3: common.Timestamp created_at,
  4: common.Timestamp last_updated_at,
  /**
   * The list of maintenance groups associated to this maintenance
   */
  5: list<common.MaintenanceGroupID> associated_group_ids,
}

/**
 * A data structure that holds progress information about maintenances applied
 * to a sequencer node.
 */
struct SequencerMaintenanceProgress {
  1: MaintenanceStatus status,
  2: SequencingState target_state,
  3: common.Timestamp created_at,
  4: common.Timestamp last_updated_at,
  /**
   * The list of maintenance groups associated to this maintenance
   */
  5: list<common.MaintenanceGroupID> associated_group_ids,
}

/**
 * ShardDataHealth defines the state of the data in a specific shard.
 */
enum ShardDataHealth {
  UNKNOWN = 0,
  /**
   * Shard has data and there are no problems.
   */
  HEALTHY = 1,
  /**
   * We cannot access the data on that shard, the cluster expects that the
   * shard will come back with its data intact. Rebuilding and readers will
   * block waiting for this shard to come back if the last copy of one or more
   * records only exist on this shard.
   */
  UNAVAILABLE = 2,
  /**
   * This shard has lost some of its records permanently in one or more time
   * ranges. In this case if rebuilding is trying to re-replicate a record that
   * had its last available copy on this shard, rebuilding will skip this record
   * and readers will not be blocked waiting for this record, readers will
   * instead get DATALOSS gaps.
   */
  LOST_REGIONS = 3,
  /**
   * The cluster considers all of the data on this shard to be wiped/lost.
   * Readers and rebuilding will behave like LOST_PARTIAL except that LOST_ALL
   * means that we have lost all data on that shard.
   */
  LOST_ALL = 4,
  /**
   * The cluster doesn't expect that this shard has any data. It's
   * safe to wipe the data on that shard. Neither rebuilding nor readers expect
   * this shard to contain any data.
   */
  EMPTY = 5,
}

/**
 * ShardOperationalState defines the operational state of a shard.
 */
enum ShardOperationalState {
  UNKNOWN = 0,
  /**
   * Shard is enabled in read/write mode.
   */
  ENABLED = 1,
  /**
   * This means that the shard can be taken down without affecting availability,
   * The cluster will take into account that this shard can disappear at any
   * time and will not allow too many nodes to be in this state at the same
   * time.
   * When the shard disappears, the cluster will wait until the rebuilding
   * supervisor gives up (usually 20 minutes). Then this will move into
   * MIGRATING_DATA and rebuilding will start.
   * Setting the shard to this state means that it's okay to perform a
   * maintanance operation on this shard safely. If the shard cannot transit to
   * this state (maintenance has state='blocked-unsafe') this means that
   * it's unsafe to do so. The current state will remain as is in this case.
   * The storage state will be set to (read-only).
   */
  MAY_DISAPPEAR = 2,
  /**
   * The shard has been fully drained. It does not contain any data.
   * It's safe to remove this shard from the cluster. Drained also means that
   * this shard is not in the metadata storage-set anymore.  This can also
   * happen after a shard was broken and the RebuildingSupervisor decided to
   * perform maintenance on it.
   */
  DRAINED = 3,
  /**
   * Transitional States
   * A transitional state at which the shard is in the process of
   * becoming DRAINED. This means that data relocation or rebuilding should be
   * in-progress whenever possible. The shard will move into DRAINED when
   * ShardDataHealth is EMPTY because of rebuilding/relocation or because all
   * data has been trimmed due to the retention period.
   *
   * While MIGRATING_DATA, you can track the progress through the
   * maintenance object. If (maintenance.state='blocked'/'blocked-unsafe') then
   * this shard is still effectively ENABLED and will remain in this state
   * unless the reason of the blockage is gone.
   */
  MIGRATING_DATA = 51,
  /**
   * The is transitioning from _any_ state into ENABLED. This might be swift
   * enough that you don't ever see this state but it's here for completeness.
   */
  ENABLING = 52,
  /**
   * Provisioning is set when this is a NEW shard that has just been added. We
   * know that there is no data on this shard and the node will skip rebuilding.
   * In this state the node is trying to converge into ENABLED. On order for
   * this to happen, the now need to acknowledge starting up and writing the
   * internal markers before we can go ahead and move into enabled.
   */
  PROVISIONING = 53,
  /**
   * This means that the shard will not be picked in new nodesets but it's still
   * available for old data rebuilding. The shard can transition to
   * MIGRATING_DATA and then DRAINED once all the data expires (passes
   * retention).
   */
  PASSIVE_DRAINING = 54,
  /**
   * INVALID means that this is not a storage node. (We should never see this)
   */
  INVALID = 99,
}

/**
 * [DEPRECATED] IN FAVOR OF Membership.StorageState
 */
enum ShardStorageState {
  DISABLED = 0,
  READ_ONLY = 1,
  READ_WRITE = 2,
  DATA_MIGRATION = 3,
}

/**
 * ShardState is per-shard state representation object. This reflect the active
 * operational state of a specific shard in a storage node.
 */
struct ShardState {
  /**
   * See the ShardDataHealth enum for info.
   */
  1: ShardDataHealth data_health,
  /**
   * [DEPRECATED]. Will be removed once callers move to storage_state instead.
   */
  2: ShardStorageState current_storage_state (deprecated),
  /**
   * See the ShardOperationalState enum for info. See the
   * maintenance for information about the active transition
   */
  3: ShardOperationalState current_operational_state,
  /*
   * If there are maintenance applied on this shard.
   */
  4: optional ShardMaintenanceProgress maintenance,
  /*
   * The current membership storage state.
   */
  5: Membership.StorageState storage_state,
  /*
   * The current membership metadata storage state. If set to
   * MetaDataStorageState.METADATA Then this shard is in the metadata nodeset.
   */
  6: Membership.MetaDataStorageState metadata_state,
}

/**
 * A enum that defines the possible states of a sequencer node.
 */
enum SequencingState {
  ENABLED = 1,
  /**
   * This node is temporarily boycotted from running sequencers due to its
   * current poor performance.
   */
  BOYCOTTED = 2,
  /**
   * Sequencing is disabled.
   */
  DISABLED = 3,
  /**
   * State could not be determined. This is usually a temporary state
   * and querying the state again should result in a valid state to
   * be returned
   */
  UNKNOWN = 4,
}

/**
 * SequencerState is the representation object for the state of a sequencer
 * node.
 */
struct SequencerState {
  1: SequencingState state,
  /**
   * If there are maintenance applied on this sequencer.
   */
  3: optional SequencerMaintenanceProgress maintenance,
}

/**
 * ServiceState represents whether the daemon is ALIVE or not from the point of
 * view of the node you are requesting this data from. This uses the gossip
 * information propagated through the cluster.
 */
enum ServiceState {
  /**
   * We don't know the service state.
   */
  UNKNOWN = 0,
  /**
   * The node is ALIVE according to the gossip table of the node
   * responding with this data structure.
   */
  ALIVE = 1,
  /**
   * The node is STARTUP according to the gossip table of the node
   * responding with this data structure.
   */
  STARTING_UP = 2,
  /**
   * Node is _probably_ shutting down.
   */
  SHUTTING_DOWN = 3,
  /**
   * This node is isolated from the rest of the cluster. This state means that
   * the node that reported the response is also isolated.
   */
  // ISOLATED = 4, // NOT SUPPORTED
  /**
   * Node is down
   */
  DEAD = 5,
}

struct NodeState {
  /**
   * The index of this node
   * [DEPECATED] use the config object instead.
   */
  1: common.NodeIndex node_index,
  /**
   * The gossip status of node.
   */
  2: ServiceState daemon_state = ServiceState.UNKNOWN,
  /**
   * This is only set if the node is a sequencer (roles has `SEQUENCER`)
   */
  3: optional SequencerState sequencer_state,
  /**
   * This is only set if the node is a storage node (roles has `STORAGE`)
   * An ordered list of shards to their state.
   */
  4: optional list<ShardState> shard_states,
  /**
   * Configuration object for this node.
   */
  5: NodeConfig config,
}

typedef list<NodeConfig> NodesConfig
typedef list<NodeState> NodesState

struct NodesFilter {
  1: optional common.NodeID node,
  2: optional common.Role role,
  /**
   * This is a prefix-based filter. Can be used to return all nodes in a rack,
   * row, cluster, and etc.
   */
  3: optional string location,
}

struct NodesConfigResponse {
  /**
   * This is an empty list if we cannot find any nodes
   */
  1: NodesConfig nodes,
  2: common.unsigned64 version,
}

struct NodesStateResponse {
  /**
   * This is an empty list if we cannot find any nodes
   */
  1: NodesState states,
  2: common.unsigned64 version,
}

struct NodesStateRequest {
  1: optional NodesFilter filter,
  /**
   * If force=true we return the state information that we have even if the node
   * is not fully ready. We don't throw NodeNotReady exception in this case.
   */
  2: optional bool force,
}
