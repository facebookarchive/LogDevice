/**
 * Copyright (c) 2018-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

include "common.thrift"

namespace cpp2 facebook.logdevice.thrift
namespace py3 logdevice.admin
namespace php LogDevice


struct SequencerConfig {
  // A relative number that describes how much this sequencer node should run
  // sequencer objects compared to other sequencer nodes on the cluster.
  // This cannot be non-positive if the sequencer field is set in NodeConfig.
  1: required double weight = 1,
}

struct StorageConfig {
  // This is a positive value indicating how much storage traffic it will get
  // relative to other nodes in the cluster. This cannot be zero.
  1: required double weight = 1,
  // How many shards does this storage node has. Each shard will have to map to
  // a directory called.
  2: required i32 num_shards = 1,
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
  // If this set is empty, the node is neither a storage node nor a sequencer.
  1: required common.NodeIndex node_index,
  2: required common.SocketAddress data_address,
  3: required set<common.Role> roles;
  4: optional common.Addresses other_addresses;
  // A string representing the physical location of the node, this has to use
  // the notation of "dc.row.cluster.rack", a dot-separated location string that
  // is used to describe the failure domain hierarchy of the cluster. length of
  // this is arbitrary.
  5: optional string location;
  // This is only set if `roles` contain SEQUENCER
  6: optional SequencerConfig sequencer;
  // This is only set if `roles` contain STORAGE
  7: optional StorageConfig storage;
}

/**
 * ShardDataHealth defines the state of the data in a specific shard.
 */
enum ShardDataHealth {
  UNKNOWN = 0,
  // Shard has data and there are no problems.
  HEALTHY = 1,
  //  We cannot access the data on that shard, the cluster expects that the
  //  shard will come back with its data intact. Rebuilding and readers will
  // block waiting for this shard to come back if the last copy of one or more
  // records only exist on this shard.
  UNAVAILABLE = 2,
  // This shard has lost some of its records permanently in one or more time
  // ranges. In this case if rebuilding is trying to re-replicate a record that
  // had its last available copy on this shard, rebuilding will skip this record
  // and readers will not be blocked waiting for this record, readers will
  // instead get DATALOSS gaps.
  LOST_REGIONS = 3,
  // The cluster considers all of the data on this shard to be wiped/lost.
  // Readers and rebuilding will behave like LOST_PARTIAL except that LOST_ALL
  // means that we have lost all data on that shard.
  LOST_ALL = 4,
  // The cluster doesn't expect that this shard has any data. It's
  // safe to wipe the data on that shard. Neither rebuilding nor readers expect
  // this shard to contain any data.
  EMPTY = 5,
}

/**
 * ShardOperationalState defines the operational state of a shard. There is a
 * clear priority of these maintenance states, DRAINED > MAY_DISAPPEAR. If both
 * target maintenances are set on a shard, DRAINED maintenance will always win.
 */
enum ShardOperationalState {
  UNKNOWN = 0,
  // Shard is enabled in read/write mode.
  ENABLED = 1,
  // This means that the shard can be taken down without affecting availability,
  // The cluster will take into account that this shard can disappear at any
  // time and will not allow too many nodes to be in this state at the same
  // time.
  // When the shard disappears, the cluster will wait until the rebuilding
  // supervisor gives up (usually 20 minutes). Then this will move into DRAINING
  // and rebuilding will start.
  // Setting the shard to this state means that it's okay to perform a
  // maintanance operation on this shard safely. If the shard cannot transit to
  // this state (active_maintenance has state='blocked-unsafe') this means that
  // it's unsafe to do so. The current state will remain as is in this case.
  // Note that this case is technically identical to ENABLED, the shard is
  // read-write enabled.
  //
  MAY_DISAPPEAR = 2,
  // The shard is broken (has I/O errors) and has been marked as (needs rebuilding)
  // by the RebuildingSupervisor. In this case, the shard is temporarily disabled
  // until it comes back with either its data intact (at which rebuilding will be
  // cancelled if it's not complete). Or wiped which in that case we will switch it
  // back to ENABLED or whatever the next logical maintenance in the pending
  // maintenance list is.
  DOWN = 3,
  // The shard has been fully drained. It does not contain any data
  // (ShardDataHealth == EMPTY). It's safe to remove this shard from the
  // cluster. Drained also means that this node is not in the metadata nodeset
  // anymore.
  DRAINED = 4,
  // ** Transitional States
  // Draining is a transitional state at which the shard is in the process of
  // becoming DRAINED. This means that data relocation or rebuilding should be
  // in-progress whenever possible. The shard will move into DRAINED when
  // ShardDataHealth is EMPTY because of rebuilding/relocation or because all
  // data has been trimmed due to the retention period.
  //
  // During DRAINING, you can track the progress through the active_maintenance
  // object. If (active_maintenance.state='blocked'/'blocked-unsafe') then
  // this shard is still effectively ENABLED and will remain in this state
  // unless the reason of the blockage is gone.
  DRAINING = 51,
  // The is transitioning from _any_ state into ENABLED. This might be swift
  // enough that you don't ever see this state but it's here for completeness.
  ENABLING = 52,
  // Provisioning is set when this is a NEW shard that has just been added. We
  // know that there is no data on this shard and the node will skip rebuilding.
  // In this state the node is trying to converge into ENABLED. On order for
  // this to happen, the now need to acknowledge starting up and writing the
  // internal markers before we can go ahead and move into enabled.
  PROVISIONING = 53,
  // INVALID means that this is not a storage node. (We should never see this)
  INVALID = 99,
}

// ShardStorageState represents the active storage state of a shard.
enum ShardStorageState {
  DISABLED = 0,
  READ_ONLY = 1,
  READ_WRITE = 2,
}

/**
 * ShardState is per-shard state representation object. This reflect the active
 * operational state of a specific shard in a storage node.
 */
struct ShardState {
  // See the ShardDataHealth enum for info.
  1: required ShardDataHealth data_health,
  // Reflects whether storage on this node is currently DISABLED, READ_ONLY, or
  // READ_WRITE
  2: required ShardStorageState current_storage_state,
  // See the ShardOperationalState enum for info. See the
  // active_maintenance for information about the active transition
  3: required ShardOperationalState current_operational_state,
  // 4: optional ShardMaintenanceState active_maintenance,
  // 5: optional list<ShardMaintenanceState> pending_maintenances,
}

/**
 * A enum that defines the possible states of a sequencer node.
 */
enum SequencingState {
  ENABLED = 1,
  // This node is temporarily boycotted from running sequencers due to its
  // current poor performance.
  BOYCOTTED = 2,
  // Sequencing is disabled.
  DISABLED = 3,
}

/**
 * SequencerState is the representation object for the state of a sequencer
 * node.
 */
struct SequencerState {
  1: required SequencingState state,
  2: optional common.Timestamp sequencer_state_last_updated,
  // TODO: Add Maintenance
}

/**
 * ServiceState represents whether the daemon is ALIVE or not from the point of
 * view of the node you are requesting this data from. This uses the gossip
 * information propagated through the cluster.
 */
enum ServiceState {
  // We don't know the service state.
  UNKNOWN = 0,
  // The node is ALIVE according to the gossip table of the node
  // responding with this data structure.
  ALIVE = 1,
  // The node is STARTUP according to the gossip table of the node
  // responding with this data structure.
  STARTING_UP = 2,
  // Node is _probably_ shutting down.
  SHUTTING_DOWN = 3,
  // This node is isolated from the rest of the cluster. This state means that
  // the node that reported the response is also isolated.
  ISOLATED = 4,
  // Node is down
  DEAD = 5,
}

struct NodeState {
  // The index of this node
  1: required common.NodeIndex node_index,
  // The gossip status of node.
  2: optional ServiceState daemon_state,
  // This is only set if the node is a sequencer (roles has SEQUENCER)
  3: optional SequencerState sequencer_state,
  // This is only set if the node is a storage node (roles has STORAGE)
  // An ordered list of shards to their state.
  4: optional list<ShardState> shard_states,
}

typedef list<NodeConfig> NodesConfig
typedef list<NodeState> NodesState

struct NodesFilter {
  1: optional common.Node address,
  2: optional common.Role role,
  3: optional common.NodeIndex node_index,
  // This is a prefix-based filter. Can be used to return all nodes in a rack,
  // row, cluster, and etc.
  4: optional string location,
}

struct NodesConfigResponse {
  // This is an empty list if we cannot find any nodes
  1: required NodesConfig nodes,
  2: required common.unsigned64 version,
}

struct NodesStateResponse {
  // This is an empty list if we cannot find any nodes
  1: required NodesState states,
  2: required common.unsigned64 version,
}

struct NodesStateRequest {
  1: optional NodesFilter filter,
  // If force=true we return the state information that we have even if the node
  // is not fully ready. We don't throw NodeNotReady exception in this case.
  2: optional bool force,
}
