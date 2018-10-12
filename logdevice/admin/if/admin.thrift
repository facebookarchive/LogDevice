/**
 * Copyright (c) 2018-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
include "common/fb303/if/fb303.thrift"

namespace cpp2 facebook.logdevice.thrift
namespace py3 logdevice
namespace php LogDevice

// Because thrift doesn't have a u64.
typedef i64 /* (cpp.type = "std::uint64_t") */ unsigned64

// *** Cluster Topology
/**
 * A Socket object is the identifier for a specific node on the cluster from the
 * Admin API point of view. This is basically a hostname and port pair. You can
 * replace the hostname with IP as long as you do this consistently.
 * You can also use the unix-socket path instead of the host/port pair if you
 * are running LogDevice in a no-network mode.
 */
enum SocketAddressFamily {
  // IPv4 or IPv6 address
  INET = 1,
  // Unix socket address
  UNIX = 2,
}

struct SocketAddress {
  1: required SocketAddressFamily address_family = SocketAddressFamily.INET,
  // This contains the unix_socket path if address_family is set to UNIX
  2: optional string address,
  // A port should be uint16_t but such a type does not exist in thrift
  // port will be unset or (-1) if this points to a unix socket or we are only
  // interested in the address value.
  3: optional i32 port = -1,
}

typedef SocketAddress Node
typedef i64 Timestamp
typedef i16 NodeIndex // node_index_t
typedef i16 ShardIndex // shard_index_t

// This data structure is used to represent one or (all) shards on a storage
// node. This is typically used in the low-level APIs of the Administrative API.
struct ShardID {
  // this can be -1 which means all shards in a node.
  1: required ShardIndex shard_index = -1,
  // You should use either node_index OR address to refer to the node. You
  // CANNOT have both unset. You will get InvalidRequest exception in this case.
  2: optional NodeIndex node_index, // if setthis has to be a positive value.
  3: optional Node address,
}

// An ordered list of shards that a record can be stored onto.
typedef list<ShardID> StorageSet
// unordered set of shard. This should be a set<> but set of non
// int/string/binary/enums are not portable in some languages.
typedef list<ShardID> ShardSet

/**
 * Role is what defines if this node is a storage node or a sequencer node,
 * this is used as a mask in `NodeConfig` so a node can have on or more roles.
 * It's important that you keep the values of this enum in distinct bits in i16
 * range
 */
enum Role {
  STORAGE = 1,
  SEQUENCER = 2,
}

struct Addresses {
  // The socket address at which we expect gossip to use.
  1: optional SocketAddress gossip;
  // The socket address for SSL (data) connections to the server.
  2: optional SocketAddress ssl;
  // The socket address for the admin API.
  3: optional SocketAddress admin;
}

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
  1: required NodeIndex node_index,
  2: required SocketAddress data_address,
  3: required set<Role> roles;
  4: optional Addresses other_addresses;
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
  2: optional Timestamp sequencer_state_last_updated,
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
  1: required NodeIndex node_index,
  // The gossip status of node.
  2: optional ServiceState daemon_state,
  // This is only set if the node is a sequencer (roles has SEQUENCER)
  3: optional SequencerState sequencer_state,
  // This is only set if the node is a storage node (roles has STORAGE)
  // An ordered list of shards to their state.
  4: optional list<ShardState> shard_states,
}


// *** LogTree Structures

// Response of getReplicationInfo()
struct LogTreeInfo {
  // The log tree version, version is u64 so we convert that to string because
  // thrift does not support u64.
  1: required string version,
  2: required i64 num_logs,
  3: required i64 max_backlog_seconds,
  4: required bool is_fully_loaded,
}

struct TolerableFailureDomain {
  1: required string domain,
  2: required i32 count,
}

// Response of getReplicationInfo()
struct ReplicationInfo {
  // The log tree version, version is u64 so we convert that to string because
  // thrift does not support u64.
  1: required string version,
  /**
   * What is the most restrictive replication policy in
   * The entire LogTree
   */
  2: required map<string, i32> narrowest_replication,
  /**
   * What is the smallest replication for a record in the
   * entire LogTree
   */
  3: required i32 smallest_replication_factor,
  /**
   * How many of failure domain (domain) we can lose
   * in theory without losing read/write availability.
   */
  4: required TolerableFailureDomain tolerable_failure_domains,
}

// Source where a setting comes from
enum SettingSource {
  CLI = 0,
  CONFIG = 1,
  ADMIN_OVERRIDE = 2,
}

// Settings structure, part of SettingsResponse
struct Setting {
  // The currently applied setting value
  1: string currentValue,
  // The default setting value
  2: string defaultValue,
  // The setting as set by each SettingSource
  3: required map<SettingSource, string> sources,
}

// The response to getSettings
struct SettingsResponse {
  1: map<string, Setting> settings,
}

// The request for getSettings
struct SettingsRequest {
  // Get all settings if left empty
  1: optional set<string> settings;
}

// Log group operations for throughput gathering
enum LogGroupOperation {
  APPENDS = 0,
  READS = 1,
}

// LogGroupThroughput structure
struct LogGroupThroughput {
  // appends or reads
  1: required LogGroupOperation operation,
  // B/s per time interval
  2: required list<i64> results,
}

// The request for getLogGroupThroughput
struct LogGroupThroughputRequest {
   // appends or reads (by default: appends)
   1: optional LogGroupOperation operation,
   // time period in seconds. Throughput is calculated for the given
   // time periods, for instance, 1 min (60 sec), 5 min (300 sec) and so on.
   // By default: 60 sec
   2: optional list<i32> time_period,
   // log group name filtering
   3: optional string log_group_name,
}

// The response to getLogGroupThroughput
struct LogGroupThroughputResponse {
  // per-log-group append/read in B/s
  1: map<string, LogGroupThroughput> throughput;
}

// *** AdminAPI Exceptions

// The node you are communicating with is not ready to respond yet.
exception NodeNotReady {
  1: string message,
}

// The server has an older version than expected
exception StaleVersion {
  1: string message,
  2: unsigned64 server_version,
}

// The operation is not supported
exception NotSupported {
  1: string message,
}

// An operation that failed for unexpected reasons
exception OperationError {
  1: string message,
  2: optional i32 error_code, // maps to E
}

// The request contains invalid parameters
exception InvalidRequest {
  1: string message,
}

typedef list<NodeConfig> NodesConfig
typedef list<NodeState> NodesState

struct NodesFilter {
  1: optional Node address,
  2: optional Role role,
  3: optional NodeIndex node_index,
  // This is a prefix-based filter. Can be used to return all nodes in a rack,
  // row, cluster, and etc.
  4: optional string location,
}

struct NodesConfigResponse {
  // This is an empty list if we cannot find any nodes
  1: required NodesConfig nodes,
  2: required unsigned64 version,
}

struct NodesStateResponse {
  // This is an empty list if we cannot find any nodes
  1: required NodesState states,
  2: required unsigned64 version,
}

struct NodesStateRequest {
  1: optional NodesFilter filter,
  // If force=true we return the state information that we have even if the node
  // is not fully ready. We don't throw NodeNotReady exception in this case.
  2: optional bool force,
}

/**
 * Defines the different location scopes that logdevice recognizes
 */
enum LocationScope {
  NODE = 1,
  RACK = 2,
  ROW = 3,
  CLUSTER = 4,
  DATA_CENTER = 5,
  REGION = 6,
}

// Replication property is how many copies per Location scope for the various
// scopes
typedef map<LocationScope, i32> ReplicationProperty // replication per scope

enum OperationImpact {
  // This means that rebuilding will not be able to complete given the current
  // status of ShardDataHealth. This assumes that rebuilding may need to run at
  // certain point. This can happen if we have epochs that have lost so many
  // _writable_ shards in its storage-set that it became impossible to amend
  // copysets according to the replication property.
  REBUILDING_STALL = 1,
  // This means that if we perform the operation, we will not be able to
  // generate a nodeset that satisfy the current replication policy for all logs
  // in the log tree.
  WRITE_AVAILABILITY_LOSS = 2,
  // This means that this operation _might_ lead to stalling readers in cases
  // were they need to establish f-majority on some records.
  READ_AVAILABILITY_LOSS = 3,
}

// A data structure that describe the operation impact on a specific epoch in a
// log.
struct ImpactOnEpoch {
  1: required unsigned64 log_id,
  2: required unsigned64 epoch,
  // What is the storage set for this epoch (aka. NodeSet)
  3: required StorageSet storage_set,
  // What is the replication policy for this particular epoch
  4: required ReplicationProperty replication,
  5: required list<OperationImpact> impact,
}

struct CheckImpactRequest {
  // Which shards/nodes we would like to check state change against. Using the
  // ShardID data structure you can refer to individual shards or entire storage
  // or sequencer nodes based on their address or index.
  1: required ShardSet shards,
  // This can be unset ONLY if disable_sequencers is set to true. In this case
  // we are only interested in checking for sequencing capacity constraints of
  // the cluster. Alternatively, you can set target_storage_state to READ_WRITE.
  2: optional ShardStorageState target_storage_state,
  // Do we want to validate if sequencers will be disabled on these nodes as
  // well?
  3: optional bool disable_sequencers,
  // The set of shards that you would like to update their state
  // How much of the location-scope do we want to keep as a safety margin. This
  // assumes that X number of LocationScope can be impacted along with the list
  // of shards/nodes that you supply in the request.
  4: optional ReplicationProperty safety_margin,
  // Choose which log-ids to check for safety. Remember that we will always
  // check the metadata and internal logs. This is to ensure that no matter
  // what, operations are safe to these critical logs.
  5: optional list<unsigned64> log_ids_to_check,
  // Defaulted to true. In case we found a reason why we think the operation is
  // unsafe, we will not check the rest of the logs.
  6: optional bool abort_on_negative_impact = true,
  // In case the operation is unsafe, how many example ImpactOnEpoch records you
  // want in return?
  7: optional i32 return_sample_size = 50,
}

struct CheckImpactResponse {
  // empty means that no impact, operation is SAFE.
  1: required list<OperationImpact> impact,
  // Only set if there is impact. This indicates whether there will be effect on
  // the metadata logs or the internal state machine logs.
  2: optional bool internal_logs_affected,
  // A sample of the affected epochs by this operation.
  3: optional list<ImpactOnEpoch> logs_affected,
}

// *** AdminAPI Service
service AdminAPI extends fb303.FacebookService {
  // Gets the config for all nodes that matches the supplied NodesFilter. If
  // NodesFilter is empty we will return all nodes. If the filter does not match
  // any nodes, an empty list of nodes is returned in the NodesConfigResponse
  // object.
  NodesConfigResponse getNodesConfig(1: NodesFilter filter) throws
      (1: NodeNotReady notready);

  // Gets the state object for all nodes that matches the supplied NodesFilter.
  // If NodesFilter is empty we will return all nodes. If the filter does not
  // match any nodes, an empty list of nodes is returned in the
  // NodesStateResponse object. `force` will force this method to return all the
  // available state even if the node is not fully ready. In this case we will
  // not throw NodeNotReady exception but we will return partial data.
  NodesStateResponse getNodesState(1: NodesStateRequest request) throws
      (1: NodeNotReady notready);

  // Safety check an operation.
  CheckImpactResponse checkImpact(1: CheckImpactRequest request) throws
      (1: NodeNotReady notready, 2: OperationError error);

  // *** LogTree specific APIs
  LogTreeInfo getLogTreeInfo();
  ReplicationInfo getReplicationInfo();

  // Get information about all or some of the settings
  SettingsResponse getSettings(1: SettingsRequest request);

  // Force the server to take new snapshot of the LogsTree state in memory. The
  // argument to this is `min_version` which means that the snapshot should only
  // be taken if the server is running with this LogTree version (or newer)
  // server will throw StaleVersion exception if that server has older version.
  // If the argument is not supplied or (0) then the server will take a snapshot
  // of whatever version it currently has.
  void takeLogTreeSnapshot(1: unsigned64 min_version) throws
      (1: StaleVersion stale, 2: NodeNotReady notready, 3: NotSupported
       notsupported);

  // Get Log Group Throughput
  LogGroupThroughputResponse getLogGroupThroughput(
                                1: LogGroupThroughputRequest request);
}
