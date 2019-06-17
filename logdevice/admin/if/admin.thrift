/**
 * Copyright (c) 2018-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
include "common/fb303/if/fb303.thrift"
include "logdevice/admin/if/common.thrift"
include "logdevice/admin/if/cluster_membership.thrift"
include "logdevice/admin/if/exceptions.thrift"
include "logdevice/admin/if/logtree.thrift"
include "logdevice/admin/if/maintenance.thrift"
include "logdevice/admin/if/nodes.thrift"
include "logdevice/admin/if/safety.thrift"
include "logdevice/admin/if/settings.thrift"

namespace cpp2 facebook.logdevice.thrift
namespace py3 logdevice
namespace php LogDevice
namespace wiki Thriftdoc.LogDevice.AdminAPI

// *** AdminAPI Service
service AdminAPI extends fb303.FacebookService {
  /**
   * Gets the config for all nodes that matches the supplied NodesFilter. If
   * NodesFilter is empty we will return all nodes. If the filter does not match
   * any nodes, an empty list of nodes is returned in the NodesConfigResponse
   * object.
   */
  nodes.NodesConfigResponse getNodesConfig(1: nodes.NodesFilter filter) throws
      (1: exceptions.NodeNotReady notready);

  /**
   * Gets the state object for all nodes that matches the supplied NodesFilter.
   * If NodesFilter is empty we will return all nodes. If the filter does not
   * match any nodes, an empty list of nodes is returned in the
   * NodesStateResponse object. `force` will force this method to return all the
   * available state even if the node is not fully ready. In this case we will
   * not throw NodeNotReady exception but we will return partial data.
   */
   nodes.NodesStateResponse getNodesState(1: nodes.NodesStateRequest request) throws
      (1: exceptions.NodeNotReady notready);

  /**
   * Add new nodes to the cluster. The request should contain the spec of each
   * added node (as nodes.NodeConfig). The admin server will then add them to
   * NodesConfiguration with disabled storage and sequencing.
   * Check the documentation of AddNodesRequest for more information.
   *
   * If any of the nodes fail, the whole request will throw
   * ClusterMembershipOperationFailed exception with the failed nodes set in the
   * exception along with the reason. No changes will get applied in this case.
   *
   * Failure reasons can be one of:
   *  - ALREADY_EXISTS: If the passed NodeID / Name / address already exists.
   */
  cluster_membership.AddNodesResponse addNodes(1:
      cluster_membership.AddNodesRequest request) throws
      (1: exceptions.NodeNotReady notready,
       2: cluster_membership.ClusterMembershipOperationFailed failed_op,
       3: exceptions.NodesConfigurationManagerError ncm_error,
       4: exceptions.NotSupported not_supported);

  /**
   * Update service discovery information of some cluster nodes. The passed
   * node configs should describe the desired final state of the node (not
   * the diff). The admin server will generate a nodes configuration update to
   * make it happen.
   * Check the documentation of UpdateNodesRequest for more information.
   *
   * If any of the nodes fail, the whole request will throw
   * ClusterMembershipOperationFailed exception with the failed nodes set in the
   * exception along with the reason. No changes will get applied in this case.
   *
   * Failure reasons can be one of:
   *  - NO_MATCH_IN_CONFIG: When the NodeID doesn't match any node.
   *  - INVALID_REQUEST_NODES_CONFIG: If the update was trying to update
   *      immutable attributes.
   */
  cluster_membership.UpdateNodesResponse updateNodes(1:
      cluster_membership.UpdateNodesRequest request) throws
      (1: exceptions.NodeNotReady notready,
       2: cluster_membership.ClusterMembershipOperationFailed failed_op,
       3: exceptions.NodesConfigurationManagerError ncm_error,
       4: exceptions.NotSupported not_supported);

  /**
   * Removes the nodes matching the passed list of NodeFilters from the
   * NodesConfiguration.
   * For the node to be removed from the config:
   *  1. It must be DISABLED for both storage and sequencing.
   *  2. It must be seen by the cluster as DEAD.
   *
   * If any of the nodes fail, the whole request will throw
   * ClusterMembershipOperationFailed exception with the failed nodes set in the
   * exception along with the reason. No changes will get applied in this case.
   *
   * Failure reasons can be one of:
   *  - NOT_DEAD: When the node is still alive.
   *  - NOT_DISABLED: When the node is still not disabled.
   */
  cluster_membership.RemoveNodesResponse removeNodes(1:
      cluster_membership.RemoveNodesRequest request) throws
      (1: exceptions.NodeNotReady notready,
       2: cluster_membership.ClusterMembershipOperationFailed failed_op,
       3: exceptions.NodesConfigurationManagerError ncm_error,
       4: exceptions.NotSupported not_supported);

  /**
   * Lists the maintenance by group-ids. This returns maintenances from the
   * state stored in the server which may include maintenances that were not
   * processed/started yet. If filter is empty, this will return all
   * maintenances.
   */
  maintenance.MaintenanceDefinitionResponse getMaintenances(1:
      maintenance.MaintenancesFilter filter) throws
      (1: exceptions.NodeNotReady notready,
       3: exceptions.InvalidRequest invalid_request,
       4: exceptions.OperationError error,
       5: exceptions.NotSupported not_supported);

  /**
   * Perform a maintenance on one or more nodes/shards declaratively. The
   * operation is accepted only if no other maintenance with different target
   * is set by the same user for _any_ of the shards/nodes passed.
   * Otherwise the MaintenanceClash exception is thrown. The same user cannot
   * have multiple maintenances that touch any given shard/node with different
   * targets.
   * Applying the same maintenance target by the same user will return the
   * existing maintenance if the shards and/or sequencers and target match the
   * existing maintenances.
   *
   * For ungrouped maintenances, the exploded maintenances are first matched
   * against the existing maintenances, for those equivalent maintenance we will
   * return the existing maintenances, for the new one we will create.
   * The whole operation will throw MaintenanceClash exception if the new ones
   * failed the rules mentioned above.
   *
   * Accepting the maintenance does not guarantee that the maintenance will be
   * executed successfully. To monitor progress of maintenance use the
   * `getNodesState` to inspect the states for particular nodes or shards.
   */
  maintenance.MaintenanceDefinitionResponse applyMaintenance(1:
      maintenance.MaintenanceDefinition request) throws
      (1: exceptions.NodeNotReady notready,
       2: exceptions.InvalidRequest invalid_request,
       3: exceptions.MaintenanceClash clash,
       4: exceptions.OperationError operation_error,
       5: exceptions.NotSupported not_supported);

  /**
   * Cancels a maintenance that has been scheduled or executed on one or more
   * shards/nodes. If the removed maintenance is the current active
   * maintenance, the MaintenanceManager will trigger a transition to
   * move the current active to the next logical maintenance, or trigger a
   * transition to ENABLE the shard/sequencer if no pending maintenances are
   * pending.
   *
   * Removing a maintenance that doesn't exist for one or more of the
   * shards is a no-op. If the filter doesn't match any maintenances, the
   * MaintenanceMatchError exception will be thrown.
   */
  maintenance.RemoveMaintenancesResponse removeMaintenances(1:
      maintenance.RemoveMaintenancesRequest filter) throws
      (1: exceptions.NodeNotReady notready,
       2: exceptions.InvalidRequest invalid_request,
       3: exceptions.OperationError operation_error,
       4: exceptions.NotSupported not_supported,
       5: exceptions.MaintenanceMatchError not_found);

  /**
   * Call this if rebuilding is currently blocked because we have too many
   * donors. Calling this will unblock rebuilding by declaring that we have
   * PERMANENTLY LOST DATA and LogDevice will unblock readers waiting for these
   * data.
   * Note that this is a _very_ dangerous operation. You should ONLY use it if
   * you know that you cannot restore the lost shards/nodes by any means.
   */
  maintenance.UnblockRebuildingResponse unblockRebuilding(1:
      maintenance.UnblockRebuildingRequest request) throws
    (1: exceptions.NodeNotReady notready,
     2: exceptions.InvalidRequest invalid_request,
     3: exceptions.OperationError operation_error,
     4: exceptions.NotSupported not_supported);

  /**
   * Validates whether it's safe to perform a storage-state change on one or
   * more shards or not. That operation does and exhaustive test on all the
   * configured logs.
   */
  safety.CheckImpactResponse checkImpact(1: safety.CheckImpactRequest request) throws
      (1: exceptions.NodeNotReady notready,
       2: exceptions.OperationError error,
       3: exceptions.InvalidRequest invalid_request,
       4: exceptions.NotSupported notsupported);

  // *** LogTree specific APIs
  logtree.LogTreeInfo getLogTreeInfo();
  logtree.ReplicationInfo getReplicationInfo();

  /**
   * Get information about all or some of the settings
   */
  settings.SettingsResponse getSettings(1: settings.SettingsRequest request);

  /**
   * Force the server to take new snapshot of the LogsTree state in memory. The
   * argument to this is `min_version` which means that the snapshot should only
   * be taken if the server is running with this LogTree version (or newer)
   * server will throw StaleVersion exception if that server has older version.
   * If the argument is not supplied or (0) then the server will take a snapshot
   * of whatever version it currently has.
   */
  void takeLogTreeSnapshot(1: common.unsigned64 min_version) throws
      (1: exceptions.StaleVersion stale,
       2: exceptions.NodeNotReady notready,
       3: exceptions.NotSupported notsupported);

  /**
   * Get Log Group Throughput
   */
  logtree.LogGroupThroughputResponse getLogGroupThroughput(
                                1: logtree.LogGroupThroughputRequest request);

  /**
   * Get Log Group custom counters
   */
  logtree.LogGroupCustomCountersResponse getLogGroupCustomCounters(
    1: logtree.LogGroupCustomCountersRequest request) throws
    (1: exceptions.NotSupported notsupported,
     2: exceptions.InvalidRequest invalid_request);
}
