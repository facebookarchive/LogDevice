/**
 * Copyright (c) 2018-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
include "common/fb303/if/fb303.thrift"
include "logdevice/admin/if/common.thrift"
include "logdevice/admin/if/exceptions.thrift"
include "logdevice/admin/if/logtree.thrift"
include "logdevice/admin/if/maintenance.thrift"
include "logdevice/admin/if/nodes.thrift"
include "logdevice/admin/if/safety.thrift"
include "logdevice/admin/if/settings.thrift"

namespace cpp2 facebook.logdevice.thrift
namespace py3 logdevice
namespace php LogDevice
namespace wiki LogDevice.AdminAPI

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

  /*
   * Lists the maintenance by group-ids.
   */
  maintenance.MaintenanceDefinitionResponse getMaintenances(1:
      maintenance.MaintenancesFilter filter) throws
      (1: exceptions.NodeNotReady notready,
       2: exceptions.Redirect not_master,
       3: exceptions.InvalidRequest invalid_request,
       4: exceptions.OperationError error);

  /*
   * Perform a maintenance on one or more nodes/shards declaratively. The
   * operation is accepted only if no other maintenance with different target
   * is set by the same user for _any_ of the shards/nodes passed.
   * Otherwise the MaintenanceClash exception is thrown.
   * Applying the same maintenance target by the same user will return the
   * existing maintenance progress.
   * Accepting the maintenance does not guarantee that the maintenance will be
   * executed successfully. There are two methods to monitor progress of
   * maintenance:
   *   - Either by calling the same applyMaintenance with the same arguments (or
   *   you can request for a subset of the shards) and inspecting the
   *   ApplyMaintenanceResponse object.
   *   - By using the getNodesState to inspect the states for particular nodes
   *   or shards.
   */
  maintenance.MaintenanceDefinitionResponse applyMaintenance(1:
      maintenance.MaintenanceDefinition request) throws
      (1: exceptions.NodeNotReady notready,
       2: exceptions.Redirect not_master,
       3: exceptions.InvalidRequest invalid_request,
       4: exceptions.MaintenanceClash clash,
       5: exceptions.OperationError operation_error);

  /*
   * Cancels a maintenance that has been scheduled or executed on one or more
   * shards/nodes. If the removed maintenance is the current active
   * maintenance, the MaintenanceManager will trigger a transition to
   * move the current active to the next logical maintenance, or trigger a
   * transition to ENABLE the shard/sequencer if no pending maintenances are
   * pending.
   *
   * Removing a maintenance that doesn't exist for one or more of the
   * shards is a no-op.
   */
  maintenance.RemoveMaintenancesResponse removeMaintenances(1:
      maintenance.RemoveMaintenancesRequest filter) throws
      (1: exceptions.NodeNotReady notready,
       2: exceptions.Redirect not_master,
       3: exceptions.InvalidRequest invalid_request,
       4: exceptions.OperationError operation_error);

  /*
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
     3: exceptions.OperationError operation_error);

  /**
   * Safety check an operation.
   */
  safety.CheckImpactResponse checkImpact(1: safety.CheckImpactRequest request) throws
      (1: exceptions.NodeNotReady notready,
       2: exceptions.OperationError error,
       3: exceptions.InvalidRequest invalid_request);

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
