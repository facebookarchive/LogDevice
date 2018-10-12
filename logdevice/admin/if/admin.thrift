/**
 * Copyright (c) 2018-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
include "common/fb303/if/fb303.thrift"
include "common.thrift"
include "exceptions.thrift"
include "logtree.thrift"
include "nodes.thrift"
include "safety.thrift"
include "settings.thrift"

namespace cpp2 facebook.logdevice.thrift
namespace py3 logdevice
namespace php LogDevice

// *** AdminAPI Service
service AdminAPI extends fb303.FacebookService {
  // Gets the config for all nodes that matches the supplied NodesFilter. If
  // NodesFilter is empty we will return all nodes. If the filter does not match
  // any nodes, an empty list of nodes is returned in the NodesConfigResponse
  // object.
  nodes.NodesConfigResponse getNodesConfig(1: nodes.NodesFilter filter) throws
      (1: exceptions.NodeNotReady notready);

  // Gets the state object for all nodes that matches the supplied NodesFilter.
  // If NodesFilter is empty we will return all nodes. If the filter does not
  // match any nodes, an empty list of nodes is returned in the
  // NodesStateResponse object. `force` will force this method to return all the
  // available state even if the node is not fully ready. In this case we will
  // not throw NodeNotReady exception but we will return partial data.
  nodes.NodesStateResponse getNodesState(1: nodes.NodesStateRequest request) throws
      (1: exceptions.NodeNotReady notready);

  // Safety check an operation.
  safety.CheckImpactResponse checkImpact(1: safety.CheckImpactRequest request) throws
      (1: exceptions.NodeNotReady notready,
       2: exceptions.OperationError error,
       3: exceptions.InvalidRequest invalid_request);

  // *** LogTree specific APIs
  logtree.LogTreeInfo getLogTreeInfo();
  logtree.ReplicationInfo getReplicationInfo();

  // Get information about all or some of the settings
  settings.SettingsResponse getSettings(1: settings.SettingsRequest request);

  // Force the server to take new snapshot of the LogsTree state in memory. The
  // argument to this is `min_version` which means that the snapshot should only
  // be taken if the server is running with this LogTree version (or newer)
  // server will throw StaleVersion exception if that server has older version.
  // If the argument is not supplied or (0) then the server will take a snapshot
  // of whatever version it currently has.
  void takeLogTreeSnapshot(1: common.unsigned64 min_version) throws
      (1: exceptions.StaleVersion stale,
       2: exceptions.NodeNotReady notready,
       3: exceptions.NotSupported notsupported);

  // Get Log Group Throughput
  logtree.LogGroupThroughputResponse getLogGroupThroughput(
                                1: logtree.LogGroupThroughputRequest request);
}
