/**
 * Copyright (c) 2018-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

include "logdevice/admin/if/common.thrift"
include "logdevice/admin/if/nodes.thrift"

namespace cpp2 facebook.logdevice.thrift
namespace py3 logdevice.admin

/**
 * This enum contains all the possible ways the cluster membership change
 * request can fail because of an invalid input.
 */
enum ClusterMembershipFailureReason {
  UNKNOWN = 0,

  /**
   * The passed common.NodeID didn't match any node in the nodes configuration.
   */
  NO_MATCH_IN_CONFIG = 1,

  /**
   * The operation requires that the node to be DEAD but it's not.
   */
  NOT_DEAD = 2,

  /**
   * The operation requires that the node to be DISABLED/DRAINED for both
   * sequencing and storage, but it's not.
   */
  NOT_DISABLED = 3,

  /**
   * The node that you are trying to add already exists.
   */
  ALREADY_EXISTS = 4,

  /**
   * If the passed nodes config is invalid for the operation. Examples of
   * invalid configs can be:
   *   - Update immutable fields (e.g. "location" or "roles").
   *   - Adding a node with a negative sequencer weight.
   *   - Changing non service discovery info in an Update request.
   * More information about the failure will be in the message.
   */
  INVALID_REQUEST_NODES_CONFIG = 5,
}

/**
 * The reason why an operation on a node failed in cluster membership change.
 */
struct ClusterMembershipFailedNode  {
  1: common.NodeID node_id,
  2: ClusterMembershipFailureReason reason,
  /**
   * Human readable error description.
   */
  3: string message,
}

exception ClusterMembershipOperationFailed {
  1: list<ClusterMembershipFailedNode> failed_nodes,
}

////////////////////////////////////////////////////////////////////////////
/////////////////////////// Add Node Request ///////////////////////////////
////////////////////////////////////////////////////////////////////////////

const common.NodeIndex ANY_NODE_IDX = -1

struct AddSingleNodeRequest {
  /**
   * Config of the newly added node. Admin server will use this struct to fill
   * the information needed for the NodesConfiguration.
   *
   * Notes:
   *   - If node_index = ANY_NODE_IDX, the admin server will pick the NodeIndex.
   *   - The name, addresses & node_index (if set) should all be unique.
   *   - Roles can't be empty.
   *   - location_per_scope field is ignored.
   */
  1: nodes.NodeConfig new_config,
}

struct AddNodesRequest {
  1: list<AddSingleNodeRequest> new_node_requests,
}

struct AddNodesResponse {
  /**
   * NodeConfigs of the newly added nodes.
   */
  1: list<nodes.NodeConfig> added_nodes,
  /**
   * The version of the updated NodesConfiguration.
   */
  2: common.unsigned64 new_nodes_configuration_version,
}


////////////////////////////////////////////////////////////////////////////
/////////////////////////// Update Node Request ////////////////////////////
////////////////////////////////////////////////////////////////////////////

struct UpdateSingleNodeRequest {
  /**
   * Node to be updated.
   */
  1: common.NodeID node_to_be_updated,

  /**
   * The desired final config for the node. It should include all the attributes
   * of the node (even the ones that doesn't need to be updated). The admin
   * server will then figure out the fields that changed and apply the update.
   *
   * Changing immutable attributes (e.g. node_index, location or roles) or
   * changing non service discovery info (e.g. sequenecer weight) will fail with
   * INVALID_REQUEST_NODES_CONFIG.
   */
  2: nodes.NodeConfig new_config,
}

struct UpdateNodesRequest {
  /**
   * Update requests for each node that needs to be updated.
   */
  1: list<UpdateSingleNodeRequest> node_requests,
}

struct UpdateNodesResponse {
  /**
   * The new NodeConfigs for the updated nodes.
   */
  1: list<nodes.NodeConfig> updated_nodes,
  /**
   * The version of the updated NodesConfiguration.
   */
  2: common.unsigned64 new_nodes_configuration_version,
}

////////////////////////////////////////////////////////////////////////////
/////////////////////////// Remove Node Request ////////////////////////////
////////////////////////////////////////////////////////////////////////////

/**
 * Request to remove a list of nodes represented by one or more NodesFilters.
 */
struct RemoveNodesRequest {
  /**
   * List of NodeFilters to remove from the nodes configuration. Matches from
   * each filter are union-ed together and are removed from the nodes
   * configuration in a single transaction.
   */
  1: list<nodes.NodesFilter> node_filters,
}

struct RemoveNodesResponse {
  /**
   * List of nodes that were successfully removed from config.
   */
  1: list<common.NodeID> removed_nodes,
  /**
   * The version of the updated NodesConfiguration.
   */
  2: common.unsigned64 new_nodes_configuration_version,
}
