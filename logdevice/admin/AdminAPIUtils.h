/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <folly/Optional.h>

#include "logdevice/admin/if/gen-cpp2/admin_types.h"
#include "logdevice/common/Sockaddr.h"
#include "logdevice/common/configuration/Node.h"
#include "logdevice/common/event_log/EventLogRebuildingSet.h"
#include "logdevice/common/toString.h"
#include "logdevice/common/types_internal.h"

namespace facebook { namespace logdevice {
class EventLogRebuildingSet;
class FailureDetector;
class ClusterState;
namespace configuration { namespace nodes {
class NodesConfiguration;
}} // namespace configuration::nodes

std::string toString(const thrift::SocketAddressFamily& address);
std::string toString(const thrift::SocketAddress& address);

using NodeFunctor = std::function<void(node_index_t)>;

void forFilteredNodes(
    const configuration::nodes::NodesConfiguration& nodes_configuration,
    const thrift::NodesFilter* filter,
    NodeFunctor fn);

void fillNodeConfig(
    thrift::NodeConfig& out,
    node_index_t node_index,
    const configuration::nodes::NodesConfiguration& nodes_configuration);

void fillSocketAddress(thrift::SocketAddress& out, const Sockaddr& addr);

void fillNodeState(
    thrift::NodeState& out,
    node_index_t node_index,
    const configuration::nodes::NodesConfiguration& nodes_configuration,
    const EventLogRebuildingSet* rebuilding_set,
    const ClusterState* cluster_state);

/**
 * Finds a node in the nodes configuration and return its node_index. The
 * function returns folly::none if the node cannot be found or the NodeID
 * matches multiple nodes.
 */
folly::Optional<node_index_t> findNodeIndex(
    const thrift::NodeID& node,
    const configuration::nodes::NodesConfiguration& nodes_configuration);

/**
 * Resolves a ShardID into a set of shards. If the input shard refers to a node
 * by address, server-instance, or index. It wil get the node_index_t for it.
 * Also if the shard_id is -1, it will expand it into num_shards instances of
 * ShardID.
 *
 * @param ignore_missing  Will simply ignore nodes that were not found in the
 * configuration file silently. Otherwise, exception will be thrown.
 *
 * @param ignore_non_storage_nodes This will ignore nodes that are not storage
 * nodes instead of failing with InvalidRequest exception.
 */
ShardSet resolveShardOrNode(
    const thrift::ShardID& shard,
    const configuration::nodes::NodesConfiguration& nodes_configuration,
    bool ignore_missing = false,
    bool ignore_non_storage_nodes = false);

/**
 * Expands a thrift ShardSet structure into logdevice equivalent. This looks up
 * the nodes via address if specified in the input.
 *
 * @param ignore_missing  Will simply ignore nodes that were not found in the
 * configuration file silently. Otherwise, exception will be thrown.
 *
 * @param ignore_non_storage_nodes This will ignore nodes that are not storage
 * nodes instead of failing with InvalidRequest exception.
 */
ShardSet expandShardSet(
    const thrift::ShardSet& thrift_shards,
    const configuration::nodes::NodesConfiguration& nodes_configuration,
    bool ignore_missing = false,
    bool ignore_non_storage_nodes = false);

thrift::ShardOperationalState
toShardOperationalState(membership::StorageState storage_state,
                        const EventLogRebuildingSet::NodeInfo* node_info);

/**
 * Checks if the NodeID has at least one filter set.
 */
bool isNodeIDSet(const thrift::NodeID& id);
/**
 * Checks if the passed node (with the index node_index) matches the passed the
 * NodeID. The node mathes if all the set ID attributes match the node's
 * attributes.
 */
bool nodeMatchesID(node_index_t node_index,
                   const configuration::nodes::NodeServiceDiscovery& node_sd,
                   const thrift::NodeID& id);
/*
 * Returns whether the node_id exists in the vector<thrift::NodeID> which is
 * commonly used to refer to nodes in maintenance definitions.
 */
bool isInNodeIDs(node_index_t node_id,
                 const std::vector<thrift::NodeID>& nodes);

// helpers to convert logdevice to thrift types
thrift::ShardID mkShardID(node_index_t node_id, shard_index_t shard);
thrift::ShardID mkShardID(const ShardID& shard);
thrift::NodeID mkNodeID(node_index_t node_id);

}} // namespace facebook::logdevice
