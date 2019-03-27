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
    thrift::NodesFilter* filter,
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

ShardSet resolveShardOrNode(
    const thrift::ShardID& shard,
    const configuration::nodes::NodesConfiguration& nodes_configuration);

/**
 * Expands a thrift ShardSet structure into logdevice equivalent. This looks up
 * the nodes via address if specified in the input.
 */
ShardSet expandShardSet(
    const thrift::ShardSet& thrift_shards,
    const configuration::nodes::NodesConfiguration& nodes_configuration);

thrift::ShardOperationalState
toShardOperationalState(configuration::StorageState storage_state,
                        const EventLogRebuildingSet::NodeInfo* node_info);

}} // namespace facebook::logdevice
