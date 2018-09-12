/**
 * Copyright (c) 2017-present, Facebook, Inc.
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
#include "logdevice/common/types_internal.h"

namespace facebook { namespace logdevice {
class EventLogRebuildingSet;
class FailureDetector;
class ClusterState;
namespace configuration {
class Node;
enum class NodeRole : unsigned int;
} // namespace configuration

using NodeFunctor = std::function<void(
    const std::pair<const node_index_t, configuration::Node>&)>;

void forFilteredNodes(const configuration::Nodes& nodes,
                      thrift::NodesFilter* filter,
                      NodeFunctor fn);

void fillNodeConfig(thrift::NodeConfig& out,
                    node_index_t node_index,
                    const configuration::Node& node);

void fillSocketAddress(thrift::SocketAddress& out, const Sockaddr& addr);

thrift::Role toThriftRole(configuration::NodeRole role);

folly::Optional<configuration::NodeRole> toLDRole(thrift::Role role);

thrift::ShardDataHealth toShardDataHealth(AuthoritativeStatus auth_status,
                                          bool has_dirty_ranges);

thrift::ShardOperationalState
toShardOperationalState(configuration::StorageState storage_state,
                        const EventLogRebuildingSet::NodeInfo* node_info);

thrift::ShardStorageState
toShardStorageState(configuration::StorageState storage_state);

void fillNodeState(thrift::NodeState& out,
                   node_index_t my_node_index,
                   node_index_t node_index,
                   const configuration::Node& node,
                   const EventLogRebuildingSet* rebuilding_set,
                   const FailureDetector* failure_detector,
                   const ClusterState* cluster_state);

}} // namespace facebook::logdevice
