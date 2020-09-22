/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <thread>

#include <gtest/gtest.h>

#include "logdevice/admin/if/gen-cpp2/AdminAPI.h"
#include "logdevice/admin/maintenance/types.h"
#include "logdevice/common/debug.h"
#include "logdevice/include/Client.h"
#include "logdevice/test/utils/IntegrationTestUtils.h"

namespace facebook { namespace logdevice {

/**
 * Retry a lambda for a number of attempts with a delay as long as it's throwing
 * NodeNotReady exception.
 */
void retry_until_ready(int32_t attempts,
                       std::chrono::seconds delay,
                       folly::Function<void()> operation);
/**
 * Writes a maintenance delta to the internal maintenance log, note that this
 * requires both the ClusterMaintenanceState machine and maintenance manager
 * components to be running in order for the delta to be consumed and acted
 * upon. This returns an LSN_INVALID if it failed.
 */
lsn_t write_to_maintenance_log(Client& client,
                               maintenance::MaintenanceDelta& delta);

bool wait_until_service_state(thrift::AdminAPIAsyncClient& admin_client,
                              const std::vector<node_index_t>& nodes,
                              thrift::ServiceState state,
                              std::chrono::steady_clock::time_point deadline =
                                  std::chrono::steady_clock::time_point::max());

/**
 * Returns the ShardState object for a given shard. Returns folly::none if not
 * found.
 */
folly::Optional<thrift::ShardState>
get_shard_state(const thrift::NodesStateResponse& response,
                const ShardID& shard);

/**
 * A helper to query the admin server for NodesState.
 */
thrift::NodesStateResponse
get_nodes_state(thrift::AdminAPIAsyncClient& admin_client);

/**
 * Returns ShardOperationalState::UNKNOWN if the node does not exist. Otherwise
 * will return the current ShardOperationalState for a given shard in a node.
 */
thrift::ShardOperationalState
get_shard_operational_state(const thrift::NodesStateResponse& response,
                            const ShardID& shard);

/**
 * Returns ShardOperationalState::UNKNOWN if the node does not exist. Otherwise
 * will return the current ShardOperationalState for a given shard in a node.
 */
thrift::ShardOperationalState
get_shard_operational_state(thrift::AdminAPIAsyncClient& admin_client,
                            const ShardID& shard);

/**
 * Returns ShardDataHealth::UNKNOWN if the node does not exist. Otherwise
 * will return the current ShardOperationalState for a given shard in a node.
 */
thrift::ShardDataHealth
get_shard_data_health(const thrift::NodesStateResponse& response,
                      const ShardID& shard);
/**
 * Returns ShardDataHealth::UNKNOWN if the node does not exist. Otherwise
 * will return the current ShardOperationalState for a given shard in a node.
 */
thrift::ShardDataHealth
get_shard_data_health(const thrift::AdminAPIAsyncClient& admin_client,
                      const ShardID& shard);
}} // namespace facebook::logdevice
