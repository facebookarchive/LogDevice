/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "logdevice/test/utils/AdminAPITestUtils.h"

#include <chrono>

#include <thrift/lib/cpp/util/EnumUtils.h>

#include "logdevice/common/ThriftCodec.h"
#include "logdevice/common/test/TestUtil.h"
#include "logdevice/lib/ClientImpl.h"
#include "logdevice/test/utils/IntegrationTestUtils.h"

using namespace apache::thrift::util;
using apache::thrift::util::enumNameSafe;
using facebook::logdevice::IntegrationTestUtils::Cluster;
using facebook::logdevice::IntegrationTestUtils::Node;

namespace facebook { namespace logdevice {

void retry_until_ready(int32_t attempts,
                       std::chrono::seconds delay,
                       folly::Function<void()> operation) {
  do {
    // let's keep trying until the server hits that version
    try {
      operation();
      // No exception thrown.
      return;
    } catch (thrift::NodeNotReady& e) {
      ld_info("Got NodeNotReady exception, attempts left: %i", attempts);
      // retry
      std::this_thread::sleep_for(delay);
      attempts--;
    }
  } while (attempts > 0);
}

lsn_t write_to_maintenance_log(Client& client,
                               maintenance::MaintenanceDelta& delta) {
  logid_t maintenance_log_id =
      configuration::InternalLogs::MAINTENANCE_LOG_DELTAS;

  // Retry for at most 30s to avoid test failures due to transient failures
  // writing to the maintenance log.
  std::chrono::steady_clock::time_point deadline =
      std::chrono::steady_clock::now() + std::chrono::seconds{30};

  std::string serializedData =
      ThriftCodec::serialize<apache::thrift::BinarySerializer>(delta);

  lsn_t lsn = LSN_INVALID;
  auto clientImpl = dynamic_cast<ClientImpl*>(&client);
  clientImpl->allowWriteInternalLog();
  auto rv = wait_until("writes to the maintenance log succeed",
                       [&]() {
                         lsn = clientImpl->appendSync(
                             maintenance_log_id, serializedData);
                         return lsn != LSN_INVALID;
                       },
                       deadline);

  if (rv != 0) {
    ld_check(lsn == LSN_INVALID);
    ld_error("Could not write delta in maintenance log(%lu): %s(%s)",
             maintenance_log_id.val_,
             error_name(err),
             error_description(err));
    return lsn; /* LSN_INVALID in this case */
  }

  ld_info(
      "Wrote maintenance log delta with lsn %s", lsn_to_string(lsn).c_str());
  return lsn;
}

bool wait_until_service_state(thrift::AdminAPIAsyncClient& admin_client,
                              const std::vector<node_index_t>& nodes,
                              thrift::ServiceState state,
                              std::chrono::steady_clock::time_point deadline) {
  int rv = wait_until(
      ("Waiting for nodes " + toString(nodes) + " to be in state " +
       enumNameSafe(state))
          .c_str(),
      [&]() {
        auto response = get_nodes_state(admin_client);
        const std::vector<thrift::NodeState>& states = response.get_states();
        return std::all_of(
            states.begin(), states.end(), [&](const auto& node_state) {
              auto node_id = node_state.get_config().get_node_index();
              // Is this one of the nodes we are interested in
              // C++ for: `node_id in nodes`
              if (std::find(nodes.begin(), nodes.end(), node_id) !=
                  nodes.end()) {
                if (node_state.get_daemon_state() != state) {
                  ld_info("Node %d service state is %s, expected  %s",
                          node_id,
                          enumNameSafe(node_state.get_daemon_state()).c_str(),
                          enumNameSafe(state).c_str());
                  return false;
                }
              }
              // Returns true for both nodes that reached the target state and
              // nodes that we are not even interested in. A side effect of that
              // is if the nodes passed do not exist in the config we will still
              // succeed in this check.
              return true;
            });
      },
      deadline);
  if (rv == 0) {
    ld_info("Nodes %s service state are now %s",
            toString(nodes).c_str(),
            enumNameSafe(state).c_str());
    return true;
  } else {
    ld_info("Timed out waiting for nodes %s to be in service state %s",
            toString(nodes).c_str(),
            enumNameSafe(state).c_str());
  }
  return false;
}

thrift::NodesStateResponse
get_nodes_state(thrift::AdminAPIAsyncClient& admin_client) {
  thrift::NodesStateRequest req;
  thrift::NodesStateResponse resp;
  // Under stress runs, the initial initialization might take a while, let's
  // be patient and increase the timeout here.
  auto rpc_options = apache::thrift::RpcOptions();
  rpc_options.setTimeout(std::chrono::minutes(1));
  admin_client.sync_getNodesState(rpc_options, resp, req);
  return resp;
}

folly::Optional<thrift::ShardState>
get_shard_state(const thrift::NodesStateResponse& response,
                const ShardID& shard) {
  const std::vector<thrift::NodeState>& states = response.get_states();
  for (const auto& node_state : states) {
    if (node_state.get_config().get_node_index() == shard.node()) {
      const auto& shards = node_state.shard_states_ref().value_or({});
      if (shard.shard() >= 0 && shard.shard() < shards.size()) {
        auto shard_state = shards[shard.shard()];
        return shard_state;
      }
      ld_warning(
          "Shard %u doesn't exist on node %u", shard.shard(), shard.node());
      return folly::none;
    }
  }
  ld_warning("Node ID %u doesn't exist", shard.node());
  return folly::none;
}

std::unordered_map<shard_index_t, thrift::ShardState>
get_shards_state(const thrift::NodesStateResponse& response,
                 node_index_t node_id,
                 std::unordered_set<shard_index_t> shards) {
  const std::vector<thrift::NodeState>& node_states = response.get_states();
  std::unordered_map<shard_index_t, thrift::ShardState> results;
  for (const auto& node_state : node_states) {
    if (node_state.get_config().get_node_index() == node_id) {
      const auto& states = node_state.shard_states_ref().value_or({});
      for (int shard_idx = 0; shard_idx < states.size(); ++shard_idx) {
        auto shard_state = states[shard_idx];
        // Is it one of those we are interested in?
        if (shards.find(shard_idx) != shards.end()) {
          results[shard_idx] = shard_state;
        }
      }
    }
  }
  return results;
}

thrift::ShardOperationalState
get_shard_operational_state(const thrift::NodesStateResponse& response,
                            const ShardID& shard) {
  auto shard_state = get_shard_state(response, shard);
  if (shard_state) {
    return shard_state->get_current_operational_state();
  }
  return thrift::ShardOperationalState::UNKNOWN;
}

thrift::ShardOperationalState
get_shard_operational_state(thrift::AdminAPIAsyncClient& admin_client,
                            const ShardID& shard) {
  return get_shard_operational_state(get_nodes_state(admin_client), shard);
}

thrift::ShardDataHealth
get_shard_data_health(const thrift::NodesStateResponse& response,
                      const ShardID& shard) {
  auto shard_state = get_shard_state(response, shard);
  if (shard_state) {
    return shard_state->get_data_health();
  }
  return thrift::ShardDataHealth::UNKNOWN;
}

thrift::ShardDataHealth
get_shard_data_health(thrift::AdminAPIAsyncClient& admin_client,
                      const ShardID& shard) {
  return get_shard_data_health(get_nodes_state(admin_client), shard);
}

bool wait_until_shards_enabled_and_healthy(
    Cluster& cluster,
    node_index_t node_id,
    std::unordered_set<shard_index_t> shards,
    std::chrono::steady_clock::time_point deadline) {
  auto admin_client = cluster.getAdminServer()->createAdminClient();
  ld_check(admin_client);

  if (!wait_until_service_state(
          *admin_client, {node_id}, thrift::ServiceState::ALIVE, deadline)) {
    ld_error("Node %u is not ALIVE/FULLY_STARTED.", node_id);
    return false;
  }

  auto cb = [&]() {
    // Check that Admin Server believes that shards are enabled.
    auto nodes_state = get_nodes_state(*admin_client);
    std::unordered_map<shard_index_t, thrift::ShardState> states =
        get_shards_state(nodes_state, node_id, shards);
    for (const auto& [shard_idx, shard_state] : states) {
      if (
          // Shard is not FULLY_AUTHORITATIVE
          shard_state.get_data_health() != thrift::ShardDataHealth::HEALTHY ||
          // Shard is not WRITEABLE
          shard_state.get_storage_state() !=
              membership::thrift::StorageState::READ_WRITE) {
        ld_info("N%u:S%u still has StorageState=%s, DataHealth=%s.",
                node_id,
                shard_idx,
                enumNameSafe(shard_state.get_storage_state()).c_str(),
                enumNameSafe(shard_state.get_data_health()).c_str());
        return false;
      }
    }
    Node& node = cluster.getNode(node_id);
    // Check that the node itself doesn't think that these shards need
    // rebuilding.
    //
    std::map<shard_index_t, std::string> rebuilding_state =
        node.rebuildingStateInfo();
    for (const auto& [shard_id, rebuilding_str] : rebuilding_state) {
      // TODO: Let's add an Admin API to query this instead. This is temporary.
      if (shards.find(shard_id) != shards.end()) {
        if (rebuilding_str != "NONE") {
          ld_info("N%u:S%u has its rebuilding state = %s",
                  node_id,
                  shard_id,
                  rebuilding_str.c_str());
          return false;
        }
      }
    }
    return true;
  };

  int rv = wait_until(("Node " + std::to_string(node_id) + " has shards " +
                       toString(shards) + " healthy")
                          .c_str(),
                      cb,
                      deadline);
  if (rv != 0) {
    ld_info("Failed on waiting for shards %s on node %u to become healthy",
            toString(shards).c_str(),
            node_id);
    return false;
  }
  return true;
}

bool wait_until_enabled_and_healthy(
    IntegrationTestUtils::Cluster& cluster,
    node_index_t node_id,
    std::chrono::steady_clock::time_point deadline) {
  auto& node = cluster.getNode(node_id);
  int num_shards = node.num_db_shards_;
  std::unordered_set<shard_index_t> shards;
  for (int i = 0; i < num_shards; ++i) {
    shards.insert(i);
  }
  return wait_until_shards_enabled_and_healthy(
      cluster, node_id, shards, deadline);
}
}} // namespace facebook::logdevice
