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
#include "logdevice/lib/ClientImpl.h"

using namespace apache::thrift::util;

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
       apache::thrift::util::enumNameSafe(state))
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
                          apache::thrift::util::enumNameSafe(
                              node_state.get_daemon_state())
                              .c_str(),
                          apache::thrift::util::enumNameSafe(state).c_str());
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
            apache::thrift::util::enumNameSafe(state).c_str());
    return true;
  } else {
    ld_info("Timed out waiting for nodes %s to be in service state %s",
            toString(nodes).c_str(),
            apache::thrift::util::enumNameSafe(state).c_str());
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

}} // namespace facebook::logdevice
