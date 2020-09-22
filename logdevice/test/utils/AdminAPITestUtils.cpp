/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "logdevice/test/utils/AdminAPITestUtils.h"

#include <chrono>

#include "logdevice/common/ThriftCodec.h"
#include "logdevice/lib/ClientImpl.h"

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

thrift::ShardOperationalState
get_shard_operational_state(thrift::AdminAPIAsyncClient& admin_client,
                            node_index_t node_idx,
                            uint32_t shard_idx) {
  thrift::NodesStateRequest req;
  thrift::NodesFilter filter;
  thrift::NodeID node;
  node.set_node_index(node_idx);
  filter.set_node(std::move(node));
  req.set_filter(std::move(filter));
  thrift::NodesStateResponse resp;
  // Under stress runs, the initial initialization might take a while, let's
  // be patient and increase the timeout here.
  auto rpc_options = apache::thrift::RpcOptions();
  rpc_options.setTimeout(std::chrono::minutes(1));
  admin_client.sync_getNodesState(rpc_options, resp, req);
  const std::vector<thrift::NodeState>& states = resp.get_states();
  // We expect this to match exactly one node (or none).
  if (states.size() != 1) {
    ld_warning("Node %u doesn't exist", node_idx);
    return thrift::ShardOperationalState::UNKNOWN;
  }
  auto node_state = states[0];
  const auto& shards = node_state.shard_states_ref().value_or({});
  if (shard_idx >= 0 && shard_idx < shards.size()) {
    auto shard = shards[shard_idx];
    return shard.get_current_operational_state();
  }
  ld_warning("Shard %u doesn't exist on node %u", shard_idx, node_idx);
  return thrift::ShardOperationalState::UNKNOWN;
}
}} // namespace facebook::logdevice
