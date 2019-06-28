/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "logdevice/admin/cluster_membership/AddNodesHandler.h"

#include <folly/container/F14Set.h>

#include "logdevice/admin/AdminAPIUtils.h"
#include "logdevice/admin/cluster_membership/ClusterMembershipUtils.h"
#include "logdevice/admin/if/gen-cpp2/cluster_membership_constants.h"
#include "logdevice/common/membership/StorageStateTransitions.h"
#include "logdevice/common/types_internal.h"

namespace facebook { namespace logdevice { namespace admin {
namespace cluster_membership {

using namespace facebook::logdevice::configuration::nodes;

folly::Expected<AddNodesHandler::Result,
                thrift::ClusterMembershipOperationFailed>
AddNodesHandler::buildNodesConfigurationUpdates(
    std::vector<thrift::AddSingleNodeRequest> add_requests,
    const configuration::nodes::NodesConfiguration& nodes_configuration,
    NodeIndicesAllocator allocator) const {
  Result addition_result;

  // Allocate a NodeID for each request. We may endup using less that the
  // allocated NodeIDs if requests specify their own NodeIDs.
  auto allocated_indices = allocator.allocate(
      *nodes_configuration.getServiceDiscovery(), add_requests.size());
  for (auto& req : add_requests) {
    if (req.new_config.node_index ==
        thrift::cluster_membership_constants::ANY_NODE_IDX()) {
      ld_assert(!allocated_indices.empty());
      req.new_config.set_node_index(allocated_indices.front());
      allocated_indices.pop_front();
    }
    addition_result.to_be_added.push_back(req.new_config.node_index);
  }

  // Validate the uniquness of the new requests
  auto not_unique = validateUniquness(add_requests, nodes_configuration);
  if (not_unique.hasValue()) {
    return folly::makeUnexpected(std::move(not_unique).value());
  }

  // Build the Update structure of each request.
  thrift::ClusterMembershipOperationFailed failures;
  for (const auto& req : add_requests) {
    auto update_error = buildUpdateFromNodeConfig(
        addition_result.update, req.new_config, nodes_configuration);
    if (update_error.hasValue()) {
      failures.failed_nodes.push_back(std::move(update_error).value());
    } else {
    }
  }

  if (!failures.failed_nodes.empty()) {
    return folly::makeUnexpected(std::move(failures));
  }

  return addition_result;
}

folly::Optional<thrift::ClusterMembershipOperationFailed>
AddNodesHandler::validateUniquness(
    const std::vector<thrift::AddSingleNodeRequest>& add_requests,
    const configuration::nodes::NodesConfiguration& nodes_configuration) const {
  folly::F14FastSet<node_index_t> node_idxs;
  folly::F14FastSet<std::string> names;
  folly::F14FastSet<std::string> addresses;

  for (const auto& sd : *nodes_configuration.getServiceDiscovery()) {
    thrift::SocketAddress addr;
    fillSocketAddress(addr, sd.second.address);

    node_idxs.insert(sd.first);
    names.insert(sd.second.name);
    addresses.insert(toString(addr));
  }
  ld_assert_eq(nodes_configuration.clusterSize(), node_idxs.size());
  ld_assert_eq(nodes_configuration.clusterSize(), names.size());
  ld_assert_eq(nodes_configuration.clusterSize(), addresses.size());

  auto make_failure = [](thrift::NodeIndex idx,
                         const std::string& type,
                         const std::string& value) {
    return buildNodeFailure(
        idx,
        thrift::ClusterMembershipFailureReason::ALREADY_EXISTS,
        folly::sformat(
            "N{} has a duplicate '{}' with value '{}'", idx, type, value));
  };

  thrift::ClusterMembershipOperationFailed failures;
  for (const auto& req : add_requests) {
    const auto& cfg = req.new_config;

    ld_assert(cfg.node_index >= 0);
    if (node_idxs.count(cfg.node_index) > 0) {
      failures.failed_nodes.push_back(make_failure(
          cfg.node_index, "NodeIndex", std::to_string(cfg.node_index)));
      continue;
    }

    if (names.count(cfg.name) > 0) {
      failures.failed_nodes.push_back(
          make_failure(cfg.node_index, "Name", cfg.name));
      continue;
    }

    if (addresses.count(toString(cfg.data_address)) > 0) {
      failures.failed_nodes.push_back(
          make_failure(cfg.node_index, "Address", toString(cfg.data_address)));
      continue;
    }

    node_idxs.insert(cfg.node_index);
    names.insert(cfg.name);
    addresses.insert(toString(cfg.data_address));
  }

  if (failures.failed_nodes.empty()) {
    return folly::none;
  }

  return failures;
}

folly::Optional<thrift::ClusterMembershipFailedNode>
AddNodesHandler::buildUpdateFromNodeConfig(
    NodesConfiguration::Update& update,
    const thrift::NodeConfig& cfg,
    const NodesConfiguration& nodes_configuration) const {
  auto maybe_update_builder = nodeUpdateBuilderFromNodeConfig(cfg);
  if (maybe_update_builder.hasError()) {
    return std::move(maybe_update_builder).error();
  }

  auto update_builder = std::move(maybe_update_builder).value();

  auto result =
      std::move(update_builder)
          .buildAddNodeUpdate(
              update /* output update is filled in here */,
              nodes_configuration.getSequencerMembership()->getVersion(),
              nodes_configuration.getStorageMembership()->getVersion());
  // Should always be a non nullptr given that the validation passed.
  ld_assert(result.status == Status::OK);
  return folly::none;
}

}}}} // namespace facebook::logdevice::admin::cluster_membership
