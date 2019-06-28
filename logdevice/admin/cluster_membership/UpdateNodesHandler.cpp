/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "logdevice/admin/cluster_membership/UpdateNodesHandler.h"

#include "logdevice/admin/AdminAPIUtils.h"
#include "logdevice/admin/cluster_membership/ClusterMembershipUtils.h"
#include "logdevice/admin/if/gen-cpp2/cluster_membership_constants.h"
#include "logdevice/common/membership/StorageStateTransitions.h"
#include "logdevice/common/types_internal.h"

namespace facebook { namespace logdevice { namespace admin {
namespace cluster_membership {

using namespace facebook::logdevice::configuration::nodes;

folly::Expected<UpdateNodesHandler::Result,
                thrift::ClusterMembershipOperationFailed>
UpdateNodesHandler::buildNodesConfigurationUpdates(
    const std::vector<thrift::UpdateSingleNodeRequest>& update_requests,
    const configuration::nodes::NodesConfiguration& nodes_configuration) const {
  Result update_result;

  // Build the Update structure of each request.
  thrift::ClusterMembershipOperationFailed failures;
  for (const auto& req : update_requests) {
    auto maybe_update = buildUpdateFromNodeConfig(
        update_result.update, req, nodes_configuration);
    if (maybe_update.hasError()) {
      failures.failed_nodes.push_back(std::move(maybe_update).error());
    } else {
      update_result.to_be_updated.push_back(std::move(maybe_update).value());
    }
  }

  if (!failures.failed_nodes.empty()) {
    return folly::makeUnexpected(std::move(failures));
  }

  return update_result;
}

folly::Expected<node_index_t, thrift::ClusterMembershipFailedNode>
UpdateNodesHandler::buildUpdateFromNodeConfig(
    NodesConfiguration::Update& update,
    const thrift::UpdateSingleNodeRequest& req,
    const NodesConfiguration& nodes_configuration) const {
  auto maybe_node_index =
      findNodeIndex(req.node_to_be_updated, nodes_configuration);

  if (!maybe_node_index.hasValue()) {
    return folly::makeUnexpected(buildNodeFailure(
        req.node_to_be_updated,
        logdevice::thrift::ClusterMembershipFailureReason::NO_MATCH_IN_CONFIG,
        "Couldn't find matches in config"));
  }

  auto node_index = maybe_node_index.value();

  if (node_index != req.new_config.node_index) {
    return folly::makeUnexpected(buildNodeFailure(
        req.node_to_be_updated,
        logdevice::thrift::ClusterMembershipFailureReason::
            INVALID_REQUEST_NODES_CONFIG,
        folly::sformat("Matched node's index (N{}), doesn't match the index in "
                       "the update request: {}",
                       node_index,
                       req.new_config.node_index)));
  }

  auto maybe_update_builder = nodeUpdateBuilderFromNodeConfig(req.new_config);
  if (maybe_update_builder.hasError()) {
    return folly::makeUnexpected(std::move(maybe_update_builder).error());
  }

  auto result = std::move(maybe_update_builder)
                    .value()
                    .buildUpdateNodeUpdate(update, nodes_configuration);

  if (result.status != Status::OK) {
    if (result.status == E::NOTINCONFIG) {
      return folly::makeUnexpected(buildNodeFailure(
          node_index,
          logdevice::thrift::ClusterMembershipFailureReason::NO_MATCH_IN_CONFIG,
          result.message));
    } else if (result.status == E::INVALID_PARAM) {
      return folly::makeUnexpected(
          buildNodeFailure(node_index,
                           logdevice::thrift::ClusterMembershipFailureReason::
                               INVALID_REQUEST_NODES_CONFIG,
                           result.message));
    } else {
      return folly::makeUnexpected(buildNodeFailure(
          node_index,
          logdevice::thrift::ClusterMembershipFailureReason::UNKNOWN,
          result.message));
    }
  }
  return node_index;
}

}}}} // namespace facebook::logdevice::admin::cluster_membership
