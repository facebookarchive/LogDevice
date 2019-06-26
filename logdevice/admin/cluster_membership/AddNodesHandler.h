/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <folly/Optional.h>

#include "logdevice/admin/if/gen-cpp2/cluster_membership_types.h"
#include "logdevice/common/configuration/nodes/NodeIndicesAllocator.h"
#include "logdevice/common/configuration/nodes/NodesConfiguration.h"
#include "logdevice/common/types_internal.h"

namespace facebook { namespace logdevice { namespace admin {
namespace cluster_membership {

class AddNodesHandler {
 public:
  struct Result {
    // IDs of the added nodes
    std::vector<node_index_t> to_be_added{};

    // The NodesConfiguration update to apply
    configuration::nodes::NodesConfiguration::Update update{};
  };

  /**
   * This function builds a NodesConfiguration::Update to add nodes
   * defined with @param add_requests to the NodesConfiguration. The new nodes
   * are allocated IDs using the passed NodeIndiciesAllocator if they don't
   * specify a specific ID that they want to use.
   * The function can also return a failure, for multiple reasons, for example:
   * if the request is missing some required fields, doesn't pass config
   * validation, required ID is not unique, etc. The failure struct will contain
   * more information about the exact reason the node failed.
   */
  folly::Expected<Result, thrift::ClusterMembershipOperationFailed>
  buildNodesConfigurationUpdates(
      std::vector<thrift::AddSingleNodeRequest> add_requests,
      const configuration::nodes::NodesConfiguration&,
      configuration::nodes::NodeIndicesAllocator) const;

 private:
  // Validates the uniquness of indicies, names and addresses.
  folly::Optional<thrift::ClusterMembershipOperationFailed>
  validateUniquness(const std::vector<thrift::AddSingleNodeRequest>&,
                    const configuration::nodes::NodesConfiguration&) const;

  folly::Optional<thrift::ClusterMembershipFailedNode>
  buildUpdateFromNodeConfig(
      configuration::nodes::NodesConfiguration::Update& update,
      const thrift::NodeConfig&,
      const configuration::nodes::NodesConfiguration&) const;
};
}}}} // namespace facebook::logdevice::admin::cluster_membership
