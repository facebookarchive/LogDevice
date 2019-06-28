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
#include "logdevice/common/configuration/nodes/NodesConfiguration.h"
#include "logdevice/common/types_internal.h"

namespace facebook { namespace logdevice { namespace admin {
namespace cluster_membership {

class UpdateNodesHandler {
 public:
  struct Result {
    std::vector<node_index_t> to_be_updated{};
    configuration::nodes::NodesConfiguration::Update update{};
  };

  folly::Expected<Result, thrift::ClusterMembershipOperationFailed>
  buildNodesConfigurationUpdates(
      const std::vector<thrift::UpdateSingleNodeRequest>&,
      const configuration::nodes::NodesConfiguration&) const;

 private:
  folly::Expected<node_index_t, thrift::ClusterMembershipFailedNode>
  buildUpdateFromNodeConfig(
      configuration::nodes::NodesConfiguration::Update& update,
      const thrift::UpdateSingleNodeRequest&,
      const configuration::nodes::NodesConfiguration&) const;
};
}}}} // namespace facebook::logdevice::admin::cluster_membership
