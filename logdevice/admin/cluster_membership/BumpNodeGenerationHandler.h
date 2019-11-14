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
#include "logdevice/common/ClusterState.h"
#include "logdevice/common/types_internal.h"

namespace facebook { namespace logdevice { namespace admin {
namespace cluster_membership {

class BumpNodeGenerationHandler {
 public:
  struct Result {
    std::vector<thrift::NodeID> to_be_bumped{};
    configuration::nodes::NodesConfiguration::Update update{};
  };

  Result buildNodesConfigurationUpdates(
      const std::vector<thrift::NodesFilter>&,
      const configuration::nodes::NodesConfiguration&) const;

 private:
  void buildNodesConfigurationUpdateForNode(
      configuration::nodes::NodesConfiguration::Update&,
      node_index_t node_idx,
      const configuration::nodes::NodesConfiguration& nodes_configuration)
      const;
};
}}}} // namespace facebook::logdevice::admin::cluster_membership
