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
#include "logdevice/admin/if/gen-cpp2/exceptions_types.h"
#include "logdevice/common/configuration/nodes/NodeIndicesAllocator.h"
#include "logdevice/common/configuration/nodes/NodesConfiguration.h"
#include "logdevice/common/types_internal.h"

namespace facebook { namespace logdevice { namespace admin {
namespace cluster_membership {

class BootstrapClusterHandler {
 public:
  struct Result {
    // The NodesConfiguration update to apply
    configuration::nodes::NodesConfiguration::Update update{};
  };

  folly::Expected<Result, thrift::OperationError>
  buildNodesConfigurationUpdates(
      const thrift::BootstrapClusterRequest&,
      const configuration::nodes::NodesConfiguration&) const;

 private:
  configuration::nodes::NodesConfiguration::Update
  buildNodesConfigurationUpdatesImpl(
      const configuration::nodes::NodesConfiguration&,
      ReplicationProperty metadata_replication_property,
      std::set<node_index_t> metadata_nodeset) const;
};
}}}} // namespace facebook::logdevice::admin::cluster_membership
