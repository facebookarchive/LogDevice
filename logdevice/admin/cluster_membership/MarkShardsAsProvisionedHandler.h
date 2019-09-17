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

class MarkShardsAsProvisionedHandler {
 public:
  struct Result {
    // IDs of the updated shards
    ShardSet updated_shards{};

    // The NodesConfiguration update to apply
    configuration::nodes::NodesConfiguration::Update update{};
  };

  /**
   * For each shard in @shards, build a MARK_SHARD_PROVISIONED storage state
   * transition.
   */
  Result buildNodesConfigurationUpdates(
      const thrift::ShardSet& shards,
      const configuration::nodes::NodesConfiguration&) const;
};
}}}} // namespace facebook::logdevice::admin::cluster_membership
