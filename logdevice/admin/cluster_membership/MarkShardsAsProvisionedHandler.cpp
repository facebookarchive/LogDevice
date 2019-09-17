/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "logdevice/admin/cluster_membership/MarkShardsAsProvisionedHandler.h"

#include "logdevice/admin/AdminAPIUtils.h"
#include "logdevice/admin/cluster_membership/ClusterMembershipUtils.h"
#include "logdevice/admin/if/gen-cpp2/cluster_membership_constants.h"
#include "logdevice/common/membership/StorageStateTransitions.h"
#include "logdevice/common/types_internal.h"

namespace facebook { namespace logdevice { namespace admin {
namespace cluster_membership {

using namespace facebook::logdevice::configuration::nodes;

MarkShardsAsProvisionedHandler::Result
MarkShardsAsProvisionedHandler::buildNodesConfigurationUpdates(
    const thrift::ShardSet& thrift_shards,
    const configuration::nodes::NodesConfiguration& nodes_configuration) const {
  ShardSet shards = expandShardSet(thrift_shards, nodes_configuration);

  if (shards.empty()) {
    return Result{};
  }

  NodesConfiguration::Update update;
  update.storage_config_update = std::make_unique<StorageConfig::Update>();
  update.storage_config_update->membership_update =
      std::make_unique<membership::StorageMembership::Update>(
          nodes_configuration.getStorageMembership()->getVersion());

  for (const auto& shard : shards) {
    update.storage_config_update->membership_update->addShard(
        shard,
        {
            membership::StorageStateTransition::MARK_SHARD_PROVISIONED,
            membership::Condition::FORCE,
        });
  }

  return Result{std::move(shards), std::move(update)};
}
}}}} // namespace facebook::logdevice::admin::cluster_membership
