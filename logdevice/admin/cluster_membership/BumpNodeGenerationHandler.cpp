/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "logdevice/admin/cluster_membership/BumpNodeGenerationHandler.h"

#include "logdevice/admin/AdminAPIUtils.h"
#include "logdevice/admin/cluster_membership/ClusterMembershipUtils.h"
#include "logdevice/common/types_internal.h"

namespace facebook { namespace logdevice { namespace admin {
namespace cluster_membership {

using namespace facebook::logdevice::configuration::nodes;

BumpNodeGenerationHandler::Result
BumpNodeGenerationHandler::buildNodesConfigurationUpdates(
    const std::vector<thrift::NodesFilter>& filters,
    const NodesConfiguration& nodes_configuration) const {
  auto node_idxs = allMatchingNodesFromFilters(nodes_configuration, filters);

  std::vector<thrift::NodeID> to_be_bumped;
  NodesConfiguration::Update update;
  for (const auto& idx : node_idxs) {
    to_be_bumped.emplace_back(mkNodeID(idx));
    buildNodesConfigurationUpdateForNode(update, idx, nodes_configuration);
  }

  return BumpNodeGenerationHandler::Result{
      std::move(to_be_bumped), std::move(update)};
}

void BumpNodeGenerationHandler::buildNodesConfigurationUpdateForNode(
    NodesConfiguration::Update& update,
    node_index_t node_idx,
    const NodesConfiguration& nodes_configuration) const {
  auto node_attribute = nodes_configuration.getNodeStorageAttribute(node_idx);
  if (node_attribute != nullptr) {
    if (update.storage_config_update == nullptr) {
      update.storage_config_update = std::make_unique<StorageConfig::Update>();
      update.storage_config_update->attributes_update =
          std::make_unique<StorageAttributeConfig::Update>();
    }

    auto new_attributes =
        std::make_unique<StorageNodeAttribute>(*node_attribute);
    new_attributes->generation++;

    update.storage_config_update->attributes_update->addNode(
        node_idx,
        {StorageAttributeConfig::UpdateType::RESET, std::move(new_attributes)});
  }
}

}}}} // namespace facebook::logdevice::admin::cluster_membership
