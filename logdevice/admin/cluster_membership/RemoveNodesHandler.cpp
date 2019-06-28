/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "logdevice/admin/cluster_membership/RemoveNodesHandler.h"

#include "logdevice/admin/AdminAPIUtils.h"
#include "logdevice/admin/cluster_membership/ClusterMembershipUtils.h"
#include "logdevice/common/membership/StorageStateTransitions.h"
#include "logdevice/common/types_internal.h"

namespace facebook { namespace logdevice { namespace admin {
namespace cluster_membership {

using namespace facebook::logdevice::configuration::nodes;

folly::Expected<RemoveNodesHandler::Result,
                thrift::ClusterMembershipOperationFailed>
RemoveNodesHandler::buildNodesConfigurationUpdates(
    const std::vector<thrift::NodesFilter>& filters,
    const NodesConfiguration& nodes_configuration,
    const ClusterState& cluster_state) const {
  auto node_idxs = findNodes(filters, nodes_configuration);

  auto not_met =
      checkPreconditions(node_idxs, nodes_configuration, cluster_state);

  if (not_met.hasValue()) {
    return folly::makeUnexpected(std::move(not_met).value());
  }

  std::vector<thrift::NodeID> to_be_removed;
  NodesConfiguration::Update update;
  for (const auto& idx : node_idxs) {
    // TODO fill the returned NodeID
    to_be_removed.emplace_back(mkNodeID(idx));
    buildNodesConfigurationUpdateForNode(update, idx, nodes_configuration);
  }

  return RemoveNodesHandler::Result{
      std::move(to_be_removed), std::move(update)};
}

std::vector<node_index_t> RemoveNodesHandler::findNodes(
    const std::vector<thrift::NodesFilter>& filters,
    const NodesConfiguration& nodes_configuration) const {
  std::unordered_set<node_index_t> nodes;

  for (const auto& filter : filters) {
    forFilteredNodes(nodes_configuration, &filter, [&nodes](node_index_t idx) {
      nodes.emplace(idx);
    });
  }

  return {std::make_move_iterator(nodes.begin()),
          std::make_move_iterator(nodes.end())};
}

namespace {
folly::Optional<thrift::ClusterMembershipFailedNode>
check_is_disabled(const NodesConfiguration& nodes_configuration,
                  node_index_t idx) {
  bool seq_disabled = !nodes_configuration.isSequencerNode(idx) ||
      !nodes_configuration.getSequencerMembership()->isSequencerEnabledFlagSet(
          idx);

  bool storage_disabled = true;
  if (nodes_configuration.isStorageNode(idx)) {
    const auto& states =
        nodes_configuration.getStorageMembership()->getShardStates(idx);
    for (const auto& state : states) {
      storage_disabled &=
          state.second.storage_state == membership::StorageState::NONE;
    }
  }

  if (seq_disabled && storage_disabled) {
    return folly::none;
  }
  return buildNodeFailure(
      idx,
      thrift::ClusterMembershipFailureReason::NOT_DISABLED,
      folly::sformat(
          "N{} should have both sequencing and storage disabled before "
          "getting removed found: sequencer_enabled={} storage_enabled={}",
          idx,
          !seq_disabled,
          !storage_disabled));
};

folly::Optional<thrift::ClusterMembershipFailedNode>
check_is_dead(const ClusterState& cluster_state, node_index_t idx) {
  auto node_state = cluster_state.getNodeState(idx);
  if (node_state == ClusterState::NodeState::DEAD) {
    return folly::none;
  }

  return buildNodeFailure(
      idx,
      thrift::ClusterMembershipFailureReason::NOT_DEAD,
      folly::sformat("N{} must be DEAD before getting removed, but it was {}",
                     idx,
                     ClusterState::getNodeStateString(node_state)));
};

} // namespace

folly::Optional<thrift::ClusterMembershipOperationFailed>
RemoveNodesHandler::checkPreconditions(
    const std::vector<node_index_t>& node_idxs,
    const NodesConfiguration& nodes_configuration,
    const ClusterState& cluster_state) const {
  thrift::ClusterMembershipOperationFailed failure;
  for (const auto& idx : node_idxs) {
    if (auto fail = check_is_dead(cluster_state, idx); fail.hasValue()) {
      failure.failed_nodes.push_back(std::move(fail).value());
      continue;
    }

    if (auto fail = check_is_disabled(nodes_configuration, idx);
        fail.hasValue()) {
      failure.failed_nodes.push_back(std::move(fail).value());
      continue;
    }
  }
  if (failure.failed_nodes.empty()) {
    return folly::none;
  }
  return failure;
}

void RemoveNodesHandler::buildNodesConfigurationUpdateForNode(
    NodesConfiguration::Update& update,
    node_index_t node_idx,
    const NodesConfiguration& nodes_configuration) const {
  using namespace facebook::logdevice::membership;

  // Service Discovery Update
  if (update.service_discovery_update == nullptr) {
    update.service_discovery_update =
        std::make_unique<ServiceDiscoveryConfig::Update>();
  }
  update.service_discovery_update->addNode(
      node_idx, {ServiceDiscoveryConfig::UpdateType::REMOVE, nullptr});

  // Sequencer Config Update
  if (nodes_configuration.isSequencerNode(node_idx)) {
    // Sequencer Membership Update
    if (update.sequencer_config_update == nullptr) {
      update.sequencer_config_update =
          std::make_unique<SequencerConfig::Update>();
      update.sequencer_config_update->membership_update =
          std::make_unique<SequencerMembership::Update>(
              nodes_configuration.getSequencerMembership()->getVersion());
      update.sequencer_config_update->attributes_update =
          std::make_unique<SequencerAttributeConfig::Update>();
    }
    update.sequencer_config_update->membership_update->addNode(
        node_idx,
        {SequencerMembershipTransition::REMOVE_NODE,
         /* sequencer_enabled= */ false,
         /* weight= */ 0,
         /* active_maintenance= */ MaintenanceID::MAINTENANCE_NONE});

    // Sequencer Config
    update.sequencer_config_update->attributes_update->addNode(
        node_idx, {SequencerAttributeConfig::UpdateType::REMOVE, nullptr});
  }

  // Storage Config Update
  if (nodes_configuration.isStorageNode(node_idx)) {
    // Storage Membership Update
    if (update.storage_config_update == nullptr) {
      update.storage_config_update = std::make_unique<StorageConfig::Update>();
      update.storage_config_update->membership_update =
          std::make_unique<StorageMembership::Update>(
              nodes_configuration.getStorageMembership()->getVersion());
      update.storage_config_update->attributes_update =
          std::make_unique<StorageAttributeConfig::Update>();
    }

    auto shards =
        nodes_configuration.getStorageMembership()->getShardStates(node_idx);
    for (const auto& shard : shards) {
      update.storage_config_update->membership_update->addShard(
          ShardID(node_idx, shard.first),
          {StorageStateTransition::REMOVE_EMPTY_SHARD,
           Condition::NONE,
           MaintenanceID::MAINTENANCE_NONE});
    }

    // Storage Attributes Update
    update.storage_config_update->attributes_update->addNode(
        node_idx, {StorageAttributeConfig::UpdateType::REMOVE, nullptr});
  }
}

}}}} // namespace facebook::logdevice::admin::cluster_membership
