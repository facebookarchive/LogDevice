/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "logdevice/admin/cluster_membership/BootstrapClusterHandler.h"

#include "logdevice/admin/AdminAPIUtils.h"
#include "logdevice/admin/Conv.h"
#include "logdevice/admin/MetadataNodesetSelector.h"
#include "logdevice/admin/cluster_membership/ClusterMembershipUtils.h"
#include "logdevice/admin/if/gen-cpp2/cluster_membership_constants.h"
#include "logdevice/common/membership/StorageStateTransitions.h"
#include "logdevice/common/types_internal.h"

namespace facebook { namespace logdevice { namespace admin {
namespace cluster_membership {

using namespace facebook::logdevice::configuration::nodes;

folly::Expected<BootstrapClusterHandler::Result, thrift::OperationError>
BootstrapClusterHandler::buildNodesConfigurationUpdates(
    const thrift::BootstrapClusterRequest& req,
    const configuration::nodes::NodesConfiguration& nc) const {
  if (!nc.getSequencerMembership()->isBootstrapping() ||
      !nc.getStorageMembership()->isBootstrapping()) {
    return folly::makeUnexpected(
        thrift::OperationError("The cluster is already bootstrapped."));
  }

  auto metadata_replication_property =
      toLogDevice<ReplicationProperty, thrift::ReplicationProperty>(
          req.get_metadata_replication_property());

  // Create a dummy NC without a metadata nodeset to pass to the nodeset
  // generator.
  auto dummy_nc = nc.applyUpdate(buildNodesConfigurationUpdatesImpl(
      nc, metadata_replication_property, {}));
  auto metadata_nodeset = MetadataNodeSetSelector::getNodeSet(dummy_nc, {});
  if (metadata_nodeset.hasError()) {
    return folly::makeUnexpected(thrift::OperationError(
        folly::sformat("Failed to generate metadata nodeset: {}",
                       error_description(metadata_nodeset.error()))));
  }

  return Result{buildNodesConfigurationUpdatesImpl(
      nc, metadata_replication_property, *metadata_nodeset)};
}

configuration::nodes::NodesConfiguration::Update
BootstrapClusterHandler::buildNodesConfigurationUpdatesImpl(
    const configuration::nodes::NodesConfiguration& nc,
    ReplicationProperty metadata_replication_property,
    std::set<node_index_t> metadata_nodeset) const {
  const auto& storage_mem = nc.getStorageMembership();
  const auto& sequencer_mem = nc.getSequencerMembership();

  NodesConfiguration::Update update;

  // Finalize bootstrapping of sequencers
  update.sequencer_config_update = std::make_unique<SequencerConfig::Update>();
  update.sequencer_config_update->membership_update =
      std::make_unique<membership::SequencerMembership::Update>(
          sequencer_mem->getVersion());
  update.sequencer_config_update->membership_update->finalizeBootstrapping();

  // Finalize bootstrapping of stroage
  update.storage_config_update = std::make_unique<StorageConfig::Update>();
  update.storage_config_update->membership_update =
      std::make_unique<membership::StorageMembership::Update>(
          storage_mem->getVersion());
  update.storage_config_update->membership_update->finalizeBootstrapping();

  // Set replication property
  update.metadata_logs_rep_update =
      std::make_unique<MetaDataLogsReplication::Update>(
          nc.getMetaDataLogsReplication()->getVersion());
  update.metadata_logs_rep_update->replication =
      std::move(metadata_replication_property);

  // Enable all the sequencer and shars that are in NONE state (aka finished
  // provisioned)
  for (const auto& [idx, node] : *nc.getServiceDiscovery()) {
    if (node.hasRole(NodeRole::SEQUENCER)) {
      // Only enable disabled sequencers. Although in reality, all the
      // sequencers will be disabled while bootstrapping, but let's not fail
      // the whole update because of that.
      if (!sequencer_mem->getNodeState(idx)->sequencer_enabled) {
        update.sequencer_config_update->membership_update->addNode(
            idx,
            {membership::SequencerMembershipTransition::SET_ENABLED_FLAG,
             /* enabled= */
             true,
             // Setting the weight here doesn't matter because SET_ENABLED_FLAG
             // doesn't touch the weight
             /* weight= */ 0});
      }
    }

    if (node.hasRole(NodeRole::STORAGE)) {
      for (const auto& [shard, state] : storage_mem->getShardStates(idx)) {
        // Only enabled nodes in NONE state. Ignore PROVISIONING shards as they
        // are still not ready.
        if (state.storage_state == membership::StorageState::NONE) {
          update.storage_config_update->membership_update->addShard(
              ShardID(idx, shard),
              {metadata_nodeset.find(idx) == metadata_nodeset.end()
                   ? membership::StorageStateTransition::BOOTSTRAP_ENABLE_SHARD
                   : membership::StorageStateTransition::
                         BOOTSTRAP_ENABLE_METADATA_SHARD,
               membership::Condition::FORCE});
        }
      }
    }
  }

  return update;
}
}}}} // namespace facebook::logdevice::admin::cluster_membership
