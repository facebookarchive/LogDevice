/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "CopySetSelectorFactory.h"

#include "logdevice/common/CrossDomainCopySetSelector.h"
#include "logdevice/common/LinearCopySetSelector.h"
#include "logdevice/common/PassThroughCopySetManager.h"
#include "logdevice/common/StickyCopySetManager.h"
#include "logdevice/common/WeightedCopySetSelector.h"
#include "logdevice/common/Worker.h"

namespace facebook { namespace logdevice {

static StorageSet
getWritableShards(const StorageSet& ns,
                  const std::shared_ptr<ServerConfig> config) {
  return config->getNodesConfiguration()->getStorageMembership()->writerView(
      ns);
}

std::unique_ptr<CopySetSelector>
CopySetSelectorFactory::create(logid_t logid,
                               const EpochMetaData& epoch_metadata,
                               std::shared_ptr<NodeSetState> nodeset_state,
                               const std::shared_ptr<ServerConfig> config,
                               const logsconfig::LogAttributes* log_attrs,
                               const Settings& settings,
                               RNG& init_rng) {
  ld_check(nodeset_state != nullptr);
  ld_check(config != nullptr);

  auto legacy_replication = epoch_metadata.replication.toOldRepresentation();

  // If we have weights or a relatively complicated replication property,
  // use the new experimental copyset selector that supports that.
  if (!legacy_replication.hasValue() || !epoch_metadata.weights.empty() ||
      settings.weighted_copyset_selector) {
    bool locality_enabled =
        epoch_metadata.replication.getBiggestReplicationScope() >=
        settings.copyset_locality_min_scope;
    return std::make_unique<WeightedCopySetSelector>(logid,
                                                     epoch_metadata,
                                                     nodeset_state,
                                                     config,
                                                     log_attrs,
                                                     locality_enabled,
                                                     Worker::stats(),
                                                     init_rng);
  }

  if (legacy_replication->sync_replication_scope == NodeLocationScope::NODE ||
      legacy_replication->replication_factor == 1) {
    return std::make_unique<LinearCopySetSelector>(
        legacy_replication->replication_factor,
        getWritableShards(epoch_metadata.shards, config),
        nodeset_state);
  }

  // currently accept RACK, ROW, CLUSTER and REGION as failure domain
  // scopes
  ld_check(
      legacy_replication->sync_replication_scope == NodeLocationScope::RACK ||
      legacy_replication->sync_replication_scope == NodeLocationScope::ROW ||
      legacy_replication->sync_replication_scope ==
          NodeLocationScope::CLUSTER ||
      legacy_replication->sync_replication_scope == NodeLocationScope::REGION);
  return std::make_unique<CrossDomainCopySetSelector>(
      logid,
      getWritableShards(epoch_metadata.shards, config),
      nodeset_state,
      config,
      legacy_replication->replication_factor,
      legacy_replication->sync_replication_scope);
}

std::unique_ptr<CopySetManager> CopySetSelectorFactory::createManager(
    logid_t logid,
    const EpochMetaData& epoch_metadata,
    std::shared_ptr<NodeSetState> nodeset_state,
    std::shared_ptr<ServerConfig> config,
    const logsconfig::LogAttributes* log_attrs,
    const Settings& settings,
    bool sticky_copysets,
    size_t sticky_copysets_block_size,
    std::chrono::milliseconds sticky_copysets_block_max_time) {
  std::unique_ptr<CopySetSelector> copyset_selector =
      create(logid, epoch_metadata, nodeset_state, config, log_attrs, settings);
  std::unique_ptr<CopySetManager> res;
  if (sticky_copysets) {
    res = std::unique_ptr<CopySetManager>(
        new StickyCopySetManager(std::move(copyset_selector),
                                 nodeset_state,
                                 sticky_copysets_block_size,
                                 sticky_copysets_block_max_time));
  } else {
    res = std::unique_ptr<CopySetManager>(new PassThroughCopySetManager(
        std::move(copyset_selector), nodeset_state));
  }
  res->prepareConfigMatchCheck(epoch_metadata.shards, *config);
  return res;
}

}} // namespace facebook::logdevice
