/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <folly/container/F14Set.h>

#include "logdevice/admin/safety/LogMetaDataFetcher.h"
#include "logdevice/admin/safety/SafetyAPI.h"
#include "logdevice/common/ClusterState.h"
#include "logdevice/common/ShardAuthoritativeStatusMap.h"
#include "logdevice/common/configuration/LogsConfig.h"
#include "logdevice/common/configuration/MetaDataLogsConfig.h"
#include "logdevice/common/configuration/nodes/NodesConfiguration.h"

namespace facebook { namespace logdevice { namespace safety {

/**
 * Performs safety check on given logs
 */
folly::Expected<Impact, Status> checkImpactOnLogs(
    const std::vector<logid_t>& log_ids,
    const std::shared_ptr<LogMetaDataFetcher::Results>& metadata,
    const ShardAuthoritativeStatusMap& shard_status,
    const ShardSet& op_shards,
    const folly::F14FastSet<node_index_t>& sequencers,
    configuration::StorageState target_storage_state,
    const SafetyMargin& safety_margin,
    bool internal_logs,
    bool abort_on_error,
    size_t error_sample_size,
    const std::shared_ptr<const configuration::nodes::NodesConfiguration>&
        nodes_config,
    ClusterState* cluster_state);
/**
 * Perform safety check on a single log.
 */
folly::Expected<Impact::ImpactOnEpoch, Status> checkImpactOnLog(
    logid_t log_id,
    const std::shared_ptr<LogMetaDataFetcher::Results>& metadata,
    const ShardAuthoritativeStatusMap& shard_status,
    const ShardSet& op_shards,
    const folly::F14FastSet<node_index_t>& sequencers,
    configuration::StorageState target_storage_state,
    const SafetyMargin& safety_margin,
    const std::shared_ptr<const configuration::nodes::NodesConfiguration>&
        nodes_config,
    ClusterState* cluster_state);

/**
 * Checks whether a node is alive in the FailureDetector (gossip) or not.
 */
bool isAlive(ClusterState* cluster_state,
             node_index_t index,
             bool require_fully_started);

/**
 * Validates the storage set of the metadata log for a given data log.
 */
Impact checkMetadataStorageSet(
    const ShardAuthoritativeStatusMap& shard_status,
    const ShardSet& op_shards,
    const folly::F14FastSet<node_index_t>& sequencers,
    configuration::StorageState target_storage_state,
    const SafetyMargin& safety_margin,
    const std::shared_ptr<const configuration::nodes::NodesConfiguration>&
        nodes_config,
    ClusterState* cluster_state,
    size_t error_sample_size);

/**
 * Returns (safe_for_reads, safe_for_write) pair.
 */
std::pair<bool, bool> checkReadWriteAvailablity(
    const ShardAuthoritativeStatusMap& shard_status,
    const ShardSet& op_shards,
    const StorageSet& storage_set,
    configuration::StorageState target_storage_state,
    const ReplicationProperty& replication_property,
    const SafetyMargin& safety_margin,
    const std::shared_ptr<const configuration::nodes::NodesConfiguration>&
        nodes_config,
    ClusterState* cluster_state,
    bool require_fully_started_nodes);

/**
 * Create modified ReplicationProperty which takes into account Safety Margin.
 * For write check (canDrain) we should add it, for read check (isFmajority)
 * we should subtract
 **/
ReplicationProperty
extendReplicationWithSafetyMargin(const SafetyMargin& safety_margin,
                                  const ReplicationProperty& replication_base,
                                  bool add);

Impact::StorageSetMetadata getStorageSetMetadata(
    const StorageSet& storage_set,
    const ShardAuthoritativeStatusMap& shard_status,
    const std::shared_ptr<const configuration::nodes::NodesConfiguration>&
        nodes_config,
    ClusterState* cluster_state,
    bool require_fully_started);

}}} // namespace facebook::logdevice::safety
