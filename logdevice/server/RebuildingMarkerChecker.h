/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include "logdevice/common/configuration/nodes/NodesConfigurationAPI.h"
#include "logdevice/common/membership/StorageMembership.h"
#include "logdevice/common/membership/types.h"
#include "logdevice/server/RebuildingSupervisor.h"
#include "logdevice/server/locallogstore/ShardedRocksDBLocalLogStore.h"

namespace facebook { namespace logdevice {

/**
 * @file RebuildingMarkerChecker checks the rebuilding markers
 * (RebuildingCompleteMetadata) in the local store and returns the status of
 * each shard.
 * The algorithm is as follows:
 *
 * If this node's generation is 1 or the shard is in PROVISIONING state, write
 * RebuildingCheckpointMetadata for each shard and mark it as not missing data.
 * Also, transition the node out of the PROVISIONING state to NONE if NCM is
 * enabled.
 *
 * If this node's generation is >1, try and retrieve the
 * RebuildingCheckpointMetadata for each shard. For each shard:
 * - If the marker does exist, mark the shard as not missing data.
 * - If the marker does not exist, we mark it as missing data.
 *   Once it is rebuilt, we will write the RebuildingCheckpointMetadata before
 *   we send SHARD_ACK_REBUILT in the event log.
 *
 * @return 0 on success, or -1 on failure.
 */
class RebuildingMarkerChecker {
 public:
  enum class CheckResult {
    /* Shard is a new shard or had the rebuilding marker */
    SHARD_NOT_MISSING_DATA = 0,
    /* The shard's store is disabled, we can't confirm whether it's missing data
       or not */
    SHARD_DISABLED = 1,
    /* The shard is not a new shard, and it doesn't have the rebuilding marker
     */
    SHARD_MISSING_DATA = 2,
    /* We tried reading/writing to the shard, but we got an error. The
       underlying store got disabled so it won't be accepting reads/writes. */
    SHARD_ERROR = 3,
    /* We got an unexpected behavior from the underlying store. This server
       should terminate. */
    UNEXPECTED_ERROR = 4,
  };

  RebuildingMarkerChecker(
      const std::unordered_map<shard_index_t, membership::ShardState>& shards,
      NodeID my_node_id,
      configuration::NodesConfigurationAPI* nc_api,
      ShardedLocalLogStore* sharded_store);

  folly::Expected<std::unordered_map<shard_index_t, CheckResult>, E>
  checkAndWriteMarkers();

 private:
  /**
   * True if the node's generation <= 1 or the shard is PROVISIONING.
   */
  bool isNewShard(shard_index_t shard) const;

  CheckResult writeMarker(shard_index_t shard);
  CheckResult readMarker(shard_index_t shard);

  /**
   * For all the shards that are not missing data and are still PROVISIONING,
   * transition them out to NONE storage state.
   */
  Status
  markShardsAsProvisioned(const std::unordered_set<shard_index_t>& shards);

 private:
  // Shards to checkm
  std::unordered_map<shard_index_t, membership::ShardState> shards_;
  NodeID my_node_id_;
  // The NCAPI to apply the NodesConfiguration update. The update is ignored
  // if it's a nullptr (meaning that the NCM is disabled).
  // If the NCAPI is set, it has to outlive the RebuildingMarkerChecker.
  configuration::NodesConfigurationAPI* const nc_api_;
  // The ShardedLocalLogStore has to outlive the RebuildingMarkerChecker.
  ShardedLocalLogStore* const sharded_store_;
};
}} // namespace facebook::logdevice
