/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <chrono>
#include <memory>
#include <numeric>
#include <random>

#include <folly/Memory.h>

#include "logdevice/common/NodeSetSelector.h"
#include "logdevice/common/configuration/Configuration.h"
#include "logdevice/include/types.h"

namespace facebook { namespace logdevice {

/**
 * @file RandomNodeSetSelector selects nodeset randomly. The total nodes to
 * select and the number of nodes to pick are from Configuration.
 */

class RandomNodeSetSelector : public NodeSetSelector {
 public:
  // A function to map a log id to a shard offset on a node given the number of
  // shards on that node.
  using MapLogToShardFn = std::function<int(logid_t, shard_size_t)>;

  explicit RandomNodeSetSelector(MapLogToShardFn map_log_to_shard)
      : rnd_(std::random_device{}()), map_log_to_shard_(map_log_to_shard) {}

  Result getStorageSet(
      logid_t log_id,
      const Configuration* cfg,
      const configuration::nodes::NodesConfiguration& nodes_configuration,
      nodeset_size_t target_nodeset_size,
      uint64_t seed,
      const EpochMetaData* prev,
      const Options* options = nullptr) override;

 protected:
  // randomly select a nodeset of size @nodeset_size from a pool of candidate
  // nodes @eligible_nodes
  std::unique_ptr<StorageSet> randomlySelectNodes(
      logid_t log_id,
      const Configuration* config,
      const configuration::nodes::NodesConfiguration& nodes_configuration,
      const NodeSetIndices& eligible_nodes,
      size_t nodeset_size,
      const Options* options);

 private:
  std::default_random_engine rnd_;
  MapLogToShardFn map_log_to_shard_;

  storage_set_size_t getStorageSetSize(
      logid_t log_id,
      const configuration::nodes::NodesConfiguration& nodes_configuration,
      nodeset_size_t target_nodeset_size,
      ReplicationProperty replication,
      const Options* options = nullptr);
};

}} // namespace facebook::logdevice
