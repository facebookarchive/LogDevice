/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include "logdevice/common/NodeSetSelector.h"

namespace facebook { namespace logdevice {

// A NodeSetSelector that plays nice with weights and WeightedCopySetSelector.
// It doesn't actually use the weights when selecting nodeset, but it decides
// what weights copyset selector should use.
//
// Handling of temporarily disabled (unwritable) shards:
// If there are temporarily disabled (unwritable) shards in config,
// WeightAwareNodeSetSelector may pick some of them,
// but only _in addition_ to a full-size nodeset of writable nodes.
// E.g. if a rack is unwritable, we'll pick a nodeset as if that rack didn't
// exist, then add a few nodes from the rack, ending up with a nodeset bigger
// than target_size. (In actual code these two steps happen in reverse order.)
// Picking unwritable shards is needed to allow these shards to receive
// rebuilding writes after they become available again. Picking a full-size
// nodeset of writable shards is needed to make sure the data distribution is
// good while the unwritable shards are unwritable.
//
// TODO (#T21664344): Weight calculation and propagation is not implemented
// at the moment. The idea is to move the weight calculation and adjustment
// from WeightedCopySetSelector into here.
class WeightAwareNodeSetSelector : public NodeSetSelector {
 public:
  using MapLogToShardFn = std::function<int(logid_t, shard_size_t)>;

  explicit WeightAwareNodeSetSelector(MapLogToShardFn map_log_to_shard,
                                      bool hash_flag)
      : mapLogToShard_(map_log_to_shard), consistentHashing_(hash_flag) {}

  Result getStorageSet(
      logid_t log_id,
      const Configuration* cfg,
      const configuration::nodes::NodesConfiguration& nodes_configuration,
      nodeset_size_t target_nodeset_size,
      uint64_t seed,
      const EpochMetaData* prev,
      const Options* options = nullptr) override;

 private:
  MapLogToShardFn mapLogToShard_;
  bool consistentHashing_;
};

}} // namespace facebook::logdevice
