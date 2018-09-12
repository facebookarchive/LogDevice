/**
 * Copyright (c) 2017-present, Facebook, Inc.
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
// TODO (#T21664344): Weight calculation and propagation is not implemented
// at the moment. The idea is to move the weight calculation and adjustment
// from WeightedCopySetSelector into here.
class WeightAwareNodeSetSelector : public NodeSetSelector {
 public:
  using MapLogToShardFn = std::function<int(logid_t, shard_size_t)>;

  explicit WeightAwareNodeSetSelector(MapLogToShardFn map_log_to_shard,
                                      bool hash_flag)
      : mapLogToShard_(map_log_to_shard), consistentHashing_(hash_flag) {}

  std::tuple<Decision, std::unique_ptr<StorageSet>>
  getStorageSet(logid_t log_id,
                const std::shared_ptr<Configuration>& cfg,
                const StorageSet* prev,
                const Options* options = nullptr) override;

 private:
  MapLogToShardFn mapLogToShard_;
  bool consistentHashing_;
};

}} // namespace facebook::logdevice
