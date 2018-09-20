/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/common/NodeSetSelectorFactory.h"

#include "logdevice/common/LegacyLogToShard.h"
#include "logdevice/common/RandomCrossDomainNodeSetSelector.h"
#include "logdevice/common/RandomNodeSetSelector.h"
#include "logdevice/common/SelectAllNodeSetSelector.h"
#include "logdevice/common/SelectAllShardsNodeSetSelector.h"
#include "logdevice/common/WeightAwareNodeSetSelector.h"

namespace facebook { namespace logdevice {

// Function that NodeSetSelectors that select at most one shard per node can use
// to decide which shard to use.
// Prior to Flexible Log Sharding, all nodeset selectors would use
// getLegacyShardIndexForLog() which does not hash the log id and thus leads to
// poor distribution for certain configurations.
// TODO(T15517759): this function should not be used by a nodeset selector in
// production until FLS is fully implemented.
static int getShardIndexForLog(logid_t log_id, shard_size_t num_shards) {
  return folly::hash::twang_mix64(log_id.val_) % num_shards;
}

std::unique_ptr<NodeSetSelector>
NodeSetSelectorFactory::create(NodeSetSelectorType type) {
  switch (type) {
    case NodeSetSelectorType::SELECT_ALL:
      return std::make_unique<SelectAllNodeSetSelector>();
    case NodeSetSelectorType::SELECT_ALL_SHARDS:
      return std::make_unique<SelectAllShardsNodeSetSelector>();
    case NodeSetSelectorType::RANDOM:
      return std::make_unique<RandomNodeSetSelector>(getLegacyShardIndexForLog);
    case NodeSetSelectorType::RANDOM_CROSSDOMAIN:
      return std::make_unique<RandomCrossDomainNodeSetSelector>(
          getLegacyShardIndexForLog);
    case NodeSetSelectorType::WEIGHT_AWARE:
      return std::make_unique<WeightAwareNodeSetSelector>(
          getLegacyShardIndexForLog, false);
    case NodeSetSelectorType::RANDOM_V2:
      return std::make_unique<RandomNodeSetSelector>(getShardIndexForLog);
    case NodeSetSelectorType::RANDOM_CROSSDOMAIN_V2:
      return std::make_unique<RandomCrossDomainNodeSetSelector>(
          getShardIndexForLog);
    case NodeSetSelectorType::WEIGHT_AWARE_V2:
      return std::make_unique<WeightAwareNodeSetSelector>(
          getShardIndexForLog, false);
    case NodeSetSelectorType::CONSISTENT_HASHING:
      return std::make_unique<WeightAwareNodeSetSelector>(
          getLegacyShardIndexForLog, true);
    case NodeSetSelectorType::CONSISTENT_HASHING_V2:
      return std::make_unique<WeightAwareNodeSetSelector>(
          getShardIndexForLog, true);
    case NodeSetSelectorType::INVALID:
      break;
  }

  ld_check(false);
  return nullptr;
}

}} // namespace facebook::logdevice
