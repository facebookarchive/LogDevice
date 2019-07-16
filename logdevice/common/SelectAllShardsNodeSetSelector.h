/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <memory>

#include <folly/Memory.h>

#include "logdevice/common/NodeSetSelector.h"
#include "logdevice/common/debug.h"
#include "logdevice/include/types.h"

namespace facebook { namespace logdevice {

/**
 * @file A NodeSetSelector thats select all shards in the cluster config.
 * This NodeSetSelector is used for testing Flexible Log Sharding and should not
 * be used in production.
 */

class SelectAllShardsNodeSetSelector : public NodeSetSelector {
 public:
  Result getStorageSet(
      logid_t log_id,
      const Configuration* cfg,
      const configuration::nodes::NodesConfiguration& nodes_configuration,
      nodeset_size_t target_nodeset_size,
      uint64_t seed,
      const EpochMetaData* prev,
      const Options* options = nullptr /* ignored */
      ) override {
    Result res;
    const std::shared_ptr<LogsConfig::LogGroupNode> logcfg =
        cfg->getLogGroupByIDShared(log_id);
    if (!logcfg) {
      res.decision = Decision::FAILED;
      return res;
    }
    if (logcfg->attrs().nodeSetSize().value().hasValue()) {
      ld_error("nodeSetSize property set for log %lu, unable to select all "
               "shards",
               log_id.val_);
      res.decision = Decision::FAILED;
      return res;
    }

    const auto& membership = nodes_configuration.getStorageMembership();
    for (const auto node : *membership) {
      if ((!options || !options->exclude_nodes.count(node))) {
        auto num_shards = nodes_configuration.getNumShards(node);
        for (shard_index_t s = 0; s < num_shards; ++s) {
          ShardID shard(node, s);
          if (membership->shouldReadFromShard(shard)) {
            res.storage_set.push_back(shard);
          }
        }
      }
    }

    std::sort(res.storage_set.begin(), res.storage_set.end());
    res.decision = (prev && prev->shards == res.storage_set)
        ? Decision::KEEP
        : Decision::NEEDS_CHANGE;
    return res;
  }
};

}} // namespace facebook::logdevice
