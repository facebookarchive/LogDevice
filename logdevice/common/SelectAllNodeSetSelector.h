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

#include "logdevice/common/LegacyLogToShard.h"
#include "logdevice/common/NodeSetSelector.h"
#include "logdevice/common/debug.h"
#include "logdevice/include/types.h"

namespace facebook { namespace logdevice {

/**
 * @file A trivial NodeSetSelector thats select all nodes in the cluster
 *       config. Can be used to simulate behavior of the orginal logdevice
 *       that does not have nodeset support. Requires nodeSetSize in Log
 *       configuration not set.
 */

class SelectAllNodeSetSelector : public NodeSetSelector {
 public:
  std::tuple<Decision, std::unique_ptr<StorageSet>> getStorageSet(
      logid_t log_id,
      const std::shared_ptr<Configuration>& cfg,
      const StorageSet* prev,
      const Options* options = nullptr /* ignored */
      ) override {
    const LogsConfig::LogGroupNode* logcfg = cfg->getLogGroupByIDRaw(log_id);
    if (!logcfg) {
      err = E::NOTFOUND;
      return std::make_tuple(Decision::FAILED, nullptr);
    }
    if (logcfg->attrs().nodeSetSize().value().hasValue()) {
      ld_error("nodeSetSize property set for log %lu, unable to select all "
               "nodes",
               log_id.val_);
      err = E::FAILED;
      return std::make_tuple(Decision::FAILED, nullptr);
    }

    auto indices = std::make_unique<StorageSet>();
    for (const auto& it : cfg->serverConfig()->getNodes()) {
      if ((!options || !options->exclude_nodes.count(it.first)) &&
          it.second.isReadableStorageNode()) {
        auto num_shards = it.second.getNumShards();
        ld_check(num_shards > 0);
        shard_index_t shard_idx = getLegacyShardIndexForLog(log_id, num_shards);
        indices->push_back(ShardID(it.first, shard_idx));
      }
    }
    std::sort(indices->begin(), indices->end());
    if (prev && *prev == *indices) {
      return std::make_tuple(Decision::KEEP, nullptr);
    }
    return std::make_tuple(Decision::NEEDS_CHANGE, std::move(indices));
  }

  storage_set_size_t
  getStorageSetSize(logid_t,
                    const std::shared_ptr<Configuration>& cfg,
                    folly::Optional<int> /*storage_set_size_target*/,
                    ReplicationProperty /*replication*/,
                    const Options* /*options*/ = nullptr) override {
    storage_set_size_t count = 0;
    for (const auto& it : cfg->serverConfig()->getNodes()) {
      if (it.second.isReadableStorageNode()) {
        ++count;
      }
    }
    return count;
  }
};

}} // namespace facebook::logdevice
