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
  std::tuple<Decision, std::unique_ptr<StorageSet>> getStorageSet(
      logid_t log_id,
      const std::shared_ptr<Configuration>& cfg,
      const StorageSet* prev,
      const Options* options = nullptr /* ignored */
      ) override {
    const std::shared_ptr<LogsConfig::LogGroupNode> logcfg =
        cfg->getLogGroupByIDShared(log_id);
    if (!logcfg) {
      err = E::NOTFOUND;
      return std::make_tuple(Decision::FAILED, nullptr);
    }
    if (logcfg->attrs().nodeSetSize().value().hasValue()) {
      ld_error("nodeSetSize property set for log %lu, unable to select all "
               "shards",
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
        for (shard_index_t s = 0; s < num_shards; ++s) {
          indices->push_back(ShardID(it.first, s));
        }
      }
    }
    std::sort(indices->begin(), indices->end());
    if (prev && *prev == *indices) {
      return std::make_tuple(Decision::KEEP, nullptr);
    }
    return std::make_tuple(Decision::NEEDS_CHANGE, std::move(indices));
  }

  storage_set_size_t
  getStorageSetSize(logid_t /*log_id*/,
                    const std::shared_ptr<Configuration>& cfg,
                    folly::Optional<int> /*storage_set_size_target*/,
                    ReplicationProperty /*replication*/,
                    const Options* /*options*/ = nullptr) override {
    storage_set_size_t count = 0;
    for (const auto& it : cfg->serverConfig()->getNodes()) {
      if (it.second.isReadableStorageNode()) {
        count += it.second.getNumShards();
      }
    }
    return count;
  }
};

}} // namespace facebook::logdevice
