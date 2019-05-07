/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include "logdevice/common/Processor.h"
#include "logdevice/server/admincommands/AdminCommand.h"
#include "logdevice/server/locallogstore/RocksDBLogStoreBase.h"
#include "logdevice/server/locallogstore/ShardedRocksDBLocalLogStore.h"
#include "logdevice/server/storage_tasks/ShardedStorageThreadPool.h"

namespace facebook { namespace logdevice { namespace commands {

class InfoSST : public AdminCommand {
 public:
  void run() override {
    if (server_->getProcessor()->runningOnStorageNode()) {
      auto sharded_store = server_->getShardedLocalLogStore();
      shard_index_t shard_lo = 0;
      const shard_index_t shard_hi = sharded_store->numShards() - 1;
      ld_check(shard_lo <= shard_hi);

      for (; shard_lo <= shard_hi; ++shard_lo) {
        auto store = sharded_store->getByIndex(shard_lo);
        auto rocks_store = dynamic_cast<const RocksDBLogStoreBase*>(store);
        if (rocks_store == nullptr) {
          continue;
        }

        std::vector<rocksdb::LiveFileMetaData> metadata;
        rocks_store->getDB().GetLiveFilesMetaData(&metadata);

        // Sort sst files by level/size.
        auto compare = [](const rocksdb::LiveFileMetaData& a,
                          const rocksdb::LiveFileMetaData& b) -> bool {
          if (a.level == b.level) {
            return a.size < b.size;
          } else {
            return a.level < b.level;
          }
        };
        std::sort(metadata.begin(), metadata.end(), compare);

        out_.printf("Shard %d:\r\n", shard_lo);
        for (const auto& table : metadata) {
          out_.printf("[%i] %s%s (size: %lu)\r\n",
                      table.level,
                      table.db_path.c_str(),
                      table.name.c_str(),
                      table.size);
        }
      }
    }
  }
};

}}} // namespace facebook::logdevice::commands
