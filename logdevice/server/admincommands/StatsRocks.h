/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <rocksdb/cache.h>

#include "logdevice/server/ServerProcessor.h"
#include "logdevice/server/admincommands/AdminCommand.h"
#include "logdevice/server/locallogstore/PartitionedRocksDBStore.h"
#include "logdevice/server/locallogstore/RocksDBLogStoreBase.h"
#include "logdevice/server/storage_tasks/ShardedStorageThreadPool.h"

namespace facebook { namespace logdevice { namespace commands {

class StatsRocks : public AdminCommand {
 private:
  shard_index_t shard_ = -1;
  bool full_;

 public:
  void getOptions(
      boost::program_options::options_description& out_options) override {
    out_options.add_options()(
        "shard", boost::program_options::value<shard_index_t>(&shard_))(
        "full", boost::program_options::bool_switch(&full_));
  }
  void getPositionalOptions(
      boost::program_options::positional_options_description& out_options)
      override {
    out_options.add("shard", 1);
  }
  std::string getUsage() override {
    return "stats rocksdb [<shard>] [--full]";
  }

  void run() override {
    if (server_->getProcessor()->runningOnStorageNode()) {
      auto sharded_pool =
          server_->getServerProcessor()->sharded_storage_thread_pool_;

      shard_index_t shard_lo = 0;
      shard_index_t shard_hi = sharded_pool->numShards() - 1;
      ld_check(shard_lo <= shard_hi);

      if (shard_ < -1 || shard_ > shard_hi) {
        out_.printf("Invalid shard for `stats rocksdb: %d'\r\n", shard_);
        return;
      }
      if (shard_ != -1) {
        shard_lo = shard_hi = shard_;
      }

      // Dump info about RocksDB's block cache memory usage. Block cache is
      // shared by all shards.
      printBlockCacheStats();

      for (; shard_lo <= shard_hi; ++shard_lo) {
        auto& store = sharded_pool->getByIndex(shard_lo).getLocalLogStore();
        auto rocks_store = dynamic_cast<const RocksDBLogStoreBase*>(&store);
        if (rocks_store == nullptr) {
          continue;
        }

        if (full_) {
          printStatsRocksFull(rocks_store, shard_lo);
        } else {
          printStatsRocks(rocks_store, shard_lo);
        }
      }
    }
  }

 private:
  void printStatsRocksFull(const RocksDBLogStoreBase* store,
                           shard_index_t shard) {
    out_.printf("Shard %d:\r\n", shard);
    out_.printf("%s\r\n", store->getStats().c_str());
  }

  void printStatsRocks(const RocksDBLogStoreBase* store, shard_index_t shard) {
    for (const auto& ticker : rocksdb::TickersNameMap) {
      const uint64_t val = store->getStatsTickerCount(ticker.first);
      out_.printf(
          "STAT %s.shard%i %" PRId64 "\r\n", ticker.second.c_str(), shard, val);
    }

    std::vector<PartitionedRocksDBStore::PartitionPtr> partitions;

    if (auto partitioned_store =
            dynamic_cast<const PartitionedRocksDBStore*>(store)) {
      partitions.push_back(partitioned_store->getLatestPartition());
      PartitionedRocksDBStore::PartitionPtr second_partition;
      ld_check(partitions[0]->id_ > 0);
      if (partitioned_store->getPartition(
              partitions[0]->id_ - 1, &second_partition)) {
        partitions.push_back(second_partition);
      }
    } else {
      partitions.push_back(nullptr);
    }

    for (size_t i = 0; i < partitions.size(); ++i) {
      auto partition = partitions[i];
      rocksdb::ColumnFamilyHandle* cf = partition
          ? partition->cf_->get()
          : store->getDB().DefaultColumnFamily();

      std::vector<std::string> properties{
          "rocksdb.cur-size-active-mem-table",
          "rocksdb.num-immutable-mem-table",
      };

      int levels = partition == nullptr ? store->getDB().NumberLevels() : 1;
      for (int level = 0; level < levels; ++level) {
        std::string prop_name =
            "rocksdb.num-files-at-level" + folly::to<std::string>(level);
        properties.push_back(prop_name);
      }

      for (const std::string& property : properties) {
        std::string val;
        if (!store->getDB().GetProperty(cf, property, &val)) {
          ld_error("Cannot find property %s", property.c_str());
          continue;
        }
        std::string name = property;
        if (partition != nullptr) {
          name += ".partition-" + std::to_string(i + 1);
        }
        out_.printf("STAT %s.shard%i %s\r\n", name.c_str(), shard, val.c_str());
      }
    }
  }

  void printBlockCacheStats() {
    // Extract rocksdb::BlockBasedTableOptions from a LocalLogStore. All shards
    // share the same block cache, so it doesn't matter which one we choose.
    auto pool = server_->getServerProcessor()->sharded_storage_thread_pool_;
    auto store = dynamic_cast<const RocksDBLogStoreBase*>(
        &pool->getByIndex(0).getLocalLogStore());
    if (!store) {
      return;
    }
    auto& table_options = store->getRocksDBLogStoreConfig().table_options_;
    auto& metadata_table_options =
        store->getRocksDBLogStoreConfig().metadata_table_options_;

    printCacheUsage(table_options.block_cache.get(), "block_cache");
    printCacheUsage(
        table_options.block_cache_compressed.get(), "block_cache_compressed");
    printCacheUsage(
        metadata_table_options.block_cache.get(), "metadata_block_cache");
  }

  void printCacheUsage(rocksdb::Cache* cache, const std::string& key) {
    if (cache) {
      out_.printf("STAT rocksdb.%s.capacity %zu\r\n",
                  key.c_str(),
                  cache->GetCapacity());
      out_.printf(
          "STAT rocksdb.%s.usage %zu\r\n", key.c_str(), cache->GetUsage());
      // Cache::GetPinnedUsage() was introduced in 3.12. It returns the size
      // of all blocks there's an external reference to (mainly through
      // iterators).
      out_.printf("STAT rocksdb.%s.pinned %zu\r\n",
                  key.c_str(),
                  cache->GetPinnedUsage());
    }
  }
};

}}} // namespace facebook::logdevice::commands
