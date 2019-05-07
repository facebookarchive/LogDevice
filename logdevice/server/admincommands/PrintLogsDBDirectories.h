/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <folly/ScopeGuard.h>

#include "logdevice/server/admincommands/AdminCommand.h"
#include "logdevice/server/locallogstore/PartitionedRocksDBStore.h"
#include "logdevice/server/locallogstore/RocksDBLogStoreBase.h"

namespace facebook { namespace logdevice { namespace commands {

class PrintLogsDBDirectories : public AdminCommand {
 private:
  shard_index_t shard_ = -1;
  std::vector<logid_t> logs_;
  std::vector<partition_id_t> partitions_;
  bool partitions_option_passed_ = false;
  bool json_ = false;

 public:
  PrintLogsDBDirectories() : AdminCommand(RestrictionLevel::UNRESTRICTED) {}

  void getOptions(
      boost::program_options::options_description& out_options) override {
    out_options.add_options()(
        "shard",
        boost::program_options::value<shard_index_t>(&shard_)->default_value(
            shard_))("logs",
                     boost::program_options::value<std::string>()->notifier(
                         [&](std::string val) {
                           if (lowerCase(val) == "all") {
                             return;
                           }
                           std::vector<std::string> tokens;
                           folly::split(',', val, tokens);
                           for (const std::string& token : tokens) {
                             try {
                               logid_t log(folly::to<logid_t::raw_type>(token));
                               logs_.push_back(log);
                             } catch (std::range_error&) {
                               throw boost::program_options::error(
                                   "invalid value of --logs option: " + val);
                             }
                           }
                         }))(
        "partitions",
        boost::program_options::value<std::string>()->notifier(
            [&](std::string val) {
              if (lowerCase(val) == "all") {
                return;
              }
              partitions_option_passed_ = true;
              std::vector<std::string> tokens;
              folly::split(',', val, tokens);
              for (const std::string& token : tokens) {
                try {
                  partition_id_t partition(folly::to<partition_id_t>(token));
                  partitions_.push_back(partition);
                } catch (std::range_error&) {
                  throw boost::program_options::error(
                      "invalid value of --partitions option: " + val);
                }
              }
            }))("json", boost::program_options::bool_switch(&json_));
  }

  std::string getUsage() override {
    return "logsdb print_directory [--shard=<shard>] "
           "[--logs=<'all'|comma-separated list of logs] "
           "[--partitions=<'all'|comma-separated list of partitions] [--json]";
  }

  void run() override {
    if (!server_->getProcessor()->runningOnStorageNode()) {
      out_.printf("Error: not storage node\r\n");
      return;
    }

    auto sharded_store = server_->getShardedLocalLogStore();

    if (shard_ != -1 && (shard_ < 0 || shard_ >= sharded_store->numShards())) {
      out_.printf("Error: shard index %d out of range [0, %d]\r\n",
                  shard_,
                  sharded_store->numShards() - 1);
      return;
    }

    // Sort & drop duplicates from the given values
    if (!logs_.empty()) {
      std::sort(logs_.begin(), logs_.end());
      logs_.erase(std::unique(logs_.begin(), logs_.end()), logs_.end());
    }
    if (!partitions_.empty()) {
      std::sort(partitions_.begin(), partitions_.end());
      partitions_.erase(std::unique(partitions_.begin(), partitions_.end()),
                        partitions_.end());
    }

    PrintLogsDBDirectoriesTable table(!json_,
                                      "shard",
                                      "log_id",
                                      "partition",
                                      "first_lsn",
                                      "max_lsn",
                                      "flags",
                                      "approximate_size_bytes");

    for (shard_index_t shard = shard_ == -1 ? 0 : shard_;
         shard < (shard_ == -1 ? sharded_store->numShards() : (shard_ + 1));
         ++shard) {
      auto store = sharded_store->getByIndex(shard);
      auto rocksdb_store = dynamic_cast<RocksDBLogStoreBase*>(store);
      if (rocksdb_store == nullptr) {
        if (shard_ != -1) {
          out_.printf("Error: storage is not rocksdb-based\r\n");
        } else {
          out_.printf("Note: shard %d is not rocksdb-based\r\n", (int)shard);
        }
        continue;
      }
      if (!rocksdb_store->getSettings()->partitioned) {
        if (shard_ != -1) {
          out_.printf("Error: storage is not partitioned\r\n");
        } else {
          out_.printf("Note: shard %d is not partitioned\r\n", (int)shard);
        }
        continue;
      }
      auto partitioned_store =
          dynamic_cast<PartitionedRocksDBStore*>(rocksdb_store);
      ld_assert_ne(partitioned_store, nullptr);

      std::vector<std::pair<logid_t, PartitionedRocksDBStore::DirectoryEntry>>
          dirs;
      partitioned_store->getLogsDBDirectories(partitions_, logs_, dirs);

      for (auto pair : dirs) {
        logid_t log_id = pair.first;
        auto de = pair.second;
        table.next()
            .set<0>(shard)
            .set<1>(log_id)
            .set<2>(de.id)
            .set<3>(de.first_lsn)
            .set<4>(de.max_lsn)
            .set<5>(de.flagsToString())
            .set<6>(de.approximate_size_bytes);
      }
    }
    if (json_) {
      table.printJson(out_);
    } else {
      table.print(out_);
    }
  }
};

}}} // namespace facebook::logdevice::commands
