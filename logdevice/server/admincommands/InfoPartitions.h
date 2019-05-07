/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include "logdevice/common/AdminCommandTable.h"
#include "logdevice/server/admincommands/AdminCommand.h"
#include "logdevice/server/locallogstore/PartitionedRocksDBStore.h"
#include "logdevice/server/locallogstore/ShardedRocksDBLocalLogStore.h"

namespace facebook { namespace logdevice { namespace commands {

class InfoPartitions : public AdminCommand {
 private:
  shard_index_t shard_ = -1;
  bool spew_ = false;
  int level_ = 0;
  bool json_ = false;
  static constexpr int maxLevel() {
    return 2;
  }

 public:
  void getOptions(
      boost::program_options::options_description& out_options) override {
    // clang-format off
    out_options.add_options()
      ("shard", boost::program_options::value<shard_index_t>(&shard_))
      ("spew", boost::program_options::bool_switch(&spew_))
      ("level", boost::program_options::value<int>(&level_)
        ->default_value(level_)
        ->notifier([](int val) {
          if (val < 0 || val > maxLevel()) {
            throw boost::program_options::error(
              "Invalid value for --level. Expecting int from 0 to " +
              folly::to<std::string>(maxLevel()));
          }
        }),
        "0, 1 or 2. Defines which columns to print: 0 prints some, 1 prints "
        "most, 2 prints all but can be very slow")
      ("json", boost::program_options::bool_switch(&json_));
    // clang-format on
  }
  void getPositionalOptions(
      boost::program_options::positional_options_description& out_options)
      override {
    out_options.add("shard", 1);
  }
  std::string getUsage() override {
    return "info partitions [<shard>] [--spew] [--json] [--level {0,1,2}]";
  }

  void run() override {
    // New columns should generally be added to level 1 unless they
    // are extremely expensive to calculate. The current Level 2 stats
    // can take 10s of minutes to be computed.
    InfoPartitionsTable table(!json_,
                              // Level 0
                              "Shard",
                              "ID",
                              "Start Time",
                              "Min Time",
                              "Max Time",
                              "Last Compacted",
                              "Approx. Size",
                              "L0 files",
                              // Level 1
                              "Immutable Memtables",
                              "Memtable Flush Pending",
                              "Active Memtable Size",
                              "All Not Flushed Memtables Size",
                              "All Memtables Size",
                              "Est Num Keys",
                              "Est Mem by Readers",
                              "Live Versions",
                              "Current version",
                              "Durable Min Time",
                              "Durable Max Time",
                              "Append Dirtied By",
                              "Rebuild Dirtied By",
                              "Under Replicated",
                              // Level 2
                              "Approx. Obsolete Bytes");

    if (spew_) {
      level_ = 1;
    }

    if (server_->getProcessor()->runningOnStorageNode()) {
      auto sharded_store = server_->getShardedLocalLogStore();

      shard_index_t shard_lo = 0;
      shard_index_t shard_hi = sharded_store->numShards() - 1;

      if (shard_ != -1) {
        if (shard_ < shard_lo || shard_ > shard_hi) {
          out_.printf("Shard index %d out of range [%d, %d]\r\n",
                      shard_,
                      shard_lo,
                      shard_hi);
          return;
        }
        shard_lo = shard_hi = shard_;
      }

      for (shard_index_t shard_idx = shard_lo; shard_idx <= shard_hi;
           ++shard_idx) {
        auto store = sharded_store->getByIndex(shard_idx);
        auto partitioned_store = dynamic_cast<PartitionedRocksDBStore*>(store);
        if (partitioned_store == nullptr) {
          if (!json_) {
            out_.printf("Not partitioned\r\n\r\n");
          }
          continue;
        }

        auto partitions = partitioned_store->getPartitionList();

        for (auto partition : *partitions) {
          table.next()
              .set<0>(shard_idx)
              .set<1>(partition->id_)
              .set<2>(partition->starting_timestamp.toMilliseconds())
              .set<3>(partition->min_timestamp.toMilliseconds())
              .set<4>(partition->max_timestamp.toMilliseconds())
              .set<5>(partition->last_compaction_time.toMilliseconds())
              .set<6>(partitioned_store->getApproximatePartitionSize(
                  partition->cf_->get()))
              .set<7>(partitioned_store->getNumL0Files(partition->cf_->get()));

          if (level_ >= 1) {
            auto prop = [&](const char* name) -> std::string {
              std::string val;
              bool ok = partitioned_store->getDB().GetProperty(
                  partition->cf_->get(),
                  rocksdb::Slice(name, strlen(name)),
                  &val);
              if (ok) {
                return val;
              } else {
                return "error";
              }
            };
            auto uint_prop = [&](const char* name) -> uint64_t {
              uint64_t val;
              bool ok = partitioned_store->getDB().GetIntProperty(
                  partition->cf_->get(),
                  rocksdb::Slice(name, strlen(name)),
                  &val);
              if (ok) {
                return val;
              } else {
                return LLONG_MAX;
              }
            };

            table.set<8>(prop("rocksdb.num-immutable-mem-table"))
                .set<9>(prop("rocksdb.mem-table-flush-pending"))
                .set<10>(prop("rocksdb.cur-size-active-mem-table"))
                .set<11>(prop("rocksdb.cur-size-all-mem-tables"))
                .set<12>(prop("rocksdb.size-all-mem-tables"))
                .set<13>(prop("rocksdb.estimate-num-keys"))
                .set<14>(prop("rocksdb.estimate-table-readers-mem"))
                .set<15>(prop("rocksdb.num-live-versions"))
                .set<16>(uint_prop("rocksdb.current-super-version-number"))
                .set<17>(partition->min_durable_timestamp.toMilliseconds())
                .set<18>(partition->max_durable_timestamp.toMilliseconds());

            folly::SharedMutex::ReadHolder lock(partition->mutex_);
            PartitionDirtyMetadata meta = partition->dirty_state_.metadata();
            table.set<19>(toString(meta.getDirtiedBy(DataClass::APPEND)))
                .set<20>(toString(meta.getDirtiedBy(DataClass::REBUILD)))
                .set<21>(partition->isUnderReplicated());
          }

          if (level_ >= 2) {
            table.set<22>(
                partitioned_store->getApproximateObsoleteBytes(partition->id_));
          }
        }
      }
    }

    constexpr std::array<int, maxLevel() + 1> num_stats_per_level = {8, 14, 1};
    static_assert(table.numCols() ==
                      num_stats_per_level[0] + num_stats_per_level[1] +
                          num_stats_per_level[2],
                  "The number of columns in the table doesn't match "
                  "sum(num_stats_per_level)");
    ld_check(level_ <= maxLevel());
    unsigned int total_columns = 0;
    for (int l = 0; l <= level_; ++l) {
      total_columns += num_stats_per_level[l];
    }
    ld_check(total_columns > 0);
    if (json_) {
      table.printJson(out_, total_columns);
    } else {
      table.print(out_, total_columns);
    }
  }
};

}}} // namespace facebook::logdevice::commands
