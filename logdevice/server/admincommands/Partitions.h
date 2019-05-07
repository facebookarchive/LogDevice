/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <folly/ScopeGuard.h>

#include "logdevice/common/AdminCommandTable.h"
#include "logdevice/server/ServerWorker.h"
#include "logdevice/server/admincommands/AdminCommand.h"
#include "logdevice/server/locallogstore/PartitionedRocksDBStore.h"

namespace facebook { namespace logdevice { namespace commands {

namespace {
PartitionedRocksDBStore* getStore(Server* server,
                                  shard_index_t shard,
                                  EvbufferTextOutput& out) {
  if (!server->getProcessor()->runningOnStorageNode()) {
    out.printf("Error: not storage node\r\n");
    return nullptr;
  }

  auto sharded_store = server->getShardedLocalLogStore();

  if (shard < 0 || shard >= sharded_store->numShards()) {
    out.printf("Error: shard index %d out of range [0, %d]\r\n",
               shard,
               sharded_store->numShards() - 1);
    return nullptr;
  }

  auto store = sharded_store->getByIndex(shard);
  auto partitioned_store = dynamic_cast<PartitionedRocksDBStore*>(store);
  if (partitioned_store == nullptr) {
    out.printf("Error: storage is not partitioned\r\n");
  }
  return partitioned_store;
}
} // namespace

class DropPartitions : public AdminCommand {
  using AdminCommand::AdminCommand;

 private:
  shard_index_t shard_;
  uint64_t id_;

 public:
  void getOptions(
      boost::program_options::options_description& out_options) override {
    out_options.add_options()(
        "shard",
        boost::program_options::value<shard_index_t>(&shard_)->required())(
        "upto", boost::program_options::value<uint64_t>(&id_)->required());
  }
  void getPositionalOptions(
      boost::program_options::positional_options_description& out_options)
      override {
    out_options.add("shard", 1);
    out_options.add("upto", 1);
  }
  std::string getUsage() override {
    return "logsdb drop <shard> <up to>";
  }

  void run() override {
    auto partitioned_store = getStore(server_, shard_, out_);
    if (partitioned_store == nullptr) {
      return;
    }

    ld_info("Dropping partitions up to %lu of shard %d, triggered by admin"
            " command",
            id_,
            shard_);
    int rv = partitioned_store->dropPartitionsUpTo(id_);

    if (rv == -1) {
      if (err == E::INVALID_PARAM) {
        out_.printf("Error: trying to drop latest partition\r\n");
      } else {
        out_.printf("Error: %s\r\n", error_description(err));
      }
      return;
    }

    out_.printf("Dropped\r\n");
  }
};

class CreatePartition : public AdminCommand {
  using AdminCommand::AdminCommand;

 private:
  shard_index_t shard_;

 public:
  void getOptions(
      boost::program_options::options_description& out_options) override {
    out_options.add_options()(
        "shard",
        boost::program_options::value<shard_index_t>(&shard_)->required());
  }
  void getPositionalOptions(
      boost::program_options::positional_options_description& out_options)
      override {
    out_options.add("shard", 1);
  }
  std::string getUsage() override {
    return "logsdb create <shard>";
  }

  void run() override {
    auto partitioned_store = getStore(server_, shard_, out_);

    if (partitioned_store == nullptr) {
      return;
    }

    auto partition = partitioned_store->createPartition();

    if (partition == nullptr) {
      out_.printf("Error: couldn't create partition\r\n");
      return;
    }

    out_.printf("Created partition %lu\r\n", partition->id_);
  }
};

class PrependPartitions : public AdminCommand {
  using AdminCommand::AdminCommand;

 private:
  shard_index_t shard_;
  uint64_t count_ = 1;

 public:
  void getOptions(
      boost::program_options::options_description& out_options) override {
    out_options.add_options()(
        "shard",
        boost::program_options::value<shard_index_t>(&shard_)->required())(
        "count",
        boost::program_options::value<size_t>(&count_)->default_value(count_));
  }
  void getPositionalOptions(
      boost::program_options::positional_options_description& out_options)
      override {
    out_options.add("shard", 1);
    out_options.add("count", 1);
  }
  std::string getUsage() override {
    return "logsdb prepend <shard> [<count>]";
  }

  void run() override {
    auto partitioned_store = getStore(server_, shard_, out_);

    if (partitioned_store == nullptr) {
      return;
    }

    ld_info("Prepending %lu partitions in shard %d, triggered by admin command",
            count_,
            shard_);
    auto partitions = partitioned_store->prependPartitions(count_);

    if (!partitions.empty()) {
      out_.printf("Prepended %lu partitions [%lu, %lu]\r\n",
                  partitions.size(),
                  partitions[0]->id_,
                  partitions.back()->id_);
    } else if (err == E::EXISTS) {
      out_.printf("Prepended 0 partitions");
    } else {
      out_.printf("Error: %s\r\n", error_description(err));
    }
  }
};

class FlushPartition : public AdminCommand {
  using AdminCommand::AdminCommand;

 private:
  shard_index_t shard_;
  // If relative_:
  //   0 == current partition
  //  -n == partition n previous to current
  //  +n == partition n-1 from first partition (first partition is '1')
  // otherwise: explicit partition index
  ssize_t partition_;
  bool relative_ = false;

 public:
  void getOptions(
      boost::program_options::options_description& out_options) override {
    out_options.add_options()(
        "shard",
        boost::program_options::value<shard_index_t>(&shard_)->required())(
        "partition,p",
        boost::program_options::value<ssize_t>(&partition_)->required())(
        "relative-id,r",
        boost::program_options::bool_switch()->notifier(
            [&](bool v) { relative_ = v; }));
  }
  void getPositionalOptions(
      boost::program_options::positional_options_description& out_options)
      override {
    out_options.add("shard", 1);
    out_options.add("partition", 1);
  }
  std::string getUsage() override {
    return "logsdb flush <shard> <partition> [--relative-id,-r]";
  }

  void run() override {
    auto partitioned_store = getStore(server_, shard_, out_);

    if (partitioned_store == nullptr) {
      return;
    }

    auto partitions = partitioned_store->getPartitionList();
    if (partitions->empty()) {
      out_.printf("Error: empty partition list for this shard.\r\n");
      return;
    }

    PartitionedRocksDBStore::PartitionPtr partition;
    bool result;
    if (relative_) {
      partition = partitioned_store->getRelativePartition(partition_);
    } else if (partition_ < 0) {
      out_.printf("Error: must specify '--relative-id' with a negative "
                  "(and therefore relative) partition identifier.\r\n");
      return;
    } else {
      partitioned_store->getPartition(partition_, &partition);
    }

    if (!partition) {
      out_.printf("Error: partition does not exist.\r\n");
      return;
    }

    if (!partitioned_store->flushPartitionAndDependencies(partition)) {
      out_.printf("Error: couldn't flush partition\r\n");
    }
  }
};

class CompactPartitionStorageTask : public StorageTask {
 public:
  explicit CompactPartitionStorageTask(partition_id_t partition_id)
      : StorageTask(StorageTask::Type::COMPACT_PARTITION),
        partition_id_(partition_id) {}

  Principal getPrincipal() const override {
    return Principal::METADATA;
  }

  void execute() override {
    auto partitioned_store = dynamic_cast<PartitionedRocksDBStore*>(
        &storageThreadPool_->getLocalLogStore());
    ld_check(partitioned_store != nullptr);

    partitioned_store->performCompaction(partition_id_);
  }

  void onDone() override {}
  void onDropped() override {}

 private:
  partition_id_t partition_id_;
};

class CompactPartitionRequest : public Request {
 public:
  explicit CompactPartitionRequest(shard_index_t shard,
                                   partition_id_t partition_id)
      : Request(RequestType::ADMIN_CMD_COMPACT_PARTITION),
        shard_(shard),
        partition_id_(partition_id) {}

  Execution execute() override {
    auto task = std::make_unique<CompactPartitionStorageTask>(partition_id_);
    ServerWorker::onThisThread()->getStorageTaskQueueForShard(shard_)->putTask(
        std::move(task));
    return Execution::COMPLETE;
  }

 private:
  shard_index_t shard_;
  partition_id_t partition_id_;
};

class CompactPartition : public AdminCommand {
  using AdminCommand::AdminCommand;

 private:
  shard_index_t shard_;
  partition_id_t partition_id_{PARTITION_INVALID};
  bool cancel_{false};
  bool hi_pri_{false};
  bool list_{false};
  bool json_{false};

 public:
  void getOptions(
      boost::program_options::options_description& out_options) override {
    out_options.add_options()(
        "shard",
        boost::program_options::value<shard_index_t>(&shard_)->required())(
        "partition", boost::program_options::value<uint64_t>(&partition_id_))(
        "hi-pri", boost::program_options::bool_switch(&hi_pri_))(
        "cancel", boost::program_options::bool_switch(&cancel_))(
        "list", boost::program_options::bool_switch(&list_))(
        "json", boost::program_options::bool_switch(&json_));
  }
  void getPositionalOptions(
      boost::program_options::positional_options_description& out_options)
      override {
    out_options.add("shard", 1);
    out_options.add("partition", 1);
  }
  std::string getUsage() override {
    return "logsdb compact <shard> [partition] "
           "[--hi-pri | --cancel | --list [--json]]";
  }

  void listCompactions(PartitionedRocksDBStore* partitioned_store) {
    auto list = partitioned_store->getManualCompactionList();
    InfoManualCompactionsTable table(!json_, "Partition ID", "Hi-pri");
    for (auto& p : list) {
      table.next()
          .set<0>(p.first)   // partition id
          .set<1>(p.second); // hi-pri
    }
    json_ ? table.printJson(out_) : table.print(out_);
  }

  void scheduleOrCancelCompactions(PartitionedRocksDBStore* partitioned_store) {
    ld_info("%s partition %lu of shard %d, triggered by admin "
            "command",
            cancel_ ? "Cancelling compaction of" : "Compacting",
            partition_id_,
            shard_);

    if (!cancel_) {
      partitioned_store->scheduleManualCompaction(partition_id_, hi_pri_);
    } else {
      partitioned_store->cancelManualCompaction(partition_id_);
    }
    out_.printf("Successfully scheduled %s of partition\r\n",
                cancel_ ? "cancelling compaction" : "compaction");
  }

  void run() override {
    if (int(cancel_) + int(list_) + int(hi_pri_) > 1) {
      out_.printf(
          "Error: at most one of --hi-pri, --cancel or --list should be "
          "specified\r\n");
      return;
    }
    if (!list_ && !cancel_ && partition_id_ == PARTITION_INVALID) {
      out_.printf(
          "Error: partition is required when scheduling a compaction\r\n");
      return;
    }
    if (json_ && !list_) {
      out_.printf("Error: --json is only valid with --list\r\n");
      return;
    }

    auto partitioned_store = getStore(server_, shard_, out_);

    if (partitioned_store == nullptr) {
      return;
    }

    if (list_) {
      listCompactions(partitioned_store);
    } else {
      scheduleOrCancelCompactions(partitioned_store);
    }
  }
};

class ApplyRetention : public AdminCommand {
  using AdminCommand::AdminCommand;

 private:
  shard_index_t shard_ = -1;

 public:
  void getOptions(
      boost::program_options::options_description& out_options) override {
    out_options.add_options()(
        "shard",
        boost::program_options::value<shard_index_t>(&shard_)->default_value(
            shard_));
  }
  void getPositionalOptions(
      boost::program_options::positional_options_description& /*out_options*/)
      override {}
  std::string getUsage() override {
    return "logsdb apply_retention_approximate [--shard=<shard>]\n\n"
           "Trims logs based on time. Not precise. This operation makes all\n"
           "records older than [backlog_duration + partition_duration]\n"
           "inaccessible to readers, where partition_duration is the value of\n"
           "--rocksdb-partition-duration command line option to logdeviced.\n"
           "The same kind of trimming also happens periodically in background.";
  }

  void run() override {
    if (!server_->getProcessor()->runningOnStorageNode()) {
      out_.printf("Error: not storage node\r\n");
      return;
    }

    auto sharded_store = server_->getShardedLocalLogStore();

    if (shard_ >= sharded_store->numShards()) {
      out_.printf("Error: shard index %d out of range [0, %d]\r\n",
                  shard_,
                  sharded_store->numShards() - 1);
      return;
    }

    int min_shard, max_shard;
    if (shard_ < 0) {
      min_shard = 0;
      max_shard = sharded_store->numShards() - 1;
    } else {
      min_shard = max_shard = shard_;
    }

    for (shard_index_t shard = min_shard; shard <= max_shard; ++shard) {
      auto store = sharded_store->getByIndex(shard);
      if (store->acceptingWrites() == E::DISABLED) {
        out_.printf("Skipping disabled shard %d\r\n", (int)shard);
        continue;
      }

      auto partitioned_store = dynamic_cast<PartitionedRocksDBStore*>(store);
      if (partitioned_store == nullptr) {
        out_.printf("Error: storage is not partitioned\r\n");
        return;
      }

      int rv = partitioned_store->trimLogsBasedOnTime();

      if (rv == -1) {
        out_.printf("Shard %d error: %s\r\n", shard, error_description(err));
        continue;
      }
    }

    out_.printf("Done\r\n");
  }
};

}}} // namespace facebook::logdevice::commands
