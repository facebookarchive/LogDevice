/**
 * Copyright (c) 2017-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <folly/ScopeGuard.h>

#include "logdevice/common/Metadata.h"
#include "logdevice/common/request_util.h"
#include "logdevice/server/AdminCommand.h"
#include "logdevice/server/locallogstore/LocalLogStore.h"
#include "logdevice/server/read_path/AllServerReadStreams.h"

namespace facebook { namespace logdevice { namespace commands {

class Rebuilding : public AdminCommand {
  using AdminCommand::AdminCommand;

 private:
  std::string action_;
  shard_index_t shard_ = -1;
  int64_t time_from_ = -1;
  int64_t time_to_ = -1;

 public:
  void getOptions(
      boost::program_options::options_description& out_options) override {
    out_options.add_options()(
        "shard", boost::program_options::value<shard_index_t>(&shard_))(
        "action",
        boost::program_options::value<std::string>(&action_)->required())(
        "time-from", boost::program_options::value<int64_t>(&time_from_))(
        "time-to", boost::program_options::value<int64_t>(&time_to_));
  }
  void getPositionalOptions(
      boost::program_options::positional_options_description& out_options)
      override {
    out_options.add("action", 1);
    out_options.add("shard", 1);
  }
  std::string getUsage() override {
    return "rebuilding (write_checkpoint|mark_dirty) [shard] "
           "[--time-from=<unix-timestamp-seconds> "
           "--time-to=<unix-timestamp-seconds>]";
  }

  void run() override {
    if (action_ != "write_checkpoint" && action_ != "mark_dirty") {
      ld_info("Unknown action \"%s\"", action_.c_str());
      out_.printf("USAGE %s\r\n", getUsage().c_str());
      return;
    }

    if (action_ == "mark_dirty") {
      if (time_from_ < 0 || time_to_ < 0) {
        out_.printf("Error: --time-from and --time-to arguments are mandatory "
                    "with the \"mark_dirty\" action");
        return;
      }
      if (time_from_ > time_to_) {
        out_.printf("Error: --time-from has to be earlier than --time-to");
        return;
      }
    }

    if (server_->getParameters()->getRebuildingSettings()->disable_rebuilding) {
      out_.printf("Rebuilding is not enabled.\r\n");
      return;
    }

    if (!server_->getProcessor()->runningOnStorageNode()) {
      out_.printf("Not a storage node.\r\n");
      return;
    }

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
      doForShard(shard_idx, store);
    }
    doneForAllShards();
    out_.printf("Done.\r\n");
  }

  void doForShard(shard_index_t shard_idx, LocalLogStore* store) {
    if (action_ == "write_checkpoint") {
      writeCheckpointForShard(shard_idx, store);
    } else if (action_ == "mark_dirty") {
      markDirtyForShard(shard_idx, store, time_from_, time_to_);
    } else {
      assert(false);
    }
  }

  void doneForAllShards() {
    if (action_ == "mark_dirty") {
      // Notify RebuildingCoordinator of the changes in dirty state
      run_on_all_workers(server_->getProcessor(), [&]() {
        if (ServerWorker::onThisThread()->rebuilding_coordinator_) {
          ServerWorker::onThisThread()
              ->rebuilding_coordinator_->onDirtyStateChanged();
        }
        return 0;
      });
    } else if (action_ == "write_checkpoint") {
      // no-op
    } else {
      ld_check(false);
    }
  }

  void markDirtyForShard(shard_index_t shard_idx,
                         LocalLogStore* store,
                         int64_t time_from,
                         int64_t time_to) {
    // mark partitions as dirty
    auto partitioned_store = dynamic_cast<PartitionedRocksDBStore*>(store);
    if (!partitioned_store) {
      out_.printf(
          "Error: this command is only supported by PartitionedRocksDBStore, "
          "and the store type on shard %u is different",
          shard_idx);
      return;
    }
    int rv = partitioned_store->markTimeRangeUnderreplicated(
        DataClass::APPEND,
        RecordTimeInterval(std::chrono::system_clock::from_time_t(time_from),
                           std::chrono::system_clock::from_time_t(time_to)));
    if (rv != 0) {
      out_.printf("Error marking time range dirty for shard %u: %s\r\n",
                  shard_idx,
                  error_description(err));
      return;
    }
  }

  void writeCheckpointForShard(shard_index_t shard_idx, LocalLogStore* store) {
    // Step 1/ Write the RebuildingRangeMetadata and RebuildingCompleteMetadata
    //         to the shard so that next time we restart we don't try to
    //         rebuild it.

    out_.printf("Clearing dirty ranges and writting checkpoint "
                "for shard %u...\r\n",
                shard_idx);
    LocalLogStore::WriteOptions options;
    RebuildingRangesMetadata range_metadata;
    int rv = store->writeStoreMetadata(range_metadata, options);
    if (rv != 0) {
      out_.printf(
          "Error writting RebuildingRangesMetadata for shard %u: %s\r\n",
          shard_idx,
          error_description(err));
      return;
    }

    RebuildingCompleteMetadata metadata;
    rv = store->writeStoreMetadata(metadata, options);
    if (rv != 0) {
      out_.printf(
          "Error writting RebuildingCompleteMetadata for shard %u: %s\r\n",
          shard_idx,
          error_description(err));
      return;
    }

    // Step 2/ Notify Processor that it should accept reads and writes for the
    //         shard.

    out_.printf("Allowing traffic for shard %u...\r\n", shard_idx);
    server_->getProcessor()->markShardClean(shard_idx);
    server_->getProcessor()->markShardAsNotMissingData(shard_idx);

    // Step 3/ Wake up any read stream that was stalled because the shard is
    // rebuilding.

    out_.printf("Waking up read streams for shard %u...\r\n", shard_idx);
    run_on_all_workers(server_->getProcessor(), [&]() {
      ServerWorker::onThisThread()->serverReadStreams().onShardRebuilt(
          shard_idx);
      return 0;
    });
  }
};

}}} // namespace facebook::logdevice::commands
