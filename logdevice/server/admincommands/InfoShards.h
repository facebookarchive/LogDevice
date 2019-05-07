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
#include "logdevice/server/locallogstore/FailingLocalLogStore.h"
#include "logdevice/server/locallogstore/ShardedRocksDBLocalLogStore.h"

namespace facebook { namespace logdevice { namespace commands {

class InfoShards : public AdminCommand {
 private:
  bool json_ = false;
  bool dirty_as_json_ = false;

 public:
  void getOptions(
      boost::program_options::options_description& out_options) override {
    out_options.add_options()(
        "json", boost::program_options::bool_switch(&json_))(
        "dirty-as-json", boost::program_options::bool_switch(&dirty_as_json_));
  }
  std::string getUsage() override {
    return "info shards [--json] [--dirty-as-json]";
  }

  void run() override {
    InfoShardsTable table(!json_,
                          "Shard",
                          "Is Failing",
                          "Accepting writes",
                          "Rebuilding state",
                          "Default CF Version",
                          "Dirty State");

    if (server_->getProcessor()->runningOnStorageNode()) {
      auto sharded_store = server_->getShardedLocalLogStore();

      std::set<uint32_t> shards_rebuilding =
          run_on_worker(server_->getProcessor(), 0, [&]() {
            std::set<uint32_t> res;
            auto rc = server_->getRebuildingCoordinator();
            if (rc) {
              res = rc->getLocalShardsRebuilding();
            }
            return res;
          });
      shard_index_t shard_lo = 0;
      shard_index_t shard_hi = sharded_store->numShards() - 1;

      for (shard_index_t shard_idx = shard_lo; shard_idx <= shard_hi;
           ++shard_idx) {
        auto store = sharded_store->getByIndex(shard_idx);

        std::string rebuilding_state("NONE");
        if (server_->getProcessor()->isDataMissingFromShard(shard_idx)) {
          // The shard is either rebuilding or waiting to be rebuilt. Verify
          // which state is it.
          if (shards_rebuilding.find(shard_idx) != shards_rebuilding.end()) {
            rebuilding_state.assign("REBUILDING");
          } else {
            rebuilding_state.assign("WAITING_FOR_REBUILDING");
          }
        }

        std::string dirty_state(dirty_as_json_ ? "{}" : "CLEAN");
        auto partitioned_store = dynamic_cast<PartitionedRocksDBStore*>(store);
        if (partitioned_store) {
          RebuildingRangesMetadata range_metadata;
          int rv = partitioned_store->readStoreMetadata(&range_metadata);
          if (rv != 0) {
            if (err != E::NOTFOUND) {
              if (!json_) {
                out_.printf("Error reading RebuildingRangesMetadata "
                            "for shard %u: %s\r\n",
                            shard_idx,
                            error_description(err));
              }
              dirty_state = "UNKNOWN";
            }
          } else if (!range_metadata.empty()) {
            if (dirty_as_json_) {
              dirty_state = folly::toJson(range_metadata.toFollyDynamic());
            } else {
              dirty_state = range_metadata.toString();
            }
          }
        }

        table.next()
            .set<0>(shard_idx)
            .set<1>(!!dynamic_cast<FailingLocalLogStore*>(store))
            .set<2>(store->acceptingWrites())
            .set<3>(std::move(rebuilding_state))
            .set<4>(store->getVersion())
            .set<5>(std::move(dirty_state));
      }
    }

    json_ ? table.printJson(out_) : table.print(out_);
  }
};

}}} // namespace facebook::logdevice::commands
