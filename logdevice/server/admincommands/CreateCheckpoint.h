/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include "logdevice/server/admincommands/AdminCommand.h"
#include "rocksdb/utilities/checkpoint.h"

namespace facebook { namespace logdevice { namespace commands {

class CreateCheckpoint : public AdminCommand {
  using AdminCommand::AdminCommand;

 private:
  std::string path_;
  shard_index_t shard_index_;

 public:
  void getOptions(
      boost::program_options::options_description& out_options) override {
    // clang-format off
    out_options.add_options()
      ("shard",
       boost::program_options::value<shard_index_t>(&shard_index_)
       ->required())
      ("path",
       boost::program_options::value<std::string>(&path_)
       ->notifier([](std::string val) -> void {
         if (val.size() == 0 || val == "/") {
           throw boost::program_options::error(
             "Invalid value for --path. Must not be empty path or \"/\".");
         }
       })
       ->required());
    // clang-format on
  }
  void getPositionalOptions(
      boost::program_options::positional_options_description& out_options)
      override {
    out_options.add("shard", 1);
    out_options.add("path", 1);
  }
  std::string getUsage() override {
    return "create_checkpoint <shard> <path>\r\n\r\n"
           "Checkpoint is a sort of copy-on-write copy of a rocksdb DB. It "
           "copies \r\n"
           "the current DB to <path>, except that for read-only files it "
           "creates \r\n"
           "hard links instead of copies. Thus most of the space overhead of "
           "\r\n"
           "having a full copy is avoided. The new DB behaves just like a "
           "normal \r\n"
           "self-sufficient rocksdb DB.\r\n\r\n"
           "<path> needs to be on the same filesystem as <shard> since "
           "checkpoint \r\n"
           "is going to contain hard links to files in the original DB.\r\n\r\n"
           "Possible use cases: backups; inspecting DB directly with external "
           "\r\n"
           "tools, e.g. `recordtool`. Although `recordtool`, most of the time, "
           "\r\n"
           "works fine on the live DB without any checkpoints, it's not really "
           "\r\n"
           "supported by rocksdb, and may be unsafe (but we do it all the time "
           "\r\n"
           "anyway and nothing bad ever happened).\r\n";
  }

  void run() override {
    auto sharded_store = server_->getShardedLocalLogStore();
    if (shard_index_ < 0 || shard_index_ >= sharded_store->numShards()) {
      out_.printf("Error: shard index %d out of range [0, %d]\r\n",
                  shard_index_,
                  sharded_store->numShards() - 1);
      return;
    }

    auto store = sharded_store->getByIndex(shard_index_);
    if (!dynamic_cast<RocksDBLogStoreBase*>(store)) {
      out_.printf("Error: could not find a RocksDBLogStoreBase instance.\r\n");
      return;
    }
    RocksDBLogStoreBase* store_base = dynamic_cast<RocksDBLogStoreBase*>(store);
    rocksdb::DB* db = &store_base->getDB();
    rocksdb::Checkpoint* checkpoint_ptr;

    rocksdb::Status status = rocksdb::Checkpoint::Create(db, &checkpoint_ptr);
    if (!status.ok()) {
      out_.printf("Could not create checkpoint for shard %d. Error %s.\r\n",
                  shard_index_,
                  status.ToString().c_str());
      return;
    }

    std::unique_ptr<rocksdb::Checkpoint> checkpoint_unique_ptr(checkpoint_ptr);
    if (path_.back() == '/') {
      path_.resize(path_.size() - 1);
    }

    status = checkpoint_unique_ptr->CreateCheckpoint(path_);
    if (!status.ok()) {
      out_.printf("Could not create checkpoint for shard %d. Error %s.\r\n",
                  shard_index_,
                  status.ToString().c_str());
      return;
    }
    out_.printf("Created checkpoint for shard %d at %s\r\n",
                shard_index_,
                path_.c_str());
  }
};

}}} // namespace facebook::logdevice::commands
