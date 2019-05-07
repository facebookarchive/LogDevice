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
#include "logdevice/server/locallogstore/RocksDBLogStoreBase.h"
#include "logdevice/server/locallogstore/RocksDBWriter.h"

namespace facebook { namespace logdevice { namespace commands {

class ListOrEraseMetadata : public AdminCommand {
 private:
  bool erase_;

  shard_index_t shard_ = -1;
  std::vector<logid_t> logs_;
  bool logs_option_passed_ = false;
  std::vector<StoreMetadataType> per_store_types_;
  std::vector<LogMetadataType> per_log_types_;
  std::vector<PerEpochLogMetadataType> per_epoch_types_;

 public:
  ListOrEraseMetadata(bool erase)
      : AdminCommand(erase ? RestrictionLevel::LOCALHOST_ONLY
                           : RestrictionLevel::UNRESTRICTED),
        erase_(erase) {}

  void getOptions(
      boost::program_options::options_description& out_options) override {
    // clang-format off
    out_options.add_options()
      ("shard",
       boost::program_options::value<shard_index_t>(&shard_)
        ->default_value(shard_))
      ("types",
       boost::program_options::value<std::string>()->notifier(
         [this](std::string val) {
           std::vector<std::string> tokens;
           folly::split(',', val, tokens);
           for (const std::string &token : tokens) {
             if (token == "per-store") {
               per_store_types_ = storeMetadataTypeNames().allValidKeys();
               continue;
             }
             if (token == "per-log") {
               per_log_types_ = logMetadataTypeNames().allValidKeys();
               continue;
             }
             if (token == "per-epoch") {
               per_epoch_types_ = perEpochLogMetadataTypeNames().allValidKeys();
               continue;
             }
             {
               auto meta = storeMetadataTypeNames().reverseLookup(token);
               if (meta != StoreMetadataType::MAX) {
                 per_store_types_.push_back(meta);
                 continue;
               }
             }
             {
               auto meta = logMetadataTypeNames().reverseLookup(token);
               if (meta != LogMetadataType::MAX) {
                 per_log_types_.push_back(meta);
                 continue;
               }
             }
             {
               auto meta = perEpochLogMetadataTypeNames().reverseLookup(token);
               if (meta != PerEpochLogMetadataType::MAX) {
                 per_epoch_types_.push_back(meta);
                 continue;
               }
             }

             throw boost::program_options::error(
                 "invalid metadata type: " + token + "; valid values are "
                 "'per-store', 'per-log', 'per-epoch' and the enums values in "
                 "Metadata.h");
           }
         }))
      ("logs",
       boost::program_options::value<std::string>()
        ->notifier([&](std::string val) {
          logs_option_passed_ = true;
          if (lowerCase(val) == "all") {
            return;
          }
          std::vector<std::string> tokens;
          folly::split(',', val, tokens);
          for (const std::string &token : tokens) {
            try {
              logid_t log(folly::to<logid_t::raw_type>(token));
              logs_.push_back(log);
            } catch (std::range_error&) {
              throw boost::program_options::error(
                "invalid value of --logs option: " + val);
            }
          }
        }));
    // clang-format on

    if (erase_) {
      out_options.add_options()(
          "i-know-what-i-am-doing",
          boost::program_options::bool_switch()->required());
    }
  }

  std::string getUsage() override {
    std::string s =
        "[--shard=<shard>] [--types=<comma-separated list of metadata types>] "
        "[--logs=<'all'|comma-separated list of logs]";
    if (erase_) {
      s = "delete metadata --i-know-what-i-am-doing " + s;
      s += "\r\n\r\nNote that this admin command only deletes the metadata "
           "from rocksdb, without updating any in-memory state associated with "
           "it. This may have unexpected consequences, or may not take effect "
           "until logdeviced restart. E.g. if you delete TRIM_POINT metadata, "
           "the old trim point will still be used until logdeviced restart.";
      return s;
    } else {
      return "info metadata " + s;
    }
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

    if (per_store_types_.empty() && per_log_types_.empty() &&
        per_epoch_types_.empty()) {
      if (erase_) {
        out_.printf("Error: no metadata types specified; use --types\r\n");
        return;
      }
      per_store_types_ = storeMetadataTypeNames().allValidKeys();
      per_log_types_ = logMetadataTypeNames().allValidKeys();
      per_epoch_types_ = perEpochLogMetadataTypeNames().allValidKeys();
    }

    if (erase_ && !logs_option_passed_ &&
        (!per_log_types_.empty() || !per_epoch_types_.empty())) {
      // Only delete metadata of all logs if we got an explicit "--logs=all"
      // argument.
      out_.printf("Error: no logs specified; use --logs; (--logs=all is "
                  "acceptable)\r\n");
      return;
    }

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

      // Metadata should be small and mostly cached, so we can afford reading
      // right on the admin command thread.

      int rv = RocksDBWriter::enumerateStoreMetadata(
          &rocksdb_store->getDB(),
          rocksdb_store->getMetadataCFHandle(),
          per_store_types_,
          [&](StoreMetadataType type, StoreMetadata& meta) {
            out_.printf("%s %s\r\n",
                        erase_ ? "deleting " : "",
                        meta.toString().c_str());

            if (erase_) {
              int r = store->deleteStoreMetadata(type);
              if (r != 0) {
                out_.printf("Error: failed to delete store metadta\r\n");
              }
            }
          });
      if (rv != 0) {
        out_.printf(
            "Error: failed to read store metadata: %s\r\n", error_name(err));
      }

      rv = RocksDBWriter::enumerateLogMetadata(
          &rocksdb_store->getDB(),
          rocksdb_store->getMetadataCFHandle(),
          logs_,
          per_log_types_,
          [&](LogMetadataType type, logid_t log, LogMetadata& meta) {
            out_.printf("%slog %lu: %s\r\n",
                        erase_ ? "deleting metadata for " : "",
                        log.val_,
                        meta.toString().c_str());

            if (erase_) {
              int r = store->deleteLogMetadata(log, log, type);
              if (r != 0) {
                out_.printf("Error: failed to delete log metadta\r\n");
              }
            }
          });
      if (rv != 0) {
        out_.printf(
            "Error: failed to read log metadata: %s\r\n", error_name(err));
      }

      rv = RocksDBWriter::enumeratePerEpochLogMetadata(
          &rocksdb_store->getDB(),
          rocksdb_store->getMetadataCFHandle(),
          logs_,
          per_epoch_types_,
          [&](PerEpochLogMetadataType type,
              logid_t log,
              epoch_t epoch,
              PerEpochLogMetadata& meta) {
            out_.printf("%slog %lu epoch %u: %s\r\n",
                        erase_ ? "deleting metadata for " : "",
                        log.val_,
                        epoch.val_,
                        meta.toString().c_str());
            if (erase_) {
              int r = store->deletePerEpochLogMetadata(log, epoch, type);
              if (r != 0) {
                out_.printf("Error: failed to delete epoch metadta\r\n");
              }
            }
          });
      if (rv != 0) {
        out_.printf(
            "Error: failed to read epoch metadata: %s\r\n", error_name(err));
      }
    }
  }
};

}}} // namespace facebook::logdevice::commands
