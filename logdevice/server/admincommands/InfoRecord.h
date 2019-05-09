/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <folly/Synchronized.h>

#include "logdevice/common/AdminCommandTable.h"
#include "logdevice/common/LocalLogStoreRecordFormat.h"
#include "logdevice/common/Processor.h"
#include "logdevice/common/Request.h"
#include "logdevice/common/types_internal.h"
#include "logdevice/server/ServerWorker.h"
#include "logdevice/server/admincommands/AdminCommand.h"
#include "logdevice/server/locallogstore/LocalLogStore.h"
#include "logdevice/server/storage_tasks/PerWorkerStorageTaskQueue.h"

namespace facebook { namespace logdevice { namespace commands {

typedef AdminCommandTable<logid_t,                   /* Log ID */
                          admin_command_table::LSN,  /* LSN */
                          uint32_t,                  /* Shard */
                          uint32_t,                  /* Wave */
                          uint32_t,                  /* Recovery epoch */
                          std::chrono::milliseconds, /* Timestamp */
                          uint32_t,                  /* Last known good */
                          std::string,               /* Copyset / Storage set */
                          std::string,               /* Flags */
                          std::string,               /* Offsets within epoch */
                          std::string,               /* Optional keys */
                          bool,       /* Is written by recovery */
                          std::string /* Payload */
                          >
    InfoRecordTable;

class InfoRecordStorageTask : public StorageTask {
 public:
  explicit InfoRecordStorageTask(logid_t log_id,
                                 lsn_t lsn_start,
                                 lsn_t lsn_end,
                                 size_t payload_max_len,
                                 Semaphore& sem,
                                 folly::Synchronized<EvbufferTextOutput*>& out,
                                 InfoRecordTable& table,
                                 bool print_table,
                                 size_t max_records,
                                 bool csi_only)
      : StorageTask(StorageTask::Type::INFO_RECORD),
        log_id_(log_id),
        lsn_start_(lsn_start),
        lsn_end_(lsn_end),
        payload_max_len_(payload_max_len),
        max_records_(max_records),
        sem_(sem),
        out_(out),
        table_(table),
        print_table_(print_table),
        csi_only_(csi_only) {}

  Principal getPrincipal() const override {
    return Principal::METADATA;
  }

  void execute() override {
    LocalLogStore::ReadOptions options("commands::InfoRecord");
    options.allow_blocking_io = true;
    options.tailing = false;
    options.fill_cache = false;
    if (csi_only_) {
      options.allow_copyset_index = true;
      options.csi_data_only = true;
    }
    std::unique_ptr<LocalLogStore::ReadIterator> it =
        storageThreadPool_->getLocalLogStore().read(log_id_, options);

    ShardID* copyset = (ShardID*)alloca(COPYSET_SIZE_MAX * sizeof(ShardID));
    int count = 0;

    for (it->seek(lsn_start_); it->state() == IteratorState::AT_RECORD &&
         it->getLSN() <= lsn_end_ && count < max_records_;
         it->next(), ++count) {
      ld_check_ge(it->getLSN(), lsn_start_);
      std::chrono::milliseconds timestamp;
      copyset_size_t copyset_size;
      esn_t last_known_good;
      LocalLogStoreRecordFormat::flags_t flags;
      Payload payload;
      uint32_t wave;
      OffsetMap offsets_within_epoch;
      std::map<KeyType, std::string> optional_keys;

      int rv =
          LocalLogStoreRecordFormat::parse(it->getRecord(),
                                           &timestamp,
                                           &last_known_good,
                                           &flags,
                                           &wave,
                                           &copyset_size,
                                           copyset,
                                           COPYSET_SIZE_MAX,
                                           &offsets_within_epoch,
                                           &optional_keys,
                                           &payload,
                                           storageThreadPool_->getShardIdx());
      if (rv != 0) {
        (*out_.wlock())->printf("Could not parse record.\r\n");
        continue;
      }

      const bool written_by_recovery =
          flags & LocalLogStoreRecordFormat::FLAG_WRITTEN_BY_RECOVERY;
      std::string optional_keys_string = toString(optional_keys);

      auto copyset_str = toString(copyset, copyset_size);
      auto flags_str = LocalLogStoreRecordFormat::flagsToString(flags);
      auto payload_str = payload.size() > 0
          ? hexdump_buf(payload.data(), payload.size(), payload_max_len_)
          : "(empty)";

      if (print_table_) {
        table_.next();

        table_.set<0>(log_id_);
        table_.set<1>(it->getLSN());
        table_.set<2>(storageThreadPool_->getShardIdx());

        if (!written_by_recovery) {
          table_.set<3>(wave);
        } else {
          table_.set<4>(wave);
        }

        table_.set<5>(timestamp)
            .set<6>(last_known_good.val_)
            .set<7>(copyset_str)
            .set<8>(flags_str);

        if (flags & LocalLogStoreRecordFormat::FLAG_OFFSET_WITHIN_EPOCH) {
          table_.set<9>(offsets_within_epoch.toString());
        }

        table_.set<10>(optional_keys_string.c_str())
            .set<11>(written_by_recovery)
            .set<12>(payload_str.c_str());
      } else {
        auto timestamp_str = format_time(timestamp);
        (*out_.wlock())
            ->printf("%s: %u, "
                     "timestamp: %s, lng: %u, copyset: %s, flags: %s, "
                     "offsets within epoch: %s, "
                     "key: %s, "
                     "written_by_recovery: %s, "
                     "payload: %s\r\n",
                     written_by_recovery ? "recovery_epoch" : "wave",
                     wave,
                     timestamp_str.c_str(),
                     last_known_good.val_,
                     copyset_str.c_str(),
                     flags_str.c_str(),
                     flags & LocalLogStoreRecordFormat::FLAG_OFFSET_WITHIN_EPOCH
                         ? offsets_within_epoch.toString().c_str()
                         : "(empty)",
                     optional_keys_string.c_str(),
                     written_by_recovery ? "true" : "false",
                     payload_str.c_str());
      }
    }

    if (it->state() == IteratorState::ERROR) {
      (*out_.wlock())->printf("Failed to read from log store.\r\n");
    }
  }

  StorageTaskPriority getPriority() const override {
    return StorageTaskPriority::VERY_HIGH;
  }

  void onDone() override {
    sem_.post();
  }
  void onDropped() override {
    sem_.post();
  }

 private:
  const logid_t log_id_;
  const lsn_t lsn_start_, lsn_end_;
  const size_t payload_max_len_, max_records_;
  Semaphore& sem_;
  folly::Synchronized<EvbufferTextOutput*>& out_;
  InfoRecordTable& table_;
  const bool print_table_;
  const bool csi_only_;
};

class InfoRecordRequest : public Request {
 public:
  explicit InfoRecordRequest(logid_t log_id,
                             lsn_t lsn_start,
                             lsn_t lsn_end,
                             size_t payload_max_len,
                             Semaphore& sem,
                             folly::Synchronized<EvbufferTextOutput*>& out,
                             InfoRecordTable& table,
                             bool print_table,
                             size_t max_records,
                             bool csi_only)
      : Request(RequestType::ADMIN_CMD_INFO_RECORD),
        log_id_(log_id),
        lsn_start_(lsn_start),
        lsn_end_(lsn_end),
        payload_max_len_(payload_max_len),
        max_records_(max_records),
        sem_(sem),
        out_(out),
        table_(table),
        print_table_(print_table),
        csi_only_(csi_only) {}

  Execution execute() override {
    auto pool =
        ServerWorker::onThisThread()->processor_->sharded_storage_thread_pool_;
    ld_check(pool); // We should not be here if not a storage node.

    for (shard_index_t s = 0; s < pool->numShards(); ++s) {
      std::unique_ptr<InfoRecordStorageTask> task(
          new InfoRecordStorageTask(logid_t(log_id_),
                                    lsn_start_,
                                    lsn_end_,
                                    payload_max_len_,
                                    sem_,
                                    out_,
                                    table_,
                                    print_table_,
                                    max_records_,
                                    csi_only_));
      ServerWorker::onThisThread()->getStorageTaskQueueForShard(s)->putTask(
          std::move(task));
    }

    return Execution::COMPLETE;
  }

 private:
  const logid_t log_id_;
  const lsn_t lsn_start_, lsn_end_;
  const size_t payload_max_len_, max_records_;
  Semaphore& sem_;
  folly::Synchronized<EvbufferTextOutput*>& out_;
  InfoRecordTable& table_;
  const bool print_table_;
  const bool csi_only_;
};

class InfoRecord : public AdminCommand {
 private:
  logid_t::raw_type logid_;
  std::string lsn_start_, lsn_end_;
  ssize_t payload_max_len_ = 100;
  ssize_t max_records_ = 1000;
  bool table_ = false;
  bool csi_only_ = false;
  bool json_ = false;

 public:
  void getOptions(
      boost::program_options::options_description& out_options) override {
    // clang-format off
    out_options.add_options()
      ("logid", boost::program_options::value<logid_t::raw_type>(&logid_)
        ->required())
      ("lsn_start", boost::program_options::value<std::string>(&lsn_start_)
        ->required())
      ("lsn_end", boost::program_options::value<std::string>(&lsn_end_))
      ("payload-max-len",
        boost::program_options::value<ssize_t>(&payload_max_len_)
          ->default_value(payload_max_len_),
        "Truncate payload to at most this many characters. Negative value to "
        "print full payload. Zero to omit payload altogether.")
      ("table", boost::program_options::bool_switch(&table_))
      ("json", boost::program_options::bool_switch(&json_))
      ("max",
        boost::program_options::value<ssize_t>(&max_records_)
          ->default_value(max_records_),
          "Truncate result to at most this many rows.")
      ("csi", boost::program_options::bool_switch(&csi_only_));
    // clang-format on
  }

  void getPositionalOptions(
      boost::program_options::positional_options_description& out_options)
      override {
    out_options.add("logid", 1);
    out_options.add("lsn_start", 1);
    out_options.add("lsn_end", 1);
  }

  std::string getUsage() override {
    return "info record <logid> <lsn_start> [<lsn_end>] "
           "[--payload-max-len=<-1|0|n>] "
           "[--table] "
           "[--json] "
           "[--max=MAX_RECORDS]";
  }

  void run() override {
    if (!server_->getProcessor()->runningOnStorageNode()) {
      out_.printf(
          "Cannot process 'info record' command, not a storage node\r\n");
      return;
    }

    lsn_t lsn_start;
    if (string_to_lsn(lsn_start_, lsn_start) != 0) {
      out_.printf("Invalid lsn '%s' for 'info record' command.\r\n",
                  lsn_start_.c_str());
      return;
    }

    lsn_t lsn_end = lsn_start;
    if (!lsn_end_.empty()) {
      if (string_to_lsn(lsn_end_, lsn_end) != 0) {
        out_.printf("Invalid lsn '%s' for 'info record' command.\r\n",
                    lsn_end_.c_str());
        return;
      }
    }

    if (lsn_end < lsn_start) {
      out_.printf("<lsn_end> must be greater or equal than <lsn_start>.\r\n");
      return;
    }

    // Several storage threads may want to write to the evbuffer. Wrap it with a
    // folly::Synchronized to protect concurrent writes.
    folly::Synchronized<EvbufferTextOutput*> out_ptr(&out_);

    InfoRecordTable table(!json_,
                          "Log ID",
                          "LSN",
                          "Shard",
                          "Wave",
                          "Recovery epoch",
                          "Timestamp",
                          "Last known good",
                          "Copyset",
                          "Flags",
                          "Offset within epoch",
                          "Optional keys",
                          "Is written by recovery",
                          "Payload");

    Semaphore sem;
    size_t max_len = payload_max_len_ >= 0 ? (size_t)payload_max_len_
                                           : std::numeric_limits<size_t>::max();
    std::unique_ptr<Request> req =
        std::make_unique<InfoRecordRequest>(logid_t(logid_),
                                            lsn_start,
                                            lsn_end,
                                            max_len,
                                            sem,
                                            out_ptr,
                                            table,
                                            table_,
                                            max_records_,
                                            csi_only_);
    if (server_->getProcessor()->postRequest(req) != 0) {
      out_.printf("Error: cannot post request on worker\r\n");
      return;
    }

    auto pool = server_->getServerProcessor()->sharded_storage_thread_pool_;
    for (shard_index_t s = 0; s < pool->numShards(); ++s) {
      sem.wait();
    }

    if (table_) {
      json_ ? table.printJson(out_) : table.print(out_);
    }
  }
};

}}} // namespace facebook::logdevice::commands
