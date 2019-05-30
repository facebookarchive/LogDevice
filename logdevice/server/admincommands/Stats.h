/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <sstream>

#include "logdevice/common/PriorityMap.h"
#include "logdevice/common/Processor.h"
#include "logdevice/server/admincommands/AdminCommand.h"
#include "logdevice/server/storage_tasks/StorageTask.h"

namespace facebook { namespace logdevice { namespace commands {

inline void printStats(const Stats& stats,
                       EvbufferTextOutput& out,
                       bool include_log_groups,
                       const char* key_prefix = "") {
  class Callbacks : public Stats::EnumerationCallbacks {
   public:
    Callbacks(EvbufferTextOutput& out,
              const char* key_prefix,
              bool include_log_groups)
        : out_(out),
          keyPrefix_(key_prefix),
          includeLogGroups_(include_log_groups) {}

    // Simple stats.
    void stat(const std::string& name, int64_t val) override {
      out_.printf("STAT %s%s %" PRId64 "\r\n", keyPrefix_, name.c_str(), val);
    }
    // Per-message-type stats.
    void stat(const std::string& name, MessageType msg, int64_t val) override {
      out_.printf("STAT %s%s.%s %" PRId64 "\r\n",
                  keyPrefix_,
                  name.c_str(),
                  messageTypeNames()[msg].c_str(),
                  val);
    }
    // Per-shard stats.
    void stat(const std::string& name,
              shard_index_t shard,
              int64_t val) override {
      out_.printf("STAT %s%s.shard%u %" PRId64 "\r\n",
                  keyPrefix_,
                  name.c_str(),
                  shard,
                  val);
    }
    // Per-traffic-class stats.
    void stat(const std::string& name, TrafficClass tc, int64_t val) override {
      out_.printf("STAT %s%s.%s %" PRId64 "\r\n",
                  keyPrefix_,
                  name.c_str(),
                  trafficClasses()[tc].c_str(),
                  val);
    }
    // Per-flow-group stats.
    void stat(const std::string& name,
              NodeLocationScope flow_group,
              int64_t val) override {
      out_.printf("STAT %s%s.%s %" PRId64 "\r\n",
                  keyPrefix_,
                  name.c_str(),
                  NodeLocation::scopeNames()[flow_group].c_str(),
                  val);
    }
    // Per-flow-group-and-msg-priority stats.
    void stat(const std::string& name,
              NodeLocationScope flow_group,
              Priority pri,
              int64_t val) override {
      out_.printf("STAT %s%s.%s.%s %" PRId64 "\r\n",
                  keyPrefix_,
                  name.c_str(),
                  NodeLocation::scopeNames()[flow_group].c_str(),
                  PriorityMap::toName()[pri].c_str(),
                  val);
    }
    // Per-msg-priority stats (totals of the previous one).
    void stat(const std::string& name, Priority pri, int64_t val) override {
      out_.printf("STAT %s%s.%s %" PRId64 "\r\n",
                  keyPrefix_,
                  name.c_str(),
                  PriorityMap::toName()[pri].c_str(),
                  val);
    }
    // Per-request-type stats.
    void stat(const std::string& name, RequestType rq, int64_t val) override {
      out_.printf("STAT %s%s.%s %" PRId64 "\r\n",
                  keyPrefix_,
                  name.c_str(),
                  requestTypeNames[rq].c_str(),
                  val);
    }
    // Per-storage-task-type stats.
    void stat(const std::string& name,
              StorageTask::Type type,
              int64_t val) override {
      out_.printf("STAT %s%s.%s %" PRId64 "\r\n",
                  keyPrefix_,
                  name.c_str(),
                  storageTaskTypeNames[type].c_str(),
                  val);
    }
    // Per-worker stats
    void stat(const std::string& name,
              worker_id_t worker_id,
              uint64_t load) override {
      out_.printf("STAT %s%s_%d %" PRIu64 "\r\n",
                  keyPrefix_,
                  name.c_str(),
                  worker_id.val(),
                  load);
    }
    // Per-log stats.
    void stat(const char* name,
              const std::string& log_group0,
              int64_t val) override {
      if (!includeLogGroups_) {
        return;
      }
      auto log_group = log_group0;
      std::replace(log_group.begin(), log_group.end(), ' ', '_');
      out_.printf("STAT %s%s.%s %" PRId64 "\r\n",
                  keyPrefix_,
                  log_group.c_str(),
                  name,
                  val);
    }

    // Don't show histograms.
    void histogram(const std::string& /*name*/,
                   const HistogramInterface& /*hist*/) override {}
    void histogram(const std::string& /*name*/,
                   shard_index_t /*shard*/,
                   const HistogramInterface& /*hist*/) override {}

   private:
    EvbufferTextOutput& out_;
    const char* keyPrefix_;
    bool includeLogGroups_;
  };

  Callbacks cb(out, key_prefix, include_log_groups);
  stats.enumerate(&cb, /* list_all */ true);
}

class Stats : public AdminCommand {
 private:
  bool include_log_groups_;

 public:
  std::string getUsage() override {
    return "stats [--include-log-groups]";
  }

  void getOptions(
      boost::program_options::options_description& out_options) override {
    // clang-format off
    out_options.add_options()
      // A synonym for include-log-groups for backwards compatibility.
      ("full", boost::program_options::bool_switch(&include_log_groups_))

      ("include-log-groups",
        boost::program_options::bool_switch(&include_log_groups_));
    // clang-format on
  }

  void run() override {
    if (server_->getParameters()->getStats()) {
      printStats(server_->getParameters()->getStats()->aggregate(),
                 out_,
                 include_log_groups_);
    }
  }
};

class StatsWorker : public AdminCommand {
 private:
  bool include_log_groups_;
  bool only_useful_;

 public:
  std::string getUsage() override {
    return "stats worker [--include-log-groups] [--only-useful]";
  }

  void getOptions(
      boost::program_options::options_description& out_options) override {
    // clang-format off
    out_options.add_options()
      // A synonym for include-log-groups for backwards compatibility.
      ("full", boost::program_options::bool_switch(&include_log_groups_))

      ("include-log-groups",
        boost::program_options::bool_switch(&include_log_groups_))
      ("only-useful", boost::program_options::bool_switch(&only_useful_));
    // clang-format on
  }

  void run() override {
    if (server_->getParameters()->getStats()) {
      auto func = [&](logdevice::Stats& stats) {
        if (stats.worker_id != worker_id_t(-1)) {
          char key[32];
          snprintf(key, sizeof(key), "W%d.", stats.worker_id.val_);
          if (only_useful_) {
            printOnlyUsefulStats(stats, out_, key);
          } else {
            printStats(stats, out_, include_log_groups_, key);
          }
        }
      };
      server_->getParameters()->getStats()->runForEach(func);
    }
  }

  // Print only the few stats that are interesting per-worker.
  static void printOnlyUsefulStats(const logdevice::Stats& stats,
                                   EvbufferTextOutput& out,
                                   const char* key_prefix = "") {
    out.printf("STAT %sworker_requests_executed %" PRId64 "\r\n",
               key_prefix,
               stats.worker_requests_executed.load());
  }
};

class StatsReset : public AdminCommand {
  void run() override {
    if (server_->getParameters()->getStats()) {
      server_->getParameters()->getStats()->reset();
    }
  }
};

}}} // namespace facebook::logdevice::commands
