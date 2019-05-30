/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <algorithm>

#include <folly/ScopeGuard.h>

#include "logdevice/common/Sender.h"
#include "logdevice/common/SocketTypes.h"
#include "logdevice/common/debug.h"
#include "logdevice/server/IOFaultInjection.h"
#include "logdevice/server/admincommands/AdminCommand.h"

namespace facebook { namespace logdevice { namespace commands {

class InjectShardFault : public AdminCommand {
  using AdminCommand::AdminCommand;
  using DataType = IOFaultInjection::DataType;
  using IOType = IOFaultInjection::IOType;
  using FaultType = IOFaultInjection::FaultType;
  using InjectMode = IOFaultInjection::InjectMode;

 private:
  std::string shard_name_;
  DataType data_type_ = DataType::NONE;
  IOType io_type_ = IOType::NONE;
  FaultType fault_type_ = FaultType::NONE;
  bool single_shot_ = false;
  double percent_chance_ = 100;
  uint32_t latency_ms_{0};
  bool force_{false};

 public:
  void getOptions(
      boost::program_options::options_description& out_options) override {
    // clang-format off
    out_options.add_options()
      ("shard", boost::program_options::value<std::string>(&shard_name_)
        ->required())
      ("data_type", boost::program_options::value<std::string>()
        ->required()
        ->notifier([this](const std::string &value) {
          if (value == "data" || value == "d") {
            data_type_ = DataType::DATA;
          } else if (value == "metadata" || value == "m") {
            data_type_ = DataType::METADATA;
          } else if (value == "all" || value == "a") {
            data_type_ = DataType::ALL;
          } else if (value == "none" || value == "n") {
            data_type_ = DataType::NONE;
          } else {
            throw boost::program_options::error(
              "value of 'data_type' must be either 'data', 'metadata', "
              "'all', or 'none'; " + value + " given.");
          }
        })
      )
      ("io_type", boost::program_options::value<std::string>()
        ->required()
        ->notifier([this](const std::string &value) {
          if (value == "read" || value == "r") {
            io_type_ = IOType::READ;
          } else if (value == "write" || value == "w") {
            io_type_ = IOType::WRITE;
          } else if (value == "all" || value == "a") {
            io_type_ = IOType::ALL;
          } else if (value == "none" || value == "n") {
            io_type_ = IOType::NONE;
          } else {
            throw boost::program_options::error(
              "value of 'type' must be either 'read', 'write', 'all', "
              " or 'none'; " + value + " given.");
          }
        })
      )
      ("code", boost::program_options::value<std::string>()
        ->required()
        ->notifier([this](std::string value) {
          std::transform(value.begin(), value.end(), value.begin(), ::tolower);
          fault_type_ = fault_type_names.reverseLookup(value);
          if (fault_type_ == FaultType::INVALID) {
            throw boost::program_options::error(
              "value of 'type' must be either 'io_error', 'corruption', "
              "'latency' or 'none'; " + value + " given.");
          }
        })
      )
      ("single_shot", boost::program_options::bool_switch(&single_shot_))
      ("latency",  boost::program_options::value<uint32_t>(&latency_ms_))
      ("chance", boost::program_options::value<double>(&percent_chance_)
        ->notifier([this](const double& value) {
          if (value < 0 || value > 100) {
            throw boost::program_options::error(
              "'chance' must be between 0 and 100.'; " +
              std::to_string(value) + " given.");
          }
        })
      )
      ("force", boost::program_options::bool_switch(&force_));
    // clang-format on
  }

  void getPositionalOptions(
      boost::program_options::positional_options_description& out_options)
      override {
    out_options.add("shard", 1);
    out_options.add("data_type", 1);
    out_options.add("io_type", 1);
    out_options.add("code", 1);
  }

  std::string getUsage() override {
    return "inject shard_fault "
           "<shard#|a|all> "
           "<d|data|m|metadata|a|all|n|none> "
           "<r|read|w|write|a|all|n|none> "
           "<io_error|corruption|latency|none> "
           "[--single_shot] "
           "[--chance=PERCENT]"
           "[--latency=LATENCY_MS]";
  }

  void run() override {
    // If this is a production build, require passing --force.
    if (!folly::kIsDebug) {
      if (!force_) {
        out_.write("inject: Production build."
                   "Use --force to proceed anyway.\r\n");
        return;
      }
    }

    if (!server_->getProcessor()->runningOnStorageNode()) {
      out_.printf("Error: Not a storage node.\r\n");
      return;
    }

    std::transform(
        shard_name_.begin(), shard_name_.end(), shard_name_.begin(), ::toupper);

    auto sharded_store = server_->getShardedLocalLogStore();
    shard_index_t shard_idx = 0;
    shard_index_t shard_end_idx = sharded_store->numShards() - 1;
    if (shard_name_ != "ALL" && shard_name_ != "A") {
      try {
        shard_idx = std::stoi(shard_name_);
      } catch (...) {
        out_.printf("Error: shard %s must be 'all' or an integer\r\n",
                    shard_name_.c_str());
        return;
      }
      if (shard_idx < 0 || shard_idx >= sharded_store->numShards()) {
        out_.printf("Error: shard index %d out of range [0, %d]\r\n",
                    shard_idx,
                    sharded_store->numShards() - 1);
        return;
      }
      shard_end_idx = shard_idx;
    }

    for (; shard_idx <= shard_end_idx; ++shard_idx) {
      auto& io_fault_injection = IOFaultInjection::instance();
      io_fault_injection.setFaultInjection(
          shard_idx,
          data_type_,
          io_type_,
          fault_type_,
          single_shot_ ? InjectMode::SINGLE_SHOT : InjectMode::PERSISTENT,
          percent_chance_,
          std::chrono::milliseconds(latency_ms_));
    }
  }
};

}}} // namespace facebook::logdevice::commands
