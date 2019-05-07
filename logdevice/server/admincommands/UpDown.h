/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include "logdevice/server/admincommands/AdminCommand.h"
#include "logdevice/server/admincommands/SequencerDeactivationRequest.h"

namespace facebook { namespace logdevice { namespace commands {

class Up : public AdminCommand {
  using AdminCommand::AdminCommand;

 private:
  logid_t::raw_type logid_;

 public:
  void getOptions(
      boost::program_options::options_description& out_options) override {
    out_options.add_options()(
        "logid",
        boost::program_options::value<logid_t::raw_type>(&logid_)->required());
  }
  void getPositionalOptions(
      boost::program_options::positional_options_description& out_options)
      override {
    out_options.add("logid", 1);
  }
  std::string getUsage() override {
    return "up <logid>";
  }

  void run() override {
    if (logid_ <= 0) {
      out_.printf("Invalid parameter for 'up' command. "
                  "Expected a positive log id, got %lu.\r\n",
                  logid_);
      return;
    }

    if (!server_->getParameters()->isSequencingEnabled()) {
      out_.printf("This node does not run sequencers.\r\n");
      return;
    }

    int rv = server_->getProcessor()->allSequencers().activateSequencer(
        logid_t(logid_), "admin command", [](const Sequencer&) {
          return true;
        });

    if (rv == 0) {
      out_.printf("Started sequencer activation for log %lu\r\n", logid_);
      return;
    }

    switch (err) {
      case E::NOTFOUND:
        out_.printf("Log %lu not found in config\r\n", logid_);
        break;
      case E::NOBUFS:
        out_.printf("Maximum number of sequencers was reached\r\n");
        break;
      case E::FAILED:
        out_.printf("Failed to make the epoch store request\r\n");
        break;
      case E::TOOMANY:
        out_.printf(
            "Reached sequencer activation limit for log %lu\r\n", logid_);
        break;
      case E::EXISTS:
        out_.printf("Sequencer for log %lu is active\r\n", logid_);
        break;
      case E::INPROGRESS:
        out_.printf(
            "Activation of sequencer for log %lu is in progress\r\n", logid_);
        break;
      case E::SYSLIMIT:
        out_.printf("Sequencer for log %lu is in ERROR state\r\n", logid_);
        break;
      case E::ISOLATED:
        out_.printf("Node is isolated. Sequencer activation is suspended\r\n");
        break;
      default:
        out_.printf("Unexpected status code %s from "
                    "AllSequencers::activateSequencer()\r\n",
                    error_name(err));
    }
  }
};

class Down : public AdminCommand {
  using AdminCommand::AdminCommand;

 private:
  logid_t::raw_type logid_;

 public:
  void getOptions(
      boost::program_options::options_description& out_options) override {
    out_options.add_options()(
        "logid",
        boost::program_options::value<logid_t::raw_type>(&logid_)->required());
  }
  void getPositionalOptions(
      boost::program_options::positional_options_description& out_options)
      override {
    out_options.add("logid", 1);
  }
  std::string getUsage() override {
    return "down <logid>";
  }

  void run() override {
    out_.printf("Deprecated. Sequencer no long needs deactivation.\r\n");
  }
};

class DeactivateSequencer : public AdminCommand {
  using AdminCommand::AdminCommand;

 private:
  std::queue<logid_t> logs_;
  int batch_{100};
  bool all_{false};

 public:
  void getOptions(
      boost::program_options::options_description& out_options) override {
    // clang-format off
    out_options.add_options()(
    "logs",
    boost::program_options::value<std::string>()->notifier(
        [&](std::string val) {
          if (lowerCase(val) == "all") {
            all_ = true;
            return;
          }
          std::vector<std::string> tokens;
          folly::split(',', val, tokens);
          for (const std::string& token : tokens) {
            try {
              logid_t log(folly::to<logid_t::raw_type>(token));
              logs_.push(log);
            } catch (std::range_error&) {
              throw boost::program_options::error(
                  "invalid value of --logs option: " + val);
            }
          }
        }))(
        "batch",
        boost::program_options::value<int>(&batch_)
            ->required());
    // clang-format on
  }
  void getPositionalOptions(
      boost::program_options::positional_options_description& out_options)
      override {
    out_options.add("logs", 1);
    out_options.add("batch", 2);
  }
  std::string getUsage() override {
    return "sequencer_stop "
           "<'all'|comma-separated list of logs> <batch>";
  }

  void run() override {
    try {
      if (!server_->getParameters()->isSequencingEnabled()) {
        out_.printf("This node does not run sequencers.\r\n");
        return;
      }
      if (all_) {
        // get all the log ids
        for (const auto& seq :
             server_->getProcessor()->allSequencers().accessAll()) {
          const auto state = seq.getState();
          if (state == Sequencer::State::UNAVAILABLE ||
              state == Sequencer::State::PERMANENT_ERROR) {
            continue;
          }
          logs_.push(seq.getLogID());
        }
      }

      int log_count = logs_.size();

      if (batch_ == 0) {
        out_.printf("Batch size can't be zero, terminating this command\r\n");
        return;
      }

      if (log_count == 0) {
        out_.printf(
            "There are not logs to deactivate, terminating this command\r\n");
        return;
      }

      out_.printf("Will send sequencer deactivation request for %d logs\r\n",
                  (int)logs_.size());

      // Post a request to gradually deactivate sequencers
      std::unique_ptr<Request> req =
          std::make_unique<SequencerDeactivationRequest>(
              std::move(logs_), batch_);
      if (server_->getServerProcessor()->postImportant(req) != 0) {
        out_.printf(
            "Failed to post SequencerDeactivationRequest, error: %s.\r\n",
            error_name(err));
        // also log it
        ld_error("Failed to post SequencerDeactivationRequest, error: %s.",
                 error_name(err));
        return;
      }
    } catch (const boost::program_options::error& ex) {
      out_.printf("Error: %s.\r\n", ex.what());
    }
  }
};

}}} // namespace facebook::logdevice::commands
