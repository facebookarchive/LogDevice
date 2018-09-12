/**
 * Copyright (c) 2017-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include "logdevice/server/AdminCommand.h"

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
        logid_t(logid_), [](const Sequencer&) { return true; });

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

}}} // namespace facebook::logdevice::commands
