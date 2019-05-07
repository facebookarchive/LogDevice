/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include "logdevice/common/request_util.h"
#include "logdevice/server/admincommands/AdminCommand.h"

namespace facebook { namespace logdevice { namespace commands {

class StartRecovery : public AdminCommand {
 private:
  logid_t log_id_{LOGID_INVALID};

 public:
  explicit StartRecovery(
      RestrictionLevel restrictionLevel = RestrictionLevel::UNRESTRICTED)
      : AdminCommand(restrictionLevel) {}

  void getOptions(boost::program_options::options_description& opts) override {
    opts.add_options()(
        "logid", boost::program_options::value<uint64_t>(&log_id_.val_));
  }

  void getPositionalOptions(
      boost::program_options::positional_options_description& opts) override {
    opts.add("logid", 1);
  }

  std::string getUsage() override {
    return "startrecovery logid";
  }

  void run() override {
    auto seq = server_->getProcessor()->allSequencers().findSequencer(log_id_);
    if (seq) {
      int rc = run_on_worker(server_->getProcessor(), -1, [seq] {
        seq->getMetaDataLogWriter()->recoverMetaDataLog();

        return seq->startRecovery();
      });
      out_.printf("Started recovery for logid %lu, result %s\r\n",
                  log_id_.val_,
                  rc == 0 ? "success" : "failure");
    } else {
      out_.printf("log_id_ %lu invalid or none specified", log_id_.val_);
    }
  }
};
}}} // namespace facebook::logdevice::commands
