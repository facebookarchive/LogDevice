/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include "logdevice/common/Processor.h"
#include "logdevice/server/admincommands/AdminCommand.h"

namespace facebook { namespace logdevice { namespace commands {

class Stop : public AdminCommand {
 public:
  void run() override {
    ld_info("STOP admin command received");
    out_.printf("OK\r\n");
    server_->requestStop();
  }
};

class FastShutdown : public AdminCommand {
 private:
  bool enable_;

 public:
  void getOptions(
      boost::program_options::options_description& out_options) override {
    out_options.add_options()(
        "enable", boost::program_options::value<bool>(&enable_)->required());
  }
  void getPositionalOptions(
      boost::program_options::positional_options_description& out_options)
      override {
    out_options.add("enable", 1);
  }
  std::string getUsage() override {
    return "fast_shutdown true|false";
  }

  void run() override {
    server_->getParameters()->setFastShutdownEnabled(enable_);
    out_.printf("OK\r\n");
  }
};

}}} // namespace facebook::logdevice::commands
