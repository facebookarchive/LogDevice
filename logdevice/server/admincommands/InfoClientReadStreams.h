/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <folly/Memory.h>

#include "logdevice/common/AdminCommandTable.h"
#include "logdevice/common/Processor.h"
#include "logdevice/common/client_read_stream/AllClientReadStreams.h"
#include "logdevice/common/request_util.h"
#include "logdevice/common/util.h"
#include "logdevice/server/admincommands/AdminCommand.h"

namespace facebook { namespace logdevice { namespace commands {

class InfoClientReadStreams : public AdminCommand {
 private:
  bool json_ = false;

 public:
  void getOptions(
      boost::program_options::options_description& out_options) override {
    out_options.add_options()(
        "json", boost::program_options::bool_switch(&json_));
  }

  void getPositionalOptions(
      boost::program_options::positional_options_description& /*out_options*/)
      override {}

  std::string getUsage() override {
    return "info client_read_streams [--json]";
  }

  void run() override {
    std::string data = AllClientReadStreams::getAllReadStreamsDebugInfo(
        !json_, json_, *server_->getProcessor());
    out_.printf("%s", data.c_str());
  }
};

}}} // namespace facebook::logdevice::commands
