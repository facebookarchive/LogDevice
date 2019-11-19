/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include "logdevice/server/admincommands/AdminCommand.h"

namespace facebook { namespace logdevice { namespace commands {

class Fill : public AdminCommand {
  using AdminCommand::AdminCommand;

 public:
  std::string getUsage() override {
    return "fill <size> <char>";
  }

  void getOptions(boost::program_options::options_description& opts) override {
    // clang-format off
    opts.add_options()
        ("size", boost::program_options::value<int>(&size_)->required())
        ("char", boost::program_options::value<char>(&char_)->required());
    // clang-format on
  }

  void getPositionalOptions(
      boost::program_options::positional_options_description& out_options)
      override {
    out_options.add("size", 1);
    out_options.add("char", 2);
  }

  void run() override {
    out_.ensure(size_);
    memset(out_.writableData(), char_, size_);
    out_.append(size_);
  }

 private:
  int size_;
  char char_;
};

}}} // namespace facebook::logdevice::commands
