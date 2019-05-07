/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include "logdevice/common/AdminCommandTable.h"
#include "logdevice/server/admincommands/AdminCommand.h"
#include "logdevice/server/admincommands/SettingOverrideTTLRequest.h"

namespace facebook { namespace logdevice { namespace commands {

// TODO(T8584641): force the user to pass a TTL and reason for the setting
// override.

class SettingSet : public AdminCommand {
  using AdminCommand::AdminCommand;

 private:
  std::string name_;
  std::string value_;
  std::chrono::microseconds ttl_;

 public:
  void getOptions(
      boost::program_options::options_description& out_options) override {
    out_options.add_options()(
        "name", boost::program_options::value<std::string>(&name_)->required())(
        "value",
        boost::program_options::value<std::string>(&value_)->required())(
        "ttl",
        boost::program_options::value<std::chrono::microseconds>(&ttl_)
            ->required());
  }

  void getPositionalOptions(
      boost::program_options::positional_options_description& out_options)
      override {
    out_options.add("name", 1);
    out_options.add("value", 2);
  }

  std::string getUsage() override {
    return "set <name> <value> --ttl <ttl> (ttl can be 1ms, 2s, min, max...)";
  }

  void run() override;
};

class SettingUnset : public AdminCommand {
  using AdminCommand::AdminCommand;

 private:
  std::string name_;

 public:
  void getOptions(
      boost::program_options::options_description& out_options) override {
    out_options.add_options()(
        "name", boost::program_options::value<std::string>(&name_)->required());
  }
  void getPositionalOptions(
      boost::program_options::positional_options_description& out_options)
      override {
    out_options.add("name", 1);
  }
  std::string getUsage() override {
    return "unset <name>";
  }

  void run() override;
};

}}} // namespace facebook::logdevice::commands
