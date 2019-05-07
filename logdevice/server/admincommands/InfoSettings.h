/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include "logdevice/common/AdminCommandTable.h"
#include "logdevice/common/settings/SettingsUpdater.h"
#include "logdevice/server/admincommands/AdminCommand.h"

namespace facebook { namespace logdevice { namespace commands {

class InfoSettings : public AdminCommand {
 private:
  bool json_ = false;

  typedef AdminCommandTable<std::string, // Bundle Name
                            std::string, // Name
                            std::string, // Current Value
                            std::string, // Default Value
                            std::string, // From CLI
                            std::string, // From Config
                            std::string  // From Admin Cmd
                            >
      InfoSettingsTable;

 public:
  void getOptions(
      boost::program_options::options_description& out_options) override {
    out_options.add_options()(
        "json", boost::program_options::bool_switch(&json_));
  }
  std::string getUsage() override {
    return "info settings [--json]";
  }

  void run() override {
    // TODO(T8584641): add more columns:
    // - flags;
    // - timestamp of last update;
    // - etc.

    InfoSettingsTable table(!json_,
                            "Bundle Name",
                            "Name",
                            "Current Value",
                            "Default Value",
                            "From CLI",
                            "From Config",
                            "From Admin Cmd");

    const SettingsUpdater& settings = server_->getSettings();
    for (const auto& setting : settings.getState()) {
      auto& set = setting.second;
      auto get = [&](SettingsUpdater::Source src) {
        return settings.getValueFromSource(setting.first, src).value_or("");
      };
      table.next()
          .set<0>(set.bundle_name)
          .set<1>(setting.first)
          .set<2>(get(SettingsUpdater::Source::CURRENT))
          .set<3>(folly::join(" ", setting.second.descriptor.default_value));
      std::string cli = get(SettingsUpdater::Source::CLI);
      std::string config = get(SettingsUpdater::Source::CONFIG);
      std::string admin_cmd = get(SettingsUpdater::Source::ADMIN_CMD);
      if (!cli.empty()) {
        table.set<4>(std::move(cli));
      }
      if (!config.empty()) {
        table.set<5>(std::move(config));
      }
      if (!admin_cmd.empty()) {
        table.set<6>(std::move(admin_cmd));
      }
    }

    json_ ? table.printJson(out_) : table.print(out_);
  }
};

}}} // namespace facebook::logdevice::commands
