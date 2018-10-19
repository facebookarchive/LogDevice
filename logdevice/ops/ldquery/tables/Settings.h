/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <map>
#include <vector>

#include "../Context.h"
#include "AdminCommandTable.h"

namespace facebook {
  namespace logdevice {
    namespace ldquery {
      namespace tables {

class Settings : public AdminCommandTable {
 public:
  using AdminCommandTable::AdminCommandTable;
  static std::string getName() {
    return "settings";
  }
  std::string getDescription() override {
    return "Dumps the state of all settings for all nodes in the cluster.";
  }
  TableColumns getFetchableColumns() const override {
    return {{"bundle_name",
             DataType::TEXT,
             "Name of the bundle this setting is for."},
            {"name", DataType::TEXT, "Name of the setting."},
            {"current_value", DataType::TEXT, "Current value of the setting."},
            {"default_value", DataType::TEXT, "Default value of the setting."},
            {"from_cli", DataType::TEXT, "Value provided by the CLI, or null."},
            {"from_config",
             DataType::TEXT,
             "Value provided by the config or null."},
            {"from_admin_cmd",
             DataType::TEXT,
             "Value provided by the \"set\" admin command or null."}};
  }
  std::string getCommandToSend(QueryContext& /*ctx*/) const override {
    return std::string("info settings --json\n");
  }
};

}}}} // namespace facebook::logdevice::ldquery::tables
