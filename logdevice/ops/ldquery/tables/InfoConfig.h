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

class InfoConfig : public AdminCommandTable {
 public:
  explicit InfoConfig(std::shared_ptr<Context> ctx) : AdminCommandTable(ctx) {}
  static std::string getName() {
    return "info_config";
  }
  std::string getDescription() override {
    return "A table that dumps information about all the configurations "
           "loaded by each node in the cluster.   For each node, there will "
           "be one row for the node's configuration which is in the main "
           "config.  A second row will be present for each node if they load "
           "the log configuration from a separate config file (see "
           "\"include_log_config\" section in the main config).";
  }
  TableColumns getFetchableColumns() const override {
    return {{"uri", DataType::TEXT, "URI of the config."},
            {"source",
             DataType::INTEGER,
             "ID of the node this config originates from.  This may not "
             "necessarily be the same as \"node\" as nodes can synchronize "
             "configuration between each other."},
            {"hash", DataType::TEXT, "Hash of the config."},
            {"last_modified",
             DataType::TIME,
             "Date and Time when the config was last modified."},
            {"last_loaded",
             DataType::TIME,
             "Date and Time when the config was last loaded."}};
  }
  std::string getCommandToSend(QueryContext& /*ctx*/) const override {
    return std::string("info config --metadata --json\n");
  }
};

}}}} // namespace facebook::logdevice::ldquery::tables
