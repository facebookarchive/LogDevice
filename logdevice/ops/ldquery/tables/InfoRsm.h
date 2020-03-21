/**
 * Copyright (c) 2020-present, Facebook, Inc. and its affiliates.
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

class InfoRsm : public AdminCommandTable {
 public:
  explicit InfoRsm(std::shared_ptr<Context> ctx) : AdminCommandTable(ctx) {}
  static std::string getName() {
    return "info_rsm";
  }
  std::string getDescription() override {
    return "Show RSM in-memory and durable version information in the cluster";
  }
  TableColumns getFetchableColumns() const override {
    return {
        {"peer_id", DataType::INTEGER, "Peer ID"},
        {"state", DataType::TEXT, "State of Peer Node In FailureDetector"},
        {"logsconfig_in_memory_version",
         DataType::LSN,
         "logsconfig in-memory version"},
        {"logsconfig_durable_version",
         DataType::LSN,
         "logsconfig durable version"},
        {"eventlog_in_memory_version",
         DataType::LSN,
         "eventlog in-memory version"},
        {"eventlog_durable_version", DataType::LSN, "eventlog durable version"},
    };
  }
  std::string getCommandToSend(QueryContext& /*ctx*/) const override {
    // TODO: replace with info rsm versions
    return std::string("info rsm --json\n");
  }
};

}}}} // namespace facebook::logdevice::ldquery::tables
