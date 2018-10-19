/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <map>
#include <tuple>
#include <vector>

#include "../Context.h"
#include "AdminCommandTable.h"

namespace facebook {
  namespace logdevice {
    namespace ldquery {
      namespace tables {

class AppendThroughput : public AdminCommandTable {
 public:
  explicit AppendThroughput(std::shared_ptr<Context> ctx)
      : AdminCommandTable(ctx, AdminCommandTable::Type::STAT) {}
  static std::string getName() {
    return "append_throughput";
  }
  std::string getDescription() override {
    return "For each sequencer node, reports the estimated per-log-group "
           "append throughput over various time periods.  Because different "
           "logs in the same log group may have their sequencer on different "
           "nodes in the cluster, it is necessary to aggregate these rates "
           "across all nodes in the cluster to get an estimate of the global "
           "append throughput of a log group.  If Sequencer Batching is "
           "enabled, this table reports the rate of appends incoming, ie "
           "before batching and compression on the sequencer.";
  }
  TableColumns getFetchableColumns() const override {
    return {
        {"log_group_name", DataType::TEXT, "The name of the log group."},
        {"throughput_1min",
         DataType::BIGINT,
         "Throughput average in the past 1 minute."},
        {"throughput_5min",
         DataType::BIGINT,
         "Throughput average in the past 5 minutes."},
        {"throughput_10min",
         DataType::BIGINT,
         "Throughput average in the past 10 minutes."},
    };
  }
  std::string getCommandToSend(QueryContext& /*ctx*/) const override {
    return std::string("stats throughput appends 1min 5min 10min\n");
  }
};

}}}} // namespace facebook::logdevice::ldquery::tables
