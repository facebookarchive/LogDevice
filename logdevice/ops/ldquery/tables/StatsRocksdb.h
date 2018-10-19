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

class StatsRocksdb : public AdminCommandTable {
 public:
  explicit StatsRocksdb(std::shared_ptr<Context> ctx)
      : AdminCommandTable(ctx, AdminCommandTable::Type::STAT) {}
  static std::string getName() {
    return "stats_rocksdb";
  }
  std::string getDescription() override {
    return "Return RocksDB statistics for all nodes in the cluster.";
  }
  TableColumns getFetchableColumns() const override {
    return {{"name", DataType::TEXT, "Name of the stat counter."},
            {"value", DataType::BIGINT, "Value of the stat counter."}};
  }
  std::string getCommandToSend(QueryContext& /*ctx*/) const override {
    return std::string("stats rocksdb\n");
  }
};

}}}} // namespace facebook::logdevice::ldquery::tables
