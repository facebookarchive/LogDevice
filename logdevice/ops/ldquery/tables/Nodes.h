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
#include "../Table.h"

namespace facebook {
  namespace logdevice {
    namespace ldquery {
      namespace tables {

class Nodes : public Table {
 public:
  explicit Nodes(std::shared_ptr<Context> ctx) : Table(ctx) {}
  static std::string getName() {
    return "nodes";
  }
  std::string getDescription() override {
    return "Lists the nodes in the cluster from the configuration.";
  }
  TableColumns getColumns() const override;
  std::shared_ptr<TableData> getData(QueryContext& ctx) override;
};

}}}} // namespace facebook::logdevice::ldquery::tables
