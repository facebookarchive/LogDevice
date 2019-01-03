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

class Graylist : public AdminCommandTable {
 public:
  explicit Graylist(std::shared_ptr<Context> ctx) : AdminCommandTable(ctx) {}
  static std::string getName() {
    return "graylist";
  }
  std::string getDescription() override {
    return "Provides information on graylisted storage nodes per worker per "
           "node. This works only for the outlier based graylisting.";
  }
  TableColumns getFetchableColumns() const override {
    return {
        {"worker_id",
         DataType::INTEGER,
         "The id of the worker running on the node."},
        {"graylisted_node_index", DataType::INTEGER, "The graylisted node ID"},
    };
  }

  std::string getCommandToSend(QueryContext& /*ctx*/) const override {
    return std::string("info graylist --json\n");
  }
};

}}}} // namespace facebook::logdevice::ldquery::tables
