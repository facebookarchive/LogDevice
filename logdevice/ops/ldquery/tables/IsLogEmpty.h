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

class IsLogEmpty : public Table {
 public:
  explicit IsLogEmpty(std::shared_ptr<Context> ctx) : Table(std::move(ctx)) {}
  std::string getDescription() override {
    return "This table provides a way to check which logs are empty or not. It "
           "is implemented on top of the islogEmpty API (see "
           "\"logdevice/include/Client.h\").";
  }
  static std::string getName() {
    return "is_log_empty";
  }
  TableColumns getColumns() const override;
  std::shared_ptr<TableData> getData(QueryContext& ctx) override;
};

}}}} // namespace facebook::logdevice::ldquery::tables
