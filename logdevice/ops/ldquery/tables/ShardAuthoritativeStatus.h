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

class ShardAuthoritativeStatus : public Table {
 public:
  explicit ShardAuthoritativeStatus(std::shared_ptr<Context> ctx)
      : Table(ctx) {}
  static std::string getName() {
    return "shard_authoritative_status";
  }
  std::string getDescription() override {
    return "Show the current state in the event log.  This contains each "
           "shard's authoritative status (see "
           "\"logdevice/common/AuthoritativeStatus.h\"), as well as additional "
           "information related to rebuilding.";
  }
  TableColumns getColumns() const override;
  std::shared_ptr<TableData> getData(QueryContext& ctx) override;
  void newQuery() override;

 private:
  std::shared_ptr<TableData> cached_;
};

}}}} // namespace facebook::logdevice::ldquery::tables
