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

class EpochStore : public Table {
 public:
  explicit EpochStore(std::shared_ptr<Context> ctx) : Table(ctx) {}
  static std::string getName() {
    return "epoch_store";
  }
  std::string getDescription() override {
    return "EpochStore is the data store that contains epoch-related metadata "
           "for all the logs provisioned on a cluster.   This table allows "
           "querying the metadata in epoch-store for a set of logs.";
  }
  TableColumns getColumns() const override;
  std::shared_ptr<TableData> getData(QueryContext& ctx) override;
};

}}}} // namespace facebook::logdevice::ldquery::tables
