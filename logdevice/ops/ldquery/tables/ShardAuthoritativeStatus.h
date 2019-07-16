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
  enum Verbose {
    NORMAL,
    VERBOSE,
    SPEW,
  };

  explicit ShardAuthoritativeStatus(std::shared_ptr<Context> ctx,
                                    Verbose verbose)
      : Table(ctx), verbose_(verbose) {}
  std::string getName() const {
    std::string res = "shard_authoritative_status";
    switch (verbose_) {
      case Verbose::NORMAL:
        break;
      case Verbose::VERBOSE:
        res += "_verbose";
        break;
      case Verbose::SPEW:
        res += "_spew";
        break;
    }
    return res;
  }
  std::string getDescription() override {
    switch (verbose_) {
      case Verbose::NORMAL:
        return "Show the current state in the event log. This contains each "
               "shard's authoritative status (see "
               "\"logdevice/common/AuthoritativeStatus.h\"), as well as "
               "additional "
               "information related to rebuilding.";
      case Verbose::VERBOSE:
        return "Like shard_authoritative_status but has more columns and "
               "prints "
               "all the shards contained in the RSM state, including the noisy "
               "ones, e.g. nodes that were removed from config. Can be useful "
               "for investigating specifics of the event log RSM behavior, but "
               "not for much else.";
      case Verbose::SPEW:
        return "Like shard_authoritative_status_verbose but has even more "
               "columns.";
    }
    ld_check(false);
    return "???";
  }
  TableColumns getColumns() const override;
  std::shared_ptr<TableData> getData(QueryContext& ctx) override;
  void newQuery() override;

 private:
  std::shared_ptr<TableData> cached_;
  const Verbose verbose_;
};

}}}} // namespace facebook::logdevice::ldquery::tables
