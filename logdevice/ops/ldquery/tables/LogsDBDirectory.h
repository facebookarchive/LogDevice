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

class LogsDBDirectory : public AdminCommandTable {
 public:
  explicit LogsDBDirectory(std::shared_ptr<Context> ctx)
      : AdminCommandTable(ctx) {}
  static std::string getName() {
    return "logsdb_directory";
  }
  std::string getDescription() override {
    return "Contains debugging information about the LogsDB directory on "
           "storage shards.";
  }
  TableColumns getFetchableColumns() const override {
    return {
        {"shard", DataType::INTEGER, "ID of the shard."},
        {"log_id", DataType::BIGINT, "ID of the log."},
        {"partition", DataType::BIGINT, "ID of the partition."},
        {"first_lsn",
         DataType::LSN,
         "Lower bound of the LSN range for that log in this partition."},
        {"max_lsn",
         DataType::LSN,
         "Upper bound of the LSN range for that log in this partition."},
        {"flags",
         DataType::TEXT,
         "Flags for this partition. \"UNDER_REPLICATED\" means that some "
         "writes for this partition were lost (for instance due to the server "
         "crashing) and these records have not yet been rebuilt."},
        {"approximate_size_bytes",
         DataType::BIGINT,
         "Approximate data size in this partition for the given log."},
    };
  }
  std::string getCommandToSend(QueryContext& ctx) const override {
    std::string shard_expr;
    std::string shard_constraint;
    logid_t logid;
    std::string log_constraint;
    std::string partition_expr;
    std::string partitions_constraint;

    if (columnHasEqualityConstraint(1, ctx, shard_expr)) {
      shard_constraint = std::string(" --shard=") + shard_expr;
    }

    if (columnHasEqualityConstraintOnLogid(2, ctx, logid)) {
      log_constraint = std::string(" --logs=") + std::to_string(logid.val_);
    }

    if (columnHasEqualityConstraint(3, ctx, partition_expr)) {
      partitions_constraint = std::string(" --partitions=") + partition_expr;
    }

    return folly::format("logsdb print_directory --json{}{}{}\n",
                         shard_constraint,
                         log_constraint,
                         partitions_constraint)
        .str();
  }
};

}}}} // namespace facebook::logdevice::ldquery::tables
