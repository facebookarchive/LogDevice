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

class RecordCache : public AdminCommandTable {
 public:
  explicit RecordCache(std::shared_ptr<Context> ctx) : AdminCommandTable(ctx) {}
  static std::string getName() {
    return "record_cache";
  }
  std::string getDescription() override {
    return "Dumps debugging information about the EpochRecordCache entries "
           "in each storage shard in the cluster.  EpochRecordCache caches "
           "records for a log and epoch that are not yet confirmed as fully "
           "stored by the sequencer (ie they are \"unclean\").";
  }
  TableColumns getFetchableColumns() const override {
    return {
        {"log_id", DataType::LOGID, "Log ID for this EpochRecordCache enrty."},
        {"shard",
         DataType::INTEGER,
         "Shard ID for this EpochRecordCache entry."},
        {"epoch",
         DataType::BIGINT,
         "The epoch of this EpochRecordCache entry."},
        {"payload_bytes",
         DataType::BIGINT,
         "Total size of payloads of records above LNG held by this "
         "EpochRecordCache."},
        {"num_records",
         DataType::BIGINT,
         "Number of records above LNG held by this EpochRecordCache."},
        {"consistent",
         DataType::BOOL,
         "True if the cache is in consistent state and it is safe to consult "
         "it as the source of truth."},
        {"disabled", DataType::BOOL, "Whether the cache is disabled."},
        {"head_esn", DataType::BIGINT, "ESN of the head of the buffer."},
        {"max_esn", DataType::BIGINT, "Largest ESN ever put in the cache."},
        {"first_lng",
         DataType::BIGINT,
         "The first LNG the cache has ever seen since its creation."},
        // TODO (T36984535) : deprecate column offset_within_epoch
        {"offset_within_epoch",
         DataType::TEXT,
         "Most recent value of the amount of data written in the given epoch "
         "as seen by this shard."},
        {"tail_record_lsn",
         DataType::BIGINT,
         "LSN of the tail record of this epoch."},
        {"tail_record_ts",
         DataType::BIGINT,
         "Timestamp of the tail record of this epoch."}};
  }
  std::string getCommandToSend(QueryContext& ctx) const override {
    std::string log_constraint;
    logid_t logid;
    if (columnHasEqualityConstraintOnLogid(1, ctx, logid)) {
      log_constraint = std::string(" ") + std::to_string(logid.val_);
    }

    std::string shard_constraint;
    std::string shard_expr;
    if (columnHasEqualityConstraint(2, ctx, shard_expr)) {
      shard_constraint = std::string(" --shard=") + shard_expr.c_str();
    }

    return std::string("info record_cache --json") + log_constraint +
        shard_constraint + "\n";
  }
};

}}}} // namespace facebook::logdevice::ldquery::tables
