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

class StoredLogs : public AdminCommandTable {
 public:
  explicit StoredLogs(std::shared_ptr<Context> ctx) : AdminCommandTable(ctx) {}
  static std::string getName() {
    return "stored_logs";
  }
  std::string getDescription() override {
    return "List of logs that have at least one record currently present in "
           "LogsDB, per shard. Doesn't include internal logs (metadata logs, "
           "event log, config log).  Note that it is possible that all the "
           "existing records are behind the trim point but haven't been "
           "removed from the DB yet (by dropping or compacting partitions); "
           "see also the \"rocksdb-partition-compaction-schedule\" setting.";
  }
  TableColumns getFetchableColumns() const override {
    return {
        {"log_id", DataType::LOGID, "Log ID present in this shard."},
        {"shard", DataType::BIGINT, "Shard that contains this log."},
        {"highest_lsn",
         DataType::LSN,
         "Highest LSN that this shard has ever seen for this log."},
        {"highest_partition",
         DataType::BIGINT,
         "ID of the highest LogsDB partition that contains at least one record "
         "for this log. You can use the \"partitions\" table to inspect LogsDB "
         "partitions."},
        {"highest_timestamp_approx",
         DataType::TIME,
         "Approximate value of the highest timestamp of records for this log "
         "on this shard. This is an upper bound, as long as timestamps are "
         "non-decreasing with LSN in this log.  Can be overestimated by up to "
         "\"rocksdb-partition-duration\" setting."}};
  }
  std::string getCommandToSend(QueryContext& /*ctx*/) const override {
    return std::string("info stored_logs --extended --json\n");
  }
};

}}}} // namespace facebook::logdevice::ldquery::tables
