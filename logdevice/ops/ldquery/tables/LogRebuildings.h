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

class LogRebuildings : public AdminCommandTable {
 public:
  explicit LogRebuildings(std::shared_ptr<Context> ctx)
      : AdminCommandTable(ctx) {}
  static std::string getName() {
    return "log_rebuildings";
  }
  std::string getDescription() override {
    return "This table dumps debugging information about the state of "
           "LogRebuilding state machines (see "
           "\"logdevice/server/LogRebuilding.h\") which are state machines "
           "running on donor storage nodes and responsible for rebuilding "
           "records of that log.";
  }
  TableColumns getFetchableColumns() const override {
    return {
        {"log_id", DataType::LOGID, "ID of the log."},
        {"shard",
         DataType::INTEGER,
         "Index of the shard from which the LogRebuilding state machine is "
         "reading."},
        {"started",
         DataType::TIME,
         "Date and Time of when that state machine was started."},
        {"rebuilding_set",
         DataType::TEXT,
         "Information provided to the LogRebuilding state machine which "
         "defines the list of shards that lost record copies which need to be "
         "re-replicated elsewhere. Expressed in the form "
         "\"<shard-id>*?[<dirty-ranges>],...\". \"*\" indicates that the shard "
         "may be up but we want to drain its data by replicating it elsewhere. "
         " If <dirty-ranges> is not empty, this means that the storage shard "
         "only lost data within the specified ranges."},
        {"version",
         DataType::LSN,
         "Rebuilding version.  This version comes from the event log RSM that "
         "coordinates rebuilding.  See the \"event_log\" table."},
        {"until_lsn",
         DataType::LSN,
         "LSN up to which the log must be rebuilt.  See "
         "\"logdevice/server/rebuilding/RebuildingPlanner.h\" for how this LSN "
         "is computed."},
        {"max_timestamp",
         DataType::TIME,
         "Maximum timestamp that this LogRebuilding state machine is "
         "instructed to read through."},
        {"rebuilt_up_to",
         DataType::LSN,
         "Next LSN to be considered by this state machine for rebuilding."},
        {"num_replicated",
         DataType::BIGINT,
         "Number of records replicated by this state machine so far."},
        {"bytes_replicated",
         DataType::BIGINT,
         "Number of bytes replicated by this state machine so far."},
        {"rr_in_flight",
         DataType::BIGINT,
         "Number of stores currently in flight, ie pending acknowledgment from "
         "the recipient storage shard."},
        {"nondurable_stores",
         DataType::BIGINT,
         "Number of stores for which we are pending acknowledgment that they "
         "are durable on disk."},
        {"durable_stores",
         DataType::BIGINT,
         "Number of records that were durably replicated and now are pending "
         "an amend."},
        {"rra_in_flight",
         DataType::BIGINT,
         "Number of amends that are in flight, ie pending acknowledgment from "
         "the recipient storage shard."},
        {"nondurable_amends",
         DataType::BIGINT,
         "Number of amends for which we are pending acknowledgment that they "
         "are durable on disk."},
        {"last_storage_task_status",
         DataType::TEXT,
         "Status of the last batch of records we read from disk."},
    };
  }
  std::string getCommandToSend(QueryContext& /*ctx*/) const override {
    // TODO (#35636262): Use "info rebuilding logs --json" instead.
    return std::string("info rebuildings --json\n");
  }
};

}}}} // namespace facebook::logdevice::ldquery::tables
