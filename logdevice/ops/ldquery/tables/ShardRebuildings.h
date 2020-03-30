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

class ShardRebuildings : public AdminCommandTable {
 public:
  explicit ShardRebuildings(std::shared_ptr<Context> ctx)
      : AdminCommandTable(ctx) {}
  static std::string getName() {
    return "shard_rebuildings";
  }
  std::string getDescription() override {
    return "Show debugging information about the ShardRebuilding state "
           "machines (see "
           "\"logdevice/server/rebuilding/ShardRebuilding.h\"). This state "
           "machine is responsible for coordinating reads and re-replication "
           "on a donor shard.";
  }
  TableColumns getFetchableColumns() const override {
    return {
        {"shard_id", DataType::BIGINT, "Donor shard."},
        {"rebuilding_set",
         DataType::TEXT,
         "The list of shards that lost record copies which need to be "
         "re-replicated elsewhere. Expressed in the form "
         "\"<shard-id>*?[<dirty-ranges>],...\". \"*\" indicates that the shard "
         "may be up but we want to drain its data by replicating it elsewhere. "
         " If <dirty-ranges> is not empty, this means that the storage shard "
         "only lost data within the specified ranges."},
        {"version",
         DataType::LSN,
         "Rebuilding version.  This version comes from the event log RSM that "
         "coordinates rebuilding.  See the \"event_log\" table."},
        {"global_window_end",
         DataType::TIME,
         "End of the global window (if enabled with "
         "--rebuilding-global-window).  This is a time window used to "
         "synchronize all ShardRebuilding state machines across all donor "
         "shards."},
        {"progress_timestamp",
         DataType::TIME,
         "Approximately how far rebuilding has progressed on this donor, "
         "timestamp-wise. This may be the min timestamp of records of "
         "in-flight RecordRebuilding-s, or partition timestamp that "
         "ReadStorageTask has reached, or something else."},
        {"num_logs_waiting_for_plan",
         DataType::BIGINT,
         "Number of logs that are waiting for a plan.  See "
         "\"logdevice/include/RebuildingPlanner.h\"."},
        {"total_memory_used",
         DataType::BIGINT,
         "Approximate total amount of memory used by ShardRebuilding state "
         "machine."},
        {"num_active_logs",
         DataType::BIGINT,
         "Set of logs being rebuilt for this shard.  The shard completes "
         "rebuilding when this number reaches zero."},
        {"participating",
         DataType::INTEGER,
         "true if this shard is a donor for this rebuilding and hasn't "
         "finished rebuilding yet."},
        {"time_by_state",
         DataType::TEXT,
         "Time spent in each state. 'stalled' means either waiting for global "
         "window or aborted because of a persistent error."},
        {"task_in_flight",
         DataType::INTEGER,
         "True if a storage task for reading records is in queue or in flight "
         "right now."},
        {"persistent_error",
         DataType::INTEGER,
         "True if we encountered an unrecoverable error when reading. Shard "
         "shouldn't stay in this state for more than a few seconds: it's "
         "expected that RebuildingCoordinator will request a rebuilding for "
         "this shard, and rebuilding will rewind without this node's "
         "participation."},
        {"read_buffer_bytes",
         DataType::BIGINT,
         "Bytes of records that we've read but haven't started re-replicating "
         "yet."},
        {"records_in_flight",
         DataType::BIGINT,
         "Number of records that are being re-replicated right now."},
        {"read_pointer",
         DataType::TEXT,
         "How far we have read: partition, log ID, LSN."},
        {"progress",
         DataType::REAL,
         "Approximately what fraction of the work is done, between 0 and 1. "
         "-1 if the implementation doesn't support progress estimation."},
    };
  }
  std::string getCommandToSend(QueryContext& /*ctx*/) const override {
    return std::string("info rebuilding shards --json\n");
  }
};

}}}} // namespace facebook::logdevice::ldquery::tables
