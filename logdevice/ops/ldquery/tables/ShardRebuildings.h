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
           "\"logdevice/server/rebuilding/ShardRebuildingV1.h\").  This state "
           "machine is responsible for running all LogRebuilding state "
           "machines (see \"logs_rebuilding\" table) for all logs in a donor "
           "shard.";
  }
  TableColumns getFetchableColumns() const override {
    return {
        {"shard_id", DataType::BIGINT, "Donor shard."},
        {"rebuilding_set",
         DataType::TEXT,
         "Rebuilding set considered.  See \"rebuilding_set\" column of "
         "the \"log_rebuilding\" table."},
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
        {"local_window_end",
         DataType::TIME,
         "ShardRebuilding schedules reads for all logs within a time window "
         "called the local window.  This shows the end of the current window."},
        {"num_logs_waiting_for_plan",
         DataType::BIGINT,
         "Number of logs that are waiting for a plan.  See "
         "\"logdevice/include/RebuildingPlanner.h\"."},
        {"num_logs_catching_up",
         DataType::BIGINT,
         "Number of LogRebuilding state machines currently active."},
        {"num_logs_queued_for_catch_up",
         DataType::BIGINT,
         "Number of LogRebuilding state machines that are inside the local "
         "window and queued for catch up."},
        {"num_logs_in_restart_queue",
         DataType::BIGINT,
         "Number of LogRebuilding state machines that are ready to be "
         "restarted as soon as a slot is available.  Logs are scheduled for a "
         "restart if we waited too long for writes done by the state machine "
         "to be acknowledged as durable."},
        {"total_memory_used",
         DataType::BIGINT,
         "Total amount of memory used by all LogRebuilding state machines."},
        {"stall_timer_active",
         DataType::INTEGER,
         "If true, all LogRebuilding state machines are stalled until memory "
         "usage decreased."},
        {"num_restart_timers_active",
         DataType::BIGINT,
         "Number of logs that have completed but for which we are still "
         "waiting for acknowlegments that writes were durable."},
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
         "window or aborted because of a persistent error. V2 only"},
        {"task_in_flight",
         DataType::INTEGER,
         "True if a storage task for reading records is in queue or in flight "
         "right now. V2 only."},
        {"persistent_error",
         DataType::INTEGER,
         "True if we encountered an unrecoverable error when reading. Shard "
         "shouldn't stay in this state for more than a few seconds: it's "
         "expected that RebuildingCoordinator will request a rebuilding for "
         "this shard, and rebuilding will rewind without this node's "
         "participation. V2 only."},
        {"read_buffer_bytes",
         DataType::BIGINT,
         "Bytes of records that we've read but haven't started re-replicating "
         "yet. V2 only."},
        {"records_in_flight",
         DataType::BIGINT,
         "Number of records that are being re-replicated right now. V2 only."},
        {"read_pointer",
         DataType::TEXT,
         "How far we have read: partition, log ID, LSN. V2 only."},
        {"progress",
         DataType::REAL,
         "Approximately what fraction of the work is done, between 0 and 1. "
         "-1 if the implementation doesn't support progress estimation."},
    };
  }
  std::string getCommandToSend(QueryContext& /*ctx*/) const override {
    // TODO (#35636262): Use "info rebuilding shards --json" instead.
    return std::string("info rebuildings --shards --json\n");
  }
};

}}}} // namespace facebook::logdevice::ldquery::tables
