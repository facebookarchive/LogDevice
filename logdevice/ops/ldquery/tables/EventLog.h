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

class EventLog : public AdminCommandTable {
 public:
  explicit EventLog(std::shared_ptr<Context> ctx) : AdminCommandTable(ctx) {}
  static std::string getName() {
    return "event_log";
  }
  std::string getDescription() override {
    return "Dump debug information about the EventLogStateMachine objects "
           "running on nodes in the cluster.  The event log is the Replicated "
           "State Machine that coordinates rebuilding and contains the "
           "authoritative status of all shards in the cluster.  This table can "
           "be used to debug whether all nodes in the cluster are caught up to "
           "the same state.";
  }
  TableColumns getFetchableColumns() const override {
    return {
        {"delta_log_id", DataType::LOGID, "Id of the delta log."},
        {"snapshot_log_id", DataType::LOGID, "Id of the snapshot log."},
        {"version", DataType::LSN, "Version of the state."},
        {"delta_read_ptr",
         DataType::LSN,
         "LSN of the last record or gap read from the delta log."},
        {"delta_replay_tail",
         DataType::LSN,
         "On startup, the state machine reads the delta log up to that lsn "
         "(inclusive) "
         "before delivering the initial state to subscribers."},
        {"snapshot_read_ptr",
         DataType::LSN,
         "Read pointer in the snapshot log."},
        {"snapshot_replay_tail",
         DataType::LSN,
         "On startup, the state machine reads the snapshot log up to that lsn "
         "before delivering the initial state to subscribers."},
        {"stalled_waiting_for_snapshot",
         DataType::LSN,
         "If not null, this means the state machine is stalled because it "
         "missed data in the delta log either because it saw a DATALOSS or "
         "TRIM gap.  The state machine will be stalled until it sees a "
         "snapshot with a version greather than this LSN.  Unless another node "
         "writes a snapshot with a bigger version, the operator may have to "
         "manually write a snapshot to recover the state machine."},
        {"delta_appends_in_flight",
         DataType::BIGINT,
         "How many deltas are currently being appended to the delta log by "
         "this node."},
        {"deltas_pending_confirmation",
         DataType::BIGINT,
         "How many deltas are currently pending confirmation on this node, ie "
         "these are deltas currently being written with the CONFIRM_APPLIED "
         "flag, and the node is waiting for the RSM to sync up to that delta's "
         "version to confirm whether or not it was applied."},
        {"snapshot_in_flight",
         DataType::BIGINT,
         "Whether a snapshot is being appended by this node.  Only one node in "
         "the cluster is responsible for creating snapshots (typically the "
         "node with the smallest node id that's alive according to the failure "
         "detector)."},
        {"delta_log_bytes",
         DataType::BIGINT,
         "Number of bytes of delta records that are past the last snapshot."},
        {"delta_log_records",
         DataType::BIGINT,
         "Number of delta records that are past the last snapshot."},
        {"delta_log_healthy",
         DataType::BOOL,
         "Whether the ClientReadStream state machine used to read the delta "
         "log reports itself as healthy, ie it has enough healthy connections "
         "to the storage nodes in the delta log's storage set such that it "
         "should be able to not miss any delta."},
        {"propagated_read_ptr",
         DataType::LSN,
         "All updates up to this LSN (exclusive) were fully propagated to all "
         "state machines (in particular, to RebuildingCoordinator)."}};
  }
  std::string getCommandToSend(QueryContext& /*ctx*/) const override {
    return std::string("info event_log --json\n");
  }
};

}}}} // namespace facebook::logdevice::ldquery::tables
