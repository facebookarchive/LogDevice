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

class Sequencers : public AdminCommandTable {
 public:
  explicit Sequencers(std::shared_ptr<Context> ctx) : AdminCommandTable(ctx) {}
  static std::string getName() {
    return "sequencers";
  }
  std::string getDescription() override {
    return "This table dumps information about all the Sequencer objects in "
           "the cluster.  See \"logdevice/common/Sequencer.h\".";
  }
  TableColumns getFetchableColumns() const override {
    return {
        {"log_id", DataType::LOGID, "Log ID this sequencer is for."},
        {"metadata_log_id",
         DataType::TEXT,
         "ID of the corresponding metadata log."},
        {"state",
         DataType::TEXT,
         "State of the sequencer.  Can be one of: \"UNAVAILABLE\" (Sequencer "
         "has not yet gotten a valid epoch metadata with an epoch number), "
         "\"ACTIVATING\" (Sequencer is in the process of getting an epoch "
         "number and retrieving metadata from the epoch store), \"ACTIVE\" "
         "(Sequencer is able to replicate), \"PREEMPTED\" (Sequencer has been "
         "preempted by another sequencer \"preempted_by\", appends to this "
         "node will be redirected to it), \"PERMANENT_ERROR\" (Permanent "
         "process-wide error such as running out of ephemera ports)."},
        {"epoch", DataType::BIGINT, "Epoch of the sequencer."},
        {"next_lsn",
         DataType::LSN,
         "Next LSN to be issued to a record by this sequencer."},
        {"meta_last_released",
         DataType::LSN,
         "Last released LSN of the metadata log."},
        {"last_released", DataType::LSN, "Last released LSN for the data log."},
        {"last_known_good",
         DataType::LSN,
         "Last known good LSN for this data log.  This is the highest ESN such "
         "that all records up to that ESN are known to be fully replicated."},
        {"in_flight",
         DataType::BIGINT,
         "Number of appends currently in flight."},
        {"last_used_ms",
         DataType::BIGINT,
         "Timestamp of the last record appended by this sequencer."},
        {"state_duration_ms",
         DataType::BIGINT,
         "Amount of time in milliseconds the sequencer has been in the current "
         "\"state\"."},
        {"nodeset_state",
         DataType::TEXT,
         "Contains debugging information about shards in that epoch's storage "
         "set.  \"H\" means that the shard is healthy.  \"L\" means that the "
         "shard reached the low watermark for space usage.  \"O\" means that "
         "the shard reported being overloaded.  \"S\" means that the shard is "
         "out of space.  \"U\" means that the sequencer cannot establish a "
         "connection to the shard.  \"D\" means that the shard's local log "
         "store is not accepting writes.  \"G\" means that the shard is "
         "greylisting for copyset selection because it is too slow.  \"P\" "
         "means that the sequencer is currently probling the health of this "
         "shard."},
        {"preempted_epoch",
         DataType::BIGINT,
         "Epoch of the sequencer that preempted this sequencer (if any)."},
        {"preempted_by",
         DataType::BIGINT,
         "ID of the sequencer that preempted this sequencer (if any)."},
        {"draining",
         DataType::BIGINT,
         "Epoch that is draining (if any).  Draining means that the sequencer "
         "stopped accepting new writes but is completing appends curretnly in "
         "flight."},
        {"metadata_log_written",
         DataType::BIGINT,
         "Whether the epoch metadata used by this sequencer has been written "
         "to the metadata log."},
        {"trim_point", DataType::LSN, "The current trim point for this log."},
        // TODO (T36984535) : deprecate column last_byte_offset
        {"last_byte_offset", DataType::TEXT, "Offsets of the tail record."},
        {"bytes_per_second",
         DataType::REAL,
         "Append throughput averaged over the last throughput_window_seconds "
         "seconds."},
        {"throughput_window_seconds",
         DataType::REAL,
         "Time window over which append throughput estimate bytes_per_second "
         "was obtained."},
        {"seconds_until_nodeset_adjustment",
         DataType::REAL,
         "Time until the next potential nodeset size adjustment or nodeset "
         "randomization. Zero if nodeset adjustment is disabled or if "
         "the sequencer reactivation is in progress."},
    };
  }
  std::string getCommandToSend(QueryContext& ctx) const override {
    logid_t logid;
    if (columnHasEqualityConstraintOnLogid(1, ctx, logid)) {
      return std::string("info sequencers ") + std::to_string(logid.val_) +
          " --json\n";
    } else {
      return std::string("info sequencers --json\n");
    }
  }
};

}}}} // namespace facebook::logdevice::ldquery::tables
