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

class LogStorageState : public AdminCommandTable {
 public:
  explicit LogStorageState(std::shared_ptr<Context> ctx)
      : AdminCommandTable(ctx) {}

  static std::string getName() {
    return "log_storage_state";
  }

  std::string getDescription() override {
    return "Tracks all in-memory metadata for logs on storage nodes "
           "(see \"info log_storage_state\" admin command).";
  }

  TableColumns getFetchableColumns() const override {
    return {
        {"log_id", DataType::LOGID, "Log id for which the storage state is."},
        {"shard",
         DataType::INTEGER,
         "Shard for which that log storage state is."},
        {"last_released",
         DataType::LSN,
         "Last Released LSN as seen by the shard.  If this does not match "
         "the sequencer's last_released_lsn, (See \"sequencer\" table) this "
         "means the shard has not completed purging."},
        {"last_released_src",
         DataType::TEXT,
         "Where the last_released value was gotten from.  Either \"sequencer\""
         " if the sequencer sent a release message, or \"local log store\" "
         "if the value was persisted on disk in the storage shard."},
        {"trim_point",
         DataType::LSN,
         "Trim point for that log on this storage node."},
        {"per_epoch_metadata_trim_point",
         DataType::LSN,
         "Trim point of per-epoch metadata.  PerEpochLogMetadata whose epoch "
         "is <= than this value should be trimmed."},
        {"seal",
         DataType::BIGINT,
         "Normal seal. The storage node will reject all stores with sequence "
         "numbers belonging to epochs that are <= than this value."},
        {"sealed_by",
         DataType::TEXT,
         "Sequencer node that set the normal seal."},
        {"soft_seal",
         DataType::BIGINT,
         "Similar to normal seal except that the sequencer did not explictly "
         "seal the storage node, the seal is implicit because a STORE message "
         "was sent by a sequencer for a new epoch."},
        {"soft_sealed_by",
         DataType::TEXT,
         "Sequencer node that set the soft seal."},
        {"last_recovery_time",
         DataType::BIGINT,
         "Latest time (number of microseconds since steady_clock's epoch) when "
         "some storage node tried to recover the state.  To not be confused "
         "with Log recovery."},
        {"log_removal_time",
         DataType::BIGINT,
         "See LogStorageState::log_removal_time_."},
        {"lce",
         DataType::BIGINT,
         "Last clean epoch.  Updated when the sequencer notifies this storage "
         "node that it has performed recovery on an epoch."},
        {"latest_epoch",
         DataType::BIGINT,
         "Latest seen epoch from the sequencer."},
        // TODO (T36984535) : deprecate column last_epoch_offset
        {"latest_epoch_offset",
         DataType::TEXT,
         "Offsets within the latest epoch"},
        {"permanent_errors",
         DataType::BIGINT,
         "Set to true if a permanent error such as an IO error has been "
         "encountered.  When this is the case, expect readers to not be able "
         "to read this log on this storage shard."}};
  }

  std::string getCommandToSend(QueryContext& ctx) const override {
    std::string log_constraint = "";
    logid_t logid;
    if (columnHasEqualityConstraintOnLogid(1, ctx, logid)) {
      log_constraint = std::string(" --logid=") + std::to_string(logid.val_);
    }

    std::string shard_constraint = "";
    std::string shard_expr;
    if (columnHasEqualityConstraint(2, ctx, shard_expr)) {
      shard_constraint = std::string(" --shard=") + shard_expr.c_str();
    }

    return std::string("info log_storage_state --json") + log_constraint +
        shard_constraint + "\n";
  }
};

}}}} // namespace facebook::logdevice::ldquery::tables
