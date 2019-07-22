/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include "../Context.h"
#include "AdminCommandTable.h"

namespace facebook {
  namespace logdevice {
    namespace ldquery {
      namespace tables {

enum class RecordQueryMode : bool {
  CSI = true,
  DATA = false
};

template <RecordQueryMode mode>
class Record : public AdminCommandTable {
 public:
  explicit Record(std::shared_ptr<Context> ctx) : AdminCommandTable(ctx) {}
  static std::string getName() {
    if (mode == RecordQueryMode::CSI) {
      return "record_csi";
    } else {
      return "record";
    }
  }
  std::string getDescription() override {
    std::string additional_info = "";
    if (mode == RecordQueryMode::CSI) {
      additional_info = " This table is different from the record table in the "
        "sense that it only queries the copyset index and therefore can be "
        "more efficient. It can be used to check for divergence between the "
        "data and copyset index.";
    }
    return "This table allows fetching information about individual record "
           "copies in the cluster.  The user must provide query constraints on "
           "the \"log_id\" and \"lsn\" columns.  This table can be useful to "
           "introspect where copies of a record are stored and see their "
           "metadata.  Do not use it to serve production use cases as this "
           "query runs very inneficiently (it bypasses the normal read "
           "protocol and instead performs a point query on all storage nodes "
           "in the cluster)." + additional_info;
  }
  TableColumns getFetchableColumns() const override {
    return {
        {"log_id", DataType::LOGID, "ID of the log this record is for."},
        {"lsn", DataType::LSN, "Sequence number of the record."},
        {"shard",
         DataType::INTEGER,
         "ID of the shard that holds this record copy."},
        {"wave",
         DataType::INTEGER,
         "If \"is_written_by_recovery\" is 0, contains the wave of that "
         "record."},
        {"recovery_epoch",
         DataType::INTEGER,
         "If \"is_written_by_recovery\" is 1, contains the \"sequencer "
         "epoch\" of the log recovery."},
        {"timestamp",
         DataType::TEXT,
         "Timestamp in milliseconds of the record."},
        {"last_known_good",
         DataType::INTEGER,
         "Highest ESN in this record's epoch such that at the time this message"
         "was originally sent by a sequencer all records with this and lower "
         "ESNs in this epoch were known to the sequencer to be fully stored on "
         "R nodes."},
        {"copyset", DataType::TEXT, "Copyset of the record."},
        {"flags",
         DataType::TEXT,
         "Flags for that record.  See "
         "\"logdevice/common/LocalLogStoreRecordFormat.h\" to see the list of "
         "flags."},
         // TODO (T36984535) : deprecate column offset_within_epoch
        {"offset_within_epoch",
         DataType::TEXT,
         "Amount of data written to that record within the epoch."},
        {"optional_keys",
         DataType::TEXT,
         "Optional keys provided by the user.  See \"AppendAttributes\" in "
         "\"logdevice/include/Record.h\"."},
        {"is_written_by_recovery",
         DataType::BOOL,
         "Whether this record was replicated by the Log Recovery."},
        {"payload", DataType::TEXT, "Payload in hex format."}};
  }

  std::string getCommandToSend(QueryContext& ctx) const override {
    logid_t logid;
    if (!columnHasEqualityConstraintOnLogid(1, ctx, logid)) {
      throw LDQueryError("No constraint on logid provided! Please include a "
                         "WHERE log_id=<logid> clause to your query!");
    }

    std::pair<lsn_t, lsn_t> range;
    if (!columnHasConstraintsOnLSN(2, ctx, range)) {
      throw LDQueryError("No constraint on lsn provided! Please include a "
                         "WHERE statement to filter by LSN");
    }

    std::string additional_option = "";
    if (mode == RecordQueryMode::CSI) {
      additional_option = " --csi";
    }
    auto str = folly::format("info record {} {} {} {} --table --json\n",
                             std::to_string(logid.val_),
                             lsn_to_string(range.first),
                             lsn_to_string(range.second),
                             additional_option)
                   .str();
    return str;
  }
};

}}}} // namespace facebook::logdevice::ldquery::tables
