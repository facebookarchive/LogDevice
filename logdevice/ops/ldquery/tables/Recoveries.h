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

class Recoveries : public AdminCommandTable {
 public:
  explicit Recoveries(std::shared_ptr<Context> ctx) : AdminCommandTable(ctx) {}
  static std::string getName() {
    return "recoveries";
  }
  std::string getDescription() override {
    return "Dumps debugging information about currently running Log Recovery "
           "procedures.  See \"logdevice/common/LogRecoveryRequest.h\" and "
           "\"logdevice/common/EpochRecovery.h\".";
  }
  TableColumns getFetchableColumns() const override {
    return {
        {"log_id", DataType::LOGID, "Log being recovered."},
        {"epoch", DataType::BIGINT, "Epoch being recovered."},
        {"state",
         DataType::TEXT,
         "State of the EpochRecovery state machine. Can be one of "
         "\"FETCHING_LCE\" (We are fetching LCE from the epoch store, an "
         "EpochRecovery state machine will be created for all epochs between "
         " LCE+1 and \"epoch\"), \"READING_METADATA_LOG\" (We are reading "
         "metadata log in order to retrieve metadata necessary to start the "
         "EpochRecovery state machines), \"READING_SEQUENCER_METADATA\" (We "
         "are waiting for the epoch metadata of \"epoch\" to appear in the "
         "metadata log), \"ACTIVE\" (EpochRecovery is active), \"INACTIVE\" "
         "(EpochRecovery is scheduled for activation)."},
        {"lng",
         DataType::BIGINT,
         "EpochRecovery's estimate of LNG for this log."},
        {"dig_sz", DataType::BIGINT, "Number of entries in the digest."},
        {"dig_fmajority",
         DataType::BIGINT,
         "Whether or not an f-majority of shards in that epoch's storage set "
         "have completed the digest phase."},
        {"dig_replic",
         DataType::BIGINT,
         "Whether or not the set of shards that have completed the digest meet "
         "the replication requirements."},
        {"dig_author",
         DataType::BIGINT,
         "Whether the digest is authoritative.  If the  digest is not "
         "authoritative, this means too many shards in the storage set are "
         "under-replicated.  This is an emergency procedure in which recovery "
         "will not plug holes in order to ensure DATALOSS gaps are reported to "
         "readers. In this mode, some legitimate holes may be reported as "
         "false positive DATALOSS gaps to the reader."},
        {"holes_plugged",
         DataType::BIGINT,
         "Number of holes plugged by this EpochRecovery."},
        {"holes_replicate",
         DataType::BIGINT,
         "Number of holes re-replicated by this EpochRecovery."},
        {"holes_conflict",
         DataType::BIGINT,
         "Number of hole/record conflicts found by this EpochRecovery."},
        {"records_replicate",
         DataType::BIGINT,
         "Number of records re-replicated by this EpochRecovery."},
        {"n_mutators",
         DataType::BIGINT,
         "Number of active Mutators.  A mutator is responsible for replicating "
         "a hole or record.  See \"logdevice/common/Mutator.h\"."},
        {"recovery_state",
         DataType::TEXT,
         "State of each shard in the recovery set.  Possible states: \"s\" "
         "means that the shard still has not sent a SEALED reply, \"S\" means "
         "that the shard has sent a SEALED reply, \"d\" means that this shard "
         "is sending a digest, \"D\" means that this shard completed the "
         "digest, \"m\" means that this shard has completed the digest and is "
         "eligible to participate in the mutation phase, \"c\" means that the "
         "shard has been sent a CLEAN request, \"C\" means that the shard has "
         "successfully processed the CLEANED request.  Recovery will stall if "
         "too many nodes are in the \"s\", \"d\" or \"c\" phases.  Suffix "
         "\"(UR) indicates that the shard is under-replicated.  Suffix "
         "\"(AE)\" indicates that the shard is empty."},
        {"created",
         DataType::TIME,
         "Date and Time of when this EpochRecovery was created."},
        {"restarted",
         DataType::TIME,
         "Date and Time of when this EpochRecovery was last restarted."},
        {"n_restarts",
         DataType::BIGINT,
         "Number of times this EpochRecovery was restarted."},
    };
  }
  std::string getCommandToSend(QueryContext& ctx) const override {
    logid_t logid;
    if (columnHasEqualityConstraintOnLogid(1, ctx, logid)) {
      return std::string("info recoveries ") + std::to_string(logid.val_) +
          " --json\n";
    } else {
      return std::string("info recoveries --json\n");
    }
  }
};

}}}} // namespace facebook::logdevice::ldquery::tables
