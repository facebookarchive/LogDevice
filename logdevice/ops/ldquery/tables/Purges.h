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

class Purges : public AdminCommandTable {
 public:
  explicit Purges(std::shared_ptr<Context> ctx) : AdminCommandTable(ctx) {}
  static std::string getName() {
    return "purges";
  }
  std::string getDescription() override {
    return "List the PurgeUncleanEpochs state machines currently active in the "
           "cluster. The responsability of this state machine is to delete any "
           "records that were deleted during log recovery on nodes that did "
           "not participate in that recovery. See "
           "\"logdevice/server/storage/PungeUncleanEpochs.h\" for more "
           "information.";
  }
  TableColumns getFetchableColumns() const override {
    return {
        {"log_id", DataType::LOGID, "Log ID the purge state machine is for."},
        {"state", DataType::TEXT, "State of the state machine."},
        {"current_last_clean_epoch",
         DataType::BIGINT,
         "Last clean epoch considered by the state machine."},
        {"purge_to",
         DataType::BIGINT,
         "Epoch up to which this state machine should purge. The state machine "
         "will purge epochs in range [\"current_last_clean_epoch\", "
         "\"purge_to\"]."},
        {"new_last_clean_epoch",
         DataType::BIGINT,
         "New \"last clean epoch\" metadata entry to write into the local log "
         "store once purging completes."},
        {"sequencer",
         DataType::TEXT,
         "ID of the sequencer node that initiated purging."},
        {"epoch_state",
         DataType::TEXT,
         "Dump the state of purging for each epoch.  See "
         "\"logdevice/server/storage/PurgeSingleEpoch.h\""},
    };
  }
  std::string getCommandToSend(QueryContext& /*ctx*/) const override {
    return std::string("info purges --json\n");
  }
};

}}}} // namespace facebook::logdevice::ldquery::tables
