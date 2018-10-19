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

class SyncSequencerRequests : public AdminCommandTable {
 public:
  explicit SyncSequencerRequests(std::shared_ptr<Context> ctx)
      : AdminCommandTable(ctx) {}
  static std::string getName() {
    return "sync_sequencer_requests";
  }
  std::string getDescription() override {
    return "List the currently running SyncSequencerRequests on that cluster.  "
           "See \"logdevice/common/SyncSequencerRequest.h\".";
  }
  TableColumns getFetchableColumns() const override {
    return {
        {"log_id", DataType::LOGID, "Log ID the SyncSequencerRequest is for."},
        {"until_lsn", DataType::LSN, "Next LSN retrieved from the sequencer."},
        {"last_released_lsn",
         DataType::LSN,
         "Last released LSN retrieved from the sequencer."},
        {"last_status",
         DataType::TEXT,
         "Status of the last GetSeqStateRequest performed by "
         "SyncSequencerRequest."},
    };
  }
  std::string getCommandToSend(QueryContext& /*ctx*/) const override {
    return std::string("info sync_sequencer_requests --json\n");
  }
};

}}}} // namespace facebook::logdevice::ldquery::tables
