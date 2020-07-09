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
    return "This table dumps some per-log state of rebuilding on this donor "
           "node, mostly related to reading. See also shard_rebuildings.";
  }
  TableColumns getFetchableColumns() const override {
    return {
        {"log_id", DataType::LOGID, "ID of the log."},
        {"shard",
         DataType::INTEGER,
         "Index of the shard from which the ShardRebuilding state machine is "
         "reading."},
        {"until_lsn",
         DataType::LSN,
         "LSN up to which the log must be rebuilt.  See "
         "\"logdevice/server/rebuilding/RebuildingPlanner.h\" for "
         "how this LSN "
         "is computed."},
        {"rebuilt_up_to",
         DataType::LSN,
         "Next LSN to be considered by this state machine for rebuilding."},
        {"num_replicated",
         DataType::BIGINT,
         "Number of records replicated by this state machine so far."},
        {"bytes_replicated",
         DataType::BIGINT,
         "Number of bytes replicated by this state machine so far."},
    };
  }
  std::string getCommandToSend(QueryContext& /*ctx*/) const override {
    return std::string("info rebuilding logs --json\n");
  }
};

}}}} // namespace facebook::logdevice::ldquery::tables
