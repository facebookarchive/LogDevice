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

class Shards : public AdminCommandTable {
 public:
  using AdminCommandTable::AdminCommandTable;
  static std::string getName() {
    return "shards";
  }
  std::string getDescription() override {
    return "Show information about all shards in a cluster.";
  }
  TableColumns getFetchableColumns() const override {
    return {
        {"shard", DataType::BIGINT, "Shard the information is for."},
        {"is_failing",
         DataType::BIGINT,
         "If true, the server could not open the DB for this shard on startup. "
         " This can happen if the disk on which this shard resides is broken "
         "for instance."},
        {"accepting_writes",
         DataType::TEXT,
         "Status indicating if this shard is accepting writes.  Can be one of: "
         "\"OK\" (the shard is accepting writes), \"LOW_ON_SPC\" (the shard is "
         "accepting writes but is low on free space), \"NOSPC\" (the shard is "
         "not accepting writes because it is low on space), \"DISABLED\" (The "
         "shard will never accept writes. This can happen if the shard entered "
         "fail-safe mode)."},
        {"rebuilding_state",
         DataType::TEXT,
         "\"NONE\": the shard is not rebuilding.  \"WAITING_FOR_REBUILDING\": "
         "the shard is missing data and is waiting for rebuilding to start.  "
         "\"REBUILDING\": the shard is missing data and rebuilding was "
         "started."},
        {"default_cf_version",
         DataType::BIGINT,
         "Returns current version of the data.  if LogsDB is  enabled, this "
         "will return the version of the default column familiy."},
    };
  }
  std::string getCommandToSend(QueryContext& /*ctx*/) const override {
    return std::string("info shards --json\n");
  }
};

}}}} // namespace facebook::logdevice::ldquery::tables
