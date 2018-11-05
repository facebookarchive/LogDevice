/**
 * Copyright (c) 2018-present, Facebook, Inc. and its affiliates.
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

class ChunkRebuildings : public AdminCommandTable {
 public:
  explicit ChunkRebuildings(std::shared_ptr<Context> ctx)
      : AdminCommandTable(ctx) {}
  static std::string getName() {
    return "chunk_rebuildings";
  }
  std::string getDescription() override {
    return "In-flight ChunkRebuilding state machines - each responsible for "
           "re-replicating a short range of records for the same log wich "
           "consecutive LSNs and the same copyset (see ChunkRebuilding.h). See "
           "also: shard_rebuildings, log_rebuildings";
  }
  TableColumns getFetchableColumns() const override {
    return {
        {"log_id", DataType::LOGID, "Log ID of the records."},
        {"shard",
         DataType::INTEGER,
         "Index of the shard to which the records belong."},
        {"min_lsn", DataType::LSN, "LSN of first record in the chunk."},
        {"max_lsn", DataType::LSN, "LSN of last record in the chunk."},
        {"chunk_id",
         DataType::BIGINT,
         "ID of the chunk, unique within a process."},
        {"block_id",
         DataType::BIGINT,
         "Sticky copyset block to which the records belong. Block can be split "
         "into multiple chunks."},
        {"total_bytes",
         DataType::BIGINT,
         "Sum of records' payload+header sizes."},
        {"oldest_timestamp",
         DataType::TIME,
         "Timestamp of the first record in the chunk."},
        {"stores_in_flight",
         DataType::BIGINT,
         "Number of records for which we're in the process of storing new "
         "copies."},
        {"amends_in_flight",
         DataType::BIGINT,
         "Number of records for which we're in the process of amending "
         "copysets of existing copies, excluding our own copy."},
        {"amend_self_in_flight",
         DataType::BIGINT,
         "Number of records for which we're in the process of amending "
         "copysets of our own copy."},
        {"started",
         DataType::TIME,
         "Time when the ChunkRebuilding was started."},
    };
  }
  std::string getCommandToSend(QueryContext& /*ctx*/) const override {
    return std::string("info rebuilding chunks --json\n");
  }
};

}}}} // namespace facebook::logdevice::ldquery::tables
