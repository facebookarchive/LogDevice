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

class Iterators : public AdminCommandTable {
 public:
  explicit Iterators(std::shared_ptr<Context> ctx) : AdminCommandTable(ctx) {}
  static std::string getName() {
    return "iterators";
  }
  std::string getDescription() override {
    return "This table allows fetching the list of RocksDB iterators on all "
           "storage nodes.";
  }
  TableColumns getFetchableColumns() const override {
    return {
        {"column_family",
         DataType::TEXT,
         "Name of the column family that the iterator is open on."},
        {"log_id", DataType::LOGID, "ID of the log the iterator is reading."},
        {"is_tailing",
         DataType::BIGINT,
         "1 if it is a tailing iterator, 0 otherwise."},
        {"is_blocking",
         DataType::BIGINT,
         "1 if this is an iterator which is allowed to block when the blocks "
         "it reads are not in RocksDB's block cache, 0 otherwise."},
        {"type",
         DataType::TEXT,
         "Type of the iterator. See \"IteratorType\" in "
         "\"logdevice/server/locallogstore/IteratorTracker.h\" for the list of "
         "iterator types."},
        {"rebuilding",
         DataType::BIGINT,
         "1 if this iterator is used for rebuilding, 0 if in other contexts."},
        {"high_level_id",
         DataType::BIGINT,
         "A unique identifier that the high-level iterator was assigned "
         "to.  Used to tie higher-level iterators with lower-level "
         "iterators created by them."},
        {"created_timestamp",
         DataType::TIME,
         "Timestamp when the iterator was created."},
        {"more_context",
         DataType::TEXT,
         "More information on where this iterator was created."},
        {"last_seek_lsn",
         DataType::LSN,
         "Last LSN this iterator was seeked to."},
        {"last_seek_timestamp",
         DataType::TIME,
         "When the iterator was last seeked."},
        {"version",
         DataType::BIGINT,
         "RocksDB superversion that this iterator points to."},
    };
  }
  std::string getCommandToSend(QueryContext& /*ctx*/) const override {
    return std::string("info iterators --json\n");
  }
};

}}}} // namespace facebook::logdevice::ldquery::tables
