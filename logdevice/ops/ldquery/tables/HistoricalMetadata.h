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
#include "../Table.h"

namespace facebook {
  namespace logdevice {
    namespace ldquery {
      namespace tables {

class HistoricalMetadataTableBase : public Table {
 public:
  explicit HistoricalMetadataTableBase(std::shared_ptr<Context> ctx)
      : Table(ctx) {}
  TableColumns getColumns() const override;
  std::shared_ptr<TableData> getDataImpl(QueryContext& ctx,
                                         bool legacy = false);
};

class HistoricalMetadata : public HistoricalMetadataTableBase {
 public:
  explicit HistoricalMetadata(std::shared_ptr<Context> ctx)
      : HistoricalMetadataTableBase(std::move(ctx)) {}
  static std::string getName() {
    return "historical_metadata";
  }
  std::string getDescription() override {
    return "This table contains information about historical epoch metadata "
           "for all logs.  While the \"epoch_store\" table provides "
           "information about the current epoch metadata of all logs, this "
           "table provides a history of that metadata for epoch ranges since "
           "the epoch of the first record that is not trimmed.";
  }
  std::shared_ptr<TableData> getData(QueryContext& ctx) override {
    return getDataImpl(ctx, /*legacy=*/false);
  }
};

// legacy version of the table that reads from metadata logs
class HistoricalMetadataLegacy : public HistoricalMetadataTableBase {
 public:
  explicit HistoricalMetadataLegacy(std::shared_ptr<Context> ctx)
      : HistoricalMetadataTableBase(std::move(ctx)) {}
  TableColumns getColumns() const override;
  static std::string getName() {
    return "historical_metadata_legacy";
  }
  std::string getDescription() override {
    return "Same as \"historical_metadata\", but retrieves the metadata less "
           "efficiently by reading the metadata logs directly instead of "
           "contacting the sequencer.  Provides two additional \"lsn\" and "
           "\"timestamp\" columns to identify the metadata log record that "
           "contains the metadata.";
  }
  std::shared_ptr<TableData> getData(QueryContext& ctx) override {
    return getDataImpl(ctx, /*legacy=*/true);
  }
};

}}}} // namespace facebook::logdevice::ldquery::tables
