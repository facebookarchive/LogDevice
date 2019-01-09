/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <chrono>
#include <sqlite3.h>
#include <string>
#include <vector>

#include "logdevice/ops/ldquery/TableRegistry.h"

namespace facebook { namespace logdevice { namespace ldquery {

class Context;

class LDQuery {
 public:
  typedef std::vector<std::string> ColumnNames;
  typedef std::vector<std::string> Row;
  typedef std::vector<Row> Rows;

  struct QueryResult {
    ColumnNames headers;
    Rows rows;
    std::vector<size_t> cols_max_size;
    ActiveQueryMetadata metadata;
    // Required in order to have boost bindings for this struct.
    bool operator==(const QueryResult& other) const;
  };

  typedef std::vector<QueryResult> QueryResults;

  /**
   * Construct an LDquery client.
   * @param config_path     Path to the LD tier's config.
   * @param command_timeout Timeout when retrieve data from a LD node through
   *                        its admin command port.
   * @param use_ssl         Indicates that ldquery should connect to admin
   *                        command port using SSL/TLS
   */
  explicit LDQuery(
      std::string config_path,
      std::chrono::milliseconds command_timeout = std::chrono::seconds{5},
      bool use_ssl = false);
  ~LDQuery();

  /**
   * Execute a SQL statement. Return an array of QueryResult objects.
   */
  QueryResults query(const std::string& query);

  std::vector<TableMetadata> getTables() const;

  void setCacheTTL(std::chrono::seconds ttl);
  std::chrono::seconds getCacheTTL() const {
    return cache_ttl_;
  }

  void enableServerSideFiltering(bool val);
  bool serverSideFilteringEnabled() const;

  /**
   * @param val if true, LSNs and timestamps will be displayed in human readable
   *            format "eXnY" or "XXXX-XX-XX XX:XX:XX.XXX" instead of raw
   *            integers.
   */
  void setPrettyOutput(bool val);
  bool getPrettyOutput() const;

 private:
  // Call sqlite3_step() to extract rows from the given statement and build a
  // QueryResult object.
  QueryResult executeNextStmt(sqlite3_stmt* pStmt);

  std::shared_ptr<Context> ctx_;
  std::string config_path_;
  std::chrono::milliseconds command_timeout_;
  bool use_ssl_{false};
  sqlite3* db_{nullptr};
  TableRegistry table_registry_;

  bool server_side_filtering_enabled_{true};
  std::chrono::seconds cache_ttl_{60};
};

}}} // namespace facebook::logdevice::ldquery
