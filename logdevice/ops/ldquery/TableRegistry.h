/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <memory>
#include <sqlite3.h>

#include <folly/Memory.h>

#include "logdevice/ops/ldquery/Table.h"

namespace facebook { namespace logdevice { namespace ldquery {

struct TableMetadata {
  std::string name;
  std::string description;
  TableColumns columns;
};

class TableRegistry {
 public:
  template <typename T, typename... Args>
  void registerTable(Args... args) {
    auto t = std::make_unique<T>(args...);
    t->init();
    std::string name = t->getName();
    return registerTable(name, std::move(t));
  }

  void registerTable(const std::string& name, std::unique_ptr<Table> table);

  Table* getTable(const std::string& name);

  std::vector<TableMetadata> getTables() const;

  int attachTables(sqlite3* db);

  // Notify each table that we are in the context of a new query.
  void notifyNewQuery();

  // Notify each table that the cache ttl changes.
  void setCacheTTL(std::chrono::seconds ttl);

  // Notify each table that server side filtering is disabled.
  void enableServerSideFiltering(bool val);

 private:
  std::unordered_map<std::string, std::unique_ptr<Table>> map_;
};

}}} // namespace facebook::logdevice::ldquery
