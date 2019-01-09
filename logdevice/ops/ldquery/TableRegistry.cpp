/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/ops/ldquery/TableRegistry.h"

#include <unordered_map>

#include "logdevice/ops/ldquery/VirtualTable.h"

namespace facebook { namespace logdevice { namespace ldquery {

void TableRegistry::registerTable(const std::string& name,
                                  std::unique_ptr<Table> table) {
  map_[name] = std::move(table);
}

Table* TableRegistry::getTable(const std::string& name) {
  if (map_.find(name) == map_.end()) {
    return nullptr;
  }

  return map_[name].get();
}

int TableRegistry::attachTables(sqlite3* db) {
  for (auto& it : map_) {
    if (VirtualTable::attach(db, it.first, this)) {
      return -1;
    }
  }

  return 0;
}

std::vector<TableMetadata> TableRegistry::getTables() const {
  std::vector<TableMetadata> values;

  for (auto& kv : map_) {
    TableMetadata t{
        kv.first, kv.second->getDescription(), kv.second->getColumns()};
    values.push_back(t);
  }
  return values;
}

void TableRegistry::notifyNewQuery() {
  for (auto& it : map_) {
    it.second->newQuery();
  }
}

void TableRegistry::setCacheTTL(std::chrono::seconds ttl) {
  for (auto& it : map_) {
    it.second->setCacheTTL(ttl);
  }
}

void TableRegistry::enableServerSideFiltering(bool val) {
  for (auto& it : map_) {
    it.second->enableServerSideFiltering(val);
  }
}

}}} // namespace facebook::logdevice::ldquery
