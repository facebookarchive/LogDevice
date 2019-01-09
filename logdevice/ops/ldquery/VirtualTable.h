/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <sqlite3.h>
#include <unordered_map>

#include <folly/Conv.h>

#include "logdevice/common/debug.h"
#include "logdevice/ops/ldquery/Table.h"
#include "logdevice/ops/ldquery/TableRegistry.h"

namespace facebook { namespace logdevice { namespace ldquery {

class VirtualTable {
 public:
  static int xOpen(sqlite3_vtab* pVTab, sqlite3_vtab_cursor** ppCursor);

  static int xClose(sqlite3_vtab_cursor* cur);

  static int xEof(sqlite3_vtab_cursor* cur);

  static int xDestroy(sqlite3_vtab* p);

  static int xNext(sqlite3_vtab_cursor* cur);

  static int xRowid(sqlite3_vtab_cursor* cur, sqlite_int64* pRowid);

  static int xCreate(sqlite3* db,
                     void* pAux,
                     int argc,
                     const char* const* argv,
                     sqlite3_vtab** ppVtab,
                     char** pzErr);

  static int xColumn(sqlite3_vtab_cursor* cur, sqlite3_context* ctx, int col);

  static int xBestIndex(sqlite3_vtab* tab, sqlite3_index_info* pIdxInfo);

  static int xFilter(sqlite3_vtab_cursor* pVtabCursor,
                     int idxNum,
                     const char* idxStr,
                     int argc,
                     sqlite3_value** argv);

  static int attach(sqlite3* db,
                    const std::string& name,
                    TableRegistry* table_registry);

 private:
  // Map a column name to the list of values for that column in each row.
  typedef std::unordered_map<ColumnName, std::vector<ColumnValue>>
      TableDataByColumn;

  struct VTab {
    sqlite3_vtab base;
    std::string name;
    Table* table;
    TableColumns columns;
    std::vector<Column*> column_ptrs; // point to elements of data->cols
    std::shared_ptr<TableData> data;
    ConstraintSet constraints;
  };

  struct BaseCursor {
    // SQLite virtual table cursor.
    sqlite3_vtab_cursor base;
    // Current cursor position.
    int row;
  };
};

}}} // namespace facebook::logdevice::ldquery
