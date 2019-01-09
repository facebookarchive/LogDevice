/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/ops/ldquery/VirtualTable.h"

#include <chrono>

#include "logdevice/common/Timestamp.h"
#include "logdevice/common/util.h"

using std::chrono::steady_clock;

namespace facebook { namespace logdevice { namespace ldquery {

int VirtualTable::xOpen(sqlite3_vtab* /*pVTab*/,
                        sqlite3_vtab_cursor** ppCursor) {
  int rc = SQLITE_NOMEM;
  BaseCursor* pCur;

  pCur = new BaseCursor;

  if (pCur) {
    memset(pCur, 0, sizeof(BaseCursor));
    *ppCursor = (sqlite3_vtab_cursor*)pCur;
    rc = SQLITE_OK;
  }

  return rc;
}

int VirtualTable::xClose(sqlite3_vtab_cursor* cur) {
  BaseCursor* pCur = (BaseCursor*)cur;
  delete pCur;
  return SQLITE_OK;
}

int VirtualTable::xEof(sqlite3_vtab_cursor* cur) {
  BaseCursor* pCur = (BaseCursor*)cur;
  auto* pVtab = (VTab*)cur->pVtab;
  ld_check(pVtab->data);
  return pCur->row >= pVtab->data->numRows();
}

int VirtualTable::xDestroy(sqlite3_vtab* p) {
  auto* pVtab = (VTab*)p;
  delete pVtab;
  return SQLITE_OK;
}

int VirtualTable::xNext(sqlite3_vtab_cursor* cur) {
  BaseCursor* pCur = (BaseCursor*)cur;
  pCur->row++;
  return SQLITE_OK;
}

int VirtualTable::xRowid(sqlite3_vtab_cursor* cur, sqlite_int64* pRowid) {
  BaseCursor* pCur = (BaseCursor*)cur;
  *pRowid = pCur->row;
  return SQLITE_OK;
}

int VirtualTable::xCreate(sqlite3* db,
                          void* pAux,
                          int argc,
                          const char* const* argv,
                          sqlite3_vtab** ppVtab,
                          char** /*pzErr*/) {
  TableRegistry* table_registry = (TableRegistry*)pAux;
  ld_check(table_registry);
  auto* pVtab = new VTab;

  if (!pVtab || argc == 0 || argv[0] == nullptr) {
    return SQLITE_NOMEM;
  }

  memset(&pVtab->base, 0, sizeof(pVtab->base));

  pVtab->name = std::string(argv[0]);
  pVtab->table = table_registry->getTable(argv[0]);
  if (pVtab->table == nullptr) {
    ld_error("Cannot find table %s in registry", argv[0]);
    return SQLITE_ERROR;
  }

  const std::string column_definition = pVtab->table->getColumnDefinition();
  const auto query = "CREATE TABLE " + pVtab->name + column_definition;
  int rc = sqlite3_declare_vtab(db, query.c_str());
  if (rc != SQLITE_OK) {
    ld_error("Error creating virtual table `%s`: %s",
             pVtab->name.c_str(),
             sqlite3_errmsg(db));
    return rc;
  }

  pVtab->columns = pVtab->table->getColumns();
  *ppVtab = (sqlite3_vtab*)pVtab;
  return rc;
}

int VirtualTable::xColumn(sqlite3_vtab_cursor* cur,
                          sqlite3_context* ctx,
                          int col) {
  BaseCursor* pCur = (BaseCursor*)cur;
  auto* pVtab = (VTab*)cur->pVtab;

  if (col >= pVtab->columns.size()) {
    ld_error("Called for column %i but table %s only has %lu columns",
             col,
             pVtab->name.c_str(),
             pVtab->columns.size());
    return SQLITE_ERROR;
  }

  ld_check(pVtab->data);
  size_t size = pVtab->data ? pVtab->data->cols.begin()->second.size() : 0ul;
  if (pCur->row >= size) {
    ld_error("Called for row %i but table only has %lu rows", pCur->row, size);
    return SQLITE_ERROR;
  }

  const ColumnName& column_name = pVtab->columns[col].name;
  auto col_it = pVtab->data->cols.find(column_name);
  if (col_it == pVtab->data->cols.end()) {
    sqlite3_result_null(ctx);
    return SQLITE_OK;
  }

  const ColumnValue& value = col_it->second[pCur->row];

  if (!value.hasValue()) {
    sqlite3_result_null(ctx);
    return SQLITE_OK;
  }

  DataType type = pVtab->columns[col].type;
  const auto& v = value.value();
  bool as_long = false;
  switch (type) {
    case DataType::INTEGER:
      try {
        sqlite3_result_int(ctx, folly::to<int>(v));
      } catch (const std::exception& e) {
        ld_error("Cannot convert %s to int: %s", v.c_str(), e.what());
        sqlite3_result_null(ctx);
      }
      break;
    case DataType::BOOL:
      try {
        sqlite3_result_int(ctx, (v == "true" || v == "1") ? 1 : 0);
      } catch (const std::exception& e) {
        ld_error("Cannot convert %s to boolean: %s", v.c_str(), e.what());
        sqlite3_result_null(ctx);
      }
      break;
    case DataType::REAL:
      try {
        sqlite3_result_double(ctx, folly::to<double>(v));
      } catch (const std::exception& e) {
        ld_error("Cannot convert %s to real: %s", v.c_str(), e.what());
        sqlite3_result_null(ctx);
      }
      break;
    case DataType::TEXT:
      sqlite3_result_text(ctx, v.c_str(), v.size(), nullptr);
      break;
    case DataType::LSN:
      if (pVtab->table->getContext().pretty_output) {
        const lsn_t lsn = folly::to<lsn_t>(v);
        std::string lsn_str = lsn_to_string(lsn);
        sqlite3_result_text(
            ctx, lsn_str.c_str(), lsn_str.size(), SQLITE_TRANSIENT);
      } else {
        as_long = true;
      }
      break;
    case DataType::TIME:
      if (pVtab->table->getContext().pretty_output) {
        const long long ms = folly::to<long long>(v);
        std::string str =
            format_time(RecordTimestamp(std::chrono::milliseconds(ms)));
        sqlite3_result_text(ctx, str.c_str(), str.size(), SQLITE_TRANSIENT);
      } else {
        as_long = true;
      }
      break;
    case DataType::LOGID:
    case DataType::BIGINT:
      as_long = true;
      break;
  }

  if (as_long) {
    long long int int_value;
    bool failed = false;
    try {
      int_value = folly::to<long long int>(v);
    } catch (...) {
      // The value does not fit in long long int. stripping first bit off.
      // TODO: this might hurt values such as LSN_MAX.
      try {
        int_value = folly::to<unsigned long long int>(v) & 0x7FFFFFFFFFFFFFFFLL;
      } catch (const std::exception& e) {
        ld_error("Cannot convert %s to bigint: %s", v.c_str(), e.what());
        failed = true;
      }
    }
    if (failed) {
      sqlite3_result_null(ctx);
    } else {
      sqlite3_result_int64(ctx, int_value);
    }
  }
  return SQLITE_OK;
}

int VirtualTable::xBestIndex(sqlite3_vtab* tab, sqlite3_index_info* pIdxInfo) {
  auto* pVtab = (VTab*)tab;
  pVtab->constraints.clear();

  int expr_index = 0;
  int cost = 0;
  for (size_t i = 0; i < pIdxInfo->nConstraint; ++i) {
    if (!pIdxInfo->aConstraint[i].usable) {
      // Increase the cost, we prefer more usable query constraints.
      cost += 10;
      continue;
    }

    ld_check(pIdxInfo->aConstraint[i].iColumn < pVtab->columns.size());
    auto col = pIdxInfo->aConstraint[i].iColumn;
    pVtab->constraints.push_back(
        std::make_pair(col, Constraint(pIdxInfo->aConstraint[i].op)));

    pIdxInfo->aConstraintUsage[i].argvIndex = ++expr_index;
  }

  ld_debug("Considering table %s with constraints: {%s}. cost=%i",
           pVtab->name.c_str(),
           pVtab->table->printConstraints(pVtab->constraints).c_str(),
           cost);

  pIdxInfo->estimatedCost = cost;
  return SQLITE_OK;
}

int VirtualTable::xFilter(sqlite3_vtab_cursor* pVtabCursor,
                          int /*idxNum*/,
                          const char* /*idxStr*/,
                          int argc,
                          sqlite3_value** argv) {
  BaseCursor* pCur = (BaseCursor*)pVtabCursor;
  auto* pVtab = (VTab*)pVtabCursor->pVtab;

  pCur->row = 0;
  QueryContext ctx;

  pVtab->data.reset();
  for (size_t i = 0; i < pVtab->columns.size(); ++i) {
    ctx.constraints[i].affinity_ = pVtab->columns[i].type;
  }

  for (size_t i = 0; i < argc; ++i) {
    auto expr = (const char*)sqlite3_value_text(argv[i]);
    // Set the expression from SQLite's now-populated argv.
    if (expr == nullptr) {
      pVtab->constraints[i].second.expr = ColumnValue();
    } else {
      pVtab->constraints[i].second.expr = std::string(expr);
    }

    // Add the constraint to the column-sorted query request map.
    ctx.constraints[pVtab->constraints[i].first].add(
        pVtab->constraints[i].second);
  }

  // Generate the data.
  pVtab->data = pVtab->table->getData(ctx);

  // Check that the data structure is well-formed.
  if (!pVtab->data->cols.empty()) {
    size_t num_rows = pVtab->data->cols.begin()->second.size();
    for (const auto& col : pVtab->data->cols) {
      if (col.second.size() != num_rows) {
        ld_critical("Table %s returned different number of rows in different "
                    "columns. This is a bug in the table's code, please fix.",
                    pVtab->name.c_str());
        std::abort();
      }
    }
  }

  ld_debug("Queried table %s with constraints: {%s}. Used constraints: {%s}.",
           pVtab->name.c_str(),
           pVtab->table->printConstraints(ctx.constraints).c_str(),
           pVtab->table->printConstraints(ctx.used_constraints).c_str());

  return SQLITE_OK;
}

int VirtualTable::attach(sqlite3* db,
                         const std::string& name,
                         TableRegistry* table_registry) {
  static sqlite3_module module = {0,
                                  VirtualTable::xCreate,
                                  VirtualTable::xCreate,
                                  VirtualTable::xBestIndex,
                                  VirtualTable::xDestroy,
                                  VirtualTable::xDestroy,
                                  VirtualTable::xOpen,
                                  VirtualTable::xClose,
                                  VirtualTable::xFilter,
                                  VirtualTable::xNext,
                                  VirtualTable::xEof,
                                  VirtualTable::xColumn,
                                  VirtualTable::xRowid};

  int rc =
      sqlite3_create_module(db, name.c_str(), &module, (void*)table_registry);
  if (rc != SQLITE_OK && rc != SQLITE_MISUSE) {
    ld_error("Error creating module `%s`: %i", name.c_str(), rc);
    return -1;
  }

  const std::string sql =
      "CREATE VIRTUAL TABLE temp." + name + " USING " + name;
  char* zErrMsg = 0;
  rc = sqlite3_exec(db, sql.c_str(), 0, 0, &zErrMsg);
  if (rc != SQLITE_OK) {
    ld_error("Error creating virtual table `%s`: %s", name.c_str(), zErrMsg);
    return -1;
  }

  return 0;
}

}}} // namespace facebook::logdevice::ldquery
