/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/ops/ldquery/Table.h"

#include <folly/Conv.h>
#include <folly/Format.h>

#include "logdevice/common/MetaDataLog.h"
#include "logdevice/common/util.h"
#include "logdevice/ops/ldquery/Errors.h"
#include "logdevice/ops/ldquery/Utils.h"

namespace facebook { namespace logdevice { namespace ldquery {

std::string Table::getColumnDefinition() const {
  std::string res = "(";
  auto columns = getColumns();
  for (int i = 0; i < columns.size(); ++i) {
    res += columns[i].name + " ";
    switch (columns[i].type) {
      case DataType::INTEGER:
        res += "INTEGER";
        break;
      case DataType::REAL:
        res += "REAL";
        break;
      case DataType::TEXT:
        res += "TEXT";
        break;
      case DataType::BOOL:
      case DataType::BIGINT:
      case DataType::LOGID:
      case DataType::LSN:
      case DataType::TIME:
        res += "BIGINT";
        break;
    }
    if (i < columns.size() - 1) {
      res += ", ";
    }
  }
  return res + ")";
}

void preprocessColumn(DataType type, ColumnValue* inout_val) {
  ld_check(inout_val != nullptr);
  if (type != DataType::LOGID || !inout_val->hasValue()) {
    return;
  }

  // If the column is of type DataType::LOGID, we have to convert metadata log
  // ids to their data log ids multiplied by -1. This is because LDQuery
  // represents metadata logs as negative log ids since SQLite does not support
  // unsigned 64b integers.

  const logid_t logid(folly::to<logid_t::raw_type>(inout_val->value()));
  if (!MetaDataLog::isMetaDataLog(logid)) {
    return;
  }

  *inout_val =
      s(-static_cast<long long int>(MetaDataLog::dataLogID(logid).val_));
}

std::string constraint_to_string(const Constraint& c) {
  std::string op;
  switch (c.op) {
    case SQLITE_INDEX_CONSTRAINT_EQ:
      op = "=";
      break;
    case SQLITE_INDEX_CONSTRAINT_LT:
      op = "<";
      break;
    case SQLITE_INDEX_CONSTRAINT_GT:
      op = ">";
      break;
    case SQLITE_INDEX_CONSTRAINT_LE:
      op = "<=";
      break;
    case SQLITE_INDEX_CONSTRAINT_GE:
      op = ">=";
      break;
    case SQLITE_INDEX_CONSTRAINT_LIKE:
      op = " LIKE ";
      break;
    case SQLITE_INDEX_CONSTRAINT_MATCH:
      op = " MATCH ";
      break;
    case SQLITE_INDEX_CONSTRAINT_GLOB:
      op = " GLOB ";
      break;
    default:
      op = "???";
      break;
  }
  if (c.expr.hasValue()) {
    return op + c.expr.value();
  } else {
    return op;
  }
  return "";
}

std::string Table::printConstraints(const ConstraintMap& constraint_map) const {
  auto columns = getColumns();

  std::string res;
  for (const auto& p : constraint_map) {
    int col = p.first;
    for (const auto& c : p.second.constraints_) {
      if (!res.empty()) {
        res += ", ";
      }
      res += columns[col].name + constraint_to_string(c);
    }
  }

  return res;
}

std::string Table::printConstraints(const ConstraintSet& constraint_set) const {
  auto columns = getColumns();

  std::string res;
  for (const auto& p : constraint_set) {
    int col = p.first;
    if (!res.empty()) {
      res += ", ";
    }
    res += columns[col].name + constraint_to_string(p.second);
  }

  return res;
}

logid_t Table::parseLogId(const std::string& expr) const {
  auto int_value = folly::to<long long int>(expr);
  if (int_value >= 0) {
    return logid_t(int_value);
  } else {
    return MetaDataLog::metaDataLogID(logid_t(-int_value));
  }
}

bool Table::columnhasconstraint(int col,
                                int op,
                                QueryContext& ctx,
                                std::string& expr) const {
  auto it_constraints = ctx.constraints.find(col);
  if (it_constraints == ctx.constraints.end()) {
    return false;
  }

  const ConstraintList& constraints = it_constraints->second;
  for (const Constraint& c : constraints.constraints_) {
    if (!c.expr.hasValue()) {
      continue;
    }

    if (c.op == op) {
      ctx.used_constraints[col].add(c);
      expr = c.expr.value();
      return true;
    }
  }

  // we could not find a filtering constraint.
  return false;
}

bool Table::columnHasEqualityConstraint(int col,
                                        QueryContext& ctx,
                                        std::string& expr) const {
  return columnhasconstraint(col, SQLITE_INDEX_CONSTRAINT_EQ, ctx, expr);
}

bool Table::columnHasEqualityConstraintOnLogid(int col,
                                               QueryContext& ctx,
                                               logid_t& logid) const {
  std::string expr;
  if (!columnHasEqualityConstraint(col, ctx, expr)) {
    return false;
  }
  logid = parseLogId(expr);
  return true;
}

bool Table::columnHasConstraintsOnLSN(int col,
                                      QueryContext& ctx,
                                      std::pair<lsn_t, lsn_t>& range) const {
  auto parse_lsn = [](const std::string& expr) {
    lsn_t lsn;
    const int rv = string_to_lsn(expr, lsn);
    if (rv == -1) {
      throw LDQueryError(
          folly::format("Cannot parse \"{}\" into a LSN", expr).str());
    }
    return lsn;
  };

  std::string expr;
  if (columnHasEqualityConstraint(col, ctx, expr)) {
    lsn_t lsn = parse_lsn(expr);
    range.first = lsn;
    range.second = lsn;
    return true;
  }

  // Whether the constraints are LE/GE or LT/GT, we use LE/GT constraints for
  // server side filtering and let the SQLite engine filter.

  bool ret = false;
  range.first = LSN_OLDEST;
  range.second = LSN_MAX;
  if (columnhasconstraint(col, SQLITE_INDEX_CONSTRAINT_LE, ctx, expr) ||
      columnhasconstraint(col, SQLITE_INDEX_CONSTRAINT_LT, ctx, expr)) {
    lsn_t lsn = parse_lsn(expr);
    range.second = lsn;
    ret = true;
  }
  if (columnhasconstraint(col, SQLITE_INDEX_CONSTRAINT_GE, ctx, expr) ||
      columnhasconstraint(col, SQLITE_INDEX_CONSTRAINT_GT, ctx, expr)) {
    lsn_t lsn = parse_lsn(expr);
    range.first = lsn;
    ret = true;
  }

  return ret;
}

}}} // namespace facebook::logdevice::ldquery
