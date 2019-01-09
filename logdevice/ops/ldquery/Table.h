/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <map>
#include <set>
#include <sqlite3.h>
#include <tuple>
#include <unordered_map>
#include <vector>

#include <folly/Conv.h>
#include <folly/Optional.h>
#include <folly/hash/Hash.h>

#include "logdevice/include/Client.h"
#include "logdevice/ops/ldquery/Context.h"

/**
 * @file Table is the base class for virtual tables. Each virtual table must
 * provide getColumns() for determining the list of columns and their type as
 * well as getData() for retrieving the data. It's up for the implementation to
 * decide how the data is fetched.
 */

namespace facebook { namespace logdevice { namespace ldquery {

enum class DataType { INTEGER, REAL, TEXT, BIGINT, LSN, BOOL, LOGID, TIME };

typedef std::string TableName;
typedef std::string ColumnName;

struct TableColumn {
  ColumnName name;
  DataType type;
  std::string description;

  std::string type_as_string() const {
    switch (type) {
      case DataType::INTEGER:
        return "int";
      case DataType::REAL:
        return "real";
      case DataType::TEXT:
        return "string";
      case DataType::BIGINT:
        return "long";
      case DataType::LSN:
        return "lsn";
      case DataType::BOOL:
        return "bool";
      case DataType::LOGID:
        return "log_id";
      case DataType::TIME:
        return "time";
      default:
        return "unknown";
    }
  }
};

// List of column names and their types in a table.
typedef std::vector<TableColumn> TableColumns;

// Content of an field of a row in string format. Using folly::Optional in order
// to represent null values.
typedef folly::Optional<std::string> ColumnValue;

// Contents of an entire single column in a table.
typedef std::vector<ColumnValue> Column;

// Result of a query: a 2d table with named columns.
struct TableData {
  // Column name -> row index -> value.
  // All Column's must have the same size.
  std::unordered_map<ColumnName, Column> cols;

  size_t numRows() const {
    return cols.empty() ? 0ul : cols.begin()->second.size();
  }

  // To fill the table you can directly push_back values into `cols`.
  // Alternatively, you can use newRow() and set() like this:
  //  for (...) {
  //    table->newRow();
  //    table->set("some_column", some_value);
  //    // This wouldn't work with push_back-s because columns would end up
  //    // having different length.
  //    if (have_another_value) {
  //      table->set("another_column", another_value);
  //    }
  //    ...
  //  }

  // Adds a new row filled with folly::none-s in all existing columns.
  // Asserts that all columns have the same length.
  void newRow() {
    if (cols.empty()) {
      return;
    }
    size_t num_rows = cols.begin()->second.size();
    for (auto& col : cols) {
      ld_check_eq(col.second.size(), num_rows);
      col.second.push_back(folly::none);
    }
  }

  // Assigns a value in the last row. If the column doesn't exist, adds it and
  // pads with folly::none-s to the length of other columns.
  void set(const ColumnName& column, ColumnValue value) {
    auto it = cols.find(column);
    if (it != cols.end()) {
      it->second.back() = std::move(value);
      return;
    }
    size_t num_rows = cols.empty() ? 1ul : cols.begin()->second.size();
    ld_check(num_rows > 0);
    auto& c = cols[column];
    c.resize(num_rows);
    c.back() = std::move(value);
  }
};

// A constraint on the value of one column.
struct Constraint {
  // Operator of the constraint. Example: if the constraint is "my_column = 42",
  // op = SQLITE_INDEX_CONSTRAINT_EQ.
  unsigned char op;
  // Expression of the constraint. Example: if the constraint is "my_column =
  // 42", then expr = "42".
  ColumnValue expr;
  explicit Constraint(unsigned char _op) {
    op = _op;
  }
  explicit Constraint(unsigned char _op, const ColumnValue& _expr) {
    op = _op;
    expr = _expr;
  }
  bool operator==(const Constraint& o) const {
    return o.op == op && o.expr == expr;
  }
};

// A list of constraints on the value of one column.
struct ConstraintList {
  void add(const struct Constraint& constraint) {
    constraints_.push_back(constraint);
  }

  ConstraintList() {
    affinity_ = DataType::TEXT;
  }

  bool operator==(const ConstraintList& o) const {
    return o.affinity_ == affinity_ && o.constraints_ == constraints_;
  }

  DataType affinity_;
  std::vector<struct Constraint> constraints_;
};

// Map the index of a column to the list of constraints for that column.
typedef std::map<int, struct ConstraintList> ConstraintMap;
// A list of constraints on a column.
typedef std::vector<std::pair<int, struct Constraint>> ConstraintSet;

std::string constraint_to_string(const Constraint& c);

/**
 * Holds the context of the query on this table.
 */
struct QueryContext {
  // Populated by the SQlite engine. Constraints that can be used to leverage an
  // index. We use this to perform server-side filtering but also populate
  // indices locally on demand.
  ConstraintMap constraints;
  // Table::getData() populates this with the constraints from `constraints`
  // that were actually used.
  ConstraintMap used_constraints;
  /// Support a limit to the number of results.
  int limit;
  QueryContext() : limit(0) {}
};

class Table {
 public:
  explicit Table(std::shared_ptr<Context> ctx) : ld_ctx_(ctx) {}

  /**
   * Will be called after construction.
   */
  virtual void init() {}

  /**
   * @return the column definition of the table.
   */
  virtual TableColumns getColumns() const = 0;

  /**
   * @param QueryContext object that can be used to filter the data efficiently
   *        at the source.
   * @return Content of the table as a vector of raws.
   */
  virtual std::shared_ptr<TableData> getData(QueryContext& ctx) = 0;

  /**
   * Notify that we are beginning a new query.
   */
  virtual void newQuery(){};

  /**
   * A description string about this table and what it represents
   */
  virtual std::string getDescription() {
    return "No Description";
  };

  /**
   * Notify that the ttl for the cache (if any) has changed.
   */
  virtual void setCacheTTL(std::chrono::seconds /*ttl*/) {}

  /**
   * Notify the table whether it should do server side filtering.
   */
  virtual void enableServerSideFiltering(bool /*val*/) {}

  /**
   * @return Return the table definition in SQL format.
   */
  std::string getColumnDefinition() const;

  const Context& getContext() const {
    return *ld_ctx_;
  }

  std::string printConstraints(const ConstraintMap& constraint_map) const;
  std::string printConstraints(const ConstraintSet& constraint_set) const;

  logid_t parseLogId(const std::string& expr) const;

  bool columnhasconstraint(int col,
                           int op,
                           QueryContext& ctx,
                           std::string& expr) const;

  bool columnHasEqualityConstraint(int col,
                                   QueryContext& ctx,
                                   std::string& expr) const;

  /**
   * Checks if there is an equality constraint on column `col` for a log id.
   * @param col Column for which to look for constraints;
   * @param ctx Query context;
   * @param if there is an equality constraint, populate this with the log id.
   * @return True if an equality constraint was found.
   */
  bool columnHasEqualityConstraintOnLogid(int col,
                                          QueryContext& ctx,
                                          logid_t& logid) const;

  /**
   * Checks if there are constraints to be applied on the column `col` that is
   * for a LSN.
   * @param col Column for which to look for constraints;
   * @param ctx Query context
   * @param range Populated with a lsn range that matches the constraints.
   * @return True if constraints were found and `range` was populated.
   */
  bool columnHasConstraintsOnLSN(int col,
                                 QueryContext& ctx,
                                 std::pair<lsn_t, lsn_t>& range) const;

  virtual ~Table() {}

 protected:
  std::shared_ptr<Context> ld_ctx_;

 private:
  // Map a column name to its position.
  std::unordered_map<ColumnName, int> nameToPosMap_;
};

/**
 * If `type` == DataType::LOGID, convert any metadata log id to the value of its
 * corresponding logid multiplied by -1. This is done because SQLite does not
 * support unsigned 64b integers.
 * If `type` != DataType::LOGID, do nothing.
 */
void preprocessColumn(DataType type, ColumnValue* inout_val);

}}} // namespace facebook::logdevice::ldquery

namespace std {
template <>
struct hash<facebook::logdevice::ldquery::Constraint> {
  inline size_t
  operator()(const facebook::logdevice::ldquery::Constraint& c) const {
    return folly::hash::hash_combine(c.op, c.expr);
  }
};

template <>
struct hash<facebook::logdevice::ldquery::ConstraintMap> {
  inline size_t
  operator()(const facebook::logdevice::ldquery::ConstraintMap& map) const {
    size_t res = 0;
    for (const auto& p : map) {
      for (const auto& c : p.second.constraints_) {
        res = folly::hash::hash_combine(res, c);
      }
    }
    return res;
  }
};

} // namespace std
