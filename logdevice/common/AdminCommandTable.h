/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <array>
#include <cstddef>
#include <string>
#include <utility>
#include <vector>

#include <folly/Optional.h>

#include "logdevice/common/AdminCommandTable-fwd.h"

/*
 * @file A class for presenting data in a table. Handy for admin commands.
 *
 * Example usage:
 *
 * // Create a table with two columns:
 * Table<logid_t, LSN> my_table(true, "log id", "lsn");
 *
 * // Add two rows:
 * my_table.next().set<0>(logid_t(1)).set<1>(lsn_t(42));
 * my_table.next().set<0>(logid_t(2)).set<1>(LSN_MAX);
 *
 * // Add another row, this time lsn is not set:
 * my_table.next().set<0>(logid_t(3));
 *
 * // Print the table:
 * my_table.print(evbuffer);
 *
 * Will display:
 *
 * log id  lsn
 * ----------------
 * 1       42
 * 2       LSN_MAX
 * 3
 *
 * You can also print the table in json format for easier parsing from scripts:
 * my_table.printJson(evbuffer);
 */

namespace facebook { namespace logdevice {

class EvbufferTextOutput;

template <typename... Args>
class AdminCommandTable {
 public:
  using MyType = AdminCommandTable<Args...>;

  /**
   * Returns number of columns in the table
   */
  static constexpr std::size_t numCols() {
    return sizeof...(Args);
  }

  using ColumnNames = std::array<std::string, numCols()>;

  /**
   * @param names    Names of the columns.
   * @param prettify Set to true if entries in the table should be more human
   *                 readable, ie special lsn values like 0 are presented as
   *                 "LSN_INVALID", std::chrono::milliseconds values are
   *                 presented in human readable format.
   */
  AdminCommandTable(ColumnNames names, bool prettify);

  /**
   * @param prettify Set to true if entries in the table should be more human
   *                 readable, ie special lsn values like 0 are presented as
   *                 "LSN_INVALID", std::chrono::milliseconds values are
   *                 presented in human readable format.
   * @param cols...  Names of the columns.
   */
  template <typename... S>
  AdminCommandTable(bool prettify, S&&... cols)
      : AdminCommandTable(ColumnNames{{std::forward<S>(cols)...}}, prettify) {
    static_assert(sizeof...(S) == sizeof...(Args),
                  "Constructor must be given as many column names as there "
                  "are columns.");
  }

  /**
   * Start a new row in the table. Subsequent calls to set() will insert data in
   * that new row.
   *
   * @return reference to *this.
   */
  MyType& next();

  /**
   * Set the value for one column in the current row. Should not be called if
   * next() has never been called.
   *
   * @param P          position of the column, first column is at position 0.
   * @param data       value of the column. The type of T should be convertible
   *                   to the type of the data in column P.
   * @param to_string  If specified, use this function to convert `data` to a
   *                   string in pretty format. Otherwise,
   *                   folly::to<std::string> will be used on the data type. If
   *                   the data type is a commonly used data type such as LSN,
   *                   chrono values, etc, a default converted will be used to
   *                   print these values in human readable format.
   */
  template <unsigned P, typename T, typename F>
  MyType& set(T data, F to_string);
  template <unsigned P, typename T>
  MyType& set(T data);

  /**
   * Acts as set() if `data` optional is non-empty, otherwise does nothing.
   */
  template <unsigned P, typename T, typename F>
  MyType& setOptional(folly::Optional<T>& data, F to_string);
  template <unsigned P, typename T>
  MyType& setOptional(folly::Optional<T>& data);

  /**
   * Merge one table with this table. The rows of `other` are added to the rows
   * of this table.
   *
   * @param other Table to merge with.
   */
  void mergeWith(MyType other);

  size_t numRows() const;

  /**
   * Print the table to the given evbuffer.
   *
   * @param output Evbuffer to print the table to.
   * @param max_col_ Only print the first `max_col_` columns. If not provided,
   *                 print all the columns.
   */
  void print(EvbufferTextOutput& output,
             std::size_t max_col_ = numCols()) const;

  void printRowVertically(unsigned int row,
                          EvbufferTextOutput& output,
                          std::size_t max_col_ = numCols()) const;

  /**
   * Print the table to the given evbuffer, in json format.
   *
   * @param output Evbuffer to print the table to.
   * @param max_col_ Only print the first `max_col_` columns. If not provided,
   *                 print all the columns.
   */
  void printJson(EvbufferTextOutput& output,
                 std::size_t max_col_ = numCols()) const;

  std::string toString(bool json = false, size_t max_col_ = numCols()) const;

 private:
  typedef std::array<folly::Optional<std::string>, numCols()> Row;
  typedef std::array<size_t, numCols()> ColumnWidths;

  ColumnWidths widths_;
  ColumnNames names_;
  std::vector<Row> rows_;
  bool prettify_;
};

}} // namespace facebook::logdevice

#include "logdevice/common/AdminCommandTable-inl.h"
