/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
// override-include-guard

/**
 * @file Included from AdminCommandTable.h
 */

#include <algorithm>
#include <iterator>
#include <numeric>
#include <tuple>

#include <folly/Conv.h>
#include <folly/dynamic.h>
#include <folly/io/Cursor.h>
#include <folly/io/IOBuf.h>
#include <folly/json.h>

#include "logdevice/common/Sockaddr.h"
#include "logdevice/common/types_internal.h"
#include "logdevice/include/types.h"

namespace facebook { namespace logdevice {
namespace admin_command_table {

template <typename T>
struct Converter {
  std::string operator()(T data, bool /*prettify*/) {
    return folly::to<std::string>(data);
  }
};

#define LOGDEVICE_CONVERTER_DECL(T)              \
  template <>                                    \
  std::string Converter<T>::operator()(T, bool); \
  extern template std::string Converter<T>::operator()(T, bool);

// Simple converters implemented in AdminCommandTable.cpp
LOGDEVICE_CONVERTER_DECL(LSN)
LOGDEVICE_CONVERTER_DECL(BYTE_OFFSET)
LOGDEVICE_CONVERTER_DECL(epoch_t)
LOGDEVICE_CONVERTER_DECL(esn_t)
LOGDEVICE_CONVERTER_DECL(logid_t)
LOGDEVICE_CONVERTER_DECL(bool)
LOGDEVICE_CONVERTER_DECL(std::chrono::microseconds)
LOGDEVICE_CONVERTER_DECL(std::chrono::milliseconds)
LOGDEVICE_CONVERTER_DECL(std::chrono::seconds)
LOGDEVICE_CONVERTER_DECL(Status)
LOGDEVICE_CONVERTER_DECL(Sockaddr)

// Externally implemented converters declared here. Admin command library can be
// compiled independent of these.
LOGDEVICE_CONVERTER_DECL(ClientID)
LOGDEVICE_CONVERTER_DECL(Address)

#undef LOGDEVICE_CONVERTER_DECL

} // namespace admin_command_table

template <typename... Args>
AdminCommandTable<Args...>::AdminCommandTable(ColumnNames names, bool prettify)
    : names_(std::move(names)), prettify_(prettify) {
  for (int i = 0; i < numCols(); ++i) {
    widths_[i] = names_[i].size();
  }
}

template <typename... Args>
AdminCommandTable<Args...>& AdminCommandTable<Args...>::next() {
  rows_.push_back(Row{});
  return *this;
}

template <typename... Args>
template <unsigned P, typename T, typename F>
AdminCommandTable<Args...>& AdminCommandTable<Args...>::set(T data,
                                                            F to_string) {
  // Caller should call next() before calling set().
  ld_check(!rows_.empty());

  // Convert `data` to a string.
  std::string converted = to_string(data, prettify_);

  // Update the current maximum width for that column.
  widths_[P] = std::max(widths_[P], converted.size());

  rows_.back()[P] = std::move(converted);
  return *this;
}

template <typename... Args>
template <unsigned P, typename T>
AdminCommandTable<Args...>& AdminCommandTable<Args...>::set(T data) {
  // T is the type of `data`. However we want the real type that was
  // explicitly defined for that column.
  typedef typename std::tuple_element<P, std::tuple<Args...>>::type real_type;
  auto f = admin_command_table::Converter<real_type>();
  return set<P>(static_cast<real_type>(data), f);
}

template <typename... Args>
template <unsigned P, typename T, typename F>
AdminCommandTable<Args...>&
AdminCommandTable<Args...>::setOptional(folly::Optional<T>& data, F to_string) {
  if (data.hasValue()) {
    // Discard return value as we return `*this` below
    set<P>(data.value(), to_string);
  }
  return *this;
}

template <typename... Args>
template <unsigned P, typename T>
AdminCommandTable<Args...>&
AdminCommandTable<Args...>::setOptional(folly::Optional<T>& data) {
  if (data.hasValue()) {
    // Discard return value as we return `*this` below
    set<P>(data.value());
  }
  return *this;
}

template <typename... Args>
void AdminCommandTable<Args...>::mergeWith(AdminCommandTable<Args...> other) {
  rows_.reserve(rows_.size() + other.rows_.size());
  rows_.insert(rows_.end(),
               std::make_move_iterator(other.rows_.begin()),
               std::make_move_iterator(other.rows_.end()));

  for (int i = 0; i < widths_.size(); ++i) {
    widths_[i] = std::max(widths_[i], other.widths_[i]);
  }
}

template <typename... Args>
size_t AdminCommandTable<Args...>::numRows() const {
  return rows_.size();
}

template <typename... Args>
void AdminCommandTable<Args...>::print(folly::io::Appender& output,
                                       std::size_t max_col_) const {
  unsigned int max_col = std::min(max_col_, numCols());

  // Print the headers
  for (int i = 0; i < max_col; ++i) {
    output.printf("%*s", -int(widths_[i] + 2), names_[i].c_str());
  }
  output.printf("%s", "\r\n");

  // Print a seperation
  size_t total_width =
      std::accumulate(widths_.begin(), widths_.begin() + max_col, 0);
  total_width += max_col * 2;
  output.printf("%s", std::string(total_width, '-').c_str());
  output.printf("%s", "\r\n");

  // Print the rows.
  for (int j = 0; j < rows_.size(); ++j) {
    for (int i = 0; i < max_col; ++i) {
      std::string data = rows_[j][i].hasValue() ? rows_[j][i].value() : "";
      output.printf("%*s", -int(widths_[i] + 2), data.c_str());
    }
    output.printf("%s", "\r\n");
  }
}

/**
 * Prints a single row vertically with "key   :  value" format where the spacing
 * is defined by the maximum column name width. This take the row index and most
 * useful when printing tables that will always have a single row (like info)
 */
template <typename... Args>
void AdminCommandTable<Args...>::printRowVertically(
    unsigned int row,
    folly::io::Appender& output,
    std::size_t max_col_) const {
  unsigned int max_col = std::min(max_col_, numCols());

  unsigned int max_width = *(std::max_element(widths_.begin(), widths_.end()));

  if (row >= rows_.size()) {
    output.printf("%s", "NO DATA\r\n");
    return;
  }

  // Print the rows.
  for (int i = 0; i < max_col; ++i) {
    std::string data = rows_[row][i].hasValue() ? rows_[row][i].value() : "";
    output.printf(
        "%*s : %s\r\n", -int(max_width + 2), names_[i].c_str(), data.c_str());
  }
  output.printf("%s", "\r\n");
}

template <typename... Args>
void AdminCommandTable<Args...>::printJson(folly::io::Appender& output,
                                           std::size_t max_col_) const {
  unsigned int max_col = std::min(max_col_, numCols());

  folly::dynamic object = folly::dynamic::object;
  object["headers"] = folly::dynamic(names_.begin(), names_.begin() + max_col);

  folly::dynamic rows = folly::dynamic::array;
  for (auto& r : rows_) {
    folly::dynamic row = folly::dynamic::array;
    for (int i = 0; i < max_col; ++i) {
      if (r[i].hasValue()) {
        row.push_back(r[i].value());
      } else {
        row.push_back(nullptr);
      }
    }
    rows.push_back(std::move(row));
  }
  object["rows"] = std::move(rows);

  auto serialized =
      folly::json::serialize(object, folly::json::serialization_opts());
  output.printf("%s", serialized.c_str());
  output.printf("%s", "\r\n");
}

template <typename... Args>
std::string AdminCommandTable<Args...>::toString(bool json,
                                                 size_t max_col_) const {
  folly::IOBuf buffer;
  constexpr static size_t kGrowth = 1024;
  folly::io::Appender wrapper(&buffer, kGrowth);
  json ? printJson(wrapper, max_col_) : print(wrapper, max_col_);
  auto byte_range = buffer.coalesce();
  return byte_range.toString();
}

}} // namespace facebook::logdevice
