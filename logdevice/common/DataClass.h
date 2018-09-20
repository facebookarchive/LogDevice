/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <array>
#include <cstdlib>
#include <string>

#include "logdevice/include/EnumMap.h"

/**
 * @file    Data classes used to group writes to the logstore.
 */

namespace facebook { namespace logdevice {

using data_class_index_t = uint8_t;
enum class DataClass : data_class_index_t {
#define DATA_CLASS(name, prefix) name,
#include "logdevice/common/data_classes.inc" // nolint
  MAX,
  INVALID
};

extern EnumMap<DataClass, std::string>& dataClassNames();
extern EnumMap<DataClass, char>& dataClassPrefixes();

inline std::string toString(DataClass dc) {
  return dataClassNames()[dc];
}

}} // namespace facebook::logdevice

// Shouldn't be necessary since c++14...
namespace std {
template <>
struct hash<facebook::logdevice::DataClass> {
  using DataClass = facebook::logdevice::DataClass;
  using data_class_index_t = facebook::logdevice::data_class_index_t;
  size_t operator()(const DataClass& dc) const noexcept {
    return std::hash<data_class_index_t>()((data_class_index_t)dc);
  }
};
} // namespace std
