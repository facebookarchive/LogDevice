/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/common/DataClass.h"

namespace facebook { namespace logdevice {

EnumMap<DataClass, std::string>& dataClassNames() {
  static EnumMap<DataClass, std::string> _dataClassNames;
  return _dataClassNames;
}

template <>
/* static */
const std::string& EnumMap<DataClass, std::string>::invalidValue() {
  static const std::string invalidDataClassName("INVALID");
  return invalidDataClassName;
}

template <>
void EnumMap<DataClass, std::string>::setValues() {
#define DATA_CLASS(name, prefix) set(DataClass::name, #name);
#include "logdevice/common/data_classes.inc" // nolint
}

EnumMap<DataClass, char>& dataClassPrefixes() {
  static EnumMap<DataClass, char> _dataClassPrefixes;
  return _dataClassPrefixes;
}

template <>
/* static */
const char& EnumMap<DataClass, char>::invalidValue() {
  static const char invalidDataClassPrefix('I');
  return invalidDataClassPrefix;
}

template <>
void EnumMap<DataClass, char>::setValues() {
#define DATA_CLASS(name, prefix) set(DataClass::name, prefix);
#include "logdevice/common/data_classes.inc" // nolint
}

}} // namespace facebook::logdevice
