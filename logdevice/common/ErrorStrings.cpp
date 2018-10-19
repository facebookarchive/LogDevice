/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include <ostream>

#include "logdevice/include/Err.h"

namespace facebook { namespace logdevice {

const ErrorCodeStringMap& errorStrings() {
  static ErrorCodeStringMap inst;
  return inst;
}

template <>
/* static */
const ErrorCodeInfo& ErrorCodeStringMap::invalidValue() {
  static const ErrorCodeInfo invalidErrorCodeInfo{
      "UNKNOWN", "invalid error code"};
  return invalidErrorCodeInfo;
}

template <>
void ErrorCodeStringMap::setValues() {
#define ERROR_CODE(e, _, d)                       \
  set(E::e, ErrorCodeInfo{#e, #e ": " d});        \
  static_assert(static_cast<int>(E::e) >= 0, #e); \
  static_assert(static_cast<int>(E::e) < static_cast<int>(E::UNKNOWN), #e);
#include "logdevice/include/errors.inc"
}

std::ostream& operator<<(std::ostream& os, const E& e) {
  return os << error_description(e);
}
}} // namespace facebook::logdevice
