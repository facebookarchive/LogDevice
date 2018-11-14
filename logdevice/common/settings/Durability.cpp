/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/common/settings/Durability.h"

namespace facebook { namespace logdevice {

const DurabilityNameMap& durabilityStrings() {
  static DurabilityNameMap name_map;
  return name_map;
}

template <>
/* static */
const std::string& DurabilityNameMap::invalidValue() {
  static const std::string invalidDurabilityName("INVALID");
  return invalidDurabilityName;
}

template <>
void DurabilityNameMap::setValues() {
#define DURABILITY(d) set(Durability::d, #d);
#include "logdevice/common/settings/durability.inc"
}

}} // namespace facebook::logdevice
