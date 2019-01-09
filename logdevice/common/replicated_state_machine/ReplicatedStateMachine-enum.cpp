/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/common/replicated_state_machine/ReplicatedStateMachine-enum.h"

namespace facebook { namespace logdevice {

EnumMap<RSMType, std::string, RSMType::UNKNOWN> rsmTypeNames;

template <>
const std::string& RSMTypeNames::invalidValue() {
  static const std::string invalidName("UNKNOWN");
  return invalidName;
}

template <>
void RSMTypeNames::setValues() {
#define RSM_TYPE(name, class_name) set(RSMType::name, class_name);
#include "logdevice/common/replicated_state_machine/rsm_types.inc"
}

std::string toString(RSMType t) {
  return rsmTypeNames[t];
}

}} // namespace facebook::logdevice
