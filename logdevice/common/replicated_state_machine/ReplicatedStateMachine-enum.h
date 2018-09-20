/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <string>

#include "logdevice/include/EnumMap.h"

/**
 * @file Enum with identifiers for all StorageTask subclasses.  In common/ so
 * that stats code can pull in.
 */

namespace facebook { namespace logdevice {

// rsm_types.inc defines the list of RSM types for which we
// want to have stats.
enum class RSMType : uint8_t {
  UNKNOWN = 0,
#define RSM_TYPE(name, class_name) name,
#include "logdevice/common/replicated_state_machine/rsm_types.inc"
  MAX
};

using RSMTypeNames = EnumMap<RSMType, std::string, RSMType::UNKNOWN>;

extern RSMTypeNames rsmTypeNames;

std::string toString(RSMType x);

}} // namespace facebook::logdevice
