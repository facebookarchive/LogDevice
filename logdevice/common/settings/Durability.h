/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <algorithm>
#include <string>

#include "logdevice/include/EnumMap.h"

/**
 * @file    Storage durability requirements for write operations.
 */

namespace facebook { namespace logdevice {

enum class Durability : uint8_t {
#define DURABILITY(name) name,
#include "logdevice/common/settings/durability.inc"
  NUM_DURABILITIES,
  INVALID
};

using DurabilityNameMap =
    EnumMap<Durability,
            std::string,
            Durability::INVALID,
            static_cast<int>(Durability::NUM_DURABILITIES)>;
const DurabilityNameMap& durabilityStrings();

inline const std::string& durability_to_string(Durability d) {
  return durabilityStrings()[d];
}

}} // namespace facebook::logdevice
