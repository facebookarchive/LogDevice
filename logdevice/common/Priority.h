/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <cstdint>

/**
 * @file  Resource prioritization levels.
 */
namespace facebook { namespace logdevice {

enum class Priority : uint8_t {
#define PRIORITY(name) name,
#include "logdevice/common/priorities.inc"
  NUM_PRIORITIES,
  INVALID
};

constexpr int asInt(Priority p) {
  return static_cast<int>(p);
}

/**
 * @return the next higher Priority, or INVALID if the given Priority
 *         is max or outside the enum's range.
 */
constexpr Priority priorityAbove(Priority p) {
  return (p > Priority::NUM_PRIORITIES || p == Priority::MAX)
      ? Priority::INVALID
      : static_cast<Priority>(asInt(p) - 1);
}

/**
 * @return the next lower Priority, or INVALID if the given Priority
 *         is already min or outside the enum's range.
 */
constexpr Priority priorityBelow(Priority p) {
  return (p >= priorityAbove(Priority::NUM_PRIORITIES))
      ? Priority::INVALID
      : static_cast<Priority>(asInt(p) + 1);
}

}} // namespace facebook::logdevice
