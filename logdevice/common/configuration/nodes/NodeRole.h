/**
 * Copyright (c) 2017-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <cstdint>
#include <string>

#include <folly/Range.h>

namespace facebook { namespace logdevice { namespace configuration {
namespace nodes {

enum class NodeRole : uint8_t { SEQUENCER = 0, STORAGE, Count };

inline constexpr folly::StringPiece toString(NodeRole role) {
  switch (role) {
    case NodeRole::SEQUENCER:
      return "SEQUENCER";
    case NodeRole::STORAGE:
      return "STORAGE";
    case NodeRole::Count:
      return "INTERNAL ERROR";
  }
  return "INTERNAL ERROR";
}

}}}} // namespace facebook::logdevice::configuration::nodes
