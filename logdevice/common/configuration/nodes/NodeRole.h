/**
 * Copyright (c) 2017-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <bitset>
#include <cstdint>
#include <string>

#include <folly/Range.h>

namespace facebook { namespace logdevice { namespace configuration {
namespace nodes {

enum class NodeRole : uint8_t { SEQUENCER = 0, STORAGE, Count };
using RoleSet = std::bitset<static_cast<size_t>(NodeRole::Count)>;

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

// check if @param roles has @param check_role
bool hasRole(RoleSet roles, NodeRole check_role);

std::string toString(RoleSet roles);

}}}} // namespace facebook::logdevice::configuration::nodes
