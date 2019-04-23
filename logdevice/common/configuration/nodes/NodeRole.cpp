/**
 * Copyright (c) 2017-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/common/configuration/nodes/NodeRole.h"

#include <type_traits>

namespace facebook { namespace logdevice { namespace configuration {
namespace nodes {

bool hasRole(RoleSet roles, NodeRole check_role) {
  auto id = static_cast<size_t>(check_role);
  return roles.test(id);
}

std::string toString(RoleSet roles) {
  using UT = std::underlying_type_t<NodeRole>;
  std::string ret;
  bool first = true;
  for (UT r = 0; r < static_cast<UT>(NodeRole::Count); ++r) {
    auto role = static_cast<NodeRole>(r);
    if (hasRole(roles, role)) {
      if (!first) {
        ret += ", ";
      }
      ret += toString(role).str();
      first = false;
    }
  }
  return ret;
}

}}}} // namespace facebook::logdevice::configuration::nodes
