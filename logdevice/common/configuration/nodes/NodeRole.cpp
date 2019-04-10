/**
 * Copyright (c) 2017-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/common/configuration/nodes/NodeRole.h"

namespace facebook { namespace logdevice { namespace configuration {
namespace nodes {

bool hasRole(RoleSet roles, NodeRole check_role) {
  auto id = static_cast<size_t>(check_role);
  return roles.test(id);
}

}}}} // namespace facebook::logdevice::configuration::nodes
