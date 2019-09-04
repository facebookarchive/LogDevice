/**
 * Copyright (c) 2018-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "logdevice/common/plugin/BuiltinPermissionCheckerFactory.h"

#include "logdevice/common/ConfigPermissionChecker.h"
#include "logdevice/common/debug.h"

namespace facebook { namespace logdevice {

std::shared_ptr<PermissionChecker> BuiltinPermissionCheckerFactory::
operator()(PermissionCheckerType type,
           const configuration::SecurityConfig& security_cfg) {
  switch (type) {
    case PermissionCheckerType::CONFIG:
      return std::make_shared<ConfigPermissionChecker>(security_cfg);
    case PermissionCheckerType::PERMISSION_STORE:
      // It's better to crash than to return nullptr and grant access to
      // everyone.
      ld_critical("PERMISSION_STORE type is not supported in the builtin "
                  "permission checker factory.");
      ld_check(false);
      break;
    case PermissionCheckerType::NONE:
      return nullptr;
    case PermissionCheckerType::MAX:
      ld_check(false);
      break;
  }
  ld_check(false);
  return nullptr;
}

}} // namespace facebook::logdevice
