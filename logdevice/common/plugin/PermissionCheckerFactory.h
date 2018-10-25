/**
 * Copyright (c) 2018-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <string>
#include <unordered_set>

#include "logdevice/common/PermissionChecker.h"
#include "logdevice/common/plugin/Plugin.h"

namespace facebook { namespace logdevice {

/**
 * @file `PermissionCheckerFactory` will be used to create a `PermissionChecker`
 * instance.
 */

class PermissionCheckerFactory : public Plugin {
 public:
  PluginType type() const override {
    return PluginType::PERMISSION_CHECKER_FACTORY;
  }

  /**
   * @param type    The PermissionCheckerType representing which
   *                PermissionChecker will be created.
   * @param domains A list of security domains to check if
   *                permission_checker_type is PERMISSION_STORE
   * @return        a shared pointer to the created permission checker if
   *                successful. A nullptr will be returned when the
   *                PermissionCheckerType is NONE, invalid or, when no suitable
   *                PermissionChecker is found for the PermissionCheckingType
   */
  virtual std::shared_ptr<PermissionChecker>
  operator()(PermissionCheckerType type,
             const std::unordered_set<std::string>& domains) = 0;
};

}} // namespace facebook::logdevice
