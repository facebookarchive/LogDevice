/**
 * Copyright (c) 2018-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include "logdevice/common/plugin/PermissionCheckerFactory.h"
#include "logdevice/common/plugin/PluginRegistry.h"

namespace facebook { namespace logdevice {

/**
 * @file `BuiltinPermissionCheckerFactory` implementation of the
 * PermissionCheckerFactory interface. It only supports config based permission
 * checker.
 */
class BuiltinPermissionCheckerFactory : public PermissionCheckerFactory {
 public:
  // Plugin identifier
  std::string identifier() const override {
    return PluginRegistry::kBuiltin().str();
  }

  // Plugin display name
  std::string displayName() const override {
    return "built-in";
  }

  std::shared_ptr<PermissionChecker>
  operator()(PermissionCheckerType type,
             const configuration::SecurityConfig& security_cfg) override;
};

}} // namespace facebook::logdevice
