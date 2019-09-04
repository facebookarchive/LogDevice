/**
 * Copyright (c) 2018-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include "logdevice/common/plugin/PluginRegistry.h"
#include "logdevice/common/plugin/PrincipalParserFactory.h"

namespace facebook { namespace logdevice {

/**
 * @file Implementation of PrincipalParserFactory that only supports
 * HELLOPrincipalParser.
 */

class BuiltinPrincipalParserFactory : public PrincipalParserFactory {
 public:
  // Plugin identifier
  std::string identifier() const override {
    return PluginRegistry::kBuiltin().str();
  }

  // Plugin display name
  std::string displayName() const override {
    return "built-in";
  }

  std::unique_ptr<PrincipalParser> operator()(AuthenticationType type) override;
};

}} // namespace facebook::logdevice
