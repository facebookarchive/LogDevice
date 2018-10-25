/**
 * Copyright (c) 2018-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include "logdevice/common/SecurityInformation.h"
#include "logdevice/common/plugin/Plugin.h"

namespace facebook { namespace logdevice {

/**
 * @file `PrincipalParserFactory` will be used to create a `PrincipalParser`
 * instance.
 */

class PrincipalParser;

class PrincipalParserFactory : public Plugin {
 public:
  PluginType type() const override {
    return PluginType::PRINCIPAL_PARSER_FACTORY;
  }

  /**
   * Creates a PrincipalParser which will be used to obtain the principal for a
   * connection after authentication has completed.
   * @param type The AuthenticationType representing which PrincipalParser
   *             will be created.
   * @return     A unique pointer to the PrincipalParser if successful. A
   *             nullptr will be returned when the AuthenticaitonType is NONE,
   *             invalid or, when no suitable PrincipalParser is found for the
   *             AuthenticationType
   */
  virtual std::unique_ptr<PrincipalParser>
  operator()(AuthenticationType type) = 0;
};

}} // namespace facebook::logdevice
