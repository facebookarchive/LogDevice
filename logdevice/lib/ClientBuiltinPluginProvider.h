/**
 * Copyright (c) 2018-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include "logdevice/common/plugin/PluginProvider.h"

namespace facebook { namespace logdevice {

class ClientBuiltinPluginProvider : public PluginProvider {
 public:
  std::string identifier() const override {
    return "client_builtin";
  }
  std::string displayName() const override {
    return "Client built-in plugin provider";
  }
  PluginVector getPlugins() override;
};

PluginVector getClientPluginProviders();

}} // namespace facebook::logdevice
