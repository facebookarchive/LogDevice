/**
 * Copyright (c) 2018-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include "logdevice/common/plugin/Plugin.h"
#include "logdevice/common/plugin/PluginProvider.h"

namespace facebook { namespace logdevice {

class ServerBuiltinPluginProvider : public PluginProvider {
 public:
  std::string identifier() const override {
    return "server_builtin";
  }
  std::string displayName() const override {
    return "Server built-in plugin provider";
  }
  PluginVector getPlugins() override;
};

}} // namespace facebook::logdevice
