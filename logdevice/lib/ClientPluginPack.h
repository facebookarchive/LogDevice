/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include "logdevice/common/plugin/Plugin.h"
#include "logdevice/common/plugin/PluginRegistry.h"
#include "logdevice/common/LegacyPluginPack.h"

namespace facebook { namespace logdevice {

class ClientPluginPack : public virtual LegacyPluginPack,
                         public virtual Plugin {
 public:
  Type type() const override {
    return Type::LEGACY_CLIENT_PLUGIN;
  }

  std::string identifier() const override {
    return PluginRegistry::kBuiltin().str();
  }

  std::string displayName() const override {
    return description();
  }

  virtual const char* description() const override {
    return "built-in client plugin";
  }

  virtual std::string getMyLocation() const {
    return "";
  }
};

}} // namespace facebook::logdevice
