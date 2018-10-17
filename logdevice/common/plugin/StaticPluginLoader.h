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

/**
 * Loads plugins by calling dlsym() on a bunch of symbols - they should already
 * be linked into the binary.
 */
class StaticPluginLoader : public PluginProvider {
 public:
  std::string identifier() const override {
    return "static_loader";
  }
  std::string displayName() const override {
    return "Static plug-in loader";
  }
  PluginVector getPlugins() override;
};

}} // namespace facebook::logdevice
