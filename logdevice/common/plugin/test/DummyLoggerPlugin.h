/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <folly/String.h>

#include "logdevice/common/plugin/Logger.h"
#include "logdevice/common/plugin/Plugin.h"

namespace facebook { namespace logdevice {

class DummyLogger : public Logger {
 public:
  PluginType type() const override {
    return PluginType::LOGGER;
  }

  virtual std::string identifier() const override {
    return "dummy_logger_plugin";
  }

  virtual std::string displayName() const override {
    return "Dummy Logger Plugin";
  }
};

}} // namespace facebook::logdevice

extern "C" __attribute__((__used__)) facebook::logdevice::Plugin*
logdevice_plugin() {
  return new facebook::logdevice::DummyLogger();
}
