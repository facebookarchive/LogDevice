/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <folly/String.h>

#include "logdevice/common/plugin/Plugin.h"

namespace facebook { namespace logdevice {

class Logger : public Plugin {
 public:
  PluginType type() const override {
    return PluginType::LOGGER;
  }

  virtual void log(const char* cluster_name,
                   int level,
                   const folly::StringPiece line) const {}
};

}} // namespace facebook::logdevice
