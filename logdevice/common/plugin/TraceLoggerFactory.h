/**
 * Copyright (c) 2018-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include "logdevice/common/plugin/Plugin.h"

namespace facebook { namespace logdevice {

/**
 * @file `TraceLoggerFactory` will be used to create a `TraceLogger`
 * instance.
 */

class TraceLogger;
class UpdateableConfig;

class TraceLoggerFactory : public Plugin {
 public:
  PluginType type() const override {
    return PluginType::TRACE_LOGGER_FACTORY;
  }

  /**
   * Creates a TraceLogger to which trace samples are pushed if tracing is on.
   */
  virtual std::unique_ptr<TraceLogger>
  operator()(const std::shared_ptr<UpdateableConfig>&) = 0;
};

}} // namespace facebook::logdevice
