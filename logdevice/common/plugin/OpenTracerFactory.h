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
 * @file `OpenTracerFactory` will be used to create `opentracing::Tracer`
 * instances.
 */

class TraceLogger;
class UpdateableConfig;

class OpenTracerFactory : public Plugin {
 public:
  PluginType type() const override {
    return PluginType::OPEN_TRACER_FACTORY;
  }

  // return opentracing::MakeNoopTracer();
  virtual std::shared_ptr<opentracing::Tracer> createOTTracer() = 0;
};

}} // namespace facebook::logdevice
