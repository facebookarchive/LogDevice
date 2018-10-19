/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <string>

#include <opentracing/tracer.h>

namespace facebook { namespace logdevice {

class TraceLogger;

/**
 * Interface for AppendRequest and TrimRequest to call back into ClientImpl.
 * Works around a dependency issue (AppendRequest is in common/ but ClientImpl
 * in lib/); more in t8775222.
 */
class ClientBridge {
 public:
  virtual const std::shared_ptr<TraceLogger> getTraceLogger() const = 0;
  virtual const std::shared_ptr<opentracing::Tracer> getOTTracer() const = 0;
  virtual bool hasWriteToken(const std::string& required) const = 0;
  virtual bool shouldE2ETrace() const {
    return false;
  }
  virtual ~ClientBridge() {}
};

}} // namespace facebook::logdevice
