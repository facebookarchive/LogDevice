/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <folly/dynamic.h>
#include <folly/Optional.h>
#include <memory>

#include "logdevice/common/SampledTracer.h"
#include "logdevice/include/Err.h"
#include "logdevice/include/types.h"

namespace facebook { namespace logdevice {

class TraceLogger;
class Sockaddr;

constexpr auto CLIENTINFO_TRACER = "clienthello";

class ClientHelloInfoTracer : SampledTracer {
 public:
  explicit ClientHelloInfoTracer(std::shared_ptr<TraceLogger> logger);

  void
  traceClientHelloInfo(const folly::Optional<folly::dynamic>& client_build_json,
                       const Sockaddr& sa,
                       const Status status);

 private:
  folly::Optional<double> getDefaultSamplePercentage() const override {
    return 5;
  }
};
}} // namespace facebook::logdevice
