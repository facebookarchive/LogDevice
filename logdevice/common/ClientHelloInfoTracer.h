/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <memory>

#include <folly/Optional.h>
#include <folly/dynamic.h>

#include "logdevice/common/PrincipalIdentity.h"
#include "logdevice/common/SampledTracer.h"
#include "logdevice/common/SocketTypes.h"
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
                       const PrincipalIdentity& principal,
                       ConnectionType conn_type,
                       const Status status);

 private:
  folly::Optional<double> getDefaultSamplePercentage() const override {
    return 5;
  }
};
}} // namespace facebook::logdevice
