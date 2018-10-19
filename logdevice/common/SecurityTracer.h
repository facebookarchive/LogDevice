/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once
#include <chrono>
#include <memory>

#include "logdevice/common/PermissionChecker.h"
#include "logdevice/common/SampledTracer.h"

namespace facebook { namespace logdevice {

constexpr auto SECURITY_TRACER = "security_tracer";
class SecurityTracer : public SampledTracer {
 public:
  explicit SecurityTracer(std::shared_ptr<TraceLogger> logger);
  void traceSecurityEvent(logid_t logid,
                          const std::string& action,
                          const PermissionCheckStatus& action_result,
                          const PrincipalIdentity& principal,
                          bool shadow,
                          const std::string& reason);

 private:
  const char* statusToString(PermissionCheckStatus status) const;

  virtual folly::Optional<double> getDefaultSamplePercentage() const;
};
}} // namespace facebook::logdevice
