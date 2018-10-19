/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/common/SecurityTracer.h"

#include "logdevice/include/Record.h"

namespace facebook { namespace logdevice {

SecurityTracer::SecurityTracer(std::shared_ptr<TraceLogger> logger)
    : SampledTracer(std::move(logger)) {}

void SecurityTracer::traceSecurityEvent(
    logid_t logid,
    const std::string& action,
    const PermissionCheckStatus& action_result,
    const PrincipalIdentity& principal,
    bool shadow,
    const std::string& reason) {
  auto sample_builder = [=]() -> std::unique_ptr<TraceSample> {
    auto sample = std::make_unique<TraceSample>();

    sample->addIntValue("log_id", logid.val());
    auto config = logger_->getConfiguration();
    ld_check(config->logsConfig()->isLocal());
    std::string log_group_name =
        config->getLogGroupPath(logid).value_or("<UNKNOWN>");
    sample->addNormalValue("log_group_name", log_group_name);
    sample->addNormalValue("action", action);
    sample->addNormalValue("action_result", statusToString(action_result));
    sample->addNormalValue("principal", principal.primary_identity.second);

    std::vector<std::string> identities;
    for (auto& identity : principal.identities) {
      identities.push_back(identity.first + ":" + identity.second);
    }

    sample->addNormVectorValue("identities", identities);
    sample->addNormalValue("csid", principal.csid);
    sample->addNormalValue("client_sock_addr", principal.client_address);
    sample->addIntValue("shadow", shadow);
    sample->addNormalValue("reason", reason);
    return sample;
  };
  publish(SECURITY_TRACER, sample_builder);
}

const char* SecurityTracer::statusToString(PermissionCheckStatus status) const {
  switch (status) {
    case PermissionCheckStatus::DENIED:
      return "denied";
    case PermissionCheckStatus::ALLOWED:
      return "allowed";
    case PermissionCheckStatus::NOTREADY:
      return "notready";
    case PermissionCheckStatus::SYSLIMIT:
      return "syslimit";
    case PermissionCheckStatus::NOTFOUND:
      return "notfound";
    default:
      ld_check(false);
      return "invalid";
  }
}

folly::Optional<double> SecurityTracer::getDefaultSamplePercentage() const {
  return 100;
}

}} // namespace facebook::logdevice
