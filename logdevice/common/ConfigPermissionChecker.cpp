/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/common/ConfigPermissionChecker.h"

#include "logdevice/common/PrincipalParser.h"
#include "logdevice/common/Worker.h"
#include "logdevice/common/configuration/Configuration.h"

namespace facebook { namespace logdevice {

void ConfigPermissionChecker::isAllowed(ACTION action,
                                        const PrincipalIdentity& principal,
                                        logid_t logid,
                                        callback_func_t cb) const {
  // If the connection is between two nodes in a cluster, then it is allowed.
  if (principal.type == Principal::CLUSTER_NODE) {
    cb(PermissionCheckStatus::ALLOWED);
    return;
  }

  // If the connection is from an admin, then it is allowed.
  if (isAdmin(principal)) {
    cb(PermissionCheckStatus::ALLOWED);
    return;
  }

  auto config = Worker::onThisThread()->getConfig();
  const std::shared_ptr<LogsConfig::LogGroupNode> log =
      config->getLogGroupByIDShared(logid);

  if (log && log->attrs().permissions()) {
    auto permissions = log->attrs().permissions().value();
    for (auto identity : principal.identities) {
      auto iter = permissions.find(identity.second);
      if (iter != permissions.end()) {
        if (iter->second[static_cast<int>(action)]) {
          cb(PermissionCheckStatus::ALLOWED);
          return;
        }
      }
    }
    // Attempt to use the default permissions
    auto iter = permissions.find(Principal::DEFAULT);
    if (iter != permissions.end()) {
      cb(iter->second[static_cast<int>(action)]
             ? PermissionCheckStatus::ALLOWED
             : PermissionCheckStatus::DENIED);
      return;
    }
  }

  cb(PermissionCheckStatus::DENIED);
}

bool ConfigPermissionChecker::isAdmin(
    const PrincipalIdentity& principal) const {
  auto config = Worker::onThisThread()->getConfig()->serverConfig();
  for (auto identity : principal.identities) {
    if (config->getSecurityConfig().isAdmin(identity.second)) {
      return true;
    }
  }
  return false;
}

}} // namespace facebook::logdevice
