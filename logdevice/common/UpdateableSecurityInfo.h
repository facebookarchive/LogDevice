/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include "logdevice/common/PermissionChecker.h"
#include "logdevice/common/Processor.h"
#include "logdevice/common/configuration/SecurityConfig.h"
#include "logdevice/include/ConfigSubscriptionHandle.h"

namespace facebook { namespace logdevice {

class UpdateableSecurityInfo {
 public:
  UpdateableSecurityInfo(Processor* processor, bool server = true);
  virtual ~UpdateableSecurityInfo() {}

  std::shared_ptr<PermissionChecker> getPermissionChecker();

  std::shared_ptr<PrincipalParser> getPrincipalParser();

  /**
   * Logs current security information that is valid in the running processor.
   */
  void dumpSecurityInfo() const;

  void onConfigUpdate();

  /**
   * Unsubscribes from config. This prevents the UpdateableSecurityInfo from
   * accessing Processor. Called before destroying Processor.
   */
  void shutdown();

 private:
  Processor* processor_;

  // are we running on server
  bool server_;

  // PermissionChecker owned by the processor used to determine if a client is
  // allowed to perform a specific action on a given resource
  UpdateableSharedPtr<PermissionChecker> permission_checker_;

  // PrincipalParser owned by the processor used to obtain the principal after
  // authentication
  UpdateableSharedPtr<PrincipalParser> principal_parser_;

  // comes last to ensure unsubscription before rest of destruction
  ConfigSubscriptionHandle config_update_sub_;

  const configuration::SecurityConfig& getSecurityConfig();
};

}} // namespace facebook::logdevice
