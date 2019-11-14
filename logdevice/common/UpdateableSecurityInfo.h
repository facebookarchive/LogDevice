/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include "logdevice/common/PermissionChecker.h"
#include "logdevice/common/PrincipalParser.h"
#include "logdevice/common/configuration/Configuration.h"
#include "logdevice/common/configuration/SecurityConfig.h"
#include "logdevice/common/plugin/PluginRegistry.h"
#include "logdevice/include/ConfigSubscriptionHandle.h"

namespace facebook { namespace logdevice {

class UpdateableSecurityInfo {
 public:
  struct SecurityInfo {
    // PermissionChecker owned by the processor used to determine if a client is
    // allowed to perform a specific action on a given resource.
    // nullptr iff permission checking is disabled.
    std::shared_ptr<PermissionChecker> permission_checker;
    // PrincipalParser owned by the processor used to obtain the principal after
    // authentication
    std::shared_ptr<PrincipalParser> principal_parser;
    std::string cluster_node_identity;
    bool enforce_cluster_node_identity{false};
  };

  UpdateableSecurityInfo(std::shared_ptr<UpdateableServerConfig> server_config,
                         std::shared_ptr<PluginRegistry> plugin_registry,
                         bool server = true);
  virtual ~UpdateableSecurityInfo() {}

  std::shared_ptr<const SecurityInfo> get() {
    return current_.get();
  }

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
  std::shared_ptr<UpdateableServerConfig> server_config_;
  std::shared_ptr<PluginRegistry> plugin_registry_;

  // are we running on server
  const bool server_;

  // Current state. Updated in config update callback. Read from anywhere.
  // Putting all fields into one UpdateableSharedPtr makes reads faster and
  // prevents torn reads (e.g. if one config update changed permission checker
  // type and principal parser type, we'll never use new permission checker with
  // old principal parser).
  UpdateableSharedPtr<const SecurityInfo> current_;

  // comes last to ensure unsubscription before rest of destruction
  ConfigSubscriptionHandle config_update_sub_;
};

}} // namespace facebook::logdevice
