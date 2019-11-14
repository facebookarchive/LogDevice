/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "logdevice/common/UpdateableSecurityInfo.h"

#include "logdevice/common/PrincipalParser.h"
#include "logdevice/common/Worker.h"
#include "logdevice/common/configuration/Configuration.h"
#include "logdevice/common/configuration/UpdateableConfig.h"
#include "logdevice/common/plugin/PermissionCheckerFactory.h"
#include "logdevice/common/plugin/PluginRegistry.h"
#include "logdevice/common/plugin/PrincipalParserFactory.h"

namespace facebook { namespace logdevice {

UpdateableSecurityInfo::UpdateableSecurityInfo(
    std::shared_ptr<UpdateableServerConfig> server_config,
    std::shared_ptr<PluginRegistry> plugin_registry,
    bool server)
    : server_config_(std::move(server_config)),
      plugin_registry_(std::move(plugin_registry)),
      server_(server) {
  config_update_sub_ = server_config_->callAndSubscribeToUpdates(
      std::bind(&UpdateableSecurityInfo::onConfigUpdate, this));
};

void UpdateableSecurityInfo::shutdown() {
  config_update_sub_.unsubscribe();
}

void UpdateableSecurityInfo::onConfigUpdate() {
  ld_debug("UpdateableSecurityInfo::onConfigUpdate");

  std::shared_ptr<const SecurityInfo> current_info = current_.get();

  bool first_update = current_info == nullptr;
  if (first_update) {
    current_info = std::make_shared<SecurityInfo>();
  }

  // nullptr if unchanged
  std::shared_ptr<SecurityInfo> new_info_ptr;

  // Creates if nullptr.
  auto new_info = [&] {
    if (new_info_ptr == nullptr) {
      // Copy on first access.
      new_info_ptr = std::make_shared<SecurityInfo>(*current_info);
    }
    return new_info_ptr;
  };

  std::shared_ptr<ServerConfig> server_config = server_config_->get();
  auto& securityConfig = server_config->getSecurityConfig();

  AuthenticationType auth_type_cur;
  if (current_info->principal_parser) {
    auth_type_cur = current_info->principal_parser->getAuthenticationType();
  } else {
    auth_type_cur = AuthenticationType::NONE;
  }
  if (auth_type_cur != server_config->getAuthenticationType()) {
    if (!first_update) {
      ld_info("PrincipalParser is changed");
    }
    auto pp_plugin = plugin_registry_->getSinglePlugin<PrincipalParserFactory>(
        PluginType::PRINCIPAL_PARSER_FACTORY);
    new_info()->principal_parser = pp_plugin
        ? (*pp_plugin)(server_config->getAuthenticationType())
        : nullptr;
  }

  PermissionCheckerType permission_checker_type_cur;
  if (current_info->permission_checker) {
    permission_checker_type_cur =
        current_info->permission_checker->getPermissionCheckerType();
  } else {
    permission_checker_type_cur = PermissionCheckerType::NONE;
  }

  PermissionCheckerType permission_checker_type_new;
  if (server_) {
    permission_checker_type_new = securityConfig.permissionCheckingEnabled()
        ? server_config->getPermissionCheckerType()
        : PermissionCheckerType::NONE;
  } else {
    permission_checker_type_new = PermissionCheckerType::NONE;
  }

  bool updateAclCache = true;
  // If we enabled/disabled the ACL cache, or changed any of its properties,
  // we must recreate the permission checker to reflect the changes.
  if (current_info->permission_checker) {
    updateAclCache = (securityConfig.enableAclCache !=
                      current_info->permission_checker->cacheEnabled());
    if (current_info->permission_checker->cacheEnabled()) {
      updateAclCache |= securityConfig.aclCacheMaxSize !=
              current_info->permission_checker->cacheSize() ||
          securityConfig.aclCacheTtl !=
              current_info->permission_checker->cacheTtl();
    }
  }

  if (permission_checker_type_cur != permission_checker_type_new ||
      updateAclCache) {
    if (!first_update) {
      ld_info("PermissionChecker is changed");
    }

    if (permission_checker_type_new == PermissionCheckerType::NONE) {
      new_info()->permission_checker = nullptr;
    } else {
      auto pc_plugin =
          plugin_registry_->getSinglePlugin<PermissionCheckerFactory>(
              PluginType::PERMISSION_CHECKER_FACTORY);
      new_info()->permission_checker = pc_plugin
          ? (*pc_plugin)(permission_checker_type_new, securityConfig)
          : nullptr;
    }
  }

  if (current_info->cluster_node_identity !=
      securityConfig.clusterNodeIdentity) {
    new_info()->cluster_node_identity = securityConfig.clusterNodeIdentity;
  }
  if (current_info->enforce_cluster_node_identity !=
      securityConfig.enforceClusterNodeIdentity) {
    new_info()->enforce_cluster_node_identity =
        securityConfig.enforceClusterNodeIdentity;
  }

  if (new_info_ptr != nullptr) {
    // Something changed. Publish the update.
    current_.update(new_info_ptr);
  }

  if (new_info_ptr != nullptr || first_update) {
    dumpSecurityInfo();
  }
}

void UpdateableSecurityInfo::dumpSecurityInfo() const {
  std::shared_ptr<const SecurityInfo> info = current_.get();
  auto principal_parser = info->principal_parser;
  ld_debug("Authentication Enabled: %d", principal_parser != nullptr);

  if (principal_parser) {
    auto server_config = server_config_->get();
    ld_debug("Allow Unauthenticated: %d",
             server_config->getSecurityConfig().allowUnauthenticated);
    ld_debug("Authentication Type: %s",
             AuthenticationTypeTranslator::toString(
                 principal_parser->getAuthenticationType())
                 .c_str());
  }

  auto permission_checker = info->permission_checker;

  ld_debug("Permission Checking Enabled: %d", permission_checker != nullptr);
  if (permission_checker) {
    ld_debug("Permission Checker Type: %s",
             PermissionCheckerTypeTranslator::toString(
                 permission_checker->getPermissionCheckerType())
                 .c_str());
  }
}

}} // namespace facebook::logdevice
