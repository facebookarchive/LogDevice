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

UpdateableSecurityInfo::UpdateableSecurityInfo(Processor* processor,
                                               bool server)
    : processor_(processor), server_(server) {
  config_update_sub_ =
      processor->config_->updateableServerConfig()->subscribeToUpdates(
          std::bind(&UpdateableSecurityInfo::onConfigUpdate, this));
  // make sure that we have things configured
  onConfigUpdate();
};

void UpdateableSecurityInfo::shutdown() {
  config_update_sub_.unsubscribe();
}

void UpdateableSecurityInfo::onConfigUpdate() {
  UpdateableSecurityInfo* security_info = this;
  ld_debug("UpdateableSecurityInfo::onConfigUpdate");
  auto processor = security_info->processor_;
  auto server_config = processor->config_->get()->serverConfig();
  auto plugin_registry = processor->getPluginRegistry();

  auto principal_parser_ptr = security_info->principal_parser_.get();
  AuthenticationType auth_type_cur;
  if (principal_parser_ptr) {
    auth_type_cur = principal_parser_ptr->getAuthenticationType();
  } else {
    auth_type_cur = AuthenticationType::NONE;
  }
  if (auth_type_cur != server_config->getAuthenticationType()) {
    ld_info("PrincipalParser is changed");
    auto pp_plugin = plugin_registry->getSinglePlugin<PrincipalParserFactory>(
        PluginType::PRINCIPAL_PARSER_FACTORY);
    std::shared_ptr<PrincipalParser> principal_parser = pp_plugin
        ? (*pp_plugin)(server_config->getAuthenticationType())
        : nullptr;
    security_info->principal_parser_.update(principal_parser);
  }

  auto permission_checker_ptr = security_info->permission_checker_.get();
  PermissionCheckerType permission_checker_type_cur;
  if (permission_checker_ptr) {
    permission_checker_type_cur =
        permission_checker_ptr->getPermissionCheckerType();
  } else {
    permission_checker_type_cur = PermissionCheckerType::NONE;
  }

  PermissionCheckerType permission_checker_type_new;
  if (security_info->server_) {
    permission_checker_type_new = server_config->getPermissionCheckerType();
  } else {
    permission_checker_type_new = PermissionCheckerType::NONE;
  }

  if (permission_checker_type_cur != permission_checker_type_new) {
    ld_info("PermissionChecker is changed");

    auto pc_plugin = plugin_registry->getSinglePlugin<PermissionCheckerFactory>(
        PluginType::PERMISSION_CHECKER_FACTORY);
    std::shared_ptr<PermissionChecker> permission_checker = pc_plugin
        ? (*pc_plugin)(permission_checker_type_new,
                       server_config->getSecurityConfig().domains)
        : nullptr;

    security_info->permission_checker_.update(permission_checker);
  }

  security_info->dumpSecurityInfo();
}

void UpdateableSecurityInfo::dumpSecurityInfo() const {
  auto principal_parser = principal_parser_.get();
  ld_debug("Authentication Enabled: %d", principal_parser != nullptr);

  if (principal_parser) {
    auto server_config = processor_->config_->get()->serverConfig();
    ld_debug("Allow Unauthenticated: %d",
             server_config->getSecurityConfig().allowUnauthenticated);
    ld_debug("Authentication Type: %s",
             AuthenticationTypeTranslator::toString(
                 principal_parser->getAuthenticationType())
                 .c_str());
  }

  auto permission_checker = permission_checker_.get();

  ld_debug("Permission Checking Enabled: %d", permission_checker != nullptr);
  if (permission_checker) {
    ld_debug("Permission Checker Type: %s",
             PermissionCheckerTypeTranslator::toString(
                 permission_checker->getPermissionCheckerType())
                 .c_str());
  }
}

std::shared_ptr<PermissionChecker>
UpdateableSecurityInfo::getPermissionChecker() {
  if (getSecurityConfig().permissionCheckingEnabled()) {
    return permission_checker_.get();
  } else {
    return nullptr;
  }
}

std::shared_ptr<PrincipalParser> UpdateableSecurityInfo::getPrincipalParser() {
  return principal_parser_.get();
}

const configuration::SecurityConfig&
UpdateableSecurityInfo::getSecurityConfig() {
  auto config = processor_->config_->get();
  return config->serverConfig()->getSecurityConfig();
}

}} // namespace facebook::logdevice
