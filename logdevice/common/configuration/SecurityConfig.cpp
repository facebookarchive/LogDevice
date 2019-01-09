/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/common/configuration/SecurityConfig.h"

#include <folly/dynamic.h>
#include <folly/json.h>

namespace facebook { namespace logdevice { namespace configuration {

folly::dynamic SecurityConfig::toFollyDynamic() const {
  folly::dynamic json_security_info = folly::dynamic::object(
      "authentication_type",
      AuthenticationTypeTranslator::toString(authenticationType))(
      "allow_unauthenticated", allowUnauthenticated)(
      "enable_server_ip_authentication", enableServerIpAuthentication)(
      "enable_permission_checking", enablePermissionChecking)(
      "allow_acl_not_found", allowIfACLNotFound);

  // This is done even is enablePermissionChecking is false, this enables
  // permission checking to be enabled/disabled without having to remove any
  // data from the configuration file.
  if (permissionCheckerType != PermissionCheckerType::NONE) {
    ld_check(permissionCheckerType < PermissionCheckerType::MAX);
    json_security_info["permission_checker_type"] =
        PermissionCheckerTypeTranslator::toString(permissionCheckerType);
  }

  if (!aclType.empty()) {
    json_security_info["acl_type"] = aclType;
  }

  setList(json_security_info, "admin_list", admins);
  setList(json_security_info, "domain_list", domains);

  return json_security_info;
}

void SecurityConfig::setList(
    folly::dynamic& json_security_info,
    const std::string& name,
    const std::unordered_set<std::string>& values) const {
  if (!values.empty()) {
    folly::dynamic array = folly::dynamic::array;

    for (const auto& value : values) {
      array.push_back(value);
    }
    json_security_info[name] = array;
  }
}
}}} // namespace facebook::logdevice::configuration
