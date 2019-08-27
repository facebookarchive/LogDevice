/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <unordered_set>

#include <folly/Optional.h>

#include "logdevice/common/SecurityInformation.h"
#include "logdevice/common/types_internal.h"
#include "logdevice/include/types.h"

/**
 * @file Config reading and parsing.
 */

namespace folly {
struct dynamic;
}

namespace facebook { namespace logdevice { namespace configuration {

struct SecurityConfig {
  /**
   * A list of administrators or administrator groups within the cluster.
   * It is administrator if permission_checker_type is CONFIG.
   * It is administrator groups, if permission_checker_type is PERMISSION_STORE
   */
  std::unordered_set<std::string> admins;

  /**
   * A list of security domains to check if permission_checker_type is
   * PERMISSION_STORE
   */
  std::unordered_set<std::string> domains;

  /**
   * This defines the type of authentication the cluster will use.
   * see SecurityInformation.h for more information
   */
  AuthenticationType authenticationType = AuthenticationType::NONE;

  /**
   * If set to false, the cluster will require all incoming connections to
   * include authentication data. The authentication data is determined by
   * the authenticationType.
   */
  bool allowUnauthenticated = true;

  /**
   * If set to false, servers will be authenticated using the same scheme as
   * set by authenticationType. By default this is set to true, meaning servers
   * will be authenticated based on their IP addresses.
   */
  bool enableServerIpAuthentication = true;

  /**
   * This defines the type of authorization the cluster will use.
   * see SecurityInformation for more information.
   */
  PermissionCheckerType permissionCheckerType = PermissionCheckerType::NONE;

  /**
   * When set to true, permission checking is enabled in the cluster.
   */
  bool enablePermissionChecking = false;

  /**
   * ACL (Access Control List) type which may be used by permission checkers
   * to  validate access rights
   */
  std::string aclType;

  /**
   * If set to true, access will be allowed in case ACL not found.
   * This is required to handle problems with local Configerator proxy, which is
   * used by Hipster. Stalled ACL may lead to access being denyed.
   */
  bool allowIfACLNotFound = true;

  /**
   * If enabled, results of all permission checks performed by the permision
   * checker will be cached in the ACL Cache for faster lookups with a TTL
   * set by aclCacheTtl.
   */
  bool enableAclCache = false;

  /**
   * Defines the TTL for entries in the ACL Cache in seconds.
   */
  std::chrono::seconds aclCacheTtl{ 180 };

  /**
   * Defines the max size for the ACL Cache.
   */
  int aclCacheMaxSize = 100000;

  /**
   * Returns whether or not the "permissions" field is allowed in the
   * configuration file. The "permissions" field should only be allowed when
   * PermissionCheckerType is set CONFIG.
   */
  bool allowPermissionsInConfig() const {
    return permissionCheckerType == PermissionCheckerType::CONFIG;
  }

  /**
   * Returns whether any of the security options are enabled in the
   * configuration. This can only occur when there is a valid AuthenticationType
   */
  bool securityOptionsEnabled() const {
    ld_check(authenticationType < AuthenticationType::MAX);
    return authenticationType != AuthenticationType::NONE;
  }

  /**
   * Returns whether permission checking is enabled within the configuration.
   */
  bool permissionCheckingEnabled() const {
    return enablePermissionChecking;
  }

  /**
   * Returns whether ACL cache usage is enabled. If enabled results of
   * permissions checks are cached in the ACL Cache.
   */
  bool aclCacheEnabled() const {
    return enableAclCache;
  }

  /**
   * Returns whether permission access allowed if ACL not found.
   */
  bool allowIfACLNotFoundEnabled() const {
    return allowIfACLNotFound;
  }

  /**
   * Returns whether or not the config provided a valid permission_checker_type
   */
  bool hasValidPermissionCheckerType() const {
    ld_check(permissionCheckerType < PermissionCheckerType::MAX);
    return permissionCheckerType != PermissionCheckerType::NONE;
  }

  /**
   * Returns whether a principal is in the admin list.
   */
  bool isAdmin(const std::string& principal) const {
    return admins.find(principal) != admins.end();
  }

  folly::dynamic toFollyDynamic() const;

 private:
  void setList(folly::dynamic& json_security_info,
               const std::string& name,
               const std::unordered_set<std::string>& values) const;
};

}}} // namespace facebook::logdevice::configuration
