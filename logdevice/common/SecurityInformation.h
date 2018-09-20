/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <array>
#include <string>
#include <utility>

#include "logdevice/common/configuration/TrafficClass.h"
#include "logdevice/include/PermissionActions.h"

namespace folly {
struct dynamic;
}

namespace facebook { namespace logdevice {

/**
 * @file This file stores values used for Authentication and Authorization
 *       within logdevice.
 */

/**
 * Reserved principals.
 */
class Principal {
 public:
  // Used to assign default permissions to a resource
  static constexpr const char* DEFAULT = "default";

  // Assigned to Socket connections that did not provide any authentication data
  static constexpr const char* UNAUTHENTICATED = "unauthenticated";

  // Assigned to Socket connections that provide any authentication data
  static constexpr const char* AUTHENTICATED = "authenticated";

  // Assigned to internal cluster nodes
  static constexpr const char* CLUSTER_NODE = "cluster_node";

  // Assigned when authentication data cannot be parsed
  static constexpr const char* INVALID = "invalid";

  static constexpr std::array<const char*, 5> well_known_principals{
      {DEFAULT, UNAUTHENTICATED, AUTHENTICATED, CLUSTER_NODE, INVALID}};

  // Returns whether a principal is allowed to be used in a configuration file
  static bool isAllowedInConfig(const std::string& str);

  explicit Principal(std::string n) : name(std::move(n)) {}

  folly::dynamic toFollyDynamic() const;

  std::string name;

  // The highest priority read traffic class that can be used
  // by clients identified by this Principal.
  //
  // A value of TrafficClass::INVALD means use
  // "traffic_shaping::default_read_traffic_class" as the max.
  TrafficClass max_read_traffic_class = TrafficClass::INVALID;

  // The RFC 2474 "Differentiated Services Field Code Point" value to use
  // for all packets sent on connections associated with this principal.
  uint8_t egress_dscp = 0;
};

/**
 * The set of all valid Authentication methods currently supported in Logdevice.
 * Used mainly when parsing the configuration file and creating new
 * PrincipalParsers.
 * Note: When updating the configuration file while the cluster is running,
 *       changes to authenticaiton_type and permission_checking_type will
 *       not take effect until the cluster is restarted.
 *
 * NONE                 This indicates that there is no authentication for the
 *                      Logdevice cluster. This is the default behavior in
 *                      Logdevice unless otherwise specified in the
 *                      configuration file.
 *
 * SELF_IDENTIFICATION  This is a completely insecure method of authentication
 *                      where the Logdevice server completely trusts the
 *                      clients. The clients can specify any value in their
 *                      credential field when first being initialized and the
 *                      server will accept that as their principal. If the
 *                      Client object does not specify a value for the
 *                      credential field, then the client will be assigned
 *                      the UNAUTHENTICATED principal.
 *                      To specify this authentication type in the config, set
 *                      "authentication_type": "self_identification"
 *
 * SSL                  This is a secure method of authentication where
 *                      logdevice client can provide a TLS certificate by using
 *                      the "--ssl-load-client-cert" and "--ssl-cert-path"
 *                      settings. The certificate must contain the desired
 *                      principal in the subject. The cert must be provided by
 *                      the same Root CA that the logdevice server has access
 *                      to. If the client does not provide a certificate then
 *                      the client will be assigned the UNAUTHENTICATED
 *                      principal.
 *                      To specify this authentication type in the config, set
 *                      "authentication_type": "ssl"
 *
 * MAX                  The maximum value of the enum type.
 */
enum class AuthenticationType {
  NONE,
  SELF_IDENTIFICATION,
  SSL,

  MAX // should always be last
};

/**
 * The set of all valid PermissionChecker Types currently supported in
 * Logdevice. Used mainly when parsing the configuration file and creating new
 * PermissionCheckers.
 * Note: When updating the configuration file while the cluster is running,
 *       changes to authenticaiton_type and permission_checking_type will
 *       not take effect until the cluster is restarted.
 *
 * NONE                 This indicates that there is no authorization for the
 *                      Logdevice cluster. This is the default behavior in
 *                      Logdevice unless otherwise specified in the
 *                      configuration file.
 *
 * CONFIG               This method of authorization stores the permission data
 *                      within the configuration file. With in the file a
 *                      "permissions" field can be specified within the
 *                      "defaults" section as well as in each individual log.
 *                      The permission field follows this format:
 *                        "permissions" : {
 *                          "principal1" : {
 *                           "append" : true, "trim" : true, "read" : true
 *                          },
 *                          "unauthenticated" : {
 *                            "append" : false, "trim" : true, "read" : false
 *                          },
 *                        }
 *                      If the "unauthenticated" principal is specified within
 *                      the permissions field, then all principals that are not
 *                      specified in the list will have those permissions.
 *                      To specify this permission checker type in the config,
 *                      set "permission_checker_type": "config"
 *
 * PERMISSION_STORE     This method of authorization stores the ACLs (Access
 *                      Control Lists) names within the configuration file.
 *                      Permissions and users are stored  in an external system
 *                      (permission store).
 *                      'type' and 'name' fields are used to resolve ACL
 *                      in external system. 'enforce' primarily used for
 *                      permissions testing. If enfore=true and user doesn't
 *                      have permissions configured, access will be denied.
 *                      ACLs combined using 'AND', i.e user must
 *                      have permissions in all ACLs with enfore=true
 *                      The acls field has following format:
 *                      "acls" : [
 *                       {"type" : "type1", "name": "name1, "enforce": true},
 *                       {"type" : "type2", "name": "name2, "enforce": false}
 *                      ],
 * MAX                  The maximum value of the enum type.
 */
enum class PermissionCheckerType {
  NONE,
  CONFIG,
  PERMISSION_STORE,

  MAX // should always be last
};

namespace AuthenticationTypeTranslator {
// Translate AuthenticationType into its string representation. Returns
// "UNKNOWN" if no string representation for the AuthenticationType is found.
std::string toString(AuthenticationType type);

// Returns the AuthenticationType that the string represents. If no
// AuthenticationType is found to match that string, AuthenticationType::MAX
// is returned.
AuthenticationType toAuthenticationType(const std::string& str);
}; // namespace AuthenticationTypeTranslator

namespace PermissionCheckerTypeTranslator {
// Translate PermissionCheckerType into its string representation. Returns
// "UNKNOWN" if no string representation for the PermissionCheckerType is
// found.
std::string toString(PermissionCheckerType type);

// Returns the PermissionCheckerType that the string represents. If no
// PermissionCheckerType is found to match that string then
// PermissionCheckerType::MAX is returned.
PermissionCheckerType toPermissionCheckerType(const std::string& str);

}; // namespace PermissionCheckerTypeTranslator

}} // namespace facebook::logdevice
