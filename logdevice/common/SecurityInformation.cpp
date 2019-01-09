/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/common/SecurityInformation.h"

#include <folly/dynamic.h>

namespace facebook { namespace logdevice {

constexpr const char* Principal::DEFAULT;
constexpr const char* Principal::UNAUTHENTICATED;
constexpr const char* Principal::CLUSTER_NODE;
constexpr const char* Principal::INVALID;
constexpr std::array<const char*, 5> Principal::well_known_principals;

bool Principal::isAllowedInConfig(const std::string& str) {
  return !(str == CLUSTER_NODE || str == INVALID);
}

folly::dynamic Principal::toFollyDynamic() const {
  return folly::dynamic::object("name", name)(
      "max_read_traffic_class", trafficClasses()[max_read_traffic_class]);
};

std::string AuthenticationTypeTranslator::toString(AuthenticationType type) {
  switch (type) {
    case AuthenticationType::NONE:
      return "NONE";
    case AuthenticationType::SELF_IDENTIFICATION:
      return "self_identification";
    case AuthenticationType::SSL:
      return "ssl";
    default:
      return "UNKNOWN";
  }
}

AuthenticationType
AuthenticationTypeTranslator::toAuthenticationType(const std::string& str) {
  if (str == "NONE") {
    return AuthenticationType::NONE;
  } else if (str == "self_identification") {
    return AuthenticationType::SELF_IDENTIFICATION;
  } else if (str == "ssl") {
    return AuthenticationType::SSL;
  } else {
    return AuthenticationType::MAX;
  }
}

std::string
PermissionCheckerTypeTranslator::toString(PermissionCheckerType type) {
  switch (type) {
    case PermissionCheckerType::NONE:
      return "NONE";
    case PermissionCheckerType::CONFIG:
      return "config";
    case PermissionCheckerType::PERMISSION_STORE:
      return "permission_store";
    case PermissionCheckerType::MAX:
      return "UNKNOWN";
  }
  return "UNKNOWN";
}

PermissionCheckerType PermissionCheckerTypeTranslator::toPermissionCheckerType(
    const std::string& str) {
  if (str == "NONE") {
    return PermissionCheckerType::NONE;
  } else if (str == "config") {
    return PermissionCheckerType::CONFIG;
  } else if (str == "permission_store") {
    return PermissionCheckerType::PERMISSION_STORE;
  } else {
    return PermissionCheckerType::MAX;
  }
}

}} // namespace facebook::logdevice
