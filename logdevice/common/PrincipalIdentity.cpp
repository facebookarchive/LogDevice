/**
 * Copyright (c) 2017-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/common/PrincipalIdentity.h"

#include <sstream>

namespace facebook { namespace logdevice {

constexpr const char* PrincipalIdentity::IDENTITY_USER;

PrincipalIdentity::PrincipalIdentity(const std::string& type) : type(type) {}

PrincipalIdentity::PrincipalIdentity(
    const std::string& type,
    const std::pair<std::string, std::string>& identity)
    : type(type), primary_idenity(identity) {
  identities.push_back(identity);
}

PrincipalIdentity::PrincipalIdentity(
    const std::string& type,
    const std::pair<std::string, std::string>& identity,
    const std::vector<std::pair<std::string, std::string>>& identities)
    : type(type), identities(identities), primary_idenity(identity) {}

std::string PrincipalIdentity::toString() const {
  std::ostringstream oss;
  oss << "Principal type: " << type << ", Identities: ";
  for (auto identity : identities) {
    oss << identity.first << ":" << identity.second << " ";
  }
  return oss.str();
}
}} // namespace facebook::logdevice
