/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <string>
#include <vector>

namespace facebook { namespace logdevice {

struct PrincipalIdentity {
  static constexpr const char* IDENTITY_USER = "USER";
  static constexpr const char* IDENTITY_SERVICE = "SERVICE_IDENTITY";
  static constexpr const char* IDENTITY_TIER = "TIER";
  static constexpr const char* IDENTITY_MACHINE = "MACHINE";
  static constexpr const char* IDENTITY_JOB = "JOB";

  /**
   * Principal type, one of Principal::well_known_principals
   */
  std::string type;

  /**
   * Vector of identities. Each identify is pair: type,name.
   * Example:
   * USER,testuser
   * MACHINE_TIER,dev
   */
  std::vector<std::pair<std::string, std::string>> identities;
  /**
   * Primary (first) identity. Used for logging only
   */
  std::pair<std::string, std::string> primary_identity;
  std::string client_address;
  std::string csid;

  explicit PrincipalIdentity() {}

  explicit PrincipalIdentity(const std::string& type);

  explicit PrincipalIdentity(
      const std::string& type,
      const std::pair<std::string, std::string>& identity);

  explicit PrincipalIdentity(
      const std::string& type,
      const std::pair<std::string, std::string>& identity,
      const std::vector<std::pair<std::string, std::string>>& identities);

  std::string toString() const;

  static bool isValidIdentityType(const std::string& idType);
};

}} // namespace facebook::logdevice
