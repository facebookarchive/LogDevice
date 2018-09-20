/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include "logdevice/common/PermissionChecker.h"

namespace facebook { namespace logdevice {

/**
 * @file ConfigPermissionChecker is a implementation of PermissionChecker.
 *       The permission information is stored directly on the config file
 *       and is accessed through the config object held in the worker.
 */

class ConfigPermissionChecker : public PermissionChecker {
 public:
  explicit ConfigPermissionChecker() : PermissionChecker(){};
  ~ConfigPermissionChecker() override{};

  /**
   * See PermissionChecker.h
   * Queries LocalLogsConfig to determine if the principal can perform the
   * specified action on the logid.
   */
  void isAllowed(ACTION action,
                 const PrincipalIdentity& principal,
                 logid_t logid,
                 callback_func_t cb) const override;

  // see PermissionChecker.h
  PermissionCheckerType getPermissionCheckerType() const override {
    return PermissionCheckerType::CONFIG;
  }

 private:
  // checks the admin list stored on the configuration file to see if the
  // principal should get admin rights.
  bool isAdmin(const PrincipalIdentity& principal) const;
};

}} // namespace facebook::logdevice
