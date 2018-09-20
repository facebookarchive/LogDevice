/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include "logdevice/common/settings/Settings.h"

/**
 * @file  Method for validating existence of SSL certs specified by settings
 */

namespace facebook { namespace logdevice {

// Validates that SSL certs specified in the settings exist and that files
// are readable. If `ca_only` is `true`, then only verifies the CA cert
bool validateSSLCertificatesExist(std::shared_ptr<const Settings> settings,
                                  bool ca_only = false);

}} // namespace facebook::logdevice
