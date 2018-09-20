/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/common/settings/SSLSettingValidation.h"

#include <fstream>

namespace facebook { namespace logdevice {

namespace {

bool validCertPath(const char* param_name, const std::string& path) {
  if (path.empty()) {
    ld_error("Setting %s is empty, unable to init SSL", param_name);
    return false;
  }
  std::ifstream file(path);
  if (!file.good()) {
    ld_error("%s is set to %s, however that file is unreadable",
             param_name,
             path.c_str());
    return false;
  }
  return true;
}
} // namespace

bool validateSSLCertificatesExist(std::shared_ptr<const Settings> settings,
                                  bool ca_only) {
  if (!validCertPath("ssl-ca-path", settings->ssl_ca_path)) {
    return false;
  }

  if (ca_only) {
    // Only checking the CA cert, not checking others
    return true;
  }

  if (!validCertPath("ssl-cert-path", settings->ssl_cert_path)) {
    return false;
  }
  if (!validCertPath("ssl-key-path", settings->ssl_key_path)) {
    return false;
  }
  return true;
}

}} // namespace facebook::logdevice
