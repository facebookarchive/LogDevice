/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/include/ClientSettings.h"

#include <string>

#include "logdevice/common/debug.h"
#include "logdevice/lib/ClientSettingsImpl.h"

namespace facebook { namespace logdevice {

//
// ClientSettings boilerplate
//

ClientSettings* ClientSettings::create() {
  return new ClientSettingsImpl();
}

ClientSettingsImpl* ClientSettings::impl() {
  return static_cast<ClientSettingsImpl*>(this);
}

int ClientSettings::set(const char* name, const char* value) {
  return impl()->set(name, value);
}

int ClientSettings::set(const char* name, int64_t value) {
  return impl()->set(name, std::to_string(value).c_str());
}

int ClientSettings::set(
    const std::vector<std::pair<std::string, std::string>>& settings) {
  for (const auto& kv : settings) {
    int rv = set(kv.first.c_str(), kv.second.c_str());
    if (rv != 0) {
      ld_info("Error setting %s to %s.", kv.first.c_str(), kv.second.c_str());
      return rv;
    }
  }
  return 0;
}

folly::Optional<std::string> ClientSettings::get(const std::string& name) {
  return impl()->getSettingsUpdater()->getValue(name);
}

bool ClientSettings::isOverridden(const std::string& name) {
  return impl()
      ->getSettingsUpdater()
      ->getValueFromSource(name, SettingsUpdater::Source::CLIENT)
      .hasValue();
}

std::vector<std::pair<std::string, std::string>> ClientSettings::getAll() {
  return impl()->getSettingsUpdater()->getAllValues();
}
}} // namespace facebook::logdevice
