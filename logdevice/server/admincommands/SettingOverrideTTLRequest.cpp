/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "logdevice/server/admincommands/SettingOverrideTTLRequest.h"

#include "logdevice/common/commandline_util_chrono.h"
#include "logdevice/common/settings/SettingsUpdater.h"
#include "logdevice/server/Server.h"
#include "logdevice/server/ServerWorker.h"

namespace facebook { namespace logdevice {

void SettingOverrideTTLRequest::onTimeout() {
  server_->getSettings().unsetFromAdminCmd(name_);
  ld_info("Setting \"%s\" has been unset because of a ttl. "
          "The value was set %s ago.",
          name_.c_str(),
          chrono_string(ttl_).c_str());
  destroy();
}

void SettingOverrideTTLRequest::setupTimer() {
  timer_.assign([this] { onTimeout(); });
  timer_.activate(ttl_);
}

Request::Execution SettingOverrideTTLRequest::execute() {
  registerRequest();
  setupTimer();
  return Execution::CONTINUE;
}

int SettingOverrideTTLRequest::getThreadAffinity(int /*nthreads*/) {
  // Always run the request on thread 0
  return 0;
}

void SettingOverrideTTLRequest::registerRequest() {
  auto& map = ServerWorker::onThisThread()->activeSettingOverrides().map;
  auto it = map.find(name_);
  if (it != map.end()) {
    // There is already an override for this setting. Override it.
    map.erase(it);
  }
  auto res = map.insert(
      std::make_pair(name_, std::unique_ptr<SettingOverrideTTLRequest>(this)));
  ld_check(res.second);
}

void SettingOverrideTTLRequest::destroy() {
  auto& map = ServerWorker::onThisThread()->activeSettingOverrides().map;
  auto it = map.find(name_);

  ld_check(it != map.end());
  map.erase(it);
}

}} // namespace facebook::logdevice
