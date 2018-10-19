/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "logdevice/server/admincommands/Setting.h"

#include "logdevice/common/commandline_util_chrono.h"
#include "logdevice/common/debug.h"
#include "logdevice/common/settings/SettingsUpdater.h"
#include "logdevice/server/ServerProcessor.h"

namespace facebook { namespace logdevice {

void commands::SettingSet::run() {
  try {
    server_->getSettings().setFromAdminCmd(name_, value_);
    out_.printf("Setting \"%s\" now set to \"%s\". "
                "This setting will be unset after %s.\r\n",
                name_.c_str(),
                value_.c_str(),
                chrono_string(ttl_).c_str());

    if (name_ == "loglevel" && dbg::currentLevel >= dbg::Level::DEBUG) {
      out_.printf(
          "\r\nNOTE: debug and spew levels produce lots of output. "
          "If this server is under nontrivial amount of load, the log file "
          "will grow rapidly. Remember to switch back to a less verbose "
          "level when you don't need the debug level anymore:\r\n\r\n"
          "  echo unset loglevel | nc ...\r\n\r\n");
    }

    // Post a request to unset the setting after ttl expires.
    // If the request fails, do nothing
    std::unique_ptr<Request> req =
        std::make_unique<SettingOverrideTTLRequest>(ttl_, name_, server_);
    if (server_->getServerProcessor()->postImportant(req) != 0) {
      out_.printf("Failed to post SettingOverrideTTLRequest, error: %s.\r\n",
                  error_name(err));
      // also log it
      ld_error("Failed to post SettingOverrideTTLRequest, error: %s.",
               error_name(err));
      return;
    }
  } catch (const boost::program_options::error& ex) {
    out_.printf("Error: %s.\r\n", ex.what());
  }
}

void commands::SettingUnset::run() {
  try {
    server_->getSettings().unsetFromAdminCmd(name_);
    out_.printf("Setting \"%s\" now unset.\r\n", name_.c_str());
  } catch (const boost::program_options::error& ex) {
    out_.printf("Error: %s.\r\n", ex.what());
  }
}

}} // namespace facebook::logdevice
