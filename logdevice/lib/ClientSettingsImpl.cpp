/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/lib/ClientSettingsImpl.h"

#include <string>

#include <boost/program_options.hpp>

#include "logdevice/common/debug.h"
#include "logdevice/include/Err.h"
#include "logdevice/lib/ClientPluginHelper.h"

namespace facebook { namespace logdevice {

//
// ClientSettings Implementation
//

ClientSettingsImpl::ClientSettingsImpl()
    // By default be paranoid and don't crash the process on failed assert.
    : settings_(
          {{"abort-on-failed-check", folly::kIsDebug ? "true" : "false"}}) {
  settings_updater_ = std::make_shared<SettingsUpdater>();
  settings_updater_->registerSettings(settings_);

  plugin_registry_ =
      std::make_shared<PluginRegistry>(getClientPluginProviders());
  plugin_registry_->addOptions(settings_updater_.get());
}

int ClientSettingsImpl::set(const char* name, const char* value) {
  ld_info("ClientSettingsImpl::set(\"%s\", \"%s\")", name, value);
  try {
    settings_updater_->setFromClient(name, value);
  } catch (const boost::program_options::error& ex) {
    using namespace boost::program_options;
    err = dynamic_cast<const unknown_option*>(&ex)
        ? E::UNKNOWN_SETTING
        : dynamic_cast<const validation_error*>(&ex) ? E::INVALID_SETTING_VALUE
                                                     : E::INVALID_PARAM;
    return -1;
  }

  return 0;
}

}} // namespace facebook::logdevice
