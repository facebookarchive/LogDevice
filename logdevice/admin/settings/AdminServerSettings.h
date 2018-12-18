/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include "logdevice/common/settings/UpdateableSettings.h"
#include "logdevice/common/util.h"

namespace boost { namespace program_options {
class options_description;
}} // namespace boost::program_options

namespace facebook { namespace logdevice {

struct AdminServerSettings : public SettingsBundle {
  const char* getName() const override {
    return "AdminServerSettings";
  }

  void defineSettings(SettingEasyInit& init) override;

  // See cpp file for a documentation about these settings.

  int safety_max_logs_in_flight;
  std::chrono::milliseconds safety_check_timeout;

 private:
  // Only UpdateableSettings can create this bundle.
  AdminServerSettings() {}
  friend class UpdateableSettingsRaw<AdminServerSettings>;
};

}} // namespace facebook::logdevice
