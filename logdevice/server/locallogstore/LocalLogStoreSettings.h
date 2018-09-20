/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <chrono>
#include <string>

#include "logdevice/common/settings/UpdateableSettings.h"

/**
 * This struct contains several local logstore related options.
 */

namespace facebook { namespace logdevice {

struct LocalLogStoreSettings : public SettingsBundle {
  const char* getName() const override {
    return "LocalLogStoreSettings";
  }

  void defineSettings(SettingEasyInit& init) override;

  // See cpp file for a documentation about these settings.

  std::string local_log_store_path;
  size_t max_in_flight_monitor_requests;
  size_t max_queued_monitor_requests;
  std::chrono::milliseconds logstore_monitoring_interval;
  std::chrono::seconds manual_compact_interval;
  double free_disk_space_threshold;
};

}} // namespace facebook::logdevice
