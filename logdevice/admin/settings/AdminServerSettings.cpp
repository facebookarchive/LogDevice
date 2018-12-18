/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "AdminServerSettings.h"

#include "logdevice/common/commandline_util_chrono.h"

namespace facebook { namespace logdevice {

void AdminServerSettings::defineSettings(SettingEasyInit& init) {
  using namespace SettingFlag;

  // clang-format off
  init
    ("safety-check-max-logs-in-flight", &safety_max_logs_in_flight, "1000",
     [](int x) -> void {
       if (x <= 0) {
         throw boost::program_options::error(
           "safety-check-max-logs-in-flight must be a positive integer"
         );
       }
     },
     "The number of concurrent logs that we runs checks against during execution"
     " of the CheckImpact operation either internally during a maintenance or "
     "through the Admin API's checkImpact() call",
     SERVER,
     SettingsCategory::AdminAPI)

    ("safety-check-timeout", &safety_check_timeout, "10min",
     [](std::chrono::milliseconds val) -> void {
       if (val.count() <= 0) {
         throw boost::program_options::error(
           "safety-check-timeout must be positive"
         );
       }
     },
     "The total time the safety check should take to run. This is the time that "
     "the CheckImpact operation need to take to scan all logs along with all "
     "the historical metadata to ensure than a maintenance is safe",
     SERVER,
     SettingsCategory::AdminAPI)
    ;
  // clang-format on
};

}} // namespace facebook::logdevice
