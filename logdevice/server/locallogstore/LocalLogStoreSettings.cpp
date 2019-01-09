/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/server/locallogstore/LocalLogStoreSettings.h"

#include <boost/program_options.hpp>

#include "logdevice/common/commandline_util_chrono.h"

namespace facebook { namespace logdevice {

void LocalLogStoreSettings::defineSettings(SettingEasyInit& init) {
  using namespace SettingFlag;

  init("local-log-store-path",
       &local_log_store_path,
       "",
       nullptr,
       "path to local log store (if storage node)",
       SERVER | REQUIRES_RESTART,
       SettingsCategory::Storage);
  init("max-in-flight-monitor-requests",
       &max_in_flight_monitor_requests,
       "1",
       [](size_t val) {
         if ((ssize_t)val < 1) {
           throw boost::program_options::error(
               "max-in-flight-monitor-requests must be larger than 0");
         }
       },
       "maximum number of in-flight monitoring requests (e.g. manual "
       "compaction) "
       "posted by the monitoring thread",
       SERVER | REQUIRES_RESTART,
       SettingsCategory::Storage);
  init("max-queued-monitor-requests",
       &max_queued_monitor_requests,
       "32",
       [](size_t val) {
         if ((ssize_t)val <= 0) {
           throw boost::program_options::error(
               "max-queued-monitor-requests must be positive");
         }
       },
       "max number of log store monitor requests buffered in the "
       "monitoring thread queue",
       SERVER | REQUIRES_RESTART,
       SettingsCategory::Storage);
  init("logstore-monitoring-interval",
       &logstore_monitoring_interval,
       "10s",
       [](std::chrono::milliseconds val) {
         if (val.count() <= 0) {
           throw boost::program_options::error(
               "logstore-monitoring-interval should be positive");
         }
       },
       "interval between consecutive health checks on the local "
       "log store",
       SERVER,
       SettingsCategory::Storage);
  init("manual-compact-interval",
       &manual_compact_interval,
       "1h",
       [](std::chrono::seconds val) -> void {
         if (val.count() <= 0) {
           throw boost::program_options::error(
               "manual-compact-interval should be positive");
         }
       },
       "minimal interval between consecutive manual "
       "compactions on log stores"
       "that are out of disk space",
       SERVER,
       SettingsCategory::LogsDB);
  init("free-disk-space-threshold",
       &free_disk_space_threshold,
       "0.2",
       [](double val) -> void {
         if (val <= 0 || val >= 1) {
           throw boost::program_options::error(
               "free-disk-space-threshold should be within "
               "(0, 1)");
         }
       },
       "threshold (relative to total diskspace) of minimal "
       "free disk space"
       " for storage partitions to accept writes. This "
       "should be a fraction "
       "between 0.0 (exclusive) and 1.0 (exclusive). "
       "Storage nodes will reject "
       "writes to partitions with free disk space less "
       "than the threshold in "
       "order to guarantee that compactions can be "
       "performed. Note: this only "
       "applies to RocksDB local storage.",
       SERVER,
       SettingsCategory::Storage);
}

}} // namespace facebook::logdevice
