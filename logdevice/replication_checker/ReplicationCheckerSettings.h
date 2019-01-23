/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include "logdevice/common/settings/UpdateableSettings.h"
#include "logdevice/replication_checker/LogErrorTracker.h"

namespace facebook { namespace logdevice {

class ReplicationCheckerSettings : public SettingsBundle {
 public:
  enum class ReportErrorsMode {
    NONE,
    SAMPLE,
    ALL,
    EVEN_IF_NO_ERRORS,
  };

  using RecordLevelError = LogErrorTracker::RecordLevelError;
  using LogLevelError = LogErrorTracker::LogLevelError;

  const char* getName() const override {
    return "ReplicationCheckerSettings";
  }

  void defineSettings(SettingEasyInit& init) override;

  // See cpp file for a documentation about these settings.
  size_t logs_in_flight_per_worker;
  size_t per_log_max_bps;
  double num_logs_to_check;
  bool csi_data_only;
  bool only_data_logs;
  bool only_metadata_logs;
  std::chrono::milliseconds sync_sequencer_timeout;
  bool json;
  bool json_continuous;
  bool summary_only;
  std::chrono::seconds idle_timeout;
  std::chrono::microseconds read_duration;
  std::chrono::seconds read_starting_point;
  std::chrono::microseconds max_execution_time;
  std::chrono::seconds client_timeout;

  bool dont_count_bridge_records;
  bool enable_noisy_errors;
  RecordLevelError dont_fail_on_errors;
  ReportErrorsMode report_errors;
  // See .cpp
  size_t stop_after_num_errors;

 private:
  std::string dont_fail_on_tmp_;
  std::string report_errors_tmp_;
  // Only UpdateableSettings can create this bundle.
  ReplicationCheckerSettings() {}
  friend class UpdateableSettingsRaw<ReplicationCheckerSettings>;
};

}} // namespace facebook::logdevice
