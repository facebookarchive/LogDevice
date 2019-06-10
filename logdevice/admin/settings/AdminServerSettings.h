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

  // If admin_unix_socket is set, we ignore the admin_port. This needs to be
  // empty in order to use the IPv4/6 interface.
  std::string admin_unix_socket;
  int admin_port;

  int safety_max_logs_in_flight;
  size_t safety_check_failure_sample_size;
  size_t safety_check_max_batch_size;

  bool enable_cluster_maintenance_state_machine;
  // Allow the maintenance log to be snapshotted onto a snapshot log
  bool maintenance_log_snapshotting;
  std::chrono::milliseconds maintenance_log_snapshotting_period;

  bool read_metadata_from_sequencers;
  bool enable_safety_check_periodic_metadata_update;
  std::chrono::milliseconds safety_check_metadata_update_period;

  // If true, start maintenance manager
  bool enable_maintenance_manager;

  // Timeout after which a reevaluation is scheduled to run in
  // MaintenanceManager
  std::chrono::milliseconds maintenance_manager_reevaluation_timeout;

 private:
  // Only UpdateableSettings can create this bundle.
  AdminServerSettings() {}
  friend class UpdateableSettingsRaw<AdminServerSettings>;
};

}} // namespace facebook::logdevice
