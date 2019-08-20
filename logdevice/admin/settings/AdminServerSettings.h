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
  // How many delta records to keep in the maintenance log before we snapshot
  // it.
  size_t maintenance_log_max_delta_records;
  // How many bytes of delta records to keep in the event log before we snapshot
  // it
  size_t maintenance_log_max_delta_bytes;
  // If the event log is snapshotted, how long to keep a history of snapshots
  // and delta
  std::chrono::milliseconds maintenance_log_retention;
  // Disable trimming of the maintenance log
  bool disable_maintenance_log_trimming;

  bool read_metadata_from_sequencers;
  bool enable_safety_check_periodic_metadata_update;
  std::chrono::milliseconds safety_check_metadata_update_period;

  // If true, start maintenance manager
  bool enable_maintenance_manager;

  // Timeout after which a reevaluation is scheduled to run in
  // MaintenanceManager
  std::chrono::milliseconds maintenance_manager_reevaluation_timeout;

  // How often to check if Metadata Nodeset needs to be updated
  std::chrono::milliseconds maintenance_manager_metadata_nodeset_update_period;
  ;

 private:
  // Only UpdateableSettings can create this bundle.
  AdminServerSettings() {}
  friend class UpdateableSettingsRaw<AdminServerSettings>;
};

}} // namespace facebook::logdevice
