/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/admin/settings/AdminServerSettings.h"

#include "logdevice/common/commandline_util_chrono.h"
#include "logdevice/common/settings/Validators.h"

using namespace facebook::logdevice::setting_validators;

namespace facebook { namespace logdevice {

void AdminServerSettings::defineSettings(SettingEasyInit& init) {
  using namespace SettingFlag;

  // clang-format off
  init
    ("admin-port", &admin_port, "6440", validate_port,
     "TCP port on which the server listens to for admin commands, supports "
     "commands over SSL",
     SERVER | REQUIRES_RESTART,
     SettingsCategory::AdminAPI)

    ("admin-unix-socket", &admin_unix_socket, "", validate_unix_socket,
     "Path to the unix domain socket the server will use to listen for admin "
     "thrift interface",
     SERVER | REQUIRES_RESTART,
     SettingsCategory::AdminAPI)

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

    ("safety-check-failure-sample-size", &safety_check_failure_sample_size, "10",
     [](int x) -> void {
       if (x <= 0) {
         throw boost::program_options::error(
           "safety-check-failure-sample-size must be a positive integer"
         );
       }
     },
     "The number of sample epochs returned by the Maintenance API for each "
     "maintenance if safety check blocks the operation.",
     SERVER,
     SettingsCategory::AdminAPI)

    ("safety-check-max-batch-size", &safety_check_max_batch_size, "15000",
     [](int x) -> void {
       if (x <= 0) {
         throw boost::program_options::error(
           "safety-check-max-batch-size must be a positive integer"
         );
       }
     },
     "The maximum number of logs to be checked in a single batch. Larger "
     "batches mean faster performance but means blocking the CPU thread pool "
     "for longer (not yielding often enough)",
     SERVER,
     SettingsCategory::AdminAPI)

    ("max-unavailable-storage-capacity-pct",
     &max_unavailable_storage_capacity_pct,
     "25",
     [](int x) -> void {
       if (x < 0 || x > 100) {
         throw boost::program_options::error(
             "max-unavailable-storage-capacity-pct must be a valid "
             "percentage, between 0 and 100."
         );
       }
     },
     "The percentage of the storage that is allowed to be taken down by "
     "operations, safety checker will take into account DEAD nodes as well. "
     "This means that if this value is 25, then safety checker will deny "
     "maintenances that will may take down more storage nodes. The percentage "
     "takes into account the weight of shards in the storage nodes, so does "
     "not necessarily equals 25% of the number of storage nodes",
     SERVER,
     SettingsCategory::AdminAPI)

    ("max-unavailable-sequencing-capacity-pct",
     &max_unavailable_sequencing_capacity_pct,
     "25",
     [](int x) -> void {
       if (x < 0 || x > 100) {
         throw boost::program_options::error(
             "max-unavailable-sequencing-capacity-pct must be a valid "
             "percentage, between 0 and 100."
         );
       }
     },
     "The percentage of the sequencing capacity that is allowed to be taken "
     "down by operations, safety checker will take into account DEAD nodes as "
     "well. This means that if this value is 25, then safety checker will deny "
     "maintenances that will may take down more sequencer nodes. The percentage "
     "takes into account the weight of the sequencer nodes, so does not "
     "necessarily equals 25% of the number of sequencer nodes",
     SERVER,
     SettingsCategory::AdminAPI)

    ("enable-cluster-maintenance-state-machine",
     &enable_cluster_maintenance_state_machine,
     "false",
     nullptr,
     "Enables the internal state replicated state machine that holds the "
     "maintenance definitions requested by the rebuilding supervisor or via the "
     " admin API. Enabling the state machine will also enable posting internal "
     "maintenance requests instead of writing to event log directly",
     SERVER | REQUIRES_RESTART,
     SettingsCategory::AdminAPI)

    ("maintenance-log-snapshotting",
     &maintenance_log_snapshotting,
     "false",
     nullptr,
     "Allow the maintenance log to be snapshotted onto a snapshot log. This "
     "requires the maintenance log group to contain two logs, the first one "
     "being the snapshot log and the second one being the delta log.",
     SERVER,
     SettingsCategory::AdminAPI)

    ("maintenance-log-snapshotting-period",
     &maintenance_log_snapshotting_period,
     "1h",
     validate_positive<ssize_t>(),
     "Controls time based snapshotting. New maintenancelog snapshot will be "
     "created after this period if there are new deltas",
     SERVER,
     SettingsCategory::AdminAPI)

    ("maintenance-log-max-delta-records",
     &maintenance_log_max_delta_records,
     "5000",
     nullptr,
     "How many delta records to keep in the maintenance log before we "
     "snapshot it.",
     SERVER,
     SettingsCategory::AdminAPI)

    ("maintenance-log-max-delta-bytes",
     &maintenance_log_max_delta_bytes,
     "10485760", // 10MB
     parse_nonnegative<ssize_t>(),
     "How many bytes of deltas to keep in the maintenance log before "
     "we snapshot it.",
     SERVER,
     SettingsCategory::AdminAPI)

    ("disable-maintenance-log-trimming",
     &disable_maintenance_log_trimming,
     "false",
     nullptr,
     "Disable trimming of the maintenance log",
     SERVER,
     SettingsCategory::AdminAPI)

    ("read-metadata-from-sequencers", &read_metadata_from_sequencers, "true",
     nullptr,
     "Safety checker to read the metadata of logs directly from sequencers.",
     SERVER,
     SettingsCategory::AdminAPI)

    ("enable-maintenance-manager",
     &enable_maintenance_manager,
     "false",
     nullptr,
     "Start Maintenance Manager. This will automatically enable the maintenance "
     "state machine as well (--enable-cluster-maintenance-state-machine).",
     SERVER | REQUIRES_RESTART,
     SettingsCategory::AdminAPI)

    ("maintenance-manager-reevaluation-timeout",
     &maintenance_manager_reevaluation_timeout,
     "2min",
     [](std::chrono::milliseconds val) -> void {
       if (val.count() <= 0) {
         throw boost::program_options::error(
           "maintenance-manager-reevaluation-timeout must be positive"
         );
       }
     },
     "Timeout after which a new run is scheduled in MaintenanceManager. Used for "
     "periodic reevaluation of the state in the absence of any state changes",
     SERVER,
     SettingsCategory::AdminAPI)

    ("maintenance-manager-metadata-nodeset-update-period",
     &maintenance_manager_metadata_nodeset_update_period,
     "2min",
     [](std::chrono::milliseconds val) -> void {
       if (val.count() <= 0) {
         throw boost::program_options::error(
           "maintenance-manager-metadata-nodeset-update-period must be positive"
         );
       }
     },
     "The period of how often to check if metadata nodeset update is required",
     SERVER,
     SettingsCategory::AdminAPI)

    ("enable-safety-check-periodic-metadata-update",
     &enable_safety_check_periodic_metadata_update,
     "false",
     nullptr,
     "Safety check to update its metadata cache periodically",
     SERVER,
     SettingsCategory::AdminAPI)

    ("safety-check-metadata-update-period",
     &safety_check_metadata_update_period,
     "10min",
     [](std::chrono::milliseconds val) -> void {
       if (val.count() <= 0) {
         throw boost::program_options::error(
           "safety-check-metadata-update-period must be positive"
         );
       }
     },
     "The period between automatic metadata updates for safety checker internal "
     "cache",
     SERVER,
     SettingsCategory::AdminAPI)
    ;
  // clang-format on
};

}} // namespace facebook::logdevice
