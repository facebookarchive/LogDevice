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

enum class RebuildingReadOnlyOption {
  // Rebuilding is not read-only.
  NONE = 0,
  // The donor node does not send STORE messages or issue AmendSelfStorageTask.
  ON_DONOR = 1,
  // The donor node sends STORE messages but the recipient does not perform the
  // actual writes when the tasks arrive to a storage thread.
  ON_RECIPIENT = 2,
};

struct RebuildingSettings : public SettingsBundle {
  const char* getName() const override {
    return "RebuildingSettings";
  }

  void defineSettings(SettingEasyInit& init) override;

  // See cpp file for a documentation about these settings.

  bool use_legacy_log_to_shard_mapping_in_rebuilding;
  bool disable_rebuilding;
  std::chrono::milliseconds local_window;
  bool local_window_uses_partition_boundary;
  std::chrono::milliseconds global_window;
  std::chrono::milliseconds planner_scheduling_delay;
  size_t max_batch_bytes;
  std::chrono::milliseconds max_batch_time;
  size_t max_records_in_flight;
  size_t max_record_bytes_in_flight;
  size_t max_amends_in_flight;
  size_t max_logs_in_flight;
  bool use_rocksdb_cache;
  RebuildingReadOnlyOption read_only;
  size_t checkpoint_interval_mb;
  double total_log_rebuilding_size_per_shard_mb;
  double max_log_rebuilding_size_mb;
  size_t max_get_seq_state_in_flight;
  chrono_interval_t<std::chrono::milliseconds> retry_timeout;
  chrono_interval_t<std::chrono::milliseconds> store_timeout;
  chrono_interval_t<std::chrono::milliseconds>
      rebuilding_planner_sync_seq_retry_interval;
  bool use_iterator_cache;
  bool rebuild_dirty_shards;
  bool disable_data_log_rebuilding;
  chrono_interval_t<std::chrono::seconds> shard_is_rebuilt_msg_delay;
  bool enable_self_initiated_rebuilding;
  std::chrono::seconds self_initiated_rebuilding_grace_period;
  std::chrono::seconds self_initiated_rebuilding_extra_period; // deprecated
  size_t max_node_rebuilding_percentage;
  ssize_t max_rebuilding_trigger_queue_size;
  bool allow_conditional_rebuilding_restarts;
  bool test_stall_rebuilding;
  bool enable_v2;
  std::chrono::milliseconds rebuilding_restarts_grace_period;
  std::chrono::seconds record_durability_timeout;
  std::chrono::milliseconds auto_mark_unrecoverable_timeout;
  chrono_expbackoff_t<std::chrono::milliseconds> wait_purges_backoff_time;
  uint64_t max_malformed_records_to_tolerate;
  rate_limit_t rate_limit;

 private:
  // Only UpdateableSettings can create this bundle.
  RebuildingSettings() {}
  friend class UpdateableSettingsRaw<RebuildingSettings>;
};

}} // namespace facebook::logdevice
