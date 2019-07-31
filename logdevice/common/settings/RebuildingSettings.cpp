/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/common/settings/RebuildingSettings.h"

#include <boost/program_options.hpp>

#include "logdevice/common/commandline_util_chrono.h"
#include "logdevice/common/debug.h"
#include "logdevice/common/settings/Validators.h"
#include "logdevice/common/util.h"

namespace facebook { namespace logdevice {

using namespace facebook::logdevice::setting_validators;

/**
 * Compose an error message about the value of an option being out of range and
 * throw a boost::program_options::error with that message.
 */
static void out_of_range(const char* optname,
                         const char* expected,
                         ssize_t val) {
  char buf[4096];

  snprintf(buf,
           sizeof(buf),
           "invalid value for --%s: expected a %s value, got "
           "%zi",
           optname,
           expected,
           val);

  throw boost::program_options::error(std::string(buf));
}

std::istream& operator>>(std::istream& in, RebuildingReadOnlyOption& val) {
  std::string token;
  in >> token;
  if (token == "none") {
    val = RebuildingReadOnlyOption::NONE;
  } else if (token == "on-donor") {
    val = RebuildingReadOnlyOption::ON_DONOR;
  } else if (token == "on-recipient") {
    val = RebuildingReadOnlyOption::ON_RECIPIENT;
  } else {
    in.setstate(std::ios::failbit);
  }
  return in;
}

void RebuildingSettings::defineSettings(SettingEasyInit& init) {
  using namespace SettingFlag;

  init("use-legacy-log-to-shard-mapping-in-rebuilding",
       &use_legacy_log_to_shard_mapping_in_rebuilding,
       "true",
       nullptr,
       "If this option is set to false, every RebuildingPlanner will go "
       "through "
       "all the logs for every single shard (use with caution).",
       SERVER | DEPRECATED,
       SettingsCategory::Rebuilding);

  init("planner-scheduling-delay",
       &planner_scheduling_delay,
       "1min",
       validate_positive<ssize_t>(),
       "Delay between a shard rebuilding plan request and its execution to "
       "allow many shards to be grouped and planned together.",
       SERVER,
       SettingsCategory::Rebuilding);

  init("disable-rebuilding",
       &disable_rebuilding,
       "false",
       nullptr,
       "Disable rebuilding. Do not use in production. Only used by tests.",
       SERVER | REQUIRES_RESTART,
       SettingsCategory::Testing);
  init("rebuilding-local-window",
       &local_window,
       "60min",
       [](std::chrono::milliseconds val) {
         if (val.count() <= 0) {
           throw boost::program_options::error(
               "rebuilding-local-window must be positive");
         }
       },
       "Rebuilding will try to keep the difference between max and min "
       "in-flight records' timestamps less than this value.",
       SERVER,
       SettingsCategory::Rebuilding);
  init("rebuilding-global-window",
       &global_window,
       "max",
       [](std::chrono::milliseconds val) {
         if (val.count() <= 0) {
           throw boost::program_options::error(
               "rebuilding-global-window must be positive");
         }
       },
       "the size of rebuilding global window expressed in units of time. The "
       "global rebuilding window is an experimental feature similar to the "
       "local window, but tracking rebuilding reads across all storage nodes "
       "in the cluster rather than per node. Whereas the local window improves "
       "the locality of reads, the global window is expected to improve the "
       "locality of rebuilding writes.",
       SERVER | EXPERIMENTAL,
       SettingsCategory::Rebuilding);
  init("rebuilding-max-batch-bytes",
       &max_batch_bytes,
       "10M",
       parse_positive<size_t>(),
       "max amount of data that a node can read in one batch for rebuilding",
       SERVER,
       SettingsCategory::Rebuilding);
  init("rebuilding-max-batch-time",
       &max_batch_time,
       "1000ms",
       nullptr,
       "Max amount of time rebuilding read storage task is allowed to "
       "take before yielding to other storage tasks. Only supported by "
       "rebuilding V2 (partition by partition). \"max\" for unlimited.",
       SERVER);
  init("rebuilding-max-records-in-flight",
       &max_records_in_flight,
       "200",
       [](size_t val) {
         if ((ssize_t)val <= 0) {
           throw boost::program_options::error(
               "rebuilding-max-records-in-flight must be "
               "positive");
         }
       },
       "Maximum number of rebuilding STORE requests that a rebuilding donor "
       "node can have in flight at the same time. Rebuilding v1: per log, "
       "rebuilding v2: per shard.",
       SERVER,
       SettingsCategory::Rebuilding);
  init(
      "rebuilding-max-record-bytes-in-flight",
      &max_record_bytes_in_flight,
      "100M",
      parse_positive<size_t>(),
      "Maximum total size of rebuilding STORE requests that a rebuilding donor "
      "node can have in flight at the same time, per shard. Only used by "
      "rebuilding v2.",
      SERVER,
      SettingsCategory::Rebuilding);
  init("rebuilding-max-amends-in-flight",
       &max_amends_in_flight,
       "100",
       [](size_t val) {
         if ((ssize_t)val <= 0) {
           throw boost::program_options::error(
               "rebuilding-max-amends-in-flight must "
               "be positive");
         }
       },
       "maximum number of requests to update (amend) a rebuilt record's "
       "copyset that a rebuilding donor node can have in flight at the same "
       "time, per log. Rebuilding v1 only.",
       SERVER,
       SettingsCategory::Rebuilding);
  init(
      "rebuilding-max-get-seq-state-in-flight",
      &max_get_seq_state_in_flight,
      "100",
      [](size_t val) {
        if ((ssize_t)val <= 0) {
          throw boost::program_options::error("rebuilding-max-get-seq-state-in-"
                                              "flight must be positive");
        }
      },
      "maximum number of 'get sequencer state' requests that a rebuilding "
      "donor node can have in flight at the same time. Every storage node "
      "participating in rebuilding gets the sequencer state for all logs "
      "residing on that node before beginning to re-replicate records. This is "
      "done in order to determine the LSN at which to stop rebuilding the log.",
      SERVER,
      SettingsCategory::Rebuilding);
  init("rebuilding-planner-sync-seq-retry-interval",
       &rebuilding_planner_sync_seq_retry_interval,
       "60s..5min",
       nullptr,
       "retry interval for individual 'get sequencer state' requests issued by "
       "rebuilding via SyncSequencerRequest API, with exponential backoff",
       SERVER,
       SettingsCategory::Rebuilding);
  init("rebuilding-max-logs-in-flight",
       &max_logs_in_flight,
       "1",
       [](size_t val) {
         if ((ssize_t)val <= 0) {
           throw boost::program_options::error("rebuilding-max-logs-"
                                               "in-flight must be "
                                               "positive");
         }
       },
       "Maximum number of logs that a donor node can be rebuilding at the same "
       "time. V1 only.",
       SERVER,
       SettingsCategory::Rebuilding);
  init("rebuilding-use-rocksdb-cache",
       &use_rocksdb_cache,
       "false",
       nullptr,
       "Allow rebuilding reads to use RocksDB block cache. Recommended: enable "
       "for rebuilding v1, disable for rebuilding v2.",
       SERVER,
       SettingsCategory::Rebuilding);
  init("rebuilding-checkpoint-interval-mb",
       &checkpoint_interval_mb,
       "100",
       [](size_t val) {
         if ((ssize_t)val <= 0) {
           throw boost::program_options::error(
               "rebuilding-checkpoint-interval-mb must be positive");
         }
       },
       "Write a per-log rebuilding checkpoint once per this many megabytes of "
       "rebuilt data in the log. A rebuilding checkpoints contains an LSN "
       "through which the log has been rebuilt by this donor and the "
       "rebuilding version number identifying this rebuilding run. If a node "
       "restarts in the middle of a rebuilding run, it resumes rebuilding of a "
       "log from that log's last checkpoint. V1 only.",
       SERVER,
       SettingsCategory::Rebuilding);
  init("total-log-rebuilding-size-per-shard-mb",
       &total_log_rebuilding_size_per_shard_mb,
       "100",
       [](double val) {
         if ((double)val <= 0) {
           throw boost::program_options::error(
               "total-log-rebuilding-size-per-shard-mb must be positive");
         }
       },
       "Maximum amount of memory that can be consumed by all LogRebuilding "
       "state machines, per shard. V1 only.",
       SERVER,
       SettingsCategory::Rebuilding);
  init("max-log-rebuilding-size-mb",
       &max_log_rebuilding_size_mb,
       "5",
       [](double val) {
         if ((double)val <= 0) {
           throw boost::program_options::error(
               "max-log-rebuilding-size-mb must be positive");
         }
       },
       "Maximum amount of memory that can be consumed by a single "
       "LogRebuilding state machine. V1 only.",
       SERVER,
       SettingsCategory::Rebuilding);
  init("rebuilding-read-only",
       &read_only,
       "none",
       nullptr,
       "Rebuilding is read-only (for testing only). Use on-donor if rebuilding "
       "should not send STORE messages, or on-recipient if these should be "
       "sent but discarded by the recipient (LogsDB only)",
       SERVER,
       SettingsCategory::Testing);
  init("rebuilding-retry-timeout",
       &retry_timeout,
       "5s..30s",
       [](chrono_interval_t<std::chrono::milliseconds> val) {
         if (val.lo.count() < 0) {
           out_of_range(
               "rebuilding-retry-timeout.lo", "nonnegative", val.lo.count());
         }
         if (val.hi.count() < 0) {
           out_of_range(
               "rebuilding-retry-timeout.hi", "nonnegative", val.hi.count());
         }
       },
       "Delay before a record rebuilding retries a failed operation",
       SERVER,
       SettingsCategory::Testing);
  init("rebuilding-store-timeout",
       &store_timeout,
       "240s.."
       "480s",
       [](chrono_interval_t<std::chrono::milliseconds> val) {
         if (val.lo.count() <= 0) {
           out_of_range(
               "rebuilding-store-timeout.lo", "positive", val.lo.count());
         }
         if (val.hi.count() <= 0) {
           out_of_range(
               "rebuilding-store-timeout.hi", "positive", val.hi.count());
         }
       },
       "Maximum timeout for attempts by rebuilding to store a record copy or "
       "amend a copyset on a specific storage node. This timeout only applies "
       "to stores and amends that appear to be in flight; a smaller timeout "
       "(--rebuilding-retry-timeout) is used if something is known to be wrong "
       "with the store, e.g. we failed to send the message, or we've got an "
       "unsuccessful reply, or connection closed after we sent the store.",
       SERVER,
       SettingsCategory::Rebuilding);
  init("rebuilding-local-window-uses-partition-boundary",
       &local_window_uses_partition_boundary,
       "true",
       nullptr,
       "If true, the local window will be moved on partition boundaries. If "
       "false, it will instead be moved on fixed time intervals, as set by "
       "--rebuilding-local-window. V1 only, V2 always reads partition by "
       "partition.",
       SERVER,
       SettingsCategory::Rebuilding);
  init("rebuilding-use-iterator-cache",
       &use_iterator_cache,
       "false",
       nullptr,
       "Place rebuilding iterators in the LogsDB iterator cache. V1 only.",
       SERVER,
       SettingsCategory::Rebuilding);
  init("rebuild-dirty-shards",
       &rebuild_dirty_shards,
       "true",
       nullptr,
       "On start-up automatically rebuild LogsDB partitions left dirty by "
       "a prior unsafe shutdown of this node. This is called mini-rebuilding. "
       "The setting should be on unless you are running with "
       "--append-store-durability=sync_write, or don't care about data loss.",
       SERVER,
       SettingsCategory::Rebuilding);
  init("disable-data-log-rebuilding",
       &disable_data_log_rebuilding,
       "false",
       nullptr,
       "If set then data logs are not rebuilt. This may be enabled for "
       "clusters with very low retention, where the probability of data-loss "
       "due to a 2nd or 3rd failure is low and the work done during rebuild "
       "interferes with the primary workload.",
       SERVER | REQUIRES_RESTART,
       SettingsCategory::Rebuilding);
  init("shard-is-rebuilt-msg-delay",
       &shard_is_rebuilt_msg_delay,
       "5s..300s",
       validate_nonnegative<ssize_t>(),
       "In large clusters SHARD_IS_REBULT messages can arrive in a thundering "
       "herd overwhelming thread 0 processing those messages. The messages "
       "will be delayed by a random time in seconds between the min and the "
       "max value specified in the range above. 0 means no delay. NOTE: "
       "changing this value only applies to upcoming rebuildings, if you want "
       "to apply it to ongoing rebuildings, you'll need to restart the node.",
       SERVER,
       SettingsCategory::Rebuilding);
  init("enable-self-initiated-rebuilding",
       &enable_self_initiated_rebuilding,
       "true",
       nullptr,
       "Upon detecting the failure of a node, LogDevice will "
       "automatically trigger rebuilding of all shards of failed node. "
       "This is the default.",
       SERVER | DEPRECATED,
       SettingsCategory::Rebuilding);
  init("self-initiated-rebuilding-grace-period",
       &self_initiated_rebuilding_grace_period,
       "1200s",
       [](std::chrono::seconds val) -> void {
         if (val.count() < 0) {
           out_of_range("rebuilding-grace-period", "non-negative", val.count());
         }
       },
       "grace period in seconds before triggering full node rebuilding "
       "after detecting the node has failed.",
       SERVER,
       SettingsCategory::Rebuilding);
  init("self-initiated-rebuilding-extra-period",
       &self_initiated_rebuilding_extra_period,
       "20min",
       [](std::chrono::seconds val) -> void {
         if (val.count() < 0) {
           out_of_range("rebuilding-extra-period", "non-negative", val.count());
         }
       },
       "ignored. deprecated.",
       SERVER | DEPRECATED);
  init("max-node-rebuilding-percentage",
       &max_node_rebuilding_percentage,
       "35",
       [](int val) -> void {
         if (val < 0 || val > 100) {
           out_of_range(
               "max-node-rebuilding-percentage", "between 0 and 100", val);
         }
       },
       "Do not initiate rebuilding if more than this percentage of storage "
       "nodes in the cluster appear to have been lost or have shards that "
       "appear to require rebuilding.",
       SERVER,
       SettingsCategory::Rebuilding);
  init(
      "max-rebuilding-trigger-queue-size",
      &max_rebuilding_trigger_queue_size,
      "-1",
      nullptr,
      "DEPRECATED. Not implemented anymore and has no effect. "
      "Maximum number of triggers in the rebuilding supervisor queue before it "
      "switches to throttling mode. This translates to the acceptable number "
      "of dead nodes for which we can trigger rebuilding simultaneously. This "
      "setting should allow at least one full rack to be queued in order to "
      "support rack failures. A negative value indicates that the rebuilding "
      "supervisor should compute a reasonable value for this setting "
      "automatically, based on the cluster configuration.",
      SERVER | DEPRECATED,
      SettingsCategory::Rebuilding);
  init("record-durability-timeout",
       &record_durability_timeout,
       "960s",
       [](std::chrono::seconds val) -> void {
         if (val.count() < 0) {
           out_of_range(
               "record-durability-timeout", "non-negative", val.count());
         }
       },
       "Time for which LogRebuilding/RebuidlingCoordinator will wait for "
       "pending records to be durable before restarting the rebuilding for "
       "the log.",
       SERVER | EXPERIMENTAL,
       SettingsCategory::Rebuilding);
  init(
      "auto-mark-unrecoverable-timeout",
      &auto_mark_unrecoverable_timeout,
      "max",
      [](std::chrono::milliseconds val) {
        if (val.count() <= 0) {
          out_of_range(
              "auto-mark-unrecoverable-timeout", "positive", val.count());
        }
      },
      "If too many storage nodes or shards are declared down by the failure "
      "detector or RocksDB, readers stall. If this setting is 'max' (default), "
      "readers remain stalled until some shards come back, or until the shards "
      "are marked _unrecoverable_ (permanently lost) by writing a special "
      "in the event log via an admin tool. If this setting contains a time "
      "value, upon the timer expiration the shards are marked unrecoverable "
      "automatically. This allows reader clients to skip over all records that "
      "could only be delivered by now unrecoverable shards, and continue "
      "reading more recent records.",
      SERVER,
      SettingsCategory::Rebuilding);
  init
      // TODO(T22614431): remove this option once it's been enabled everywhere.
      ("allow-conditional-rebuilding-restarts",
       &allow_conditional_rebuilding_restarts,
       "false",
       nullptr,
       "Used to gate the feature described in T22614431. We want to enable it "
       "only after all clients have picked up the corresponding change in RSM "
       "protocol.",
       SERVER | EXPERIMENTAL,
       SettingsCategory::Rebuilding);
  init("rebuilding-restarts-grace-period",
       &rebuilding_restarts_grace_period,
       "20s",
       [](std::chrono::milliseconds val) -> void {
         if (val.count() < 0) {
           out_of_range(
               "rebuilding-restarts-grace-period", "non-negative", val.count());
         }
       },
       "Grace period used to throttle how often rebuilding can be restarted. "
       "This protects the server against a spike of messages in the event log "
       "that would cause a restart.",
       SERVER,
       SettingsCategory::Rebuilding);
  init("rebuilding-wait-purges-backoff-time",
       &wait_purges_backoff_time,
       "1s..10s",
       nullptr,
       "Retry timeout for waiting for local shards to purge a log "
       "before "
       "rebuilding it.",
       SERVER | EXPERIMENTAL,
       SettingsCategory::Rebuilding);
  init(
      "rebuilding-max-malformed-records-to-tolerate",
      &max_malformed_records_to_tolerate,
      "1000",
      nullptr,
      "Controls how rebuilding donors handle unexpected values in local log "
      "store (e.g. caused by bugs, forward incompatibility, or other processes "
      "writing unexpected things to rocksdb directly)."
      "If rebuilding encounters invalid records, it skips them and logs "
      // TODO (T24665001): With rebuilding partition by partition,
      //                   remove the "in the same log" part.
      "warnings. But if it encounters at least this many of them in the same "
      "log, it freaks out, logs a critical error and stalls indefinitely. The "
      "rest of the server keeps trying to run normally, to the extent to which "
      "you can run normally when you can't parse most of the records in the "
      "DB.",
      SERVER | REQUIRES_RESTART);
  init("test-stall-rebuilding",
       &test_stall_rebuilding,
       "false",
       nullptr,
       "Makes rebuilding pretend to start but not make any actual progress. "
       "Used in tests.",
       SERVER,
       SettingsCategory::Testing);
  init("rebuilding-v2",
       &enable_v2,
       "true",
       nullptr,
       "Enables a new implementation of rebuilding. The old one is deprecated.",
       SERVER,
       SettingsCategory::Rebuilding);
  init("rebuilding-rate-limit",
       &rate_limit,
       "unlimited",
       [](const std::string& val) -> rate_limit_t {
         rate_limit_t res;
         int rv = parse_rate_limit(val.c_str(), &res);
         if (rv != 0) {
           throw boost::program_options::error(
               "Invalid value(" + val +
               ") for --rebuilding-rate-limit."
               "Expected format is <count>/<duration><unit>, e.g. 5M/1s, or "
               "'unlimited'");
         }
         return res;
       },
       "Rebuilding V2 only. Limit on how fast rebuilding reads, in bytes per "
       "unit of time, per shard. Example: 5M/1s will make rebuilding read at "
       "most one megabyte per second in each shard. Note that it counts "
       "pre-filtering bytes; if rebuilding has high read amplification "
       "(e.g. if copyset index is disabled or is not very effective because "
       "records are small), much fewer bytes per second will actually get "
       "re-replicated. Also note that this setting doesn't affect batch size; "
       "e.g. if --rebuilding-max-batch-bytes=10M and "
       "--rebuilding-rate-limit=1M/1s, rebuilding will probably read a 10 MB "
       "batch every 10 seconds.",
       SERVER,
       SettingsCategory::Rebuilding);
}

}} // namespace facebook::logdevice
