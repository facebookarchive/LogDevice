/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/replication_checker/ReplicationCheckerSettings.h"

#include <boost/program_options.hpp>

#include "logdevice/common/commandline_util_chrono.h"

namespace facebook { namespace logdevice {

void ReplicationCheckerSettings::defineSettings(SettingEasyInit& init) {
  using namespace SettingFlag;

  init("logs-in-flight",
       &logs_in_flight_per_worker,
       "100",
       nullptr,
       "how many logs to read at a time per worker thread",
       CLIENT);
  init("per-log-max-bps",
       &per_log_max_bps,
       "1000000",
       nullptr,
       "Maximum read throughput per log in bytes per seconds",
       CLIENT);
  init("num-logs-to-check",
       &num_logs_to_check,
       "-1",
       nullptr,
       "how many random logs to check; either an integer >= 1 meaning the "
       "absolute number of logs, or a real in (0, 1) meaning this fraction of "
       "all logs in config",
       CLIENT);
  init("csi-data-only",
       &csi_data_only,
       "false",
       nullptr,
       "Try to read data from CSI index only.",
       CLIENT);
  init("only-data-logs",
       &only_data_logs,
       "false",
       nullptr,
       "Don't read metadata logs",
       CLIENT);
  init("only-metadata-logs",
       &only_metadata_logs,
       "false",
       nullptr,
       "Only read metadata logs",
       CLIENT);
  init("sync-sequencer-timeout",
       &sync_sequencer_timeout,
       "10s",
       nullptr,
       "Timeout after which we give up reading a log because we can't sync "
       "with its Sequencer.",
       CLIENT);
  init("json",
       &json,
       "false",
       nullptr,
       "If enabled, dump the per replication factor and per log summary "
       "json format. Useful for integration with scripts.",
       CLIENT);
  init("json-continuous",
       &json_continuous,
       "false",
       nullptr,
       "If enabled, dump the per replication factor and per log summary "
       "json format after every log. Useful for integration with scripts.",
       CLIENT);
  init("summary-only",
       &summary_only,
       "false",
       nullptr,
       "Use with 'json' option to show summary only and skip per-log results."
       "Useful for integration with scripts.",
       CLIENT);
  init("log-idle-timeout",
       &idle_timeout,
       "0",
       nullptr,
       "Give up reading a log if we are not able to read more data after the "
       "specified timeout. If not specified, wait forever until we can read "
       "more records from the log.",
       CLIENT);
  init(
      "log-read-duration",
      &read_duration,
      "240h",
      nullptr,
      "Read each log for no more than the specified amount of time. "
      "Some logs have much more data than others. This option can be used to "
      "give a limit on the amount of time this tool should spend reading a log."
      "This can be used in a context where we want to limit the execution "
      "time.",
      CLIENT);
  init("read-starting-point",
       &read_starting_point,
       "0s",
       nullptr,
       "Read each log from a point in time which is read-starting-point before "
       "now",
       CLIENT);
  init("max-execution-time",
       &max_execution_time,
       "240h",
       nullptr,
       "Specifies total amount of time that checker is allowed to run. ",
       CLIENT);
  init("enable-noisy-errors",
       &enable_noisy_errors,
       "false",
       nullptr,
       "Report benign errors, like DIFFERENT_COPYSET_IN_LATEST_WAVE and "
       "BAD_REPLICATION_LAST_WAVE",
       CLIENT);
  init(
      "dont-fail-on",
      &dont_fail_on_tmp_,
      "NONE",
      [& dont_fail_on_errors = dont_fail_on_errors](std::string val) {
        dont_fail_on_errors = LogErrorTracker::parseRecordLevelErrors(val);
        if (dont_fail_on_errors == LogErrorTracker::RecordLevelError::MAX) {
          throw boost::program_options::error(
              "unrecognized value of --dont-fail-on: " + val +
              "; "
              "expected one or more members of the "
              "LogErrorTracker::RecordLevelError enum, separated by ','");
        }
      },
      "Exclude the listed error types from causing checker to exit with a "
      "failure exit status. If unspecified, checker exits with failure for any "
      "error type.",
      CLIENT);
  init("dont-count-bridge-records",
       &dont_count_bridge_records,
       "false",
       nullptr,
       "Exclude bridge records from counts. That means replication errors on "
       "bridge records will not be surfaced.",
       CLIENT);
  init("report-errors",
       &report_errors_tmp_,
       "none",
       [& report_errors = report_errors](const std::string& val) {
         if (val == "none") {
           report_errors = ReportErrorsMode::NONE;
         } else if (val == "sample") {
           report_errors = ReportErrorsMode::SAMPLE;
         } else if (val == "all") {
           report_errors = ReportErrorsMode::ALL;
         } else if (val == "even-if-no-errors") {
           report_errors = ReportErrorsMode::EVEN_IF_NO_ERRORS;
         } else {
           throw boost::program_options::error(
               "unrecognized value of --report-errors: " + val +
               "; expected one "
               "of: none, sample, all, even-if-no-errors");
         }
       },
       "whether to report to stdout each record that has errors; disabled by "
       "default; --report-errors=all enables for all records that have errors, "
       "--report-errors=sample reports only some records, trying to diversify "
       "the result (different error types, different log types). "
       "--report-errors=even-if-no-errors prints _all_ records, regardless of "
       "whether they have any errors or not; this can produce a lot of output, "
       "use with caution",
       CLIENT);
  init("client-timeout",
       &client_timeout,
       "120s",
       nullptr,
       "Logdevice client timeout",
       CLIENT);
  init("stop-after-num-errors",
       &stop_after_num_errors,
       "0",
       nullptr,
       "Stop checker process and bail after reporting these many number of "
       "errors. This allows quick failure of tests instead of spending a lot "
       "of time continuing and failing later.",
       CLIENT);
}

}} // namespace facebook::logdevice
