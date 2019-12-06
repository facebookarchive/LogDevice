/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "logdevice/test/ldbench/worker/Options.h"

#include <cinttypes>
#include <cstdio>
#include <ctime>

#include <boost/program_options.hpp>
#include <folly/Optional.h>
#include <folly/Random.h>
#include <folly/experimental/EnvUtil.h>

#include "logdevice/common/buffered_writer/BufferedWriterOptionsUtil.h"
#include "logdevice/common/commandline_util_chrono.h"
#include "logdevice/common/debug.h"
#include "logdevice/lib/ClientSettingsImpl.h"
#include "logdevice/test/ldbench/worker/Worker.h"
#include "logdevice/test/ldbench/worker/WorkerRegistry.h"

namespace facebook { namespace logdevice { namespace ldbench {

using namespace boost::program_options;

Options options;
std::unique_ptr<ClientSettings> client_settings;

std::set<std::string>& OptionsRestrictions::commonOptions() {
  static std::set<std::string> opts = {
      "help",
      "loglevel",
      "verbose",
      "bench-name",
      "cluster-name",
      "config-path",
      "credentials",
      "client-authentication",
      "client-timeout",
      "event-sample-ratio",
      "ignore-errors",
      "log-range-name",
      "log-hash-salt",
      "log-hash-range",
      "partition-by",
      "publish-dir",
      "stats-interval",
      "sys-name",
      "write-bytes-increase-interval",
      "write-bytes-increase-step",
      "write-bytes-increase-factor"
      "start_time",
      "worker-id",
  };
  return opts;
}

/**
 * Positional command-line options.
 */
void Options::get_positional_options(
    positional_options_description& positional) {
  positional.add("bench-name", 1);
}

/**
 * Named command-line options and their descriptions.
 */
void Options::get_named_options(options_description& named) {
  named.add_options()("help,H", "Produce this help message and exit");
  named.add_options()(
      "verbose,V",
      "Also include a description of all LogDevice Client settings in the "
      "help message");
  named.add_options()("bench-name",
                      value<std::string>(&bench_name)->required(),
                      "Benchmark name");
  named.add_options()(
      "cluster-name",
      value<std::string>(&cluster_name)->default_value("integration_test"),
      "Cluster name. Doesn't affect anything");
  named.add_options()(
      "loglevel",
      value<std::string>()->default_value("error")->notifier(
          dbg::parseLoglevelOption),
      "One of the following: critical, error, warning, info, debug");
  named.add_options()(
      "config-path",
      value<std::string>(&config_path)
          ->default_value("/dev/shm/tmp/cluster/logdevice.conf"),
      "Path to config file");
  named.add_options()(
      "credentials",
      value<std::string>(&credentials),
      "The credentials used to identify client used with config "
      "authorization");
  named.add_options()(
      "client-authentication",
      value<bool>(&client_authentication)->default_value(true),
      "Sets up client authentication using environment variables");
  named.add_options()(
      "log-range-name",
      value<std::string>()
          ->default_value("/ns/test_logs")
          ->notifier([this](const std::string& val) {
            if (val == "*") {
              return;
            }
            folly::split(',', val, log_range_names);
          }),
      "Comma-separated list of names of log groups to use. \"*\" to use all "
      "log "
      "groups. (If you have a log group named \"*\" or log groups with commas "
      "in the name, that's too bad.)");
  named.add_options()("log-hash-salt",
                      value<uint64_t>(&log_hash_salt)->default_value(0),
                      "See log-hash-range");
  named.add_options()(
      "log-hash-range",
      value<std::string>()->default_value("0%:100%")->notifier(
          [this](const std::string& val) {
            try {
              std::vector<std::string> tokens;
              folly::split(':', val, tokens);
              if (tokens.size() != 2) {
                throw std::range_error("");
              }
              for (size_t i = 0; i < 2; ++i) {
                if (tokens[i].size() < 2 || tokens[i].back() != '%') {
                  throw std::range_error("");
                }
                (i ? log_hash_range.second : log_hash_range.first) =
                    folly::to<double>(
                        tokens[i].substr(0, tokens[i].size() - 1)) /
                    100;
              }
              if (log_hash_range.first < 0 || log_hash_range.second > 1 ||
                  log_hash_range.second < log_hash_range.first) {
                throw std::range_error("");
              }
            } catch (std::range_error& e) {
              throw boost::program_options::error(
                  "Invalid value of --log-hash-range: " + val);
            }
          }),
      "A range of percentage values, e.g. \"10%:30%\". "
      "'log-range-name', 'log-hash-salt' and 'log-hash-range' define which "
      "logs "
      "the ensemble of workers will use. These logs are then split among "
      "workers according to 'partition-by' and 'worker-id'. A log is used if "
      "hash(log_id, log-hash-salt) is inside 'log-hash-range' (hash is scaled "
      "to range [0%, 100%]). So, the length of the range is the percentage of "
      "logs that will be affected by this ensemble of workers. You can point "
      "multiple workers to the same logs by using the same range and salt, or "
      "point to disjoint sets of logs by using disjoint ranges with the same "
      "salt, or point to independently selected random logs by using different "
      "salts.");
  named.add_options()(
      "partition-by",
      value<std::string>()->default_value("default")->notifier(
          [this](const std::string& val) {
            if (val == "default") {
              partition_by = PartitioningMode::DEFAULT;
            } else if (val == "log") {
              partition_by = PartitioningMode::LOG;
            } else if (val == "log_and_idx") {
              partition_by = PartitioningMode::LOG_AND_IDX;
            } else if (val == "record") {
              partition_by = PartitioningMode::RECORD;
            } else {
              throw boost::program_options::error(
                  "Unexpected value of --partition-by: " + val);
            }
          }),
      "How to split the work among workers. Possible values:\n"
      "  \"log\": Each worker takes a subset of logs. Worker i out of n works "
      "on log_id if hash(log_id)%n == i.\n"
      "  \"log_and_idx\": For readers, each worker takes a subset of "
      "log+tailer_idx pairs, where tailer_idx is in [0, fanout - 1]. Not "
      "applicable to writers."
      "  \"record\": Each worker takes all the logs, but divides the "
      "throughput "
      "or request rate by number of workers. Not applicable to readers.");
  named.add_options()("worker-id",
                      value<std::string>()->default_value("0/1")->notifier(
                          [this](const std::string& worker_id) {
                            if (std::sscanf(worker_id.c_str(),
                                            "%" PRIu32 "/%" PRIu32,
                                            &worker_id_index,
                                            &worker_id_count) != 2) {
                              throw boost::program_options::error(
                                  "Failed to parse worker_id: " + worker_id);
                            }
                            if (worker_id_index >= worker_id_count) {
                              throw boost::program_options::error(
                                  "Invalid worker_id: " + worker_id);
                            }
                          }),
                      "Pair of \"{worker_index}/{worker_count}\"");
  named.add_options()("client-timeout",
                      chrono_value(&client_timeout),
                      "Timeout for the Client object");
  named.add_options()("init-window",
                      value<uint64_t>(&init_window)->default_value(10),
                      "Initial client window size");
  named.add_options()("max-window",
                      value<uint64_t>(&max_window)->default_value(1000),
                      "Maximum client window size");
  named.add_options()("warmup-duration",
                      value<uint64_t>(&warmup_duration)->default_value(10),
                      "Warmup duration, in seconds");
  named.add_options()(
      "duration",
      value<int64_t>(&duration)->default_value(60),
      "Experiment duration, in seconds, negative means infinite"
      "In distributed testings, workers will be stopped by coordinator. This"
      "duration options will be force to negative");
  named.add_options()("cooldown-duration",
                      value<uint64_t>(&cooldown_duration)->default_value(5),
                      "Cooldown duration, in seconds");
  named.add_options()("backfill-interval",
                      value<uint64_t>(&backfill_interval)->default_value(15),
                      "Backfill interval, in seconds");
  named.add_options()("payload-size",
                      value<uint64_t>(&payload_size)->default_value(1024),
                      "Record payload size, in bytes, per write");
  named.add_options()(
      "histogram-bucket-count",
      value<uint64_t>(&histogram_bucket_count)->default_value(1000),
      "Number of output histogram buckets");
  named.add_options()("write-rate",
                      value<double>(&write_rate)->default_value(1000),
                      "Write load in appends per second. For 'write' bench use "
                      "--write-bytes-per-sec instead.");
  named.add_options()(
      "filter-selectivity",
      value<double>(&filter_selectivity)
          ->default_value(1.0)
          ->notifier([](double val) {
            if (val < 0 || val > 1) {
              throw boost::program_options::error(
                  "Invalid filter selectivity: " + std::to_string(val) +
                  ". It should be in the range of [0.0, 1.0].");
            }
          }),
      "Selectivity is a fraction between 0 and 1.0. Setting selectivity to "
      "0 means filter out all records with key. 1.0 means no filtering.");
  named.add_options()("ignore-errors",
                      value<bool>(&ignore_errors)->default_value(false),
                      "Ignore most errors");
  named.add_options()("pretend",
                      value<bool>(&pretend)->default_value(false),
                      "Only pretend to do anything, for testing");
  named.add_options()(
      "read-all",
      value<bool>(&read_all)->default_value(false),
      "Start reading at the oldest untrimmed LSN instead of tailing from the "
      "last released record. See also: pct-readers-consider-backlog-on-start");
  named.add_options()(
      "filter-type",
      value<std::string>()
          ->default_value("nofilter")
          ->notifier([this](const std::string& type) {
            if (type == "equality") {
              filter_type = ServerRecordFilterType::EQUALITY;
            } else if (type == "range") {
              filter_type = ServerRecordFilterType::RANGE;
            } else if (type == "nofilter") {
              filter_type = ServerRecordFilterType::NOFILTER;
            } else {
              throw boost::program_options::error("Invalid filter-type: " +
                                                  type);
            }
          }),
      "Use this parameter to enable record filtering. This parameter denotes "
      " filter type. It must be one of the following: nofilter, equality or "
      " range.");
  named.add_options()("fanout",
                      value<double>(&fanout)->default_value(1),
                      "How many tailers to run for each log.");
  named.add_options()(
      "fanout-distribution",
      value<std::string>()
          ->default_value("constant")
          ->notifier([this](const std::string& val) {
            if (!fanout_distribution.parse(val)) {
              throw boost::program_options::error(
                  "Invalid fanout-distribution: " + val);
            }
          }),
      "Describes how number of readers varies from log to log. Comma-separated "
      "list of numbers representing a histogram with exponentially increasing "
      "bucket widths. If you need to just fuzz the fanout a little, use "
      "something like --fanout-distribution=\"1,2\". See doc/ldbench.md for "
      "details. The average fanout is given by --fanout. The number of tailers "
      "for each log is picked deterministically.");
  named.add_options()(
      "reader-restart-period",
      chrono_value(&reader_restart_period),
      "How often to restart each tailer, on average. Negative to never restart "
      "tailers.");
  named.add_options()(
      "reader-restart-period-distribution",
      value<std::string>()
          ->default_value("constant")
          ->notifier([this](const std::string& val) {
            if (!reader_restart_period_distribution.parse(val)) {
              throw boost::program_options::error(
                  "Invalid reader-restart-period-distribution: " + val);
            }
          }),
      "Describes how the rate of reader restarts varies from log to log. If "
      "you "
      "just need to fuzz the restarts a little, use something like "
      "--reader-restart-period-distribution=\"1,2\". See doc/ldbench.md for "
      "details. The average rate of restarts is given by "
      "--reader-restart-period. The rate of restarts is picked for each tailer "
      "deterministically.");
  named.add_options()(
      "reader-restart-spikiness",
      value<std::string>()->default_value("none")->notifier(
          [this](const std::string& val) {
            std::string er;
            if (!reader_restart_spikiness.parse(val, &er)) {
              throw boost::program_options::error(
                  "Invalid reader-restart-spikiness: " + val +
                  ", error: " + er);
            }
          }),
      "How bursty the reader restarts are. Example: \"70%/1%/15min/aligned\" "
      "would mean that 70% of all reader restarts happen in 9-second bursts "
      "every 15 minutes, the other 30% will happen uniformly throughout the "
      "rest of 15-minute periods. See doc/ldbench.md for details.");
  named.add_options()(
      "restart-backlog-probability",
      value<double>(&restart_backlog_probability)->default_value(0),
      "When restarting a tailer, how often it'll start in backlog instead of "
      "tail. In [0, 1]. See also --reader-restart-period and "
      "--restart-backlog-depth.");
  named.add_options()(
      "restart-backlog-depth",
      chrono_value(&restart_backlog_depth),
      "When restarting a reader into backlog (according to "
      "--reader-restart-period and --restart-backlog-probability), how far in "
      "the past it'll start reading. E.g. 6h means that we'll do a "
      "findTime(now - 6 hours) and startReading() from the resulting LSN.");
  named.add_options()(
      "restart-backlog-depth-distribution",
      value<std::string>()
          ->default_value("constant")
          ->notifier([this](const std::string& val) {
            if (!restart_backlog_depth_distribution.parse(val)) {
              throw boost::program_options::error(
                  "Invalid restart-backlog-depth-distribution: " + val);
            }
          }),
      "Describes variation around --restart-backlog-depth. See doc/ldbench.md "
      "for details.");
  named.add_options()(
      "write-bytes-per-sec",
      value<std::string>()->default_value("1M")->notifier(
          [this](const std::string& val) {
            if (parse_scaled_int(val.c_str(), &write_bytes_per_sec) != 0) {
              throw boost::program_options::error(
                  "Invalid write-bytes-per-sec: " + val);
            }
          }),
      "Approximate total write throughput in bytes/s. Only supported by "
      "'write' "
      "bench. Not to be confused with --write-rate, which is in records/s and "
      "is not supported by 'write' bench.");
  named.add_options()(
      "write-bytes-increase-factor",
      value<double>(&write_bytes_increase_factor)->default_value(1.0),
      "The increase factor of write byte per second. e.g. 1.1 means increasing "
      "the total through put 1.1x for every interval. This can not be used "
      "together with write-bytes-increase-step. We should only use one of them."
      "If this factor is configured, the write-bytes-increase-step will be "
      "invalid.");
  named.add_options()(
      "write-bytes-increase-step",
      value<std::string>()->default_value("0")->notifier(
          [this](const std::string& val) {
            if (parse_scaled_int(val.c_str(), &write_bytes_increase_step) !=
                0) {
              throw boost::program_options::error(
                  "Invalid write-bytes-increase-step: " + val);
            }
          }),
      "Approximate total write throughput increases in B/s, e.g. 1K, 10M. Will"
      "be invalid if write-byte-increase-factor is configured");
  named.add_options()("write-bytes-increase-interval",
                      chrono_value(&write_bytes_increase_interval),
                      "How often to increase the write-record-per-second on "
                      "average. 0 or non-configure to never increase");
  named.add_options()(
      "rate-limit-bytes",
      value<std::string>()
          ->default_value("unlimited")
          ->notifier([this](const std::string& val) {
            rate_limit_t res;
            int rv = parse_rate_limit(val.c_str(), &res);
            if (rv != 0) {
              throw boost::program_options::error(
                  "Invalid value(" + val +
                  ") for --rate-limit-bytes."
                  "Expected format is <count>/<duration><unit>, e.g. 100M/1s");
            }
            rate_limit_bytes = res;
          }),
      "Target total read throughput in bytes per unit of time, e.g. "
      "\"100M/1s\"."
      " If --worker-id is used, the rate limit is evenly divided among "
      "workers. "
      "Note that, although the observed read throughput will match this option "
      "pretty closely, this option tends to increase average size of delivered "
      "records because logs with smaller records check the rate limit more "
      "often.");
  named.add_options()(
      "log-write-bytes-per-sec-distribution",
      value<std::string>()
          ->default_value("constant")
          ->notifier([this](const std::string& val) {
            if (!log_write_bytes_per_sec_distribution.parse(val)) {
              throw boost::program_options::error(
                  "Invalid restart-backlog-depth-distribution: " + val);
            }
          }),
      "Describes the variation of write throughput from log to log (in "
      "addition "
      "to --write-bytes-per-sec which describes the total throughput). The "
      "default is no variation. See doc/ldbench.md for format description. "
      "Example: --log-write-bytes-per-sec-distribution=1,2 makes the "
      "throughput "
      "uniformly distributed between x and 4x, where x is chosen so that the "
      "total throughput matches --write-bytes-per-sec.");
  named.add_options()(
      "write-spikiness",
      value<std::string>()->default_value("none")->notifier(
          [this](const std::string& val) {
            std::string er;
            if (!write_spikiness.parse(val, &er)) {
              throw boost::program_options::error(
                  "Invalid write-spikiness: " + val + ", error: " + er);
            }
          }),
      "How bursty writes are. Example: \"70%/10%/30s/aligned\" would mean that "
      "70% of all appends happen in 3-second bursts every 30 seconds, the "
      "other 30% will happen uniformly throughout the rest of 30-second "
      "periods. '/aligned' means that the bursts happen at the same time in "
      "all "
      "logs. Without '/aligned', each log gets its own sequence of bursts, "
      "randomly shifted relative to other logs. See doc/ldbench.md for "
      "details.");
  named.add_options()(
      "payload-size-distribution",
      value<std::string>()
          ->default_value("constant")
          ->notifier([this](const std::string& val) {
            if (!payload_size_distribution.parse(val)) {
              throw boost::program_options::error(
                  "Invalid restart-backlog-depth-distribution: " + val);
            }
          }),
      "Describes the random variation of payload size from record to record "
      "(in "
      "addition to --payload-size which describes the average payload size). "
      "The default is no variation. See doc/ldbench.md for format description. "
      "Not to be confused with --log-payload-size-distribution.");
  named.add_options()(
      "log-payload-size-distribution",
      value<std::string>()
          ->default_value("constant")
          ->notifier([this](const std::string& val) {
            if (!log_payload_size_distribution.parse(val)) {
              throw boost::program_options::error(
                  "Invalid restart-backlog-depth-distribution: " + val);
            }
          }),
      "Describes the variation of payload size from log to log (in addition to "
      "--payload-size which describes the average payload size). The default "
      "is "
      "no variation. See doc/ldbench.md for format description. Not to be "
      "confused with --payload-size-distribution. The difference is that "
      "if you use only --log-payload-size-distribution, each log will use "
      "constant payload size, but the constant will be different for different "
      "logs; if you use only --payload-size-distribution, each record will "
      "have "
      "independently random payload size.");
  named.add_options()(
      "max-appends-in-flight",
      value<uint64_t>(&max_appends_in_flight)->default_value(10000),
      "Limit on the total number of outstanding appends in a write worker "
      "process. When exceeded, the worker stops appending until some of the "
      "in-flight appends complete.");
  named.add_options()(
      "max-append-bytes-in-flight",
      value<std::string>()->default_value("100M")->notifier(
          [this](const std::string& val) {
            if (parse_scaled_int(val.c_str(), &max_append_bytes_in_flight) !=
                0) {
              throw boost::program_options::error(
                  "Invalid max-append-bytes-in-flight: " + val);
            }
          }),
      "Like --max-appends-in-flight but in bytes.");
  named.add_options()("use-buffered-writer",
                      value<bool>(&use_buffered_writer)->default_value(false),
                      "Append using BufferedWriter.");
  named.add_options()(
      "payload-entropy",
      value<double>(&payload_entropy)
          ->default_value(1)
          ->notifier([](double val) {
            if (val > 1.0 || val < 0.0) {
              throw boost::program_options::error(
                  "--payload-entropy must be between 0 and 1");
            }
          }),
      "How compressible the payload should be. E.g. 0.25 would produce "
      "payloads "
      "that are easily compressed by 4x. Where such compression happens "
      "depends "
      "on a number of factors. If buffered writer is enabled (with "
      "compression), the compression will happen on the client side. "
      "Otherwise, "
      "if sequencer batching is enabled (with compression), the compression "
      "will happen in sequencer. Otherwise, if rocksdb compression is enabled, "
      "compression will happen when flushing sst files on storage nodes. "
      "Otherwise there'll be no compression at all.");
  named.add_options()(
      "payload-entropy-sequencer",
      value<double>(&payload_entropy_sequencer)
          ->default_value(1)
          ->notifier([](double val) {
            if (val > 1.0 || val < 0.0) {
              throw boost::program_options::error(
                  "--payload-entropy-sequencer must be between 0 and 1");
            }
          }),
      "This can be used to simulate the phenomenon that payloads that are "
      "already compressed by client-side buffered writer can achieve a much "
      "better compression ratio when recompressed by sequencer batching "
      "(because of increased block size). E.g. typical numbers are ~2.2x "
      "compression on client followed by additional ~2x compression in "
      "sequencer; this can be simulated with --payload-entropy=0.455 "
      "--payload-entropy-sequencer=0.5");
  named.add_options()(
      "meta-requests-per-sec",
      value<uint64_t>(&meta_requests_per_sec)->default_value(100),
      "The number of requests of the given type (findTime, etc) per second "
      "that each worker should do on average.");
  named.add_options()(
      "meta-requests-spikiness",
      value<std::string>()->default_value("none")->notifier(
          [this](const std::string& val) {
            std::string er;
            if (!meta_requests_spikiness.parse(val, &er)) {
              throw boost::program_options::error(
                  "Invalid meta-requests-spikiness: " + val + ", error: " + er);
            }
          }),
      "How bursty metadata API requests are. See write-spikiness for "
      "examples.");
  named.add_options()(
      "max-requests-in-flight",
      value<uint64_t>(&max_requests_in_flight)->default_value(10000),
      "Limit on the total number of outstanding metadata API requests in a "
      "meta worker process. When exceeded, the worker stops making new "
      "requests "
      " until some of the in-flight requests complete.");
  named.add_options()(
      "findtime-avg-time-ago",
      chrono_value(&findtime_avg_time_ago),
      "The average time ago to use as timestamp for findTime calls. Also see "
      "findtime-timestamp-distribution.");
  named.add_options()(
      "findtime-timestamp-distribution",
      value<std::string>()
          ->default_value("constant")
          ->notifier([this](const std::string& val) {
            if (!findtime_timestamp_distribution.parse(val)) {
              throw boost::program_options::error(
                  "Invalid findtime-timestamp-distribution: " + val);
            }
          }),
      "Describes the variation of timestamps for which to do findTime calls, "
      "as a distance from findtime-avg-minutes-ago. See doc/ldbench.md for "
      "details. The default is no variation.");
  named.add_options()(
      "log-requests-per-sec-distribution",
      value<std::string>()
          ->default_value("constant")
          ->notifier([this](const std::string& val) {
            if (!log_requests_per_sec_distribution.parse(val)) {
              throw boost::program_options::error(
                  "Invalid log-requests-per-sec-distribution: " + val);
            }
          }),
      "Describes the variation of the number of requests over the different "
      "logs. See doc/ldbench.md for details. The default is no variation.");
  named.add_options()(
      "pct-readers-consider-backlog-on-start",
      value<double>(&pct_readers_consider_backlog_on_start)
          ->default_value(0.5)
          ->notifier([](double val) {
            if (val > 1.0 || val < 0.0) {
              throw boost::program_options::error(
                  "--pct-readers-consider-backlog-on-start must be between 0 "
                  "and 1");
            }
          }),
      "This fraction of the readers will each start in the backlog with a "
      "probability of --restart-backlog-probability.");
  named.add_options()(
      "sys-name",
      value<std::string>(&sys_name)
          ->default_value("logdevice")
          ->notifier([](const std::string& name) {
            if (name != "logdevice" && name != "kafka") {
              throw boost::program_options::error("Invalid sys-name: " + name);
            }
          }),
      "logdevice or kafka");
  named.add_options()(
      "start-time",
      value<std::string>()->default_value("")->notifier(
          [this](const std::string& time) {
            if (time != "" && !SystemTimestamp::fromString(time, start_time)) {
              throw std::invalid_argument(
                  "Do not match format YYYY-MM-DD_HH:MM:SS");
            }
          }),
      "HH:MM:SS,MM/DD/YYYY. This option is only used for distributed testing."
      "In distributed testings, we set a start time and a durations for all "
      "workers, and let coordinator stop the workers. We will not set durations"
      "for each worker."
      "We want to make sure every workers start and increase the throughput"
      "at the same time. However, if the write-bytes-increase-interval is too"
      "small, it may still not work well.");
  named.add_options()(
      "publish-dir",
      value<std::string>(&publish_dir)->default_value(""),
      "If configured, stats and events will be written to files");
  named.add_options()("stats-interval",
                      value<uint64_t>(&stats_interval)->default_value(60),
                      "Time Interval (Int for Seconds) to publish stats");
  named.add_options()("event-sample-ratio",
                      value<double>(&event_ratio)->default_value(0),
                      "Double 0~1. The ratio to sample events.");
  named.add_options()(
      "record-writer-info",
      value<bool>(&record_writer_info)->default_value(true),
      "If enabled, writers will put some information into payloads "
      "(e.g. client-assigned timestamp), and readers will use this information "
      "to update stats.");

  // Default buffered writer options, taken from scribe config as of the time of
  // writing.
  buffered_writer_options.time_trigger = std::chrono::seconds(1);
  buffered_writer_options.retry_count = 5;
  buffered_writer_options.retry_initial_delay = std::chrono::seconds(10);
  buffered_writer_options.retry_max_delay = std::chrono::seconds(600);
  buffered_writer_options.compression =
      BufferedWriter::Options::Compression::ZSTD;

  describeBufferedWriterOptions(named, &buffered_writer_options);
}

static std::string getenv_safe(const std::string& var) {
  auto environment =
      folly::experimental::EnvironmentState::fromCurrentEnvironment();
  auto iterator = environment->find(var);
  if (iterator != environment->end()) {
    return iterator->second;
  }
  return "";
}

folly::Optional<int> parse_commandline_options(ClientSettingsImpl& settings,
                                               Options& options,
                                               int argc,
                                               const char** argv,
                                               std::ostream& output) try {
  // We first parse any logdevice client settings. Everything else goes to the
  // fallback parser, which looks for the options defined in
  // create_options_description().
  options_description named("Benchmark options");
  options.get_named_options(named);
  positional_options_description positional;
  options.get_positional_options(positional);
  variables_map parsed;
  auto fallback_parser = [&](int tmp_argc, const char** tmp_argv) {
    ld_check(tmp_argc <= argc);
    std::copy(tmp_argv, tmp_argv + tmp_argc, argv);
    argc = tmp_argc;
    command_line_parser parser(argc, argv);
    parser.options(named).positional(positional);
    store(parser.run(), parsed);
  };
  settings.getSettingsUpdater()->parseFromCLI(
      argc, argv, SettingsUpdater::mustBeClientOption, fallback_parser);

  // Create list of registered benchmark names.
  const auto& bench_map = getWorkerFactoryMap();
  std::string bench_names;
  for (const auto& entry : bench_map) {
    bench_names.append(2, ' ');
    bench_names.append(entry.first);
    bench_names.append(1, '\n');
  }

  // Check for --help before calling notify(), so that required options aren't
  // required.
  if (parsed.count("help")) {
    output << "Generic LogDevice benchmark worker.\n\n"
              "Usage: worker BENCH-NAME [OPTION]...\n\n"
              "Benchmark name BENCH-NAME must be one of:\n"
           << bench_names << '\n'
           << named;
    if (parsed.count("verbose")) {
      output << '\n'
             << settings.getSettingsUpdater()->help(SettingFlag::CLIENT);
    }
    // Not an error but caller is still supposed to exit.
    return 0;
  }

  // Surface any parse errors. May throw exception.
  boost::program_options::notify(parsed);

  // Make sure benchmark name is valid.
  if (bench_map.count(options.bench_name) == 0) {
    output << "Invalid benchmark name: '" << options.bench_name
           << "'. Must be one of:\n"
           << bench_names;
    return 1;
  }

  const OptionsRestrictions& restrictions =
      bench_map.at(options.bench_name).accepted_options;
  for (const auto& kv : parsed) {
    bool ok = false;
    ok |= kv.second.empty() || kv.second.defaulted();
    ok |= OptionsRestrictions::commonOptions().count(kv.first);
    ok |= restrictions.allowed_options.count(kv.first);
    const char* bw = "buffered-writer-";
    ok |= restrictions.allow_buffered_writer_options &&
        kv.first.compare(0, strlen(bw), bw) == 0;
    if (!ok) {
      output << "Unexpected option for worker type " << options.bench_name
             << ": " << kv.first << std::endl;
      return 1;
    }
  }
  if (restrictions.allowed_partitioning.count(PartitioningMode::DEFAULT)) {
    if (options.partition_by != PartitioningMode::DEFAULT) {
      output << "Worker type " << options.bench_name
             << " doesn't support --partition-by" << std::endl;
      return 1;
    }
  } else {
    if (options.partition_by == PartitioningMode::DEFAULT) {
      const std::vector<PartitioningMode> precedence = {
          PartitioningMode::RECORD,
          PartitioningMode::LOG_AND_IDX,
          PartitioningMode::LOG,
      };
      for (auto m : precedence) {
        if (restrictions.allowed_partitioning.count(m)) {
          options.partition_by = m;
          break;
        }
      }
      if (options.partition_by == PartitioningMode::DEFAULT) {
        options.partition_by = *restrictions.allowed_partitioning.cbegin();
      }
    }

    if (!restrictions.allowed_partitioning.count(options.partition_by)) {
      output << "--partition-by="
             << boost::any_cast<std::string>(parsed["partition-by"])
             << " is invalid for worker type " << options.bench_name
             << std::endl;
      return 1;
    }
  }

  // TODO (#34402103): This is copy-pasted from readtestapp. Move it (or some
  // other authentication logic) to client plugin so readtestapp and ldbench
  // don't need to worry about it.
  if (options.client_authentication) {
    auto ssl_cert_path = getenv_safe("THRIFT_TLS_CL_CERT_PATH");
    auto ssl_key_path = getenv_safe("THRIFT_TLS_CL_KEY_PATH");
    if (ssl_cert_path != "" && ssl_key_path != "") {
      settings.set("ssl-cert-path", ssl_cert_path.c_str());
      settings.set("ssl-key-path", ssl_key_path.c_str());
      settings.set("ssl-load-client-cert", true);
    } else {
      ld_info("Not enabling client ssl certificate because "
              "THRIFT_TLS_CL_CERT_PATH/THRIFT_TLS_CL_KEY_PATH environment "
              "variables are not set.");
    }
  }

  // All is good.
  return folly::none;
} catch (const error& e) {
  // boost threw an exception
  output << "Failed to parse command-line options: " << e.what() << '\n';
  return 1;
}

}}} // namespace facebook::logdevice::ldbench
