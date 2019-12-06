/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <chrono>
#include <cstdint>
#include <ostream>
#include <set>
#include <string>
#include <vector>

#include <folly/Optional.h>

#include "logdevice/common/ServerRecordFilter.h"
#include "logdevice/common/Timestamp.h"
#include "logdevice/common/types_internal.h"
#include "logdevice/include/BufferedWriter.h"
#include "logdevice/lib/ClientSettingsImpl.h"
#include "logdevice/test/ldbench/worker/util.h"

namespace boost { namespace program_options {
class options_description;
class positional_options_description;
}} // namespace boost::program_options

namespace facebook { namespace logdevice {

class ClientSettingsImpl;

namespace ldbench {

// See "partition-by" in Options.cpp.
enum class PartitioningMode {
  LOG,
  LOG_AND_IDX,
  RECORD,

  DEFAULT,
};

/**
 * All command-line options as global variables.
 *
 * @seealso parse_commandline_options
 */
struct Options {
  // Options shared by all worker types. Define how to create a Client.
  std::string bench_name;
  std::string cluster_name;
  std::string config_path;
  std::string credentials;
  bool client_authentication;
  std::chrono::milliseconds client_timeout = std::chrono::seconds(30);
  // This option is supported only in some places.
  bool ignore_errors;
  // For BenchStats and BenchTracer
  std::string publish_dir; // directory to store stats and event files
  uint64_t stats_interval; // stats snapshot interval (seconds)
  double event_ratio;      // event sampling ratio (0 ~ 1)
  std::chrono::milliseconds write_bytes_increase_interval =
      std::chrono::milliseconds(0);   // throughput increase interval
  uint64_t write_bytes_increase_step; // throughput increase step (B/second)
  double write_bytes_increase_factor; // increasing factor of throughput
  std::string sys_name;               // logdevice or kafka
  SystemTimestamp start_time;         // worker start time

  // Options shared by all worker types.
  // Define on which logs the workers should operate, and how to partition the
  // work across workers. In particular, work will only be done on logs such
  // that hash(log_id, log_hash_salt)/MAX_HASH_VALUE is in log_hash_range.
  std::vector<std::string> log_range_names; // empty means all log ranges
  uint64_t log_hash_salt;
  std::pair<double, double> log_hash_range; // in [0, 1]
  PartitioningMode partition_by;
  uint32_t worker_id_index;
  uint32_t worker_id_count;

  // Options used by some worker types. Which type takes which options is not
  // well documented atm, you'll have to guess or read the code to find out.
  ServerRecordFilterType filter_type;
  uint64_t init_window;
  uint64_t max_window;
  uint64_t warmup_duration;
  int64_t duration;
  uint64_t cooldown_duration;
  uint64_t backfill_interval;
  uint64_t payload_size;
  uint64_t histogram_bucket_count;
  double filter_selectivity;
  double write_rate;
  bool pretend;
  bool record_writer_info;
  // If you're adding an option, don't forget to add it to a REGISTER_WORKER().

  // Options of "read" bench.
  double fanout;
  Log2Histogram fanout_distribution;
  bool read_all;
  std::chrono::milliseconds reader_restart_period{-1000};
  Log2Histogram reader_restart_period_distribution;
  Spikiness reader_restart_spikiness;
  double restart_backlog_probability;
  std::chrono::milliseconds restart_backlog_depth = std::chrono::hours(6);
  Log2Histogram restart_backlog_depth_distribution;
  rate_limit_t rate_limit_bytes;
  double pct_readers_consider_backlog_on_start;

  // Options of "write" bench.
  uint64_t write_bytes_per_sec;
  Log2Histogram log_write_bytes_per_sec_distribution;
  Spikiness write_spikiness;
  Log2Histogram payload_size_distribution;
  Log2Histogram log_payload_size_distribution;
  uint64_t max_appends_in_flight;
  uint64_t max_append_bytes_in_flight;
  bool use_buffered_writer;
  BufferedWriter::Options buffered_writer_options;
  double payload_entropy;
  double payload_entropy_sequencer;

  // Options of metadata API benchmarks.
  uint64_t meta_requests_per_sec;
  Spikiness meta_requests_spikiness;
  uint64_t max_requests_in_flight;
  Log2Histogram log_requests_per_sec_distribution;
  // findTime options to help pick timestamps
  std::chrono::milliseconds findtime_avg_time_ago = std::chrono::minutes(30);
  Log2Histogram findtime_timestamp_distribution;

  // Populates the given options description with the options pointing to fields
  // of this Options instance.
  void get_named_options(boost::program_options::options_description&);
  void get_positional_options(
      boost::program_options::positional_options_description&);
};

extern Options options;
extern std::unique_ptr<ClientSettings> client_settings;

// Describes which of the options are allowed by a particular worker type.
struct OptionsRestrictions {
  enum class AllowBufferedWriterOptions {
    NO,
    YES,
  };

  std::set<std::string> allowed_options;
  // If the concept of partitioning doesn't apply to this worker type, this
  // set contains a single element DEFAULT. Otherwise, this set must not contain
  // DEFAULT.
  std::set<PartitioningMode> allowed_partitioning = {PartitioningMode::DEFAULT};
  // If true, --buffered-writer-* options are allowed.
  bool allow_buffered_writer_options;

  explicit OptionsRestrictions(
      std::set<std::string> opts = {},
      std::set<PartitioningMode> p = {PartitioningMode::LOG},
      AllowBufferedWriterOptions buf = AllowBufferedWriterOptions::NO)
      : allowed_options(std::move(opts)),
        allowed_partitioning(std::move(p)),
        allow_buffered_writer_options(buf == AllowBufferedWriterOptions::YES) {}

  // Options that are allowed by all worker types regardless of their
  // OptionsRestrictions.
  static std::set<std::string>& commonOptions();
};

/**
 * Parse command-line options, including logdevice client settings.
 *
 * @param output  Stream to write errors and --help text to.
 * @return
 *  folly::none on success. In this case `output` is left empty.
 *  0 if --help was requested, `output` contains the help message.
 *  -1 if options are invalid, `output` contains error message.
 */
folly::Optional<int> parse_commandline_options(ClientSettingsImpl& settings,
                                               Options& options,
                                               int argc,
                                               const char** argv,
                                               std::ostream& output);

} // namespace ldbench
}} // namespace facebook::logdevice
