/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/common/buffered_writer/BufferedWriterOptionsUtil.h"

#include <boost/program_options.hpp>

#include "logdevice/common/commandline_util_chrono.h"

using namespace boost::program_options;

namespace facebook { namespace logdevice {

void describeBufferedWriterOptions(options_description& po,
                                   BufferedWriter::Options* opts,
                                   std::string prefix) {
  static_assert(sizeof(BufferedWriter::Options) == 7 * 8,
                "If you added fields to BufferedWriter::Options, you may want "
                "to add them here as well.");

  po.add_options()(
      (prefix + "time-trigger").c_str(),
      chrono_value(&opts->time_trigger),
      "Flush buffered writes for a log when the oldest has been buffered this "
      "long (negative for no trigger).");
  po.add_options()(
      (prefix + "size-trigger").c_str(),
      value<std::string>()
          ->default_value(std::to_string(opts->size_trigger))
          ->notifier([prefix, opts](const std::string& val) {
            if (val == "-1") {
              opts->size_trigger = -1;
              return;
            }
            uint64_t x;
            if (parse_scaled_int(val.c_str(), &x) != 0) {
              throw boost::program_options::error("Invalid " + prefix +
                                                  "size-trigger: " + val);
            }
            opts->size_trigger = x;
          }),
      "Flush buffered writes for a log as soon there are this many payload "
      "bytes buffered (negative for no trigger).");
  po.add_options()(
      (prefix + "mode").c_str(),
      value<std::string>()
          ->default_value(BufferedWriter::LogOptions::modeToString(opts->mode))
          ->notifier([prefix, opts](const std::string& val) {
            if (BufferedWriter::LogOptions::parseMode(
                    val.c_str(), &opts->mode) != 0) {
              throw boost::program_options::error("Invalid " + prefix +
                                                  "mode: " + val);
            }
          }),
      "'independent' to write each buffered append independently. "
      "'one_at_a_time' to write sequentially, avoiding reordering.");
  po.add_options()(
      (prefix + "retry-count").c_str(),
      value<int>(&opts->retry_count)->default_value(opts->retry_count),
      "Max number of times to retry. 0 for no retrying, negative for "
      "unlimited.");
  po.add_options()(
      (prefix + "retry-initial-delay").c_str(),
      chrono_value(&opts->retry_initial_delay),
      "Initial delay before retrying (negative for a default 2x the append "
      "timeout).  Subsequent retries are made after successively larger delays"
      "(exponential backoff with a factor of 2) up to retry-max-delay.");
  po.add_options()((prefix + "retry-max-delay").c_str(),
                   chrono_value(&opts->retry_max_delay),
                   "Max delay when retrying (negative for no limit).");
  po.add_options()(
      (prefix + "compression").c_str(),
      value<std::string>()
          ->default_value(compressionToString(opts->compression))
          ->notifier([prefix, opts](const std::string& val) {
            if (BufferedWriter::LogOptions::parseCompression(
                    val.c_str(), &opts->compression) != 0) {
              throw boost::program_options::error("Invalid " + prefix +
                                                  "compression: " + val);
            }
          }),
      "Algorithm to use for client-side compression in Buffered writer. 'none' "
      "for no compression. Supported values: 'zstd', 'lz4', 'lz4_hc'.");
  po.add_options()((prefix + "memory-limit-mb").c_str(),
                   value<int32_t>(&opts->memory_limit_mb)
                       ->default_value(opts->memory_limit_mb),
                   "Approximate memory budget for buffered and inflight "
                   "writes, in megabytes.");
}

std::string BufferedWriter::LogOptions::modeToString(Mode mode) {
  switch (mode) {
    case Mode::INDEPENDENT:
      return "independent";
    case Mode::ONE_AT_A_TIME:
      return "one_at_a_time";
  }
  ld_check(false);
  return "(internal error)";
}

int BufferedWriter::LogOptions::parseMode(const char* str, Mode* out_mode) {
  Mode mode;
  if (!strcmp(str, "independent")) {
    mode = Mode::INDEPENDENT;
  } else if (!strcmp(str, "one_at_a_time")) {
    mode = Mode::ONE_AT_A_TIME;
  } else {
    return -1;
  }
  if (out_mode) {
    *out_mode = mode;
  }
  return 0;
}

std::string BufferedWriter::LogOptions::compressionToString(Compression c) {
  return facebook::logdevice::compressionToString(c);
}

int BufferedWriter::LogOptions::parseCompression(const char* str,
                                                 Compression* out_c) {
  return facebook::logdevice::parseCompression(str, out_c);
}

}} // namespace facebook::logdevice
