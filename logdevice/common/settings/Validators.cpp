/**
 * Copyright (c) 2017-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <boost/program_options.hpp>
#include <folly/String.h>
#include <sys/sysinfo.h>

#include "logdevice/common/commandline_util_chrono.h"
#include "logdevice/common/settings/Validators.h"

namespace facebook { namespace logdevice { namespace setting_validators {

/**
 * Compose an error message about the value of an option being out of range
 * and throw a boost::program_options::error with that message.
 */
void throw_invalid_value(const char* optname,
                         const char* expected,
                         const std::string& val) {
  char buf[4096];
  snprintf(buf,
           sizeof(buf),
           "invalid value for --%s: expected a %s value, got %s",
           optname,
           expected,
           val.c_str());
  throw boost::program_options::error(std::string(buf));
}

/**
 * Compose an error message about the value of an option being out of range
 * and throw a boost::program_options::error with that message.
 */
void throw_invalid_value(const char* optname,
                         const char* expected,
                         ssize_t num) {
  auto num_str = std::to_string(num);
  throw_invalid_value(optname, expected, num_str);
}

void validate_string_one_of(const char* optname,
                            const std::vector<std::string>& allowed,
                            const std::string& value) {
  if (std::find(allowed.begin(), allowed.end(), value) == allowed.end()) {
    char buf[4096];
    snprintf(buf,
             sizeof(buf),
             "invalid value for --%s: expected one of {\"%s\"}, got \"%s\"",
             optname,
             folly::join("\", \"", allowed).c_str(),
             value.c_str());
    throw boost::program_options::error(std::string(buf));
  }
}

void checksum_bits_notifier(int val) {
  if (val != 0 && val != 32 && val != 64) {
    throw boost::program_options::error(
        "Invalid value for --checksum-bits. Must be 0, 32 or 64.");
  }
}

size_t parse_memory_budget::getAvailableMemory() {
  struct sysinfo linux_sysinfo;
  int rv = sysinfo(&linux_sysinfo);
  // sysinfo cannot fail unless a bad pointer is passed.
  ld_check(rv == 0);
  return linux_sysinfo.totalram * linux_sysinfo.mem_unit;
}

size_t parse_memory_budget::operator()(const char* name,
                                       const std::string& value) {
  auto throw_invalid_value = [&]() {
    char buf[4096];
    snprintf(buf,
             sizeof(buf),
             "invalid value for --%s: expected a percentage in the form "
             "\"X%%\" where X is between ]0, 100] or a positive integer "
             "with an optional multipler (KMGT), got %s",
             name,
             value.c_str());
    throw boost::program_options::error(std::string(buf));
  };

  if (value.empty()) {
    throw_invalid_value();
  }

  // First, check if this is an absolute value in the form of an integer plus
  // optional multiplier.

  size_t val;
  if (parse_scaled_int(value.c_str(), &val) == 0) {
    if (val <= 0) {
      throw_invalid_value();
    }
    return val;
  }

  // Then, check if the value is a percentage value between 0 and 100.

  char* eptr;
  float percentage = strtof(value.c_str(), &eptr);

  if (eptr == value.c_str() || strncmp(eptr, "%", 2) != 0) {
    throw_invalid_value();
  }

  if (percentage <= 0 || percentage > 100) {
    throw_invalid_value();
  }

  return floor(percentage * getAvailableMemory() / 100.0);
}

}}} // namespace facebook::logdevice::setting_validators
