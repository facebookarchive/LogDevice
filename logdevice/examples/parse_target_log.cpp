/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/examples/parse_target_log.h"

#include <algorithm>
#include <cctype>
#include <cstdio>
#include <stdexcept>
#include <utility>

#include "logdevice/include/Client.h"
#include "logdevice/include/LogsConfigTypes.h"
#include "logdevice/include/types.h"

using facebook::logdevice::logid_range_t;
using facebook::logdevice::logid_t;
using log_range_name_and_offset_pair = std::pair<std::string, int>;

// Parse as log range name with an optional offset (e.g. "/mylogs" or
// "/mylogs[0]"), which the call site later resolves to a numeric log ID using
// the client.
folly::Optional<log_range_name_and_offset_pair>
parse_as_range(const std::string& input) {
  int offset = -1;

  auto bracket_pos = input.find('[');
  bool has_offset = (bracket_pos != std::string::npos);
  if (has_offset != (input.back() == ']')) {
    fprintf(stderr,
            "error: input argument \"%s\" contains mistached brackets\n",
            input.c_str());
    return folly::none;
  }

  if (has_offset) {
    if (bracket_pos == 0) {
      fprintf(
          stderr,
          "error: input argument \"%s\" lacks a log range name. "
          "Use logid directly, \"/mylogs\", or \"/mylogs[0]\", for example.\n",
          input.c_str());
      return folly::none;
    }
    if (input.size() <= bracket_pos + 2) {
      fprintf(
          stderr,
          "error: offset in input argument \"%s\" cannot be empty. "
          "Use logid directly, \"/mylogs\", or \"/mylogs[0]\", for example.\n",
          input.c_str());
      return folly::none;
    }

    // strip brackets
    size_t offset_size = input.size() - bracket_pos - 2;
    auto sp = folly::StringPiece{
        input, /* startFrom */ bracket_pos + 1, /* size */ offset_size};
    auto expected = folly::tryTo<int>(sp);
    if (expected.hasError()) {
      fprintf(stderr,
              "error: failed to convert offset in \"%s\" to an integer. %s\n",
              input.c_str(),
              folly::makeConversionError(expected.error(), sp).what());
      return folly::none;
    }
  }

  return std::make_pair(
      input.substr(/* pos */ 0, /* count */ bracket_pos), offset);
}

bool is_string_numeric(const std::string& input) {
  return std::all_of(input.begin(), input.end(), ::isdigit);
}

logid_t parse_numeric_log_id(const std::string& input) {
  folly::StringPiece sp{input};
  auto expected = folly::tryTo<uint64_t>(sp);
  if (expected.hasError()) {
    fprintf(stderr,
            "Error parsing log identifier \"%s\": %s\n",
            input.c_str(),
            folly::makeConversionError(expected.error(), sp).what());
    return facebook::logdevice::LOGID_INVALID;
  }
  return logid_t{expected.value()};
}

bool is_offset_in_bounds(const std::string& log_range_name,
                         int offset,
                         size_t range_size) {
  if (offset >= 0 && (size_t)offset >= range_size) {
    fprintf(stderr,
            "error: log range \"%s\" contains %zu logs, offset must be between "
            "0 and %zu\n",
            log_range_name.c_str(),
            range_size,
            range_size - 1);
    return false;
  }
  return true;
}

logid_range_t parse_target_log_range(const std::string& input,
                                     facebook::logdevice::Client& client) {
  if (is_string_numeric(input)) {
    logid_t log = parse_numeric_log_id(input);
    return std::make_pair(log, log);
  }

  logid_range_t failure = std::make_pair(
      facebook::logdevice::LOGID_INVALID, facebook::logdevice::LOGID_INVALID);

  auto opt = parse_as_range(input);
  if (!opt) {
    return failure;
  }
  const std::string& log_range_name = opt->first;
  const int offset = opt->second;

  std::unique_ptr<facebook::logdevice::client::LogGroup> lookup_result =
      client.getLogGroupSync(log_range_name);
  if (!lookup_result) {
    fprintf(stderr,
            "error: failed to resolve log \"%s\"; is it configured?\n",
            log_range_name.c_str());
    return failure;
  }

  const facebook::logdevice::logid_range_t range = lookup_result->range();
  const size_t range_size = range.second.val() - range.first.val() + 1;

  if (!is_offset_in_bounds(log_range_name, offset, range_size)) {
    return failure;
  }

  if (range_size > 1 && offset >= 0) {
    logid_t log = logid_t(range.first.val() + std::max(offset, 0));
    return std::make_pair(log, log);
  }

  return range;
}

logid_t parse_target_log(const std::string& input,
                         facebook::logdevice::Client& client) {
  const facebook::logdevice::logid_range_t range =
      parse_target_log_range(input, client);
  const size_t range_size = range.second.val() - range.first.val() + 1;

  // If the log range could not be found, return an error.
  // If the log range consists of only one log, return the log's ID.
  if (range.first == facebook::logdevice::LOGID_INVALID ||
      range.first == range.second) {
    return range.first;
  }

  // In this case, there are multiple logs in the range. If an offset is
  // specified, return the ID of the log at the given offset.
  auto opt = parse_as_range(input);
  if (!opt) {
    return facebook::logdevice::LOGID_INVALID;
  }
  const std::string& log_range_name = opt->first;
  const int offset = opt->second;

  if (range_size > 1) {
    fprintf(stderr,
            "error: log range \"%s\" contains %zu logs but command line did "
            "not specify which of them to write to; pass an offset like "
            "\"%s[0]\"\n",
            log_range_name.c_str(),
            range_size,
            log_range_name.c_str());
    return facebook::logdevice::LOGID_INVALID;
  }

  return logid_t(range.first.val() + std::max(offset, 0));
}
