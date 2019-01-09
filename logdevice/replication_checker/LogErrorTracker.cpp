/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "logdevice/replication_checker/LogErrorTracker.h"

#include <array>
#include <sstream>

#include <boost/tokenizer.hpp>
#include <folly/Bits.h>
#include <folly/container/Array.h>

#include "logdevice/common/checks.h"

namespace facebook { namespace logdevice {

static constexpr uint32_t getLogErrorFlagsSize() {
  return folly::findLastSet(
      static_cast<uint32_t>(LogErrorTracker::LogLevelError::MAX));
}

static const std::array<std::string, getLogErrorFlagsSize()>&
getLogErrorDescriptions() {
  static const auto log_error_descriptions = folly::make_array(
      std::string("No error."),
      std::string(
          "A metadata log is empty, i.e. the log is not fully provisioned. "
          "The log can't be read until its metadata log is populated. Maybe "
          "the "
          "log has just been added to config and wasn't fully provisioned "
          "yet."));
  return log_error_descriptions;
}

static const std::array<std::string, getLogErrorFlagsSize()>&
getLogErrorNames() {
  static const auto log_error_names =
      folly::make_array(std::string("NONE"), std::string("EMPTY_METADATA_LOG"));
  return log_error_names;
}

static constexpr uint32_t getRecordErrorFlagsSize() {
  return folly::findLastSet(
      static_cast<uint32_t>(LogErrorTracker::RecordLevelError::MAX));
}

static const std::array<std::string, getRecordErrorFlagsSize()>&
getRecordErrorDescriptions() {
  static const auto record_error_descriptions = folly::make_array(
      std::string("No error."),
      std::string("Some records don't have any copy."),
      std::string(
          "Not all copies of the last wave of a record have the same copyset. "
          "This is unexpected and may be due to rebuilding failures leaving "
          "inconsistent copysets accross copies of the same wave."),
      std::string("A record has been found on node `NX` but `NX` is not in the "
                  "record's "
                  "copyset. This is really bad. This should never happen."),
      std::string(
          "A record has a copyset that is not a subset of its epoch's nodeset"),
      std::string("A record that should be replicated R-way has one or more of "
                  "its copies "
                  "with a copyset smaller than R. This is really bad. This "
                  "should never "
                  "happen."),
      std::string("There is no copyset such that at least r of its nodes have "
                  "a last wave"
                  "record with this copyset."),
      std::string(
          "Different copies of the same record have different payload hash or "
          "attributes. This error is not thrown for hole-record conflicts"),
      std::string(
          "Copies of the last wave of a record have copysets that do not match "
          "their expected failure domain properties. For instance, a record "
          "lies "
          "within an epoch whose metadata indicates that records should be "
          "replicated accross 2 racks but all copies are on the same rack."),
      std::string("Same as BAD_REPLICATION_LAST_WAVE but accross all waves."),
      std::string(
          "Some copies of a record are hole plugs while some others have "
          "payloads. "
          "This should never happen as the log recovery process should "
          "guarantee "
          "that there should not be hole-record conflicts for records released "
          "for delivery. This is how LogDevice guarantees that all readers "
          "will "
          "have the exact same view of the data."),
      std::string(
          "Some copies of a record are bridge records while some others have "
          "payloads. Just like HOLE_RECORD_CONFLICT, this is supposed to be "
          "impossible."),
      std::string(
          "A record's copyset contains the same shard ID multiple times."));
  return record_error_descriptions;
}

static const std::array<std::string, getRecordErrorFlagsSize()>&
getRecordErrorNames() {
  static const auto record_error_names =
      folly::make_array(std::string("NONE"),
                        std::string("DATALOSS"),
                        std::string("DIFFERENT_COPYSET_IN_LATEST_WAVE"),
                        std::string("COPYSET_WITHOUT_SELF"),
                        std::string("COPYSET_NOT_IN_NODESET"),
                        std::string("SMALL_COPYSET"),
                        std::string("NO_ACCURATE_COPYSET"),
                        std::string("DATA_MISMATCH"),
                        std::string("BAD_REPLICATION_LAST_WAVE"),
                        std::string("BAD_REPLICATION"),
                        std::string("HOLE_RECORD_CONFLICT"),
                        std::string("BRIDGE_RECORD_CONFLICT"),
                        std::string("DUPLICATES_IN_COPYSET"));
  return record_error_names;
}

const std::string& LogErrorTracker::describeLogError(LogLevelError error) {
  uint32_t flag_idx = folly::findLastSet(static_cast<uint32_t>(error));

  // Assert if multiple errors set.
  ld_check(folly::popcount(static_cast<uint32_t>(error)) <= 1);
  return getLogErrorDescriptions()[flag_idx];
}

const std::string&
LogErrorTracker::describeRecordError(RecordLevelError error) {
  uint32_t flag_idx = folly::findLastSet(static_cast<uint32_t>(error));

  // Assert if multiple errors set.
  ld_check(folly::popcount(static_cast<uint32_t>(error)) <= 1);
  return getRecordErrorDescriptions()[flag_idx];
}

std::string
LogErrorTracker::recordLevelErrorsToString(RecordLevelError errors) {
  std::stringstream res;
  bool prepend_comma{false};

  for (auto i = RecordLevelError::NONE; i < RecordLevelError::MAX; ++i) {
    if ((errors & i) == RecordLevelError::NONE) {
      continue;
    }
    if (prepend_comma) {
      res << ", ";
    }
    prepend_comma = true;
    res << getRecordErrorNames()[folly::findLastSet(static_cast<uint32_t>(i))];
  }
  if (!prepend_comma) {
    res << getRecordErrorNames()[0];
  }
  return res.str();
}

std::string LogErrorTracker::logLevelErrorsToString(LogLevelError errors) {
  std::stringstream res;
  bool prepend_comma{false};

  for (auto i = LogLevelError::NONE; i < LogLevelError::MAX; ++i) {
    if ((errors & i) == LogLevelError::NONE) {
      continue;
    }
    if (prepend_comma) {
      res << ", ";
    }
    prepend_comma = true;
    res << getLogErrorNames()[folly::findLastSet(static_cast<uint32_t>(i))];
  }
  if (!prepend_comma) {
    res << getLogErrorNames()[0];
  }
  return res.str();
}

LogErrorTracker::LogLevelError
LogErrorTracker::parseLogLevelErrors(std::string errors) {
  if (errors == getLogErrorNames()[0]) {
    return LogLevelError::NONE;
  }
  using tokenizer = boost::tokenizer<boost::escaped_list_separator<char>>;
  std::transform(errors.begin(), errors.end(), errors.begin(), ::toupper);
  tokenizer tok(errors);
  LogLevelError res{LogLevelError::NONE};
  for (const auto& t : tok) {
    auto it =
        std::find(getLogErrorNames().begin(), getLogErrorNames().end(), t);
    if (it == getLogErrorNames().end() || it == getLogErrorNames().begin()) {
      return LogLevelError::MAX;
    }
    res = res |
        static_cast<LogLevelError>(
              1u << (std::distance(getLogErrorNames().begin(), it) - 1));
  }
  return res;
}

LogErrorTracker::RecordLevelError
LogErrorTracker::parseRecordLevelErrors(std::string errors) {
  if (errors == getRecordErrorNames()[0]) {
    return RecordLevelError::NONE;
  }
  using tokenizer = boost::tokenizer<boost::escaped_list_separator<char>>;
  std::transform(errors.begin(), errors.end(), errors.begin(), ::toupper);
  tokenizer tok(errors);
  RecordLevelError res{RecordLevelError::NONE};
  for (const auto& t : tok) {
    auto it = std::find(
        getRecordErrorNames().begin(), getRecordErrorNames().end(), t);
    if (it == getRecordErrorNames().end() ||
        it == getRecordErrorNames().begin()) {
      return RecordLevelError::MAX;
    }
    res = res |
        static_cast<RecordLevelError>(
              1u << (std::distance(getRecordErrorNames().begin(), it) - 1));
  }
  return res;
}

bool operator<(LogErrorTracker::RecordLevelError l,
               LogErrorTracker::RecordLevelError r) {
  return static_cast<uint32_t>(l) < static_cast<uint32_t>(r);
}

LogErrorTracker::RecordLevelError
operator|(LogErrorTracker::RecordLevelError l,
          LogErrorTracker::RecordLevelError r) {
  return static_cast<LogErrorTracker::RecordLevelError>(
      static_cast<uint32_t>(l) | static_cast<uint32_t>(r));
}

LogErrorTracker::RecordLevelError&
operator|=(LogErrorTracker::RecordLevelError& l,
           LogErrorTracker::RecordLevelError r) {
  return l = l | r;
}

LogErrorTracker::RecordLevelError
operator&(LogErrorTracker::RecordLevelError l,
          LogErrorTracker::RecordLevelError r) {
  return static_cast<LogErrorTracker::RecordLevelError>(
      static_cast<uint32_t>(l) & static_cast<uint32_t>(r));
}

LogErrorTracker::RecordLevelError
operator~(LogErrorTracker::RecordLevelError l) {
  return static_cast<LogErrorTracker::RecordLevelError>(
      ~static_cast<uint32_t>(l));
}

LogErrorTracker::RecordLevelError
operator^(LogErrorTracker::RecordLevelError l,
          LogErrorTracker::RecordLevelError r) {
  return static_cast<LogErrorTracker::RecordLevelError>(
      static_cast<uint32_t>(l) ^ static_cast<uint32_t>(r));
}

LogErrorTracker::RecordLevelError&
operator++(LogErrorTracker::RecordLevelError& l) {
  uint32_t res = static_cast<uint32_t>(l);
  if (res) {
    res <<= 1;
  } else {
    res = 1;
  }
  l = static_cast<LogErrorTracker::RecordLevelError>(res);
  return l;
}

LogErrorTracker::RecordLevelError&
operator--(LogErrorTracker::RecordLevelError& l) {
  uint32_t res = static_cast<uint32_t>(l);
  res >>= 1;
  l = static_cast<LogErrorTracker::RecordLevelError>(res);
  return l;
}

bool operator<(LogErrorTracker::LogLevelError l,
               LogErrorTracker::LogLevelError r) {
  return static_cast<uint32_t>(l) < static_cast<uint32_t>(r);
}

LogErrorTracker::LogLevelError operator|(LogErrorTracker::LogLevelError l,
                                         LogErrorTracker::LogLevelError r) {
  return static_cast<LogErrorTracker::LogLevelError>(static_cast<uint32_t>(l) |
                                                     static_cast<uint32_t>(r));
}

LogErrorTracker::LogLevelError operator&(LogErrorTracker::LogLevelError l,
                                         LogErrorTracker::LogLevelError r) {
  return static_cast<LogErrorTracker::LogLevelError>(static_cast<uint32_t>(l) &
                                                     static_cast<uint32_t>(r));
}

LogErrorTracker::LogLevelError operator~(LogErrorTracker::LogLevelError l) {
  return static_cast<LogErrorTracker::LogLevelError>(~static_cast<uint32_t>(l));
}

LogErrorTracker::LogLevelError operator^(LogErrorTracker::LogLevelError l,
                                         LogErrorTracker::LogLevelError r) {
  return static_cast<LogErrorTracker::LogLevelError>(static_cast<uint32_t>(l) ^
                                                     static_cast<uint32_t>(r));
}

LogErrorTracker::LogLevelError& operator++(LogErrorTracker::LogLevelError& l) {
  uint32_t res = static_cast<uint32_t>(l);
  if (res) {
    res <<= 1;
  } else {
    res = 1;
  }
  l = static_cast<LogErrorTracker::LogLevelError>(res);
  return l;
}

LogErrorTracker::LogLevelError& operator--(LogErrorTracker::LogLevelError& l) {
  uint32_t res = static_cast<uint32_t>(l);
  res >>= 1;
  l = static_cast<LogErrorTracker::LogLevelError>(res);
  return l;
}

}} // namespace facebook::logdevice
