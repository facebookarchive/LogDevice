/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <cstdint>
#include <string>

namespace facebook { namespace logdevice {

class LogErrorTracker {
 public:
  enum class RecordLevelError : uint32_t {
    NONE = 0,
    // These must be consecutive powers of two.
    DATALOSS = 1u << 0,
    DIFFERENT_COPYSET_IN_LATEST_WAVE = 1u << 1,
    COPYSET_WITHOUT_SELF = 1u << 2,
    COPYSET_NOT_IN_NODESET = 1u << 3,
    SMALL_COPYSET = 1u << 4,
    NO_ACCURATE_COPYSET = 1u << 5,
    DATA_MISMATCH = 1u << 6,
    BAD_REPLICATION_LAST_WAVE = 1u << 7,
    BAD_REPLICATION = 1u << 8,
    HOLE_RECORD_CONFLICT = 1u << 9,
    BRIDGE_RECORD_CONFLICT = 1u << 10,
    DUPLICATES_IN_COPYSET = 1u << 11,
    MAX = 1u << 12,
    ALL = ~0u
  };
  enum class LogLevelError : uint32_t {
    NONE = 0,
    // These must be consecutive powers of two.
    EMPTY_METADATA_LOG = 1u << 0,
    MAX = 1u << 1,
    ALL = ~0u
  };

  /**
   * Get a string containing names of all passed error flags
   *
   * @param  errors Error flags
   * @return        String containing names of all passed error flags
   */
  static std::string recordLevelErrorsToString(RecordLevelError errors);

  /**
   * Get a string containing names of all passed error flags
   *
   * @param  errors Error flags
   * @return        String containing names of all passed error flags
   */
  static std::string logLevelErrorsToString(LogLevelError errors);

  /**
   * Get a string containing description of error
   *
   * If multiple error flags are passed it will assert in debug and in opt build
   * it will return a description of most significant bit error
   *
   * @param  error Error to describe
   * @return       String containing description of passed error
   */
  static const std::string& describeRecordError(RecordLevelError error);

  /**
   * Get a string containing description of error
   *
   * If multiple error flags are passed it will assert in debug and in opt build
   * it will return a description of most significant bit error
   *
   * @param  error Error to describe
   * @return       String containing description of passed error
   */
  static const std::string& describeLogError(LogLevelError error);

  /**
   * Parse log level error flags from string.
   *
   * Multiple error flags should be comma separated.
   *
   * @param  errors String containing error flags
   * @return        Enum with all the parsed flags or LogLevelError::MAX on
                    parse issues
   */
  static LogLevelError parseLogLevelErrors(std::string errors);

  /**
   * Parse record level error flags from string.
   *
   * Multiple error flags should be comma separated.
   *
   * @param  errors String containing error flags
   * @return        Enum with all the parsed flags or RecordLevelError::MAX on
                    parse issues
   */
  static RecordLevelError parseRecordLevelErrors(std::string errors);

  RecordLevelError getRecordLevelFilter() const;
  LogLevelError getLogLevelFilter() const;

 private:
  RecordLevelError record_level_filter_{RecordLevelError::ALL};
  LogLevelError log_level_filter_{LogLevelError::ALL};
};

bool operator<(LogErrorTracker::RecordLevelError,
               LogErrorTracker::RecordLevelError);

LogErrorTracker::RecordLevelError operator|(LogErrorTracker::RecordLevelError,
                                            LogErrorTracker::RecordLevelError);

LogErrorTracker::RecordLevelError&
operator|=(LogErrorTracker::RecordLevelError&,
           LogErrorTracker::RecordLevelError);

LogErrorTracker::RecordLevelError operator&(LogErrorTracker::RecordLevelError,
                                            LogErrorTracker::RecordLevelError);

LogErrorTracker::RecordLevelError operator~(LogErrorTracker::RecordLevelError);

LogErrorTracker::RecordLevelError operator^(LogErrorTracker::RecordLevelError,
                                            LogErrorTracker::RecordLevelError);

LogErrorTracker::RecordLevelError&
operator++(LogErrorTracker::RecordLevelError&);

LogErrorTracker::RecordLevelError&
operator--(LogErrorTracker::RecordLevelError&);

bool operator<(LogErrorTracker::LogLevelError, LogErrorTracker::LogLevelError);

LogErrorTracker::LogLevelError operator|(LogErrorTracker::LogLevelError,
                                         LogErrorTracker::LogLevelError);

LogErrorTracker::LogLevelError operator&(LogErrorTracker::LogLevelError,
                                         LogErrorTracker::LogLevelError);

LogErrorTracker::LogLevelError operator~(LogErrorTracker::LogLevelError);

LogErrorTracker::LogLevelError operator^(LogErrorTracker::LogLevelError,
                                         LogErrorTracker::LogLevelError);

LogErrorTracker::LogLevelError& operator++(LogErrorTracker::LogLevelError&);

LogErrorTracker::LogLevelError& operator--(LogErrorTracker::LogLevelError&);

}} // namespace facebook::logdevice
