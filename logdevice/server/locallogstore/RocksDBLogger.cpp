/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/server/locallogstore/RocksDBLogger.h"

namespace facebook { namespace logdevice {

using dbg::Level;
using rocksdb::InfoLogLevel;

InfoLogLevel RocksDBLogger::ldToRocksLevel(Level ld_level) {
  switch (ld_level) {
    case Level::SPEW:
    case Level::DEBUG:
      return InfoLogLevel::DEBUG_LEVEL;
    case Level::INFO:
      return InfoLogLevel::INFO_LEVEL;
    case Level::NOTIFY:
    case Level::WARNING:
      return InfoLogLevel::WARN_LEVEL;
    case Level::ERROR:
      return InfoLogLevel::ERROR_LEVEL;
    case Level::CRITICAL:
      return InfoLogLevel::FATAL_LEVEL;
    case Level::NONE:
      ld_error("Given Level::NONE - setting RocksDB::Logger level to "
               "DEBUG_LEVEL");
      ld_check(false);
      return InfoLogLevel::DEBUG_LEVEL;
      // Let compiler check that all enum values are handled.
  }
  ld_check(false);
  return InfoLogLevel::DEBUG_LEVEL;
}

void RocksDBLogger::Logv(const char* format, va_list ap) {
  Logv(InfoLogLevel::INFO_LEVEL, format, ap);
}

void RocksDBLogger::Logv(const InfoLogLevel log_level,
                         const char* format,
                         va_list ap_base) {
  // Map log level to closest equivalent
  Level ld_log_level = Level::NONE;
  switch (log_level) {
    case InfoLogLevel::DEBUG_LEVEL:
      ld_log_level = Level::DEBUG;
      break;
    case InfoLogLevel::INFO_LEVEL:
      ld_log_level = Level::INFO;
      break;
    case InfoLogLevel::WARN_LEVEL:
      ld_log_level = Level::WARNING;
      break;
    case InfoLogLevel::ERROR_LEVEL:
      ld_log_level = Level::ERROR;
      break;
    case InfoLogLevel::FATAL_LEVEL:
      ld_log_level = Level::CRITICAL;
      break;
    case InfoLogLevel::HEADER_LEVEL:
      ld_log_level = Level::INFO;
      break;
    case InfoLogLevel::NUM_INFO_LOG_LEVELS:
      ld_log_level = Level::NONE;
      ld_error("Called with InfoLogLevel::NUM_INFO_LOG_LEVELS; logging with "
               "Level::NONE");
      ld_check(false);
      // Let compiler check that all enum values are handled.
  }

  // Place args in format to create the final log message
  std::array<char, 2048> buf;
  va_list ap;
  va_copy(ap, ap_base);
  int rv = vsnprintf(buf.data(), buf.size(), format, ap);
  if (rv < 0) {
    snprintf(buf.data(), buf.size(), "Invalid printf() format");
  }
  va_end(ap);

  ld_log(ld_log_level, buf.data(), ap);
}

}} // namespace facebook::logdevice
