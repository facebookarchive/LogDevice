/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <rocksdb/db.h>

#include "logdevice/common/debug.h"

namespace facebook { namespace logdevice {

/**
 * @file RocksDBLogger is an implementation of rocksdb::Logger that writes to
 *       the LogDevice log instead of a separate log file. This is useful for
 *       certain elements of RocksDB that can be shared across multiple shards
 *       but are not noisy enough that it would make sense to have a separate
 *       log file for them, such as SstFileManager for ratelimiting deletes.
 */

class RocksDBLogger : public rocksdb::Logger {
 public:
  explicit RocksDBLogger(rocksdb::InfoLogLevel level)
      : rocksdb::Logger(level) {}

  explicit RocksDBLogger(dbg::Level level)
      : rocksdb::Logger(ldToRocksLevel(level)) {}

  // Write an entry to the log file with the specified format.
  void Logv(const char* format, va_list ap) override;

  // Write an entry to the log file with the specified log level
  // and format.  Any log with level under the internal log level
  // of *this (see @SetInfoLogLevel and @GetInfoLogLevel) will not be
  // printed.
  void Logv(const rocksdb::InfoLogLevel log_level,
            const char* format,
            va_list ap) override;

 private:
  static rocksdb::InfoLogLevel ldToRocksLevel(dbg::Level ld_level);
};

}} // namespace facebook::logdevice
