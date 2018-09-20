/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <array>
#include <string>

#include "logdevice/server/RotatingFile.h"

namespace facebook { namespace logdevice {

/**
 * @file
 *
 * This class provides functionality for loging to a local file.
 * Writes are atomic appends to open file. Multiple writers can write without
 * blocking each other.
 *
 * Reopen funtionality provides ability to reopen a file without blocking
 * writers. Reopen process is seamless to writers.
 */

class LocalLogFile {
 public:
  /**
   * Opens a file with O_APPEND | O_CREAT | O_WRONLY. Result is propagated from
   * underlying call to ::open(const char* path, int flags) and error handling
   * should be done by caller.
   */
  int open(const std::string& path);

  /**
   * Requests file close and exits immediately. File descriptor will be closed
   * when all writes in progress are completed. If close results in error it
   * will only be visible in log.
   */
  void close();

  /**
   * Reopens the file with same parameters as last open call and exits
   * immediately. All writes in progress on the old file descriptor will finish
   * before closing it.
   *
   */
  void reopen();

  /**
   * Appends log entry atomicaly to open file.
   * Log users should implement serialization function
   * size_t to_log_entry(const T& data, const char* buf, size_t buf_size)
   *
   */
  template <typename T>
  size_t write(const T& data);

 private:
  RotatingFile file_;
};

template <typename T>
size_t LocalLogFile::write(const T& data) {
  constexpr int BUFSIZE = 2048;
  std::array<char, BUFSIZE> buf;
  size_t size = to_log_entry(data, &buf.front(), buf.size());
  return file_.write(&buf.front(), size);
}
}} // namespace facebook::logdevice
