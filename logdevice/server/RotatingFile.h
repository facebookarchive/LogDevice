/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <fcntl.h>
#include <mutex>
#include <string>

#include <boost/utility.hpp>
#include <sys/stat.h>
#include <sys/types.h>

#include "logdevice/common/UpdateableSharedPtr.h"
#include "logdevice/common/debug.h"

namespace facebook { namespace logdevice {

/**
 * @file
 *
 * This class provides a wraper around file descriptor supporting reopen without
 * blocking other operations in progress.
 *
 * Currently only write operation is supported but can easily be extended to
 * read
 *
 */

class RotatingFile : private boost::noncopyable {
 public:
  virtual ~RotatingFile() {}

  /**
   * Requests file close and exits immediately. File descriptor will be closed
   * when all operations in progress are completed. If close results in error it
   * will only be visible in the log.
   */
  void close();

  /**
   * Opens a file. Result is propagated from underlying call to
   * ::open(const char* path, int flags, mode_t mode)
   * Error handling should be done by caller.
   */
  int open(const char* path, int flags, mode_t mode);

  /**
   * Reopens the file with same parameters as last open call and exits
   * immediately. All operations in progress on the old file descriptor will
   * finish before closing it.
   *
   */
  int reopen();
  ssize_t write(const void* buf, size_t count);

 protected:
  /**
   * Wrapers around system calls to make mocking of sys calls available.
   */
  virtual int open_(const char* path, int flags, mode_t mode);
  virtual ssize_t write_(int fd, const void* buf, size_t count);

 private:
  void close_();

  /**
   * Wraper around file descriptor which closes on destruct. Parent class holds
   * UpdateableSharedPtr to this making reopen seamless to other operations.
   * After update when operations close destructor will be called on this class
   * closing the underlying file descriptor.
   */

  class FileDescriptor : private boost::noncopyable {
   public:
    static const int INVALID_DESCRIPTOR{-1};
    explicit FileDescriptor() = default;
    explicit FileDescriptor(int descriptor) : descriptor_(descriptor) {}
    int getDescriptor() const {
      return descriptor_;
    }
    int close() {
      int res = 0;
      if (descriptor_ != INVALID_DESCRIPTOR) {
        res = ::close(descriptor_);
        descriptor_ = INVALID_DESCRIPTOR;
      }
      return res;
    }
    ~FileDescriptor() {
      if (::close(descriptor_) == -1) {
        ld_error("Could not close file descriptor: %s", strerror(errno));
      }
    }

   private:
    int descriptor_{INVALID_DESCRIPTOR};
  };

  UpdateableSharedPtr<FileDescriptor> descriptor_;

  /**
   * Needed for reopen operations which opens with same params as last call to
   * open
   */
  std::string path_;
  int flags_;
  int mode_;

  // Mutex for locking open/close/rotate operations
  std::mutex mutex_;
};
}} // namespace facebook::logdevice
