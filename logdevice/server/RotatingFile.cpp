/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/server/RotatingFile.h"

#include <algorithm>

namespace facebook { namespace logdevice {

void RotatingFile::close() {
  std::lock_guard<std::mutex> _(mutex_);
  close_();
}

int RotatingFile::open(const char* path, int flags, mode_t mode) {
  std::lock_guard<std::mutex> _(mutex_);
  close_();
  path_.assign(path);
  flags_ = flags;
  mode_ = mode;
  int descriptor = open_(path, flags, mode);
  if (descriptor != FileDescriptor::INVALID_DESCRIPTOR) {
    descriptor_.update(std::make_shared<FileDescriptor>(descriptor));
  }
  return descriptor;
}

int RotatingFile::reopen() {
  std::lock_guard<std::mutex> _(mutex_);

  ld_check(!path_.empty());

  int descriptor = open_(path_.c_str(), flags_, mode_);
  if (descriptor != FileDescriptor::INVALID_DESCRIPTOR) {
    descriptor_.update(std::make_shared<FileDescriptor>(descriptor));
  }
  return descriptor;
}

ssize_t RotatingFile::write(const void* buf, size_t count) {
  auto descriptor = descriptor_.get();
  if (!descriptor) {
    errno = EBADF;
    return -1;
  }
  return write_(descriptor->getDescriptor(), buf, count);
}

void RotatingFile::close_() {
  descriptor_.update(nullptr);
  path_.clear();
}
int RotatingFile::open_(const char* path, int flags, mode_t mode) {
  return ::open(path, flags, mode);
}

ssize_t RotatingFile::write_(int fd, const void* buf, size_t count) {
  return ::write(fd, buf, count);
}

}} // namespace facebook::logdevice
