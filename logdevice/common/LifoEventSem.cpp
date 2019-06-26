/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/common/LifoEventSem.h"

#include <folly/Exception.h>
#include <folly/FileUtil.h>
#include <folly/portability/Sockets.h>
#include <folly/portability/Unistd.h>

#if __linux__ && !__ANDROID__
#define FOLLY_HAVE_EVENTFD
#include <folly/io/async/EventFDWrapper.h>
#endif

namespace facebook { namespace logdevice { namespace detail {

SynchronizationFd::SynchronizationFd() {
  pid_ = pid_t(getpid());
#ifdef FOLLY_HAVE_EVENTFD
  fds_[FdType::Read] = fds_[FdType::Write] =
      eventfd(0, EFD_CLOEXEC | EFD_NONBLOCK | EFD_SEMAPHORE);
  if (fds_[FdType::Read] != -1) {
    return;
  }

  if (!(errno == ENOSYS || errno == EINVAL)) {
    folly::throwSystemError(
        "Failed to create eventfd in SynchronizationFd", errno);
  }
#endif
  if (pipe(fds_)) {
    folly::throwSystemError(
        "Failed to create pipe in SynchronizationFd", errno);
  }
  try {
    // put both ends of the pipe into non-blocking mode
    if (fcntl(fds_[FdType::Read], F_SETFL, O_RDONLY | O_NONBLOCK) != 0) {
      folly::throwSystemError("Failed to put SynchronizationFd pipe read "
                              "endpoint into non-blocking mode",
                              errno);
    }
    if (fcntl(fds_[FdType::Write], F_SETFL, O_WRONLY | O_NONBLOCK) != 0) {
      folly::throwSystemError("Failed to put SynchronizationFd pipe write "
                              "endpoint into non-blocking mode",
                              errno);
    }
  } catch (...) {
    ::close(fds_[FdType::Read]);
    ::close(fds_[FdType::Write]);
    throw;
  }
}
SynchronizationFd::~SynchronizationFd() {
  ::close(fds_[FdType::Write]);
  if (fds_[FdType::Read] != fds_[FdType::Write]) {
    ::close(fds_[FdType::Read]);
  }
}

void SynchronizationFd::write() {
  check_pid();
  ssize_t bytes_written = 0;
  size_t bytes_expected = 0;

  do {
    if (fds_[FdType::Read] == fds_[FdType::Write]) {
      // eventfd(2) dictates that we must write a 64-bit integer
      uint64_t signal = 1;
      bytes_expected = sizeof(signal);
      bytes_written = ::write(fds_[FdType::Write], &signal, bytes_expected);
    } else {
      uint8_t signal = 1;
      bytes_expected = sizeof(signal);
      bytes_written = ::write(fds_[FdType::Write], &signal, bytes_expected);
    }
  } while (bytes_written == -1 && errno == EINTR);

  if (bytes_written != ssize_t(bytes_expected)) {
    folly::throwSystemError("Failed to write to SynchronizationFd", errno);
  }
}

bool SynchronizationFd::poll(int timeoutMillis) noexcept {
  check_pid();
  while (true) {
    // wait using poll, since it has less setup for a one-off use
    struct pollfd poll_info;
    poll_info.fd = fds_[FdType::Read];
    poll_info.events = POLLIN;
    auto rv = ::poll(&poll_info, 1, timeoutMillis);

    if (rv == 1) {
      // fd is readable
      assert(poll_info.revents != 0);
      return true;
    } else if (rv == 0) {
      // timed out
      assert(timeoutMillis != -1);
      return false;
    }
    // else retry

    // errno can either be EINTR or ENOMEM
    int e = errno;
    if (e != EINTR && e != ENOMEM) {
      // this shouldn't happen by my reading of the manpage
      assert(false);
      return false;
    }

    if (e == ENOMEM) {
      // this is very unlikely, but we try to make forward progress
      // regardless
      usleep(100);
    }
  }
}

bool SynchronizationFd::read() noexcept {
  check_pid();
#ifdef FOLLY_HAVE_EVENTFD
  if (fds_[FdType::Read] == fds_[FdType::Write]) {
    uint64_t val;
    ssize_t n;
    while ((n = ::read(fds_[FdType::Read], &val, sizeof(val))) == -1) {
      if (errno == EAGAIN) {
        // eventfd has value zero
        return false;
      }
      // any error except EINTR or EAGAIN is a logic error
      assert(errno == EINTR);
    }
    // any read except uint64_t(1) is a logic error when using EFD_SEMAPHORE
    assert(n == sizeof(val));
    assert(val == 1);
    return true;
  }
#endif
  uint8_t message{0};
  ssize_t result =
      folly::readNoInt(fds_[FdType::Read], &message, sizeof(message));
  if (result != -1) {
    assert(message == 1);
  }
  return result != -1;
}

void SynchronizationFd::check_pid() {
  CHECK_EQ(pid_, pid_t(getpid()));
}
}}} // namespace facebook::logdevice::detail
