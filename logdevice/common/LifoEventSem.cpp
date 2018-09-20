/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "LifoEventSem.h"

#include <sys/eventfd.h>

#include <folly/Exception.h>
#include <folly/portability/Sockets.h>
#include <folly/portability/Unistd.h>

namespace facebook { namespace logdevice { namespace detail {

static int openNonblockingEventFd() {
  int fd = ::eventfd(0, EFD_NONBLOCK | EFD_SEMAPHORE);
  folly::checkUnixError(fd, "eventfd");
  return fd;
}

EventFdBatonBase::EventFdBatonBase() : fd(openNonblockingEventFd()) {}

EventFdBatonBase::~EventFdBatonBase() noexcept {
  ::close(fd);
}

void EventFdBatonBase::post() {
  const uint64_t val = 1;
  ssize_t n;
  while ((n = ::write(fd, &val, sizeof(val))) == -1) {
    // any errno besides EINTR violates the spec, and is therefore a
    // logic error
    ld_check(errno == EINTR);
  }
  // any return value except -1 or n violates the spec, and is therefore
  // a logic error
  ld_check(n == sizeof(val));
}

bool EventFdBatonBase::poll(int timeoutMillis) {
  while (true) {
    // wait using poll, since it has less setup for a one-off use
    struct pollfd poll_info;
    poll_info.fd = fd;
    poll_info.events = POLLIN;
    auto rv = ::poll(&poll_info, 1, timeoutMillis);

    if (rv == 1) {
      // fd is readable
      ld_check(poll_info.revents != 0);
      return true;
    } else if (rv == 0) {
      // timed out
      ld_check(timeoutMillis != -1);
      return false;
    }
    // else retry

    // errno can either be EINTR or ENOMEM
    int e = errno;
    if (e != EINTR && e != ENOMEM) {
      // this shouldn't happen by my reading of the manpage
      ld_check(false);
      return false;
    }

    if (e == ENOMEM) {
      // this is very unlikely, but we try to make forward progress
      // regardless
      usleep(100);
    }
  }
}

bool EventFdBatonBase::consume() {
  uint64_t val;
  ssize_t n;
  while ((n = ::read(fd, &val, sizeof(val))) == -1) {
    if (errno == EAGAIN) {
      // eventfd has value zero
      return false;
    }
    // any error except EINTR or EAGAIN is a logic error
    ld_check(errno == EINTR);
  }
  // any read except uint64_t(1) is a logic error when using EFD_SEMAPHORE
  ld_check(n == sizeof(val));
  ld_check(val == 1);
  return true;
}

void EventFdBatonBase::wait() {
  if (!consume()) {
    poll();
    auto f = consume();
    ld_check(f);
    (void)f;
  }
}
}}} // namespace facebook::logdevice::detail
