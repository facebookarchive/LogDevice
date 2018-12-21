/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <cerrno>
#include <chrono>
#include <memory>
#include <semaphore.h>

#include "logdevice/common/checks.h"
#include "logdevice/common/chrono_util.h"
#include "logdevice/include/Err.h"

namespace facebook { namespace logdevice {

/**
 * @file Wrapper around POSIX semaphores.  Features:
 *
 * (1) RAII semantics.
 * (2) Waiting methods swallow EINTR.
 * (3) Internally dynamically allocates the raw sem_t to work around a glibc
 * bug.  A common pattern where one thread does wait+destroy while another
 * posts is not safe because the post can unblock the waiter+destroyer and
 * then try to access sem_t data again.  Details:
 * https://sourceware.org/bugzilla/show_bug.cgi?id=12674
 */

namespace detail {
// Internal wrapper that we can use with an std::shared_ptr
struct RawSemaphore {
  explicit RawSemaphore(unsigned initial_value) {
    sem_init(&sem_, 0, initial_value);
  }
  ~RawSemaphore() {
    sem_destroy(&sem_);
  }
  sem_t sem_;
};
} // namespace detail

class Semaphore {
 public:
  explicit Semaphore(unsigned int initial_value = 0)
      : sem_(std::make_shared<detail::RawSemaphore>(initial_value)) {}

  void wait() {
    int rv;

    do {
      rv = sem_wait(rawsem());
      ld_check(rv == 0 || errno == EINTR);
    } while (rv != 0);
  }

  bool try_wait() {
    int rv;

    do {
      rv = sem_trywait(rawsem());
      ld_check(rv == 0 || errno == EINTR || errno == EAGAIN);
    } while (rv != 0 && errno == EINTR);
    return rv == 0;
  }

  /**
   * @return 0 on success, -1 with E::TIMEDOUT if semaphore could not
   *         be decremented before @param deadline.
   */
  int timedwait(std::chrono::system_clock::time_point deadline,
                bool ignore_eintr = true) {
    int rv;
    std::chrono::milliseconds deadline_ms{
        std::chrono::duration_cast<std::chrono::milliseconds>(
            deadline.time_since_epoch())};
    struct timespec abs_timeout {
      deadline_ms.count() / 1000, deadline_ms.count() % 1000 * 1000000
    };
    for (;;) {
      rv = sem_timedwait(rawsem(), &abs_timeout);
      if (rv == 0) {
        return 0;
      }

      switch (errno) {
        case ETIMEDOUT:
          err = E::TIMEDOUT;
          break;
        case EINTR:
          if (ignore_eintr) {
            continue;
          }
          break;
        default:
          ld_check(false);
          err = E::INTERNAL;
      }
      return -1;
    }
  }

  int timedwait(std::chrono::milliseconds timeout, bool ignore_eintr = true) {
    return timedwait(
        truncated_add(std::chrono::system_clock::now(), timeout), ignore_eintr);
  }

  void post() {
    // This is the critical part of working around glibc #12674.  post() pins
    // the sem_t to ensure it continues to exist until the post completes.
    std::shared_ptr<detail::RawSemaphore> pin(sem_);
    sem_post(rawsem());
    // NOTE: `this' is no longer safe to use here because waiter may have
    // destroyed it
  }

  int value() {
    int val;
    int rv = sem_getvalue(rawsem(), &val);
    ld_check(rv == 0);
    return val;
  }

  sem_t* rawsem() const {
    return &(sem_->sem_);
  }

  // Not copyable but movable
  Semaphore(const Semaphore& other) = delete;
  Semaphore& operator=(const Semaphore& other) = delete;
  Semaphore(Semaphore&& other) = default;
  Semaphore& operator=(Semaphore&& other) = default;

 private:
  std::shared_ptr<detail::RawSemaphore> sem_;
};

}} // namespace facebook::logdevice
