/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <atomic>
#include <condition_variable>
#include <mutex>

namespace facebook { namespace logdevice {

/**
 * @file SingleEvent class is a replacement for std::condition_variable for
 * cases when you don't really need a mutex, and the event only happens once.
 * The intended use is shutting down background threads.
 */

class SingleEvent {
 public:
  // Wakes up all current and future waiting threads. After this all calls to
  // wait() and waitFor() will always return immediately.
  void signal() {
    {
      std::unique_lock<std::mutex> lock(mutex_);
      signaled_.store(true);
    }
    cv_.notify_all();
  }

  bool signaled() const {
    return signaled_.load();
  }

  void wait() {
    std::unique_lock<std::mutex> lock(mutex_);
    cv_.wait(lock, [this] { return signaled_.load(); });
  }

  // @return false if timeout expired, true if a call to notifyOne() woke us.
  template <class Rep, class Period>
  bool waitFor(const std::chrono::duration<Rep, Period>& rel_time) {
    std::unique_lock<std::mutex> lock(mutex_);
    return cv_.wait_for(lock, rel_time, [this] { return signaled_.load(); });
  }

 private:
  std::mutex mutex_;
  std::condition_variable cv_;
  std::atomic<bool> signaled_{false};
};

}} // namespace facebook::logdevice
