/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <functional>
#include <pthread.h>

#include <folly/Optional.h>

#include "logdevice/common/checks.h"

namespace facebook { namespace logdevice {

// Similar to std::thread, but doesn't have the issue of discarding stack trace
// of uncaught exceptions: https://gcc.gnu.org/bugzilla/show_bug.cgi?id=55917
//
// After libstdc++ fixes it (gcc 8 probably), feel free to delete this class and
// go back to std::thread.

class PThread {
 public:
  PThread();
  explicit PThread(std::function<void()> f);
  PThread(PThread&& rhs) noexcept;
  PThread& operator=(PThread&& rhs);

  ~PThread();

  bool joinable() const;
  void join();

  // Returns true if called from the thread this PThread represents.
  bool isCurrentThread() const;

 private:
  folly::Optional<pthread_t> t_;
};

}} // namespace facebook::logdevice
