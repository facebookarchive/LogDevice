/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

// NOTE: This could change StorageThread to just be Thread<StorageThreadPool>.
// Let me know if you want me to make that change.

#include <atomic>
#include <pthread.h>
#include <string>

#include <folly/CppAttributes.h>

#include "logdevice/common/debug.h"

namespace facebook { namespace logdevice {

/**
 * @file A base class for different types of LogDevice threads.
 */

class Thread {
 public:
  /**
   * Does not do much, start() actually creates the thread.
   */
  Thread() {}

  virtual ~Thread() {}

  /**
   * Starts the thread.  (This is separate from the constructor so that it can
   * invoke the virtual run().)
   *
   * @return 0 on success, -1 on failure and sets err to
   *   NOMEM    if a pthread call failed because malloc() failed
   *   SYSLIMIT if system limits on thread stack sizes or total number of
   *            threads (if any) are reached
   */
  int start();

  /**
   * Gets the pthread handle of the thread this instance is running on.
   */
  pthread_t getThreadHandle() const {
    return thread_;
  }

  void join() {
    int rc = pthread_join(thread_, nullptr);
    if (rc != 0) {
      ld_error("pthread_join returned %d\n", rc);
    }
  }

 protected:
  // Main thread loop.
  virtual void run() = 0;

  // Subclasses must override to specify thread name
  virtual std::string threadName() = 0;

 private:
  // pthread handle
  pthread_t thread_;

  // Static wrapper of run(), passed to pthread_create()
  static void* FOLLY_NULLABLE enter(void* self);

  // stack size of this loop's thread (pthread defaults are low)
  static const size_t STACK_SIZE = (1024 * 1024);
};

}} // namespace facebook::logdevice
