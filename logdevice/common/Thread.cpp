/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/common/Thread.h"

#include <errno.h>

#include "logdevice/common/ThreadID.h"
#include "logdevice/common/debug.h"
#include "logdevice/include/Err.h"

namespace facebook { namespace logdevice {

int Thread::start() {
  int rv;
  pthread_attr_t attr;

  rv = pthread_attr_init(&attr);
  if (rv != 0) { // unlikely
    ld_check(rv == ENOMEM);
    ld_error("Failed to initialize a pthread attributes struct");
    err = E::NOMEM;
    return -1;
  }

  rv = pthread_attr_setstacksize(&attr, Thread::STACK_SIZE);
  if (rv != 0) {
    ld_check(rv == EINVAL);
    ld_error("Failed to set stack size for a Thread thread. "
             "Stack size %lu is out of range",
             Thread::STACK_SIZE);
    err = E::SYSLIMIT;
    return -1;
  }

  rv = pthread_create(&thread_, &attr, Thread::enter, this);
  if (rv != 0) {
    ld_error(
        "Failed to start a Thread thread, errno=%d (%s)", rv, strerror(rv));
    err = E::SYSLIMIT;
    return -1;
  }

  return 0;
}

void* FOLLY_NULLABLE Thread::enter(void* self) {
  Thread* t = reinterpret_cast<Thread*>(self);
  ThreadID::set(ThreadID::Type::STORAGE, t->threadName());
  t->run();
  return nullptr;
}
}} // namespace facebook::logdevice
