/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/common/PThread.h"

#include <folly/CppAttributes.h>

#include "logdevice/common/toString.h"

namespace facebook { namespace logdevice {

static void* FOLLY_NULLABLE thread_func(void* arg) {
  auto f = reinterpret_cast<std::function<void()>*>(arg);
  (*f)();
  delete f;
  return nullptr;
}

PThread::PThread() = default;

PThread::PThread(std::function<void()> f) {
  t_.emplace();
  auto f_on_heap = new std::function<void()>(std::move(f));
  int rv = pthread_create(&t_.value(),
                          /* attr */ nullptr,
                          &thread_func,
                          reinterpret_cast<void*>(f_on_heap));
  ld_check_eq(rv, 0);
}

PThread::PThread(PThread&& rhs) noexcept : t_(std::move(rhs.t_)) {}

PThread& PThread::operator=(PThread&& rhs) {
  ld_check(!t_.hasValue());
  t_ = std::move(rhs.t_);
  return *this;
}

PThread::~PThread() {
  ld_check(!t_.hasValue());
}

bool PThread::joinable() const {
  return t_.hasValue();
}

void PThread::join() {
  ld_check(t_.hasValue());

  int rv = pthread_join(t_.value(), /* retval */ nullptr);
  ld_check_eq(rv, 0);

  t_.reset();
}

bool PThread::isCurrentThread() const {
  ld_check(t_.hasValue());
  return pthread_equal(pthread_self(), t_.value());
}

}} // namespace facebook::logdevice
