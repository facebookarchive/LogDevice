/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/common/ThreadID.h"

#include "logdevice/common/debug.h"

namespace facebook { namespace logdevice {

void ThreadID::set(Type type, folly::StringPiece name) {
  ld_check(type != Type::UNKNOWN);
  type_ = type;

  // strcpy() has its problems, but its the best tool for this job.
  // %flint: pause
  strcpy(name_.data(), name.str().substr(0, 15).c_str());
  // %flint: resume

  // We wait until name_ is set before we log the error, so the correct thread
  // name appears at the start of the log line.
  if (name.size() > 15) {
    ld_error("Truncating thread name \"%s\"!", name.str().c_str());
  }

  if (!name.empty()) {
    int rv = pthread_setname_np(pthread_self(), name_.data());
    ld_check(rv == 0);
  }
}

__thread ThreadID::Type ThreadID::type_ = ThreadID::Type::UNKNOWN;
__thread std::array<char, 16> ThreadID::name_;
}} // namespace facebook::logdevice
