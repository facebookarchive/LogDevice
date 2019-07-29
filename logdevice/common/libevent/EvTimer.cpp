/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "logdevice/common/libevent/EvTimer.h"

#include <event2/event.h>
#include <folly/CppAttributes.h>

#include "logdevice/common/libevent/EvBase.h"
#include "logdevice/common/libevent/compat.h"

namespace facebook { namespace logdevice {

const timeval* FOLLY_NULLABLE
EvTimer::getCommonTimeout(std::chrono::microseconds timeout) {
  auto base = EvBase::getRunningBase();
  if (!base) {
    return nullptr;
  }
  struct timeval tv_buf;
  tv_buf.tv_sec = timeout.count() / 1000000;
  tv_buf.tv_usec = timeout.count() % 1000000;
  return LD_EV(event_base_init_common_timeout)(base->getRawBase(), &tv_buf);
}
}} // namespace facebook::logdevice
