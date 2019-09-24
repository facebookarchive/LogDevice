/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "logdevice/common/libevent/EvTimerLegacy.h"

#include <event2/event.h>
#include <folly/CppAttributes.h>

#include "logdevice/common/debug.h"
#include "logdevice/common/libevent/EvBaseLegacy.h"
#include "logdevice/common/libevent/compat.h"

namespace facebook { namespace logdevice {

EvTimerLegacy::EvTimerLegacy(IEvBase* base) : timeout_manager_(base) {
  if (!base) {
    return;
  }
  event_ =
      LD_EV(event_new)(base->getRawBaseDEPRECATED(), -1, 0, nullptr, nullptr);
  evtimer_assign(getEvent(),
                 base->getRawBaseDEPRECATED(),
                 EvTimerLegacy::libeventCallback,
                 this);
}

const timeval* FOLLY_NULLABLE
EvTimerLegacy::getCommonTimeout(std::chrono::microseconds timeout) {
  auto base = EvBaseLegacy::getRunningBase();
  if (!base) {
    return nullptr;
  }
  struct timeval tv_buf;
  tv_buf.tv_sec = timeout.count() / 1000000;
  tv_buf.tv_usec = timeout.count() % 1000000;
  return LD_EV(event_base_init_common_timeout)(
      base->getRawBaseDEPRECATED(), &tv_buf);
}

void EvTimerLegacy::attachTimeoutManager(
    folly::TimeoutManager* timeoutManager) {
  timeout_manager_ = dynamic_cast<EvBaseLegacy*>(timeoutManager);
}

bool EvTimerLegacy::scheduleTimeout(uint32_t milliseconds) {
  ld_check(callback_ != nullptr);

  ld_check(timeout_manager_->isInTimeoutManagerThread());
  // Set up the timeval and add the event
  struct timeval tv;
  tv.tv_sec = static_cast<long>(milliseconds / 1000LL);
  tv.tv_usec = static_cast<long>((milliseconds % 1000LL) * 1000LL);

  struct event* ev = getEvent();
  if (LD_EV(event_add)(ev, &tv) < 0) {
    return false;
  }
  return true;
}

bool EvTimerLegacy::scheduleTimeout(
    folly::TimeoutManager::timeout_type timeout) {
  return scheduleTimeout(timeout.count());
}

void EvTimerLegacy::cancelTimeout() {
  // AsyncTimeout does not allow to cancel timeout on incorrect evbase thread.
  // We will have to fix this once we move to asynctimeout.
  // ld_check(timeout_manager_->isInTimeoutManagerThread());
  struct event* ev = getEvent();
  evtimer_del(ev);
}

bool EvTimerLegacy::isScheduled() const {
  struct event* ev = getEvent();
  return evtimer_pending(ev, nullptr);
}

void EvTimerLegacy::libeventCallback(int, short, void* arg) {
  auto timer = static_cast<EvTimerLegacy*>(arg);
  timer->callback_();
}
}} // namespace facebook::logdevice
