/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/common/libevent/LibEventCompatibility.h"

#include "logdevice/common/debug.h"

namespace facebook { namespace logdevice {
// utility methods
namespace {
static EvBase::EvBaseType getType(IEvBase* base) {
  EvBase* specific_base = dynamic_cast<EvBase*>(base);
  EvBase::EvBaseType base_type = EvBase::UNKNOWN;
  if (specific_base) {
    base_type = specific_base->getType();
  } else if (dynamic_cast<EvBaseWithFolly*>(base)) {
    base_type = EvBase::FOLLY_EVENTBASE;
  } else if (dynamic_cast<EvBaseLegacy*>(base)) {
    base_type = EvBase::LEGACY_EVENTBASE;
  } else {
    ld_check(false);
  }
  return base_type;
}
} // namespace
// EvBase methods
void EvBase::selectEvBase(EvBaseType base_type) {
  ld_check(!curr_selection_);
  ld_check(base_type_ == UNKNOWN);
  ld_check_in(base_type, ({LEGACY_EVENTBASE, FOLLY_EVENTBASE, MOCK_EVENTBASE}));
  base_type_ = base_type;
  if (base_type_ == LEGACY_EVENTBASE) {
    curr_selection_ = &ev_base_legacy_;
  } else if (base_type == FOLLY_EVENTBASE) {
    curr_selection_ = &ev_base_with_folly_;
  }
}

EvBase::Status EvBase::init(int num_priorities) {
  return curr_selection_->init(num_priorities);
}

EvBase::Status EvBase::free() {
  return curr_selection_->free();
}

EvBase::Status EvBase::loop() {
  return curr_selection_->loop();
}

EvBase::Status EvBase::loopOnce() {
  return curr_selection_->loopOnce();
}

EvBase::Status EvBase::terminateLoop() {
  return curr_selection_->terminateLoop();
}

event_base* EvBase::getRawBaseDEPRECATED() {
  return curr_selection_->getRawBaseDEPRECATED();
}

folly::EventBase* EvBase::getEventBase() {
  return curr_selection_->getEventBase();
}

void EvBase::attachTimeoutManager(
    folly::AsyncTimeout* obj,
    folly::TimeoutManager::InternalEnum internal) {
  curr_selection_->attachTimeoutManager(obj, internal);
}

void EvBase::detachTimeoutManager(folly::AsyncTimeout* obj) {
  curr_selection_->detachTimeoutManager(obj);
}

bool EvBase::scheduleTimeout(folly::AsyncTimeout* obj,
                             folly::TimeoutManager::timeout_type timeout) {
  return curr_selection_->scheduleTimeout(obj, timeout);
}

void EvBase::cancelTimeout(folly::AsyncTimeout* obj) {
  curr_selection_->cancelTimeout(obj);
}

void EvBase::bumpHandlingTime() {
  curr_selection_->bumpHandlingTime();
}

bool EvBase::isInTimeoutManagerThread() {
  return running_base_ == curr_selection_;
}

// Event methods

Event::Event(IEvBase::EventCallback callback,
             IEvBase::Events events,
             int fd,
             IEvBase* base) {
  auto base_type = getType(base);
  switch (base_type) {
    case EvBase::LEGACY_EVENTBASE:
    case EvBase::MOCK_EVENTBASE:
      event_legacy_ =
          std::make_unique<EventLegacy>(std::move(callback), events, fd, base);
      break;
    case EvBase::FOLLY_EVENTBASE:
      event_folly_ = std::make_unique<EventWithFolly>(
          std::move(callback), events, fd, base);
      break;
    case EvBase::UNKNOWN:
      ld_check(false);
      break;
  }
}

// EvTimer methods

EvTimer::EvTimer(IEvBase* base) {
  auto base_type = getType(base);
  switch (base_type) {
    case EvBase::LEGACY_EVENTBASE:
      evtimer_legacy_ = std::make_unique<EvTimerLegacy>(base);
      break;
    case EvBase::FOLLY_EVENTBASE:
      evtimer_folly_ = std::make_unique<EvTimerWithFolly>(base);
      break;
    case EvBase::MOCK_EVENTBASE:
      is_mock_ = true;
      break;
    case EvBase::UNKNOWN:
      ld_check(false);
      break;
  }
}

void EvTimer::attachTimeoutManager(folly::TimeoutManager* tm) {
  if (is_mock_) {
    return;
  }
  if (evtimer_legacy_) {
    evtimer_legacy_->attachTimeoutManager(tm);
  } else if (evtimer_folly_) {
    evtimer_folly_->attachTimeoutManager(tm);
  } else {
    ld_check(false);
  }
}

void EvTimer::attachCallback(folly::Func callback) {
  if (is_mock_) {
    return;
  }
  if (evtimer_legacy_) {
    evtimer_legacy_->attachCallback(std::move(callback));
  } else if (evtimer_folly_) {
    evtimer_folly_->attachCallback(std::move(callback));
  } else {
    ld_check(false);
  }
}

void EvTimer::timeoutExpired() noexcept {
  if (is_mock_) {
    return;
  }
  if (evtimer_legacy_) {
    evtimer_legacy_->timeoutExpired();
  } else if (evtimer_folly_) {
    evtimer_folly_->timeoutExpired();
  } else {
    ld_check(false);
  }
}

struct event* FOLLY_NULLABLE EvTimer::getEvent() {
  if (is_mock_) {
    return nullptr;
  }
  if (evtimer_legacy_) {
    return evtimer_legacy_->getEvent();
  }
  if (evtimer_folly_) {
    return evtimer_folly_->getEvent();
  }
  ld_check(false);
  return nullptr;
}

bool EvTimer::scheduleTimeout(uint32_t ms) {
  if (is_mock_) {
    return true;
  }
  if (evtimer_legacy_) {
    return evtimer_legacy_->scheduleTimeout(ms);
  }
  if (evtimer_folly_) {
    return evtimer_folly_->scheduleTimeout(ms);
  }
  ld_check(false);
  return false;
}

bool EvTimer::scheduleTimeout(folly::TimeoutManager::timeout_type timeout) {
  if (is_mock_) {
    return true;
  }
  if (evtimer_legacy_) {
    return evtimer_legacy_->scheduleTimeout(timeout);
  }
  if (evtimer_folly_) {
    return evtimer_folly_->scheduleTimeout(timeout);
  }
  ld_check(false);
  return false;
}

void EvTimer::cancelTimeout() {
  if (is_mock_) {
    return;
  }
  if (evtimer_legacy_) {
    evtimer_legacy_->cancelTimeout();
  } else if (evtimer_folly_) {
    evtimer_folly_->cancelTimeout();
  } else {
    ld_check(false);
  }
}

bool EvTimer::isScheduled() const {
  if (is_mock_) {
    return true;
  }
  if (evtimer_legacy_) {
    return evtimer_legacy_->isScheduled();
  }
  if (evtimer_folly_) {
    return evtimer_folly_->isScheduled();
  }
  ld_check(false);
  return false;
}

const timeval* FOLLY_NULLABLE
EvTimer::getCommonTimeout(std::chrono::microseconds timeout) {
  auto base = EvBase::getRunningBase();
  if (!base) {
    return nullptr;
  }
  EvBase::EvBaseType base_type = getType(base);

  switch (base_type) {
    case EvBase::LEGACY_EVENTBASE:
      return EvTimerLegacy::getCommonTimeout(timeout);
    case EvBase::FOLLY_EVENTBASE:
      return EvTimerWithFolly::getCommonTimeout(timeout);
    case EvBase::MOCK_EVENTBASE:
      return nullptr;
    case EvBase::UNKNOWN:
      ld_check(false);
      break;
  }

  ld_check(false);
  return nullptr;
}

}} // namespace facebook::logdevice
