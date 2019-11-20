/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "logdevice/common/libevent/EvBaseLegacy.h"

#include <event2/event.h>
#include <folly/ScopeGuard.h>
#include <folly/io/async/AsyncTimeout.h>

#include "logdevice/common/libevent/compat.h"

namespace facebook { namespace logdevice {

static_assert(static_cast<int>(EvBaseLegacy::Priorities::MAX_PRIORITIES) ==
                  EVENT_MAX_PRIORITIES,
              "Adjust EvBaseLegacy::Priorities::MAX_PRIORITIES to match "
              "LibEvent EVENT_MAX_PRIORITIES");
EvBaseLegacy::Status EvBaseLegacy::init(int num_priorities) {
  if (base_) {
    return Status::ALREADY_INITIALIZED;
  }
  if (num_priorities < 1 ||
      num_priorities > static_cast<int>(Priorities::MAX_PRIORITIES)) {
    return Status::INVALID_PRIORITY;
  }
  base_ = std::unique_ptr<event_base, std::function<void(event_base*)>>(
      LD_EV(event_base_new)(), EvBaseLegacy::deleter);
  if (!base_) {
    return Status::NO_MEM;
  }
  if (LD_EV(event_base_priority_init)(getRawBase(), num_priorities) != 0) {
    base_.reset(nullptr);
    return Status::INTERNAL_ERROR;
  }

  return Status::OK;
}

EvBaseLegacy::Status EvBaseLegacy::free() {
  base_.reset(nullptr);
  return Status::OK;
}

void EvBaseLegacy::runInEventBaseThread(EventCallback /* fn */) {}

EvBaseLegacy::Status EvBaseLegacy::loop() {
  if (!base_) {
    return Status::NOT_INITIALIZED;
  }
  running_base_ = this;
  SCOPE_EXIT {
    running_base_ = nullptr;
  };
  int rv = LD_EV(event_base_loop)(getRawBase(), 0);
  Status res;
  switch (rv) {
    case 0:
    case 1:
      res = Status::OK;
      break;
    default:
      res = Status::INTERNAL_ERROR;
  }
  return res;
}
EvBaseLegacy::Status EvBaseLegacy::loopOnce() {
  if (!base_) {
    return Status::NOT_INITIALIZED;
  }
  running_base_ = this;
  SCOPE_EXIT {
    running_base_ = nullptr;
  };
  int rv = LD_EV(event_base_loop)(getRawBase(), EVLOOP_ONCE | EVLOOP_NONBLOCK);
  Status res;
  switch (rv) {
    case 0:
    case 1: // There was no work
      res = Status::OK;
      break;
    default:
      res = Status::INTERNAL_ERROR;
  }
  return res;
}
EvBaseLegacy::Status EvBaseLegacy::terminateLoop() {
  if (LD_EV(event_base_loopbreak)(getRawBase())) {
    return Status::INTERNAL_ERROR;
  }
  return Status::OK;
}

event_base* EvBaseLegacy::getRawBaseDEPRECATED() {
  return getRawBase();
}

event_base* EvBaseLegacy::getRawBase() {
  return base_.get();
}

folly::EventBase* FOLLY_NULLABLE EvBaseLegacy::getEventBase() {
  return nullptr;
}

void EvBaseLegacy::deleter(event_base* base) {
  if (!base) {
    return;
  }
  LD_EV(event_base_free)(base);
}

void EvBaseLegacy::attachTimeoutManager(
    folly::AsyncTimeout* /* obj */,
    folly::TimeoutManager::InternalEnum /* internal*/) {}

void EvBaseLegacy::detachTimeoutManager(folly::AsyncTimeout* /* obj */) {}

bool EvBaseLegacy::scheduleTimeout(
    folly::AsyncTimeout* /* obj */,
    folly::TimeoutManager::timeout_type /* timeout */) {
  return false;
}

void EvBaseLegacy::cancelTimeout(folly::AsyncTimeout* /* obj */) {}

void EvBaseLegacy::bumpHandlingTime() {}

bool EvBaseLegacy::isInTimeoutManagerThread() {
  return EvBaseLegacy::getRunningBase() == this;
}
}} // namespace facebook::logdevice
