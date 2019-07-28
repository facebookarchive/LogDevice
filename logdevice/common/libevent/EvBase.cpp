/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "logdevice/common/libevent/EvBase.h"

#include <event2/event.h>
#include <folly/ScopeGuard.h>

#include "logdevice/common/libevent/compat.h"

namespace facebook { namespace logdevice {

thread_local EvBase* EvBase::running_base_{nullptr};

static_assert(static_cast<int>(EvBase::Priorities::MAX_PRIORITIES) ==
                  EVENT_MAX_PRIORITIES,
              "Adjust EvBase::Priorities::MAX_PRIORITIES to match "
              "LibEvent EVENT_MAX_PRIORITIES");
EvBase::Status EvBase::init(int num_priorities) {
  if (base_) {
    return Status::ALREADY_INITIALIZED;
  }
  if (num_priorities < 1 ||
      num_priorities > static_cast<int>(Priorities::MAX_PRIORITIES)) {
    return Status::INVALID_PRIORITY;
  }
  base_ = std::unique_ptr<event_base, std::function<void(event_base*)>>(
      LD_EV(event_base_new)(), EvBase::deleter);
  if (!base_) {
    return Status::NO_MEM;
  }
  if (LD_EV(event_base_priority_init)(getRawBase(), num_priorities) != 0) {
    base_.reset(nullptr);
    return Status::INTERNAL_ERROR;
  }

  return Status::OK;
}

EvBase::Status EvBase::free() {
  base_.reset(nullptr);
  return Status::OK;
}

EvBase::Status EvBase::loop() {
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
EvBase::Status EvBase::loopOnce() {
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
EvBase::Status EvBase::terminateLoop() {
  if (LD_EV(event_base_loopbreak)(getRawBase())) {
    return Status::INTERNAL_ERROR;
  }
  return Status::OK;
}

event_base* EvBase::getRawBaseDEPRECATED() {
  return getRawBase();
}

event_base* EvBase::getRawBase() {
  return base_.get();
}

EvBase* EvBase::getRunningBase() {
  return running_base_;
}

void EvBase::deleter(event_base* base) {
  if (!base) {
    return;
  }
  LD_EV(event_base_free)(base);
}

}} // namespace facebook::logdevice
