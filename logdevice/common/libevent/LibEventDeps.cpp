/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "logdevice/common/libevent/LibEventDeps.h"

#include <event2/event.h>

#include "logdevice/common/libevent/compat.h"

namespace facebook { namespace logdevice {

static_assert(static_cast<int>(LibEventDeps::Priorities::MAX_PRIORITIES) ==
                  EVENT_MAX_PRIORITIES,
              "Adjust LibEventDeps::Priorities::MAX_PRIORITIES to match "
              "LibEvent EVENT_MAX_PRIORITIES");
LibEventDeps::Status LibEventDeps::init(int num_priorities) {
  if (base_) {
    return Status::ALREADY_INITIALIZED;
  }
  if (num_priorities < 1 ||
      num_priorities > static_cast<int>(Priorities::MAX_PRIORITIES)) {
    return Status::INVALID_PRIORITY;
  }
  base_ = std::unique_ptr<event_base, std::function<void(event_base*)>>(
      LD_EV(event_base_new)(), LibEventDeps::deleter);
  if (!base_) {
    return Status::NO_MEM;
  }
  if (LD_EV(event_base_priority_init)(base_.get(), num_priorities) != 0) {
    base_.reset(nullptr);
    return Status::INTERNAL_ERROR;
  }

  return Status::OK;
}

LibEventDeps::Status LibEventDeps::free() {
  base_.reset(nullptr);
  return Status::OK;
}

LibEventDeps::Status LibEventDeps::loop() {
  if (!base_) {
    return Status::NOT_INITIALIZED;
  }
  int rv = LD_EV(event_base_loop)(base_.get(), 0);
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
LibEventDeps::Status LibEventDeps::loopOnce() {
  if (!base_) {
    return Status::NOT_INITIALIZED;
  }
  int rv = LD_EV(event_base_loop)(base_.get(), EVLOOP_ONCE | EVLOOP_NONBLOCK);
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

event_base* LibEventDeps::getBaseDEPRECATED() {
  return base_.get();
}

void LibEventDeps::deleter(event_base* base) {
  if (!base) {
    return;
  }
  LD_EV(event_base_free)(base);
}

}} // namespace facebook::logdevice
