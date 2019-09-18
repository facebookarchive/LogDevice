/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/common/libevent/EventLegacy.h"

#include <stdexcept>

#include <event2/event.h>

#include "logdevice/common/libevent/compat.h"

namespace facebook { namespace logdevice {

EventLegacy::EventLegacy(EventLegacy::Callback callback,
                         EventLegacy::Events events,
                         int fd,
                         EvBaseLegacy* base)
    : callback_(std::move(callback)), fd_(fd) {
  if (!base) {
    return;
  }

  event_ = LD_EV(event_new)(
      base->getRawBase(), fd_, events, EventLegacy::evCallback, this);

  if (!event_ || LD_EV(event_add)(event_, nullptr)) {
    throw std::runtime_error("Failed to attach to event base");
  }
}

EventLegacy::operator bool() const {
  return event_ ? true : false;
}

void EventLegacy::evCallback(int, short, void* arg) {
  auto event = static_cast<EventLegacy*>(arg);
  event->callback_();
}

EventLegacy::~EventLegacy() {
  LD_EV(event_free)(event_);
}

}} // namespace facebook::logdevice
