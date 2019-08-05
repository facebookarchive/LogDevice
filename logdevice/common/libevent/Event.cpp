/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/common/libevent/Event.h"

#include <event2/event.h>

#include "logdevice/common/libevent/compat.h"

namespace facebook { namespace logdevice {

Event::Event(Callback callback, Events events, int fd, EvBase* base)
    : callback_(std::move(callback)), fd_(fd) {
  if (!base) {
    return;
  }

  event_ = LD_EV(event_new)(
      base->getRawBase(), fd_, events, Event::evCallback, this);
}

Event::operator bool() const {
  return event_ ? true : false;
}

event* Event::getRawEventDeprecated() {
  return event_;
}

void Event::evCallback(int, short, void* arg) {
  auto event = static_cast<Event*>(arg);
  event->callback_();
}

Event::~Event() {
  LD_EV(event_free)(event_);
}

}} // namespace facebook::logdevice
