/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <memory>

#include <folly/Function.h>
#include <folly/io/async/EventBase.h>
#include <folly/io/async/EventHandler.h>

#include "logdevice/common/libevent/EvBaseWithFolly.h"

namespace facebook { namespace logdevice {

class EventWithFolly : public folly::EventHandler {
 public:
  explicit EventWithFolly(
      IEvBase::EventCallback callback,
      IEvBase::Events events = IEvBase::Events::USER_ACTIVATED,
      int fd = -1,
      IEvBase* base = IEvBase::getRunningBase())
      : EventHandler(base->getEventBase(), folly::NetworkSocket(fd)),
        callback_(std::move(callback)) {
    registerHandler(events);
  }

  void handlerReady(uint16_t /* events */) noexcept override {
    callback_();
  }

 private:
  IEvBase::EventCallback callback_;
};

}} // namespace facebook::logdevice
