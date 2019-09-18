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

#include "logdevice/common/libevent/IEvBase.h"

struct event;
namespace facebook { namespace logdevice {

class EventWithFolly : public folly::EventHandler {
 public:
  using Callback = folly::Function<void()>;
  enum Events {
    USER_ACTIVATED = 0,
    READ = 0x02,
    PERSIST = 0x10,
    READ_PERSIST = READ | PERSIST
  };

  explicit EventWithFolly(
      Callback callback,
      Events events = Events::USER_ACTIVATED,
      int fd = -1,
      EvBaseWithFolly* base = EvBaseWithFolly::getRunningBase())
      : EventHandler(&dynamic_cast<EvBaseWithFolly*>(base)->base_,
                     folly::NetworkSocket(fd)),
        callback_(std::move(callback)) {
    registerHandler(events);
  }

  void handlerReady(uint16_t /* events */) noexcept override {
    callback_();
  }

 private:
  Callback callback_;
};

}} // namespace facebook::logdevice
