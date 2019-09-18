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

#include "logdevice/common/libevent/EvBaseLegacy.h"

struct event;
namespace facebook { namespace logdevice {

class EventLegacy {
 public:
  using Callback = folly::Function<void()>;
  enum Events {
    USER_ACTIVATED = 0,
    READ = 0x02,
    PERSIST = 0x10,
    READ_PERSIST = READ | PERSIST
  };
  explicit EventLegacy(Callback callback,
                       Events events = Events::USER_ACTIVATED,
                       int fd = -1,
                       EvBaseLegacy* base = EvBaseLegacy::getRunningBase());
  operator bool() const;
  ~EventLegacy();

 private:
  static void evCallback(int fd, short what, void* arg);
  static void deleter(event* ev);
  event* event_{nullptr};
  Callback callback_;
  int fd_;
};

}} // namespace facebook::logdevice
