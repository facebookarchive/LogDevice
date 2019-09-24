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
  explicit EventLegacy(IEvBase::EventCallback callback,
                       IEvBase::Events events = IEvBase::Events::USER_ACTIVATED,
                       int fd = -1,
                       IEvBase* base = IEvBase::getRunningBase());
  operator bool() const;
  ~EventLegacy();

 private:
  static void evCallback(int fd, short what, void* arg);
  static void deleter(event* ev);
  event* event_{nullptr};
  IEvBase::EventCallback callback_;
  int fd_;
};

}} // namespace facebook::logdevice
