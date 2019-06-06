/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <memory>

#include "logdevice/common/EventLoop.h"

namespace facebook { namespace logdevice {

/**
 * @file  EventLoopHandle is how other threads create, control, and schedule
 *        requests to run on logdevice::EventLoops.
 */

class EventLoopHandle {
 public:
  /**
   * Creates the EventLoop's thread and a pipe to send requests to the loop.
   *
   * Takes ownership of the EventLoop.
   */
  explicit EventLoopHandle(EventLoop* loop) : event_loop_(loop) {}

  EventLoopHandle(const EventLoopHandle&) = delete;
  EventLoopHandle(EventLoopHandle&&) = delete;
  EventLoopHandle& operator=(const EventLoopHandle&) = delete;
  EventLoopHandle& operator=(EventLoopHandle&&) = delete;

  /**
   * @return controlled EventLoop object
   */
  EventLoop* get() const {
    return event_loop_.get();
  }

  EventLoop* operator->() {
    return get();
  }

  EventLoop& operator*() {
    return *get();
  }

 private:
  // EventLoop object wrapped by this handle
  std::unique_ptr<EventLoop> event_loop_;
};
}} // namespace facebook::logdevice
