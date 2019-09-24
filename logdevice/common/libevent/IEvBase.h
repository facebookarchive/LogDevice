/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

struct event_base;

#include <folly/io/async/TimeoutManager.h>
namespace folly {
class EventBase;
}

namespace facebook { namespace logdevice {

class IEvBase : public folly::TimeoutManager {
 public:
  enum class Status {
    OK,
    NOT_INITIALIZED,
    ALREADY_INITIALIZED,
    ALREADY_RUNNING,
    INVALID_PRIORITY,
    INTERNAL_ERROR,
    NO_MEM,
  };
  enum class Priorities {
    HIGH,
    NORMAL,
    LOW,
    NUM_PRIORITIES,
    MAX_PRIORITIES = 256
  };

  using EventCallback = folly::Function<void()>;
  enum Events {
    USER_ACTIVATED = 0,
    READ = 0x02,
    PERSIST = 0x10,
    READ_PERSIST = READ | PERSIST
  };

  virtual ~IEvBase() {}

  virtual Status
  init(int num_priorities = static_cast<int>(Priorities::NUM_PRIORITIES)) = 0;
  virtual Status free() = 0;

  virtual Status loop() = 0;
  virtual Status loopOnce() = 0;
  virtual Status terminateLoop() = 0;

  /**
   * This is a function only used to slowly transition all use cases
   */
  virtual event_base* getRawBaseDEPRECATED() = 0;

  virtual folly::EventBase* getEventBase() = 0;
  static IEvBase* getRunningBase() {
    return running_base_;
  }

 protected:
  static thread_local IEvBase* running_base_;
};

}} // namespace facebook::logdevice
