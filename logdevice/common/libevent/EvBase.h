/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

/**
 * @file Isolation of <event2/event.h> include to prevent symbol conflicts and
 *       define redefinitions when including anything from folly which brings in
 *       include/event.h (libevent version 1.0)
 */

#include <functional>
#include <memory>

struct event_base;

namespace facebook { namespace logdevice {

class EvBase {
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
  EvBase() {}
  EvBase(const EvBase&) = delete;
  EvBase& operator=(const EvBase&) = delete;
  virtual ~EvBase() {}

  virtual Status
  init(int num_priorities = static_cast<int>(Priorities::NUM_PRIORITIES));
  virtual Status free();

  virtual Status loop();
  virtual Status loopOnce();
  virtual Status terminateLoop();

  /**
   * This is a function only used to slowly transition all use cases
   */
  virtual event_base* getRawBaseDEPRECATED();

 protected:
  virtual event_base* getRawBase();
  static EvBase* getRunningBase();
  static thread_local EvBase* running_base_;
  static void deleter(event_base*);
  std::unique_ptr<event_base, std::function<void(event_base*)>> base_{nullptr,
                                                                      deleter};
  friend class EvTimer;
  friend class Event;
};
}} // namespace facebook::logdevice
