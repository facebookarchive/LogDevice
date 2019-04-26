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

class LibEventDeps {
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
  LibEventDeps() {}
  LibEventDeps(const LibEventDeps&) = delete;
  LibEventDeps& operator=(const LibEventDeps&) = delete;
  virtual ~LibEventDeps() {}

  virtual Status
  init(int num_priorities = static_cast<int>(Priorities::NUM_PRIORITIES));
  virtual Status free();

  virtual Status loop();
  virtual Status loopOnce();

  /**
   * This is a function only used to slowly transition all use cases
   */
  virtual event_base* getBaseDEPRECATED();

 protected:
  static void deleter(event_base*);
  std::unique_ptr<event_base, std::function<void(event_base*)>> base_{nullptr,
                                                                      deleter};
};
}} // namespace facebook::logdevice
