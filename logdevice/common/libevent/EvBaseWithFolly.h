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

#include <folly/io/async/EventBase.h>

#include "logdevice/common/libevent/IEvBase.h"

struct event_base;

namespace facebook { namespace logdevice {

class EvBaseWithFolly : public IEvBase {
 public:
  EvBaseWithFolly() {}
  EvBaseWithFolly(const EvBaseWithFolly&) = delete;
  EvBaseWithFolly& operator=(const EvBaseWithFolly&) = delete;
  ~EvBaseWithFolly() override {}

  Status init(int /* num_priorities */ =
                  static_cast<int>(Priorities::NUM_PRIORITIES)) override;
  Status free() override;
  void runInEventBaseThread(EventCallback fn) override;
  Status loop() override;
  Status loopOnce() override;
  Status terminateLoop() override;

  /**
   * This is a function only used to slowly transition all use cases
   */
  event_base* getRawBaseDEPRECATED() override;

  folly::EventBase* getEventBase() override {
    return &base_;
  }

  void attachTimeoutManager(
      folly::AsyncTimeout* /* obj */,
      folly::TimeoutManager::InternalEnum /* internal */) override;

  void detachTimeoutManager(folly::AsyncTimeout* obj) override;

  bool scheduleTimeout(folly::AsyncTimeout* obj,
                       folly::TimeoutManager::timeout_type timeout) override;

  void cancelTimeout(folly::AsyncTimeout* obj) override;

  void bumpHandlingTime() override;

  bool isInTimeoutManagerThread() override;

  friend class EventWithFolly;
  friend class EvTimerWithFolly;

  const folly::EventBase& getEventBase() const {
    return base_;
  }

 protected:
  folly::EventBase base_;
};

}} // namespace facebook::logdevice
