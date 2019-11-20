/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include "logdevice/common/libevent/EvBaseLegacy.h"
#include "logdevice/common/libevent/EvBaseWithFolly.h"
#include "logdevice/common/libevent/EvTimerLegacy.h"
#include "logdevice/common/libevent/EvTimerWithFolly.h"
#include "logdevice/common/libevent/EventLegacy.h"
#include "logdevice/common/libevent/EventWithFolly.h"
#include "logdevice/common/libevent/IEvBase.h"
namespace facebook { namespace logdevice {

class EvBase : public IEvBase {
 public:
  enum EvBaseType { UNKNOWN, LEGACY_EVENTBASE, FOLLY_EVENTBASE };
  EvBase() {}
  EvBase(const EvBase&) = delete;
  EvBase& operator=(const EvBase&) = delete;
  ~EvBase() override {}

  void selectEvBase(EvBaseType type);
  Status init(int num_priorities =
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

  folly::EventBase* getEventBase() override;

  void attachTimeoutManager(
      folly::AsyncTimeout* /* obj */,
      folly::TimeoutManager::InternalEnum /* internal */) override;

  void detachTimeoutManager(folly::AsyncTimeout* obj) override;

  bool scheduleTimeout(folly::AsyncTimeout* obj,
                       folly::TimeoutManager::timeout_type timeout) override;

  void cancelTimeout(folly::AsyncTimeout* obj) override;

  void bumpHandlingTime() override;

  bool isInTimeoutManagerThread() override;

  EvBaseType getType() {
    return base_type_;
  }

  virtual void setAsRunningBase() {
    running_base_ = curr_selection_;
  }

 protected:
  EvBaseType base_type_{UNKNOWN};
  EvBaseLegacy ev_base_legacy_;
  EvBaseWithFolly ev_base_with_folly_;

  IEvBase* curr_selection_{nullptr};
};

class Event {
 public:
  explicit Event(IEvBase::EventCallback callback,
                 IEvBase::Events events = IEvBase::Events::USER_ACTIVATED,
                 int fd = -1,
                 IEvBase* base = IEvBase::getRunningBase());

 protected:
  std::unique_ptr<EventLegacy> event_legacy_;
  std::unique_ptr<EventWithFolly> event_folly_;
};

class EvTimer {
 public:
  explicit EvTimer(IEvBase* base);
  void attachTimeoutManager(folly::TimeoutManager* tm);
  void attachCallback(folly::Func callback);
  void timeoutExpired() noexcept;
  struct event* getEvent();
  bool scheduleTimeout(uint32_t);
  bool scheduleTimeout(folly::TimeoutManager::timeout_type timeout);
  void cancelTimeout();
  bool isScheduled() const;
  int setPriority(int pri);
  void activate(int res, short ncalls);

  static const timeval* FOLLY_NULLABLE
  getCommonTimeout(std::chrono::microseconds timeout);

 protected:
  std::unique_ptr<EvTimerLegacy> evtimer_legacy_;
  std::unique_ptr<EvTimerWithFolly> evtimer_folly_;
};

}} // namespace facebook::logdevice
