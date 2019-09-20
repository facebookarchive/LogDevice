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

#include "logdevice/common/libevent/IEvBase.h"

struct event_base;

namespace folly {
class AsyncTimeout;
}

namespace facebook { namespace logdevice {

class EvBaseLegacy : public IEvBase {
 public:
  EvBaseLegacy() {}
  EvBaseLegacy(const EvBaseLegacy&) = delete;
  EvBaseLegacy& operator=(const EvBaseLegacy&) = delete;
  ~EvBaseLegacy() override {}

  Status init(int num_priorities =
                  static_cast<int>(Priorities::NUM_PRIORITIES)) override;
  Status free() override;

  Status loop() override;
  Status loopOnce() override;
  Status terminateLoop() override;

  /**
   * This is a function only used to slowly transition all use cases
   */
  event_base* getRawBaseDEPRECATED() override;

  void attachTimeoutManager(
      folly::AsyncTimeout* /* obj */,
      folly::TimeoutManager::InternalEnum /* internal */) override;

  void detachTimeoutManager(folly::AsyncTimeout* obj) override;

  bool scheduleTimeout(folly::AsyncTimeout* obj,
                       folly::TimeoutManager::timeout_type timeout) override;

  void cancelTimeout(folly::AsyncTimeout* obj) override;

  void bumpHandlingTime() override;

  bool isInTimeoutManagerThread() override;

 protected:
  virtual event_base* getRawBase();
  static EvBaseLegacy* getRunningBase();
  static thread_local EvBaseLegacy* running_base_;
  static void deleter(event_base*);
  std::unique_ptr<event_base, std::function<void(event_base*)>> base_{nullptr,
                                                                      deleter};
  friend class EvTimerLegacy;
  friend class EventLegacy;
};

}} // namespace facebook::logdevice
