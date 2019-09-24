/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <gmock/gmock.h>

#include "logdevice/common/libevent/LibEventCompatibility.h"

namespace facebook { namespace logdevice {

class EvBaseMock : public EvBase {
 public:
  EvBaseMock(EvBase::EvBaseType type) : EvBase() {
    selectEvBase(type);
  }
  MOCK_METHOD1(init_mock, Status(int));
  virtual Status init(int num_priorities = static_cast<int>(
                          Priorities::NUM_PRIORITIES)) override {
    return init_mock(num_priorities);
  }
  MOCK_METHOD0(free, Status(void));

  MOCK_METHOD0(loop, Status(void));
  MOCK_METHOD0(loopOnce, Status(void));

  MOCK_METHOD0(getRawBaseDEPRECATED, event_base*(void));
  MOCK_METHOD0(getRawBase, event_base*(void));
  void setAsRunningBase() override {
    EvBase::running_base_ = this;
  }
  void clearRunningBase() {
    EvBase::running_base_ = nullptr;
  }

  MOCK_METHOD2(attachTimeoutManager,
               void(folly::AsyncTimeout*, folly::TimeoutManager::InternalEnum));
  MOCK_METHOD1(detachTimeoutManager, void(folly::AsyncTimeout*));
  MOCK_METHOD2(scheduleTimeout,
               bool(folly::AsyncTimeout*, folly::TimeoutManager::timeout_type));
  MOCK_METHOD2(scheduleTimeoutHighRes,
               bool(folly::AsyncTimeout* obj,
                    folly::TimeoutManager::timeout_type_high_res));
  MOCK_METHOD1(cancelTimeout, void(folly::AsyncTimeout*));
  MOCK_METHOD0(bumpHandlingTime, void());
  MOCK_METHOD0(isInTimeoutManagerThread, bool());
};
}} // namespace facebook::logdevice
