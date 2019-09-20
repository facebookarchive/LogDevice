/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "logdevice/common/libevent/EvBaseWithFolly.h"

#include <folly/io/async/EventBase.h>

namespace facebook { namespace logdevice {

thread_local EvBaseWithFolly* EvBaseWithFolly::running_base_{nullptr};

EvBaseWithFolly::Status EvBaseWithFolly::init(int) {
  // noop
  return EvBaseWithFolly::Status::OK;
}

EvBaseWithFolly::Status EvBaseWithFolly::free() {
  // noop
  return EvBaseWithFolly::Status::OK;
}

EvBaseWithFolly::Status EvBaseWithFolly::loop() {
  running_base_ = this;
  SCOPE_EXIT {
    running_base_ = nullptr;
  };

  if (base_.loop()) {
    return Status::OK;
  }

  return Status::INTERNAL_ERROR;
}

EvBaseWithFolly::Status EvBaseWithFolly::loopOnce() {
  if (base_.loopOnce()) {
    return Status::OK;
  }

  return Status::INTERNAL_ERROR;
}

EvBaseWithFolly::Status EvBaseWithFolly::terminateLoop() {
  base_.terminateLoopSoon();
  return Status::OK;
}

event_base* EvBaseWithFolly::getRawBaseDEPRECATED() {
  return base_.getLibeventBase();
}

/* static */ EvBaseWithFolly* EvBaseWithFolly::getRunningBase() {
  return running_base_;
}

void EvBaseWithFolly::attachTimeoutManager(
    folly::AsyncTimeout* obj,
    folly::TimeoutManager::InternalEnum internal) {
  base_.attachTimeoutManager(obj, internal);
}

void EvBaseWithFolly::detachTimeoutManager(folly::AsyncTimeout* obj) {
  base_.detachTimeoutManager(obj);
}

bool EvBaseWithFolly::scheduleTimeout(
    folly::AsyncTimeout* obj,
    folly::TimeoutManager::timeout_type timeout) {
  return base_.scheduleTimeout(obj, timeout);
}

void EvBaseWithFolly::cancelTimeout(folly::AsyncTimeout* obj) {
  base_.cancelTimeout(obj);
}

void EvBaseWithFolly::bumpHandlingTime() {
  base_.bumpHandlingTime();
}

bool EvBaseWithFolly::isInTimeoutManagerThread() {
  return base_.isInTimeoutManagerThread();
}
}} // namespace facebook::logdevice
