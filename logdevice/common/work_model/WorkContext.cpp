/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/common/work_model/WorkContext.h"

#include "logdevice/common/debug.h"

namespace facebook { namespace logdevice {

const WorkContext::work_context_id_t WorkContext::kAnonymousId;

WorkContext::~WorkContext() {
  ld_check(num_references_.load() == 0);
}

WorkContext::WorkContext(WorkContext::KeepAlive executor_keep_alive,
                         work_context_id_t id)
    : executor_(std::move(executor_keep_alive)), id_(id) {}

void WorkContext::add(folly::Func func) {
  executor_->add(std::move(func));
}

void WorkContext::addWithPriority(folly::Func func, int8_t priority) {
  executor_->addWithPriority(std::move(func), priority);
}

WorkContext::work_context_id_t WorkContext::getId() const {
  return id_;
}

bool WorkContext::anonymous() const {
  return id_ == kAnonymousId;
}

bool WorkContext::keepAliveAcquire() {
  num_references_++;
  return true;
}

void WorkContext::keepAliveRelease() {
  num_references_.fetch_sub(1);
}

}} // namespace facebook::logdevice
