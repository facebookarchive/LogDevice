/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/common/work_model/WorkContext.h"

namespace facebook { namespace logdevice {

const WorkContext::work_context_id_t WorkContext::kAnonymousId;

WorkContext::WorkContext(folly::Executor& executor, work_context_id_t id)
    : executor_(executor), id_(id) {}

void WorkContext::add(folly::Func func) {
  executor_.add(std::move(func));
}

WorkContext::work_context_id_t WorkContext::getId() const {
  return id_;
}

bool WorkContext::anonymous() const {
  return id_ == kAnonymousId;
}
}} // namespace facebook::logdevice
