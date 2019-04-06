/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/common/WorkerCallbackHelper.h"

#include "logdevice/common/Processor.h"
#include "logdevice/common/Worker.h"

namespace facebook { namespace logdevice {

TicketBase::TicketBase() {
  auto w = Worker::onThisThread(false);
  if (!w) {
    // Two asserts to differentiate between running on a non-EventLoop
    // thread and running on a EventLoop that's not a Worker.
    ld_check(Worker::onThisThread(false /* enforce_worker */));
    ld_check(false);
    return;
  }
  processor_ = w->processor_->weak_from_this();
  workerIdx_ = w->idx_.val();
  worker_type_ = w->worker_type_;
}

int TicketBase::postRequest(std::unique_ptr<Request>& rq) const {
  if (auto p = processor_.lock()) {
    return p->postWithRetrying(rq);
  }
  err = E::SHUTDOWN;
  return -1;
}

}} // namespace facebook::logdevice
