/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "logdevice/common/RequestExecutor.h"

namespace facebook { namespace logdevice {

int RequestExecutor::postRequest(std::unique_ptr<Request>& rq,
                                 WorkerType worker_type,
                                 int target_thread) {
  return request_poster_->postImpl(
      rq, worker_type, target_thread, /* force */ false);
}

int RequestExecutor::postRequest(std::unique_ptr<Request>& rq) {
  return postRequest(rq, rq->getWorkerTypeAffinity(), -1);
}

int RequestExecutor::blockingRequestImpl(std::unique_ptr<Request>& rq,
                                         bool force) {
  Semaphore sem;
  rq->setClientBlockedSemaphore(&sem);

  int rv = force ? postImportant(rq) : postRequest(rq);
  if (rv != 0) {
    rq->setClientBlockedSemaphore(nullptr);
    return rv;
  }

  // Block until the Request has completed
  sem.wait();
  return 0;
}

int RequestExecutor::blockingRequest(std::unique_ptr<Request>& rq) {
  return blockingRequestImpl(rq, false);
}

int RequestExecutor::blockingRequestImportant(std::unique_ptr<Request>& rq) {
  return blockingRequestImpl(rq, true);
}

int RequestExecutor::postImportant(std::unique_ptr<Request>& rq) {
  return postImportant(rq, rq->getWorkerTypeAffinity(), -1);
}

int RequestExecutor::postImportant(std::unique_ptr<Request>& rq,
                                   WorkerType worker_type,
                                   int target_thread) {
  return request_poster_->postImpl(
      rq, worker_type, target_thread, /* force */ true);
}

}} // namespace facebook::logdevice
