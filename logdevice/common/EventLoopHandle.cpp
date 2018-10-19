/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "logdevice/common/EventLoopHandle.h"

#include "logdevice/common/request_util.h"

namespace facebook { namespace logdevice {

int EventLoopHandle::runInEventThreadNonBlocking(std::function<void()> func,
                                                 bool force) {
  auto req = FuncRequest::make(WORKER_ID_INVALID,
                               WorkerType::GENERAL,
                               RequestType::MISC,
                               std::move(func));

  return force ? forcePostRequest(req) : postRequest(req);
}

int EventLoopHandle::runInEventThreadBlocking(std::function<void()> func,
                                              bool force) {
  Semaphore sem;
  int rv;
  {
    auto req = FuncRequest::make(WORKER_ID_INVALID,
                                 WorkerType::GENERAL,
                                 RequestType::MISC,
                                 std::move(func));
    req->setClientBlockedSemaphore(&sem);

    rv = force ? forcePostRequest(req) : postRequest(req);
  }

  // Block until the Request has completed.
  // If postRequest() failed, Request destructor posted to the semaphore.
  sem.wait();
  return rv;
}

}} // namespace facebook::logdevice
