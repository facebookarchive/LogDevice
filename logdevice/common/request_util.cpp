/**
 * Copyright (c) 2018-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/common/request_util.h"

#include "logdevice/common/Worker.h"

namespace facebook { namespace logdevice {

std::unique_ptr<Request> FuncRequest::make(worker_id_t worker,
                                           WorkerType worker_type,
                                           RequestType type,
                                           std::function<void()>&& func) {
  return std::make_unique<FuncRequest>(
      worker, worker_type, type, std::move(func));
}

FuncRequest::Execution FuncRequest::execute() {
  func_();
  return Request::Execution::COMPLETE;
}

FuncRequest::~FuncRequest() {}

bool run_on_worker_nonblocking(Processor* processor,
                               worker_id_t worker_id,
                               WorkerType worker_type,
                               RequestType type,
                               std::function<void()>&& func,
                               bool with_retrying) {
  auto req = FuncRequest::make(worker_id, worker_type, type, std::move(func));

  int rv;
  if (with_retrying) {
    rv = processor->postWithRetrying(req);
  } else {
    rv = processor->postRequest(req);
  }

  if (rv != 0) {
    ld_error("post request of type %s on %s failed with error(%s)",
             error_name(err),
             requestTypeNames[type].c_str(),
             Worker::getName(worker_type, worker_id).c_str());

    return false;
  }

  return true;
}

}} // namespace facebook::logdevice
