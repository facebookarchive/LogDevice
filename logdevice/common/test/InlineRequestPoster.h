/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include "logdevice/common/RequestExecutor.h"

namespace facebook { namespace logdevice {

/**
 * A RequestPoster implementation that ignores worker restrictions
 * and executes the requests inline.
 */
class InlineRequestPoster : public RequestPoster {
 public:
  virtual ~InlineRequestPoster() = default;
  int postImpl(std::unique_ptr<Request>& rq,
               WorkerType /* worker_type */,
               int /* target_thread */,
               bool /* force */) override {
    // rq should not be accessed after execute, as it may have been deleted.
    Request::Execution status = rq->execute();

    switch (status) {
      case Request::Execution::COMPLETE:
        rq.reset();
        break;
      case Request::Execution::CONTINUE:
        rq.release();
        break;
    }
    return 0;
  }
};

}} // namespace facebook::logdevice
