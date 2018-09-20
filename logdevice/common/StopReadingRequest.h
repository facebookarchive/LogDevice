/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <functional>

#include "logdevice/common/Request.h"
#include "logdevice/common/RequestType.h"
#include "logdevice/common/types_internal.h"

namespace facebook { namespace logdevice {

class Semaphore;

/**
 * @file Used by an application thread to tell a worker thread to stop
 *       reading a log.
 */
class StopReadingRequest : public Request {
 public:
  StopReadingRequest(ReadingHandle stop_handle, std::function<void()> callback)
      : Request(RequestType::STOP_READING),
        stop_handle_(stop_handle),
        callback_(std::move(callback)) {
    ld_check(stop_handle_.read_stream_id != READ_STREAM_ID_INVALID);
  }

  Request::Execution execute() override;

  int getThreadAffinity(int /*nthreads*/) override {
    // We need the request to get routed to the same worker that processed the
    // start request
    return stop_handle_.worker_id.val_;
  }

 private:
  ReadingHandle stop_handle_;
  std::function<void()> callback_;
};

}} // namespace facebook::logdevice
