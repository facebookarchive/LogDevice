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

/**
 * @file Used by an application thread to tell a worker thread to resume
 *       reading a log.
 */
class ResumeReadingRequest : public Request {
 public:
  explicit ResumeReadingRequest(ReadingHandle reading_handle)
      : Request(RequestType::RESUME_READING), reading_handle_(reading_handle) {
    ld_check(reading_handle_.read_stream_id != READ_STREAM_ID_INVALID);
  }

  Request::Execution execute() override;

  int getThreadAffinity(int /*nthreads*/) override {
    // We need the resume request to get routed to the same worker that was used
    // for stating reading.
    return reading_handle_.worker_id.val_;
  }

 private:
  ReadingHandle reading_handle_;
};

}} // namespace facebook::logdevice
