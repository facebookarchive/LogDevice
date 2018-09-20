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
 * @file Used by an application thread to ask a worker thread for some
 * statistics calculation
 */
class ReadStreamsBufferedBytesRequest : public Request {
  /**
   * @param read_handles_ The read stream handles that the AsyncReader is
   * interested in
   *
   * @param callback_ The function accumulating the result
   */
 public:
  ReadStreamsBufferedBytesRequest(std::vector<ReadingHandle> read_handles,
                                  std::function<void(size_t)> callback)
      : Request(RequestType::READER_STATISTICS),
        read_handles_(std::move(read_handles)),
        callback_(std::move(callback)) {
    ld_check(!read_handles_.empty());
    for (auto& handle_ : read_handles_) {
      ld_check(handle_.read_stream_id != READ_STREAM_ID_INVALID);
    }
  }

  Request::Execution execute() override;

  int getThreadAffinity(int /*nthreads*/) override {
    // We need the request to get routed to the same worker that processed the
    // start request
    return read_handles_[0].worker_id.val_;
  }

 private:
  std::vector<ReadingHandle> read_handles_;
  std::function<void(size_t)> callback_;
};

}} // namespace facebook::logdevice
