/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <memory>
#include <utility>

#include "logdevice/common/Request.h"
#include "logdevice/common/RequestType.h"
#include "logdevice/common/types_internal.h"

namespace facebook { namespace logdevice {

class ClientReadStream;

/**
 * @file Used by an application thread to tell a worker thread to start
 *       reading a log.  Passes ownership of ClientReadStream object to worker
 *       thread.
 */
class StartReadingRequest : public Request {
 public:
  /**
   * @param target_worker worker thread to run this request on
   *
   * @param log_id Log ID to start reading
   *
   * @param read_stream ClientReadStream instance prepared by the LogDevice
   *                    client library, worker thread will assume ownership
   */
  StartReadingRequest(worker_id_t target_worker,
                      logid_t log_id,
                      std::unique_ptr<ClientReadStream> read_stream)
      : Request(RequestType::START_READING),
        target_worker_(target_worker),
        log_id_(log_id),
        read_stream_(std::move(read_stream)) {}

  int getThreadAffinity(int /*nthreads*/) override {
    return target_worker_.val_;
  }

  Request::Execution execute() override;

 private:
  const worker_id_t target_worker_;
  const logid_t log_id_;
  std::unique_ptr<ClientReadStream> read_stream_;
};

}} // namespace facebook::logdevice
