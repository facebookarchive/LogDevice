/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include "logdevice/common/Processor.h"
#include "logdevice/common/Request.h"
#include "logdevice/common/RequestType.h"
#include "logdevice/common/types_internal.h"

namespace facebook { namespace logdevice {

/**
 * @file Forcefully abort all Appenders running on a Worker thread for a
 *       particular epoch of a log. Used for clearing all existing Appenders
 *       when an EpochSequencer is dying.
 *       Creating a request without filters of log and epoch will force abort
 *       all the appenders on the worker. Currently this is used in case of
 *       shutdown if appenders don't drain within given time.
 */

class AbortAppendersEpochRequest : public Request {
 public:
  explicit AbortAppendersEpochRequest(worker_id_t id,
                                      logid_t log_id,
                                      epoch_t epoch)
      : Request(RequestType::ABORT_APPENDERS_EPOCH),
        worker_id_(id),
        log_id_(log_id),
        epoch_(epoch) {}

  explicit AbortAppendersEpochRequest(worker_id_t id)
      : Request(RequestType::ABORT_APPENDERS_EPOCH),
        worker_id_(id),
        log_id_(LOGID_INVALID),
        epoch_(EPOCH_INVALID) {}

  Request::Execution execute() override;

  int getThreadAffinity(int) override {
    return worker_id_.val();
  }

  ~AbortAppendersEpochRequest() override {}

 private:
  const worker_id_t worker_id_;
  const logid_t log_id_;
  const epoch_t epoch_;
};

}} // namespace facebook::logdevice
