/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <set>

#include "logdevice/common/ClientID.h"
#include "logdevice/common/Request.h"
#include "logdevice/common/RequestType.h"
#include "logdevice/common/Timer.h"
#include "logdevice/common/types_internal.h"

namespace facebook { namespace logdevice {

/**
 * @file Whenever an UpdateableLogsConfig is updated, for each registered
 * Processor, an instance of this Request is posted to all workers.
 */

class LogsConfigUpdatedRequest : public Request {
 public:
  /**
   * @param id target worker id
   */
  explicit LogsConfigUpdatedRequest(std::chrono::milliseconds timeout,
                                    worker_id_t id,
                                    WorkerType worker_type)
      : Request(RequestType::LOGS_CONFIG_UPDATED),
        worker_id_(id),
        worker_type_(worker_type),
        timeout_(timeout) {}

  Request::Execution execute() override;

  int getThreadAffinity(int /* nthreads */) override {
    return worker_id_.val_;
  }

  WorkerType getWorkerTypeAffinity() override {
    return worker_type_;
  }

  ~LogsConfigUpdatedRequest() override {}

 private:
  worker_id_t worker_id_;
  WorkerType worker_type_;
  std::set<ClientID> clients_to_notify_;

  // timer for retries
  Timer timer_;

  // request timeout
  const std::chrono::milliseconds timeout_;
};

}} // namespace facebook::logdevice
