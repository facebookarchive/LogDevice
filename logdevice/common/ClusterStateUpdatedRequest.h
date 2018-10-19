/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <set>

#include "logdevice/common/Request.h"
#include "logdevice/common/RequestType.h"
#include "logdevice/common/Timer.h"
#include "logdevice/common/types_internal.h"

namespace facebook { namespace logdevice {

/**
 * @file Whenever ClusterState is updated,
 * an instance of this Request is posted to all workers.
 */

class ClusterStateUpdatedRequest : public Request {
 public:
  /**
   * @param id : target worker id
   */
  explicit ClusterStateUpdatedRequest(worker_id_t id)
      : Request(RequestType::CLUSTER_STATE_UPDATED), worker_id_(id) {}

  Request::Execution execute() override;

  int getThreadAffinity(int) override {
    return worker_id_.val_;
  }

  ~ClusterStateUpdatedRequest() override {}

 private:
  worker_id_t worker_id_;
};

}} // namespace facebook::logdevice
