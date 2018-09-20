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
 * @file When the server transitions to isolated state, or when it gets out of
 *       isolation, it sends this request to all workers so that they take
 *       appropriate action.
 *
 */

class DomainIsolationUpdatedRequest : public Request {
 public:
  explicit DomainIsolationUpdatedRequest(worker_id_t id,
                                         NodeLocationScope scope,
                                         bool isolated)
      : Request(RequestType::DOMAIN_ISOLATION_UPDATED),
        worker_id_(id),
        scope_(scope),
        isolated_(isolated) {}

  Request::Execution execute() override;

  int getThreadAffinity(int) override {
    return worker_id_.val();
  }

  ~DomainIsolationUpdatedRequest() override {}

 private:
  worker_id_t worker_id_;
  NodeLocationScope scope_;
  bool isolated_;
};

}} // namespace facebook::logdevice
