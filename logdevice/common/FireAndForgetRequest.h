/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <unordered_map>

#include "logdevice/common/Request.h"

/**
 * @file  A FireAndForgetRequest is the type of request which:
 *        1) may not synchronously complete; and
 *        2) its creator does not track the request after posting it but rely
 *           on it to complete and destroy itself afterwards.
 *
 *        This base class is created to faciliate such pattern so that:
 *        1) all FireAndForgetRequest_s are owned and managed by the Worker
 *           after execution; and
 *        2) on Worker graceful shutdown, all FireAndForgetRequest_s will be
 *           aborted to prevent further events happening on a partially
 *           destructed Worker.
 *
 *        Note: to properly destroy the request after its initial execution one
 *        must call the destroy() method rather than `delete this`.
 */

namespace facebook { namespace logdevice {

class FireAndForgetRequest;

struct FireAndForgetRequestMap {
  std::unordered_map<request_id_t,
                     std::unique_ptr<FireAndForgetRequest>,
                     request_id_t::Hash>
      map;
};

class FireAndForgetRequest : public Request {
 public:
  using Request::Request;

  // subclass cannot further override the execute() method, instead they
  // should override the executionBody() method
  Request::Execution execute() final;

  // unregister the request from Worker's map and destroy it
  void destroy();

  // subclass should override this method instead of execute() for the
  // operation to be executed
  virtual void executionBody() = 0;

  virtual ~FireAndForgetRequest() {}

 private:
  void registerRequest();
};

}} // namespace facebook::logdevice
