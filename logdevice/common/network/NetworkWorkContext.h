/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include "logdevice/common/EventLoop.h"
#include "logdevice/common/work_model/WorkContext.h"

namespace facebook { namespace logdevice {

class NetworkWorkContext : public WorkContext {
 public:
  NetworkWorkContext(EventLoop& loop, work_context_id_t id);
  NetworkWorkContext(const NetworkWorkContext&) = delete;
  NetworkWorkContext& operator=(const NetworkWorkContext&) = delete;
  virtual ~NetworkWorkContext() {}
  EventLoop& getEventLoop();

 private:
  EventLoop& eventLoop_;
};

}} // namespace facebook::logdevice
