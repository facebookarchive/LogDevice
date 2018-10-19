/**
 * Copyright (c) 2018-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <memory>
#include <string>

#include "logdevice/common/ThrottledTracer.h"

namespace facebook { namespace logdevice {

class StatsHolder;
class TraceLogger;

constexpr auto CLIENT_EVENT_TRACER = "client_event_tracer";

class ClientEventTracer : public ThrottledTracer {
 public:
  explicit ClientEventTracer(std::shared_ptr<TraceLogger> logger, StatsHolder*);

  void traceEvent(Severity sev,
                  std::string name_space,
                  std::string type,
                  std::string data,
                  std::string context);

 private:
  StatsHolder* stats_;
};

}} // namespace facebook::logdevice
