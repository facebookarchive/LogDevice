/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <unordered_map>

#include "logdevice/common/FireAndForgetRequest.h"
#include "logdevice/common/Request.h"
#include "logdevice/common/Worker.h"
#include "logdevice/common/debug.h"
#include "logdevice/include/Err.h"

namespace facebook { namespace logdevice {

class SequencerDeactivationRequest;

/**
 * @file SequencerDeactivationRequest.h
 *
 * This request will gradually execute the requests spread out
 * over the given time period
 * Since it is fire and forget - user is not blocked
 */
class SequencerDeactivationRequest : public FireAndForgetRequest {
 public:
  explicit SequencerDeactivationRequest(std::queue<logid_t> logs, int batch)
      : FireAndForgetRequest(RequestType::DEACTIVATE_SEQUENCERS),
        logs_(logs),
        batch_(batch) {}

  // see FireAndForgetRequest.h
  void executionBody() override;

 protected:
  void onTimeout();

  void setupTimer();

  WorkerType getWorkerTypeAffinity() override;

 private:
  Timer timer_;
  std::queue<logid_t> logs_;
  // wait time for timer is 20ms
  std::chrono::milliseconds timer_wait_{20};
  int batch_;
};

}} // namespace facebook::logdevice
