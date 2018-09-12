/**
 * Copyright (c) 2017-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include "logdevice/common/Request.h"
#include "logdevice/common/RequestType.h"
#include "logdevice/common/types_internal.h"

namespace facebook { namespace logdevice {

/**
 * @file Request sent by AllSequencers when sequencers reactivations need to be
 *       enqueued.
 */

class SequencerEnqueueReactivationRequest : public Request {
 public:
  explicit SequencerEnqueueReactivationRequest(std::vector<logid_t> logs)
      : Request(RequestType::SEQUENCER_ENQUEUE_REACTIVATION),
        reactivation_list_(std::move(logs)) {
    ld_check(reactivation_list_.size() > 0);
  }

  Execution execute() override;

  int getThreadAffinity(int nthreads) override;

 private:
  std::vector<logid_t> reactivation_list_;
};

}} // namespace facebook::logdevice
