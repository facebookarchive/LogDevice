/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/common/PurgingTracer.h"

namespace facebook { namespace logdevice {

namespace {

struct ThrottlerWrapper {
  TraceThrottler throttler;

  ThrottlerWrapper()
      : throttler(PURGING_TRACER, std::chrono::seconds(10), 500) {}
};

} // namespace

static folly::ThreadLocal<ThrottlerWrapper, ThrottlerWrapper>
    threadLocalThrottler;

void PurgingTracer::traceRecordPurge(TraceLogger* logger,
                                     logid_t logid,
                                     epoch_t epoch,
                                     esn_t esn,
                                     esn_t start_esn,
                                     esn_t end_esn,
                                     bool v2) {
  auto sample_builder = [=]() -> std::unique_ptr<TraceSample> {
    auto sample = std::make_unique<TraceSample>();
    sample->addIntValue("log_id", logid.val_);
    sample->addIntValue("epoch", epoch.val_);
    sample->addIntValue("esn", esn.val_);
    sample->addIntValue("start_esn", start_esn.val_);
    sample->addIntValue("end_esn", end_esn.val_);
    sample->addIntValue("v2", v2);
    return sample;
  };
  threadLocalThrottler->throttler.publish(sample_builder, logger);
}
}} // namespace facebook::logdevice
