/**
 * Copyright (c) 2018-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "logdevice/server/read_path/ReadIoShapingCallback.h"

#include "logdevice/common/FlowGroup.h"
#include "logdevice/common/checks.h"
#include "logdevice/server/read_path/CatchupQueue.h"
#include "logdevice/server/read_path/ServerReadStream.h"

namespace facebook { namespace logdevice {

ReadIoShapingCallback::~ReadIoShapingCallback() {
  auto stream_ptr = stream_.get();
  if (stream_ptr && stream_ptr->isThrottled()) {
    int64_t throttled_since_ms =
        msec_since(stream_ptr->getThrottlingStartTime());
    if (throttled_since_ms > 5000) {
      RATELIMIT_DEBUG(
          std::chrono::seconds(1),
          2,
          "Stream for log:%lu closed while in throttled mode for %ld ms",
          stream_ptr->getLogId().val_,
          throttled_since_ms);
    }
  }

  deactivate();
}

// Drain when callback happens
void ReadIoShapingCallback::operator()(FlowGroup&, std::mutex&) {
  auto stream_ptr = stream_.get();
  if (!stream_ptr) {
    return;
  }

  ld_spew("Unthrottled: log:%lu, cb:%p", stream_ptr->getLogId().val_, this);
  stream_->markThrottled(false);

  auto cq_ptr = catchup_queue_.get();
  if (!cq_ptr) {
    return;
  }
  cq_ptr->pushRecords();
}

size_t ReadIoShapingCallback::cost() const {
  return Worker::settings().max_record_bytes_read_at_once;
}

void ReadIoShapingCallback::addCQRef(WeakRef<CatchupQueue> cq) {
  catchup_queue_ = cq;
}

}} // namespace facebook::logdevice
