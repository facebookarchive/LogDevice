/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "logdevice/common/client_read_stream/RewindScheduler.h"

#include "logdevice/common/Timestamp.h"
#include "logdevice/common/Worker.h"
#include "logdevice/common/client_read_stream/ClientReadStream.h"

namespace facebook { namespace logdevice {

RewindScheduler::RewindScheduler(ClientReadStream* owner)
    : owner_(owner), timer_(owner_->deps_->createTimer([this] { rewind(); })) {}

void RewindScheduler::schedule(TimeoutMap* map, std::string reason) {
  if (isScheduled()) {
    // rewind is already scheduled. concatenate the reasons to not overwrite
    // the original one.
    reason_ += "\n" + reason;
  } else {
    reason_ = std::move(reason);

    // Decrease delay according to how much time passed since the last call to
    // positiveFeedback() (last scheduled rewind).
    delay_.positiveFeedback(SteadyTimestamp::now());
    timer_->activate(delay_.getCurrentValue(), map);
    assert(isScheduled());

    // Double delay for the next time since we are scheduling a rewind.
    delay_.negativeFeedback();

    // First tick of positive feedback so that we know how much linear decrease
    // to apply next time schedule() is called.
    delay_.positiveFeedback(SteadyTimestamp::now());
  }
}

void RewindScheduler::rewind() {
  std::string reason_tmp;
  std::swap(reason_tmp, reason_);
  owner_->rewind(std::move(reason_tmp));
  // Note: rewind() may schedule another rewind.
}

void RewindScheduler::cancel() {
  timer_->cancel();
  reason_.clear();
}

bool RewindScheduler::isScheduled() const {
  return timer_->isActive();
}

}} // namespace facebook::logdevice
