/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/common/Envelope.h"

#include "logdevice/common/FlowGroup.h"
#include "logdevice/common/Socket.h"

namespace facebook { namespace logdevice {

void Envelope::operator()(FlowGroup& fg, std::mutex& flow_meters_mutex) {
  std::unique_lock<std::mutex> lock(flow_meters_mutex);
  if (!fg.drain(*this)) {
    // Should never happen.
    ld_check(false);
    fg.push(*this, priority());
    return;
  }
  socket().releaseMessage(*this);
}

}} // namespace facebook::logdevice
