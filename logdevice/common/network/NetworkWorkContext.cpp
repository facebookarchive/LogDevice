/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/common/network/NetworkWorkContext.h"

namespace facebook { namespace logdevice {

NetworkWorkContext::NetworkWorkContext(EventLoop& eventLoop,
                                       work_context_id_t id)
    : WorkContext(getKeepAliveToken(&eventLoop), id), eventLoop_(eventLoop) {}

EventLoop& NetworkWorkContext::getEventLoop() {
  return eventLoop_;
}

}} // namespace facebook::logdevice
