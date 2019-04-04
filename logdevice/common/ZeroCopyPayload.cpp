/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "logdevice/common/ZeroCopyPayload.h"

#include <event2/buffer.h>

#include "logdevice/common/EventLoop.h"
#include "logdevice/common/debug.h"
#include "logdevice/common/libevent/compat.h"

namespace facebook { namespace logdevice {

void ZeroCopyPayloadDisposer::operator()(ZeroCopyPayload* record) {
  ev_loop_->dispose(record);
}

std::shared_ptr<ZeroCopyPayload>
ZeroCopyPayload::create(EventLoop* ev_loop, struct evbuffer* payload) {
  ld_check(payload && ev_loop);
  return std::shared_ptr<ZeroCopyPayload>(
      new ZeroCopyPayload(payload), ZeroCopyPayloadDisposer(ev_loop));
}

ZeroCopyPayload::ZeroCopyPayload(struct evbuffer* payload)
    : payload_(payload), length_(LD_EV(evbuffer_get_length)(payload)) {}

ZeroCopyPayload::~ZeroCopyPayload() {
  LD_EV(evbuffer_free)(payload_);
}

}} // namespace facebook::logdevice
