/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/common/protocol/CHECK_NODE_HEALTH_Message.h"

#include <cstdlib>

namespace facebook { namespace logdevice {

template <>
Message::Disposition CHECK_NODE_HEALTH_Message::onReceived(const Address&) {
  // Receipt handler lives in server/CHECK_NODE_HEALTH_onReceived.cpp; this
  // should never get called.
  std::abort();
}

}} // namespace facebook::logdevice
