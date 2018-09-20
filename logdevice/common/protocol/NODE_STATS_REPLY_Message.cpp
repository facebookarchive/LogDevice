/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "NODE_STATS_REPLY_Message.h"

namespace facebook { namespace logdevice {

template <>
Message::Disposition
NODE_STATS_REPLY_Message::onReceived(const Address& /*from*/) {
  // this function should not be called. Instead it lives in
  // lib/NODE_STATS_REPLY_onReceived.cpp
  std::abort();
}

template <>
uint16_t NODE_STATS_REPLY_Message::getMinProtocolVersion() const {
  return Compatibility::ProtocolVersion::CLIENT_SEND_NODE_STATS;
}
}} // namespace facebook::logdevice
