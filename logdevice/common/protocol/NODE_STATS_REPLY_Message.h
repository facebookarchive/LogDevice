/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include "logdevice/common/protocol/SimpleMessage.h"

namespace facebook { namespace logdevice {
struct NODE_STATS_REPLY_Header {
  uint64_t msg_id;
} __attribute__((__packed__));

using NODE_STATS_REPLY_Message = SimpleMessage<NODE_STATS_REPLY_Header,
                                               MessageType::NODE_STATS_REPLY,
                                               TrafficClass::HANDSHAKE,
                                               false /* no blob */>;
}} // namespace facebook::logdevice
