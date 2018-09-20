/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include "logdevice/common/Request.h"
#include "logdevice/common/protocol/FixedSizeMessage.h"

namespace facebook { namespace logdevice {

typedef uint16_t CHECK_NODE_HEALTH_REPLY_flags_t;

struct CHECK_NODE_HEALTH_REPLY_Header {
  // echoed request id from the corresponding CHECK_NODE_HEALTH message
  request_id_t rid;

  // E:OK           if node is Healthy
  // E::NOSPC       cluster is out of disk space, record cannot be delivered
  // E::OVERLOADED  cluster is overloaded, record cannot be delivered
  // E::DISABLED    Rebuiling is in progress and server is not taking appends
  // E::SHUTDOWN    Worker is shutting down
  // E::NOTSTORAGE  Node is not a storage node
  Status status;
  CHECK_NODE_HEALTH_REPLY_flags_t flags;
} __attribute__((__packed__));

using CHECK_NODE_HEALTH_REPLY_Message =
    FixedSizeMessage<CHECK_NODE_HEALTH_REPLY_Header,
                     MessageType::CHECK_NODE_HEALTH_REPLY,
                     TrafficClass::FAILURE_DETECTOR>;

}} // namespace facebook::logdevice
