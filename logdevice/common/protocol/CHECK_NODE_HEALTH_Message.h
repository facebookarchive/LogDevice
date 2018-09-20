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

typedef uint16_t CHECK_NODE_HEALTH_flags_t;

struct CHECK_NODE_HEALTH_Header {
  request_id_t rqid; // Request ID that is echoed back
  int shard_idx;
  CHECK_NODE_HEALTH_flags_t flags;

  // Used by sequencers to tell nodes to do space-based trimming if it is
  // enabled
  static const CHECK_NODE_HEALTH_flags_t SPACE_BASED_RETENTION = 1ul;
} __attribute__((__packed__));

using CHECK_NODE_HEALTH_Message =
    FixedSizeMessage<CHECK_NODE_HEALTH_Header,
                     MessageType::CHECK_NODE_HEALTH,
                     TrafficClass::FAILURE_DETECTOR>;

}} // namespace facebook::logdevice
