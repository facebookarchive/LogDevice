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
#include "logdevice/include/types.h"

namespace facebook { namespace logdevice {

using APPEND_PROBE_REPLY_flags_t = uint32_t;

struct APPEND_PROBE_REPLY_Header {
  request_id_t rqid;
  logid_t logid;
  Status status;
  NodeID redirect; // when status is PREEMPTED, indicates which sequencer
                   // should receive appends for the log
  APPEND_PROBE_REPLY_flags_t flags; // bitset of flags, currently unused
} __attribute__((__packed__));

using APPEND_PROBE_REPLY_Message =
    FixedSizeMessage<APPEND_PROBE_REPLY_Header,
                     MessageType::APPEND_PROBE_REPLY,
                     TrafficClass::APPEND>;

}} // namespace facebook::logdevice
