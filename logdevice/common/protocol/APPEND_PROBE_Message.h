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

/**
 * @file
 *
 * Sent by AppendRequest before an APPEND message, when instructed by
 * AppendProbeController.
 */

using APPEND_PROBE_flags_t = uint32_t;

struct APPEND_PROBE_Header {
  // Client's AppendRequest ID; echoed in the reply so the client can route it
  // properly
  request_id_t rqid;
  // Log ID which the AppendRequest is appending to
  logid_t logid;
  // Size of the payload that would be sent over the wire in an APPEND
  // message.  If this append went through BufferedWriter on the client, this
  // is the size of the batch (which itself is likely compressed).
  uint64_t wire_size;
  // If not -1, this append went through BufferedWriter on the client and this
  // is the total size of constituent (client-original) appends before
  // batching and compression.  If sequencer batching is used on the server
  // (so it's going to unbatch), this size can be used in a further check to
  // see if there is enough bufferspace on the server.
  int64_t unbatched_size;
  // Bitset of flags, currently unused
  APPEND_PROBE_flags_t flags;
} __attribute__((__packed__));

using APPEND_PROBE_Message = FixedSizeMessage<APPEND_PROBE_Header,
                                              MessageType::APPEND_PROBE,
                                              TrafficClass::APPEND>;

// We specialise getMinProtocolVersion() in the .cpp

}} // namespace facebook::logdevice
