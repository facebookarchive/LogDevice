/**
 * Copyright (c) 2017-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include "logdevice/common/Request.h"
#include "logdevice/common/protocol/FixedSizeMessage.h"

namespace facebook { namespace logdevice {

/**
 * @file GET_LOG_INFO_Message, sent by the client to read log configuration
 * info from the server.
 */

struct GET_LOG_INFO_Header {
  // Different types of requests
  enum class Type : uint8_t {
    BY_ID = 0,    // Get a logs config entry by log ID
    BY_NAME,      // Get a logs config entry by log range name
    BY_NAMESPACE, // Gets config for all named log ranges in a namespace
    ALL           // Get all logs config entries (not implemented)
  };

  request_id_t client_rqid;
  Type request_type;
  logid_t log_id; // logid for GET_LOG_INFO_BY_ID type
} __attribute__((__packed__));
// the log name would be stored in the blob part of the SimpleMessage

using GET_LOG_INFO_Message = SimpleMessage<GET_LOG_INFO_Header,
                                           MessageType::GET_LOG_INFO,
                                           TrafficClass::HANDSHAKE,
                                           true>;

}} // namespace facebook::logdevice
