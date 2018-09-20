/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include "logdevice/common/Request.h"
#include "logdevice/common/protocol/SimpleMessage.h"

namespace facebook { namespace logdevice {

/**
 * @file GET_LOG_INFO_Message, sent by the client to read log configuration
 * info from the server.
 */

struct GET_LOG_INFO_REPLY_Header {
  request_id_t client_rqid;
  Status status; // one of: OK, NOTFOUND, NOTREADY, SHUTDOWN
} __attribute__((__packed__));

using GET_LOG_INFO_REPLY_Message =
    SimpleMessage<GET_LOG_INFO_REPLY_Header,
                  MessageType::GET_LOG_INFO_REPLY,
                  TrafficClass::HANDSHAKE,
                  true>;

}} // namespace facebook::logdevice
