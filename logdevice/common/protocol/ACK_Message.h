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
 * @file ACK is the first message that a LogDevice entity at the
 *       passive side of a connection (currently always a server) sends to
 *       the active side. An ACK is sent in response to a HELLO from the
 *       active side. The value in ACK_Header.status tells the active side
 *       whether it can continue using the connection, or gives the reason
 *       why the connection was rejected.
 */

struct ACK_Header {
  // a bitset of flags telling the active side how it should be sending
  // messages (e.g., using a certain compression algorithm)
  uint64_t options;

  // request id copied from the coresponding HELLO, see HELLO_Message.h
  request_id_t rqid;

  // ClientID under which the client socket from which we read HELLO is
  // known to us. gcc refuses to pack the struct if this field is ClientID,
  // so using its underlying 31-bit index (NOT the full uint32_t value).
  int32_t client_idx;

  // Protocol version that the client should use to talk with this server.
  // Set to 0 if status is E::PROTONOSUPPORT.
  uint16_t proto;

  // OK if connection succeeded
  // BADMSG          if HELLO message had an invalid format
  // PROTONOSUPPORT  if this LogDevice server does not support protocol
  //                 version number in HELLO
  // ACCESS          if credentials presented in HELLO were rejected
  // INVALID_CLUSTER if this LogDevice cluster name does not match the clients
  //                 cluster name.
  // INTERNAL        if some internal error in the recipient is preventing it
  //                 from accepting the connection.
  Status status;
} __attribute__((__packed__));

using ACK_Message =
    FixedSizeMessage<ACK_Header, MessageType::ACK, TrafficClass::HANDSHAKE>;

}} // namespace facebook::logdevice
