/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <cstdint>

#include "logdevice/common/protocol/MessageType.h"

namespace facebook { namespace logdevice {

/**
 * @file  Every message of the LogDevice protocol begins with a ProtocolHeader.
 */

// message length field is a 4-byte unsigned int
typedef uint32_t message_len_t;

struct ProtocolHeader {
  ProtocolHeader() : len(0), type(MessageType::INVALID), cksum(0) {}

  message_len_t len; // size of message counting from first byte of this header
  MessageType type;  // type of message to follow

  // Checksum field is always present with the exception of the following cases:
  // - HELLO and ACK messages (since protocol gets negotiated after exchanging
  //                           these messages)
  // If checksum computing is disabled on some message(s), the field will be
  // set to 0. This helps with ignoring the checksum field on the recipient,
  // where checksumming might be enabled(because of config propagation delays)
  uint64_t cksum; // checksum of the payload that follows ProtocolHeader

  /**
   * Determine whether a given message type's ProtocolHeader
   * will include checksum field or not.
   *
   * We always reserve bytes for checksum in the header provided:
   * a) protocol version supports it, and
   * b) MessageType is not a Handshake Message.
   *
   * Whether we want to compute checksum or not, depends on the
   * setting 'checksumming-messages-allowed'
   */
  static bool needChecksumInHeader(MessageType msgtype, uint16_t proto);

  /**
   * Determine the size of ProtocolHeader to be used for
   * a given message and protocol version negotiated.
   */
  static std::size_t bytesNeeded(MessageType msgtype, uint16_t proto);

} __attribute__((__packed__));

static_assert(sizeof(ProtocolHeader) == 4 + 1 + 8,
              "Invalid size of ProtocolHeader");

}} // namespace facebook::logdevice
