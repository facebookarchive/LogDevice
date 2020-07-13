/**
 * Copyright (c) 2018-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

namespace cpp2 facebook.logdevice.thrift

// Define alias for zero-copy field types
typedef binary (cpp2.type = "folly::IOBuf") Bytes

/**
 * Wraps message not supporting Thrift serialization into Thrift structure
 */
struct LegacyMessageWrapper {
  /**
   * Serialized message in folly::IOBuf
   */
  1: Bytes payload;
}

/**
 * All messages that need to be sent through Sender API should be part
 * of message union. To support a new Thrift-native message define it
 * as a separate structure and add to the union.
 */
union Message {
   1: LegacyMessageWrapper legacyMessage;
}

/**
 * Sent by server as reply to RPC message.
 */
struct MessageReceipt {
  // Not used at the moment, reserved for the future use
}

/**
 * Request to establish session.
 */
struct SessionRequest {
  /**
   * Serialized HELLO_Message
   */
  1: LegacyMessageWrapper helloMessage;
}

/**
 * Response to establish session.
 */
struct SessionResponse {
  /**
   * Serialized ACK_Message
   */
  1: LegacyMessageWrapper ackMessage;
}
