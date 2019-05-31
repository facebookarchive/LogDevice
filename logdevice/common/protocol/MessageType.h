/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

/**
 * @file    types of messages that LD clients and servers may exchange.
 *          Constants are chars to improve the readability of raw tcpdumps.
 */

namespace facebook { namespace logdevice {

enum class MessageType : char {
  INVALID = 0,

#define MESSAGE_TYPE(name, id) name = id,
#include "logdevice/common/message_types.inc"

  MAX = 127
};

static_assert(sizeof(MessageType) == 1, "MessageType must be 1 byte");

// Some symbols are deprecated.
#define MESSAGE_TYPE(sym, num)                              \
  static_assert(num != 'y', "Message key y is deprecated"); \
  static_assert(num != 'Y', "Message key Y is deprecated");
#include "logdevice/common/message_types.inc"

constexpr bool isHELLOMessage(MessageType type) {
  return type == MessageType::HELLO;
}

constexpr bool isACKMessage(MessageType type) {
  return type == MessageType::ACK;
}

constexpr bool isHandshakeMessage(MessageType type) {
  return isHELLOMessage(type) || isACKMessage(type);
}

constexpr bool isConfigSynchronizationMessage(MessageType type) {
  return type == MessageType::CONFIG_ADVISORY ||
      type == MessageType::CONFIG_CHANGED || type == MessageType::CONFIG_FETCH;
}

constexpr bool allowedOnGossipConnection(MessageType type) {
  return type == MessageType::GOSSIP ||
      type == MessageType::GET_CLUSTER_STATE ||
      type == MessageType::GET_CLUSTER_STATE_REPLY ||
      isHandshakeMessage(type) || isConfigSynchronizationMessage(type);
}

constexpr bool shouldBeInlined(MessageType type) {
  return isHandshakeMessage(type) || isConfigSynchronizationMessage(type) ||
      type == MessageType::SHARD_STATUS_UPDATE;
}

}} // namespace facebook::logdevice
