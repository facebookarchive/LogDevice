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

}} // namespace facebook::logdevice
