/**
 * Copyright (c) 2017-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "MessageTypeNames.h"

namespace facebook { namespace logdevice {

EnumMap<MessageType, std::string> messageTypeNames;

template <>
/* static */
const std::string& EnumMap<MessageType, std::string>::invalidValue() {
  static const std::string res("UNKNOWN");
  return res;
}

template <>
void EnumMap<MessageType, std::string>::setValues() {
#define MESSAGE_TYPE(sym, num)  \
  set(MessageType::sym, #sym);  \
  static_assert(num > 0, #sym); \
  static_assert(num < static_cast<int>(MessageType::MAX), #sym);
#include "logdevice/common/message_types.inc"
}

}} // namespace facebook::logdevice
