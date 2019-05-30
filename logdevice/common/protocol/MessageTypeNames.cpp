/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/common/protocol/MessageTypeNames.h"

namespace facebook { namespace logdevice {

const EnumMap<MessageType, std::string>& messageTypeNames() {
  static EnumMap<MessageType, std::string> messageTypeNames;
  return messageTypeNames;
}

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
