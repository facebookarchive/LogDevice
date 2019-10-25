/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/common/protocol/TEST_Message.h"

#include <cstdlib>

namespace facebook { namespace logdevice {

void TEST_Message::serialize(ProtocolWriter&) const {
  // Empty.
}

MessageReadResult TEST_Message::deserialize(ProtocolReader& reader) {
  // Treat any sequence of bytes as a valid TEST_Message, even though
  // TEST_Message::serialize() always outputs zero bytes. This is because
  // VarLengthTestMessage pretends to be a TEST_Message (by using
  // MessageType::TEST) but has a less trivial serializer.
  reader.allowTrailingBytes();
  return reader.result([&] { return new TEST_Message(); });
}

Message::Disposition TEST_Message::onReceived(const Address&) {
  // does nothing
  return Message::Disposition::NORMAL;
}

}} // namespace facebook::logdevice
