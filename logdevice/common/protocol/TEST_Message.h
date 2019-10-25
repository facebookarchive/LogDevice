/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include "logdevice/common/protocol/FixedSizeMessage.h"

namespace facebook { namespace logdevice {

class TEST_Message : public Message {
 public:
  TEST_Message() : Message(MessageType::TEST, TrafficClass::REBUILD) {}

  void serialize(ProtocolWriter&) const override;
  static Message::deserializer_t deserialize;
  Disposition onReceived(const Address& from) override;
};

}} // namespace facebook::logdevice
