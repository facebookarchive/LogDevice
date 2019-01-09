/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "logdevice/common/protocol/NODE_STATS_AGGREGATE_Message.h"

#include "logdevice/common/Address.h"
#include "logdevice/common/Sender.h"
#include "logdevice/common/Worker.h"
#include "logdevice/common/debug.h"
#include "logdevice/common/protocol/MessageReadResult.h"
#include "logdevice/common/protocol/ProtocolReader.h"
#include "logdevice/common/protocol/ProtocolWriter.h"

namespace facebook { namespace logdevice {

NODE_STATS_AGGREGATE_Message::NODE_STATS_AGGREGATE_Message(
    NODE_STATS_AGGREGATE_Header header)
    : Message(MessageType::NODE_STATS_AGGREGATE,
              TrafficClass::FAILURE_DETECTOR),
      header_(header) {}

NODE_STATS_AGGREGATE_Message::NODE_STATS_AGGREGATE_Message()
    : NODE_STATS_AGGREGATE_Message(NODE_STATS_AGGREGATE_Header()) {}

void NODE_STATS_AGGREGATE_Message::serialize(ProtocolWriter& writer) const {
  writer.write(header_);
}

MessageReadResult
NODE_STATS_AGGREGATE_Message::deserialize(ProtocolReader& reader) {
  std::unique_ptr<NODE_STATS_AGGREGATE_Message> msg(
      new NODE_STATS_AGGREGATE_Message);

  reader.read(&msg->header_);

  return reader.resultMsg(std::move(msg));
}

Message::Disposition
NODE_STATS_AGGREGATE_Message::onReceived(const Address& /*from*/) {
  // should only be called in server/NODE_STATS_AGGREGATE_onReceived.cpp
  ld_check(false);
  err = E::INTERNAL;
  return Disposition::ERROR;
}

}} // namespace facebook::logdevice
