/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/common/protocol/DATA_SIZE_Message.h"

#include <folly/Memory.h>

#include "logdevice/common/DataSizeRequest.h"
#include "logdevice/common/Sender.h"
#include "logdevice/common/Worker.h"

namespace facebook { namespace logdevice {

DATA_SIZE_Message::DATA_SIZE_Message(const DATA_SIZE_Header& header)
    : Message(MessageType::DATA_SIZE, TrafficClass::READ_BACKLOG),
      header_(header) {}

void DATA_SIZE_Message::serialize(ProtocolWriter& writer) const {
  writer.write(header_);
}

MessageReadResult DATA_SIZE_Message::deserialize(ProtocolReader& reader) {
  DATA_SIZE_Header hdr;
  // Defaults for old protocols
  hdr.shard = -1;
  reader.read(&hdr);
  return reader.result([&] { return new DATA_SIZE_Message(hdr); });
}

Message::Disposition DATA_SIZE_Message::onReceived(const Address& /*from*/) {
  // Receipt handler lives in server/DATA_SIZE_onReceived.cpp; this should
  // never get called.
  std::abort();
}

void DATA_SIZE_Message::onSent(Status status, const Address& to) const {
  ld_debug(": message=DATA_SIZE st=%s to=%s",
           error_name(status),
           Sender::describeConnection(to).c_str());

  // Inform the DataSizeRequest of the outcome of sending the message
  auto& rqmap = Worker::onThisThread()->runningDataSize().map;
  auto it = rqmap.find(header_.client_rqid);
  if (it != rqmap.end()) {
    ShardID shard(to.id_.node_.index(), header_.shard);
    it->second->onMessageSent(shard, status);
  }
}

}} // namespace facebook::logdevice
