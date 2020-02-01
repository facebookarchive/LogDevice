/**
 * Copyright (c) 2019-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "logdevice/common/protocol/GET_RSM_SNAPSHOT_REPLY_Message.h"

#include "logdevice/common/Sender.h"
#include "logdevice/common/Worker.h"

namespace facebook { namespace logdevice {

GET_RSM_SNAPSHOT_REPLY_Message::GET_RSM_SNAPSHOT_REPLY_Message(
    const GET_RSM_SNAPSHOT_REPLY_Header& header,
    std::string snapshot_blob)
    : Message(MessageType::GET_RSM_SNAPSHOT_REPLY, TrafficClass::RSM),
      header_(header),
      snapshot_blob_(std::move(snapshot_blob)) {}

void GET_RSM_SNAPSHOT_REPLY_Message::serialize(ProtocolWriter& writer) const {
  writer.write(header_);
  writer.writeLengthPrefixedVector(snapshot_blob_);
}

MessageReadResult
GET_RSM_SNAPSHOT_REPLY_Message::deserialize(ProtocolReader& reader) {
  GET_RSM_SNAPSHOT_REPLY_Header hdr;
  reader.read(&hdr);
  std::string snapshot_blob;
  reader.readLengthPrefixedVector(&snapshot_blob);
  return reader.result([&] {
    return new GET_RSM_SNAPSHOT_REPLY_Message(hdr, std::move(snapshot_blob));
  });
}

Message::Disposition
GET_RSM_SNAPSHOT_REPLY_Message::onReceived(const Address& /*unused */) {
  // TODO: Introduce GetRsmSnapshotRequest map lookup
  return Disposition::NORMAL;
}

}} // namespace facebook::logdevice
