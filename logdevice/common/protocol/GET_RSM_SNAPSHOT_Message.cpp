/**
 * Copyright (c) 2019-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/common/protocol/GET_RSM_SNAPSHOT_Message.h"

#include "logdevice/common/Sender.h"
#include "logdevice/common/Worker.h"
#include "logdevice/common/protocol/ProtocolReader.h"

namespace facebook { namespace logdevice {

GET_RSM_SNAPSHOT_Message::GET_RSM_SNAPSHOT_Message(
    const GET_RSM_SNAPSHOT_Header& header,
    std::string key)
    : Message(MessageType::GET_RSM_SNAPSHOT, TrafficClass::FAILURE_DETECTOR),
      header_(header),
      key_(key) {}

void GET_RSM_SNAPSHOT_Message::serialize(ProtocolWriter& writer) const {
  writer.write(header_);
  writer.writeLengthPrefixedVector(key_);
}

MessageReadResult
GET_RSM_SNAPSHOT_Message::deserialize(ProtocolReader& reader) {
  GET_RSM_SNAPSHOT_Header hdr;
  reader.read(&hdr);
  std::string key;
  reader.readLengthPrefixedVector(&key);
  return reader.result(
      [&] { return new GET_RSM_SNAPSHOT_Message(hdr, std::move(key)); });
}

Message::Disposition
GET_RSM_SNAPSHOT_Message::onReceived(const Address& /* unused */) {
  // Receipt handler lives in server/GET_RSM_SNAPSHOT_onReceived.cpp,
  // this should never get called.
  std::abort();
}

void GET_RSM_SNAPSHOT_Message::onSent(Status /* unused */,
                                      const Address& /* unused */) const {
  // TODO: Introduce GetRsmSnapshotRequest map lookup
}

}} // namespace facebook::logdevice
