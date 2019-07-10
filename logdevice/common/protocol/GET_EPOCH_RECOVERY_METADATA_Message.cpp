/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/common/protocol/GET_EPOCH_RECOVERY_METADATA_Message.h"

namespace facebook { namespace logdevice {

GET_EPOCH_RECOVERY_METADATA_Message::GET_EPOCH_RECOVERY_METADATA_Message(
    const GET_EPOCH_RECOVERY_METADATA_Header& header)
    : Message(MessageType::GET_EPOCH_RECOVERY_METADATA, TrafficClass::RECOVERY),
      header_(header) {}

void GET_EPOCH_RECOVERY_METADATA_Message::serialize(
    ProtocolWriter& writer) const {
  writer.write(
      &header_, GET_EPOCH_RECOVERY_METADATA_Header::headerSize(writer.proto()));
}

MessageReadResult
GET_EPOCH_RECOVERY_METADATA_Message::deserialize(ProtocolReader& reader) {
  GET_EPOCH_RECOVERY_METADATA_Header hdr{};
  // Defaults for old protocols
  hdr.shard = -1;
  hdr.purging_shard = -1;
  hdr.end = EPOCH_INVALID;
  hdr.id = REQUEST_ID_INVALID;
  reader.read(
      &hdr, GET_EPOCH_RECOVERY_METADATA_Header::headerSize(reader.proto()));

  if (hdr.end == EPOCH_INVALID) {
    // The originator of this message is running an old
    // protocol version. Set end = start
    hdr.end = hdr.start;
  }

  return reader.result(
      [&] { return new GET_EPOCH_RECOVERY_METADATA_Message(hdr); });
}

}} // namespace facebook::logdevice
