/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/common/protocol/WINDOW_Message.h"

#include <cstdlib>

#include "logdevice/common/MetaDataLog.h"
#include "logdevice/common/Worker.h"
#include "logdevice/common/protocol/ProtocolReader.h"
#include "logdevice/common/protocol/ProtocolWriter.h"

namespace facebook { namespace logdevice {

WINDOW_Message::WINDOW_Message(const WINDOW_Header& header)
    : Message(MessageType::WINDOW, TrafficClass::HANDSHAKE), header_(header) {}

void WINDOW_Message::serialize(ProtocolWriter& writer) const {
  writer.write(header_);
}

MessageReadResult WINDOW_Message::deserialize(ProtocolReader& reader) {
  WINDOW_Header hdr;
  // Defaults for old protocols
  hdr.shard = -1;
  reader.read(&hdr);
  return reader.result([&] { return new WINDOW_Message(hdr); });
}

Message::Disposition WINDOW_Message::onReceived(const Address&) {
  // Receipt handler is AllServerReadStreams::onWindowMessage(); this should
  // never get called.
  std::abort();
}

bool WINDOW_Message::allowUnencrypted() const {
  return MetaDataLog::isMetaDataLog(header_.log_id) &&
      Worker::settings().read_streams_use_metadata_log_only;
}

}} // namespace facebook::logdevice
