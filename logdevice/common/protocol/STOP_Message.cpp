/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/common/protocol/STOP_Message.h"

#include <cstdlib>

#include "logdevice/common/MetaDataLog.h"
#include "logdevice/common/Worker.h"
#include "logdevice/common/protocol/ProtocolReader.h"
#include "logdevice/common/protocol/ProtocolWriter.h"

namespace facebook { namespace logdevice {

STOP_Message::STOP_Message(const STOP_Header& header)
    : Message(MessageType::STOP, TrafficClass::HANDSHAKE), header_(header) {}

void STOP_Message::serialize(ProtocolWriter& writer) const {
  writer.write(header_);
}

MessageReadResult STOP_Message::deserialize(ProtocolReader& reader) {
  STOP_Header hdr;
  reader.read(&hdr);
  return reader.result([&] { return new STOP_Message(hdr); });
}

Message::Disposition STOP_Message::onReceived(const Address&) {
  // Receipt handler is STOP_onReceived(); this should
  // never get called.
  std::abort();
}

bool STOP_Message::allowUnencrypted() const {
  return MetaDataLog::isMetaDataLog(header_.log_id) &&
      Worker::settings().read_streams_use_metadata_log_only;
}

}} // namespace facebook::logdevice
