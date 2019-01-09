/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/common/protocol/FINDKEY_Message.h"

#include <folly/Memory.h>

#include "logdevice/common/FindKeyRequest.h"
#include "logdevice/common/Worker.h"
#include "logdevice/common/protocol/ProtocolReader.h"
#include "logdevice/common/protocol/ProtocolWriter.h"

namespace facebook { namespace logdevice {

void FINDKEY_Message::serialize(ProtocolWriter& writer) const {
  writer.write(header_);
  if (header_.flags & FINDKEY_Header::USER_KEY) {
    ld_check(key_.hasValue());
    ld_check(key_.value().size() <= std::numeric_limits<uint16_t>::max());

    uint16_t key_length = key_.value().size();
    writer.write(key_length);
    writer.writeVector(key_.value());
  }
}

MessageReadResult FINDKEY_Message::deserialize(ProtocolReader& reader) {
  FINDKEY_Header header;
  // Defaults for older protocols
  header.flags = 0;
  header.log_id = LOGID_INVALID;
  header.hint_lo = LSN_INVALID;
  header.hint_hi = LSN_MAX;
  header.timeout_ms = 0;
  header.shard = -1;
  reader.read(&header);

  folly::Optional<std::string> key;
  if (header.flags & FINDKEY_Header::USER_KEY) {
    uint16_t length;

    reader.read(&length);
    std::string key_string;
    reader.readVector(&key_string, length);
    key.assign(std::move(key_string));
  }
  return reader.result(
      [&] { return new FINDKEY_Message(header, std::move(key)); });
}

void FINDKEY_Message::onSent(Status status, const Address& to) const {
  Message::onSent(status, to);

  // Inform the FindKeyRequest of the outcome of sending the message
  auto& rqmap = Worker::onThisThread()->runningFindKey().map;
  auto it = rqmap.find(header_.client_rqid);
  if (it != rqmap.end()) {
    it->second->onMessageSent(
        ShardID(to.id_.node_.index(), header_.shard), status);
  }
}

}} // namespace facebook::logdevice
