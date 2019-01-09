/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/common/protocol/TRIM_Message.h"

#include "logdevice/common/TrimRequest.h"
#include "logdevice/common/Worker.h"

namespace facebook { namespace logdevice {

void TRIM_Message::serialize(ProtocolWriter& writer) const {
  writer.write(header_);
}

MessageReadResult TRIM_Message::deserialize(ProtocolReader& reader) {
  TRIM_Header header;
  // Defaults for older protocols
  header.shard = -1;
  reader.read(&header);
  return reader.result([&] { return new TRIM_Message(header); });
}

Message::Disposition TRIM_Message::onReceived(const Address& /*from*/) {
  // Receipt handler lives in server/TRIM_onReceived.cpp; this should never
  // get called.
  std::abort();
}

void TRIM_Message::onSent(Status status, const Address& to) const {
  TrimRequestMap& rqmap = Worker::onThisThread()->runningTrimRequests();
  auto it = rqmap.map.find(header_.client_rqid);
  if (it != rqmap.map.end()) {
    ShardID shard(to.id_.node_.index(), header_.shard);
    it->second->onMessageSent(shard, status);
  }
}

}} // namespace facebook::logdevice
