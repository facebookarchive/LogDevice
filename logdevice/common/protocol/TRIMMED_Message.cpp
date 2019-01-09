/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/common/protocol/TRIMMED_Message.h"

#include "logdevice/common/Sender.h"
#include "logdevice/common/TrimRequest.h"
#include "logdevice/common/Worker.h"

namespace facebook { namespace logdevice {

void TRIMMED_Message::serialize(ProtocolWriter& writer) const {
  writer.write(header_);
}

MessageReadResult TRIMMED_Message::deserialize(ProtocolReader& reader) {
  TRIMMED_Header header;
  // Defaults for older protocols
  header.shard = -1;
  reader.read(&header);
  return reader.result([&] { return new TRIMMED_Message(header); });
}

Message::Disposition TRIMMED_Message::onReceived(const Address& from) {
  ld_spew("TRIMMED received from %s with status %s",
          Sender::describeConnection(from).c_str(),
          error_description(header_.status));

  Worker* worker = Worker::onThisThread();
  TrimRequestMap& rqmap = worker->runningTrimRequests();
  auto it = rqmap.map.find(header_.client_rqid);
  if (it != rqmap.map.end()) {
    auto scfg = worker->getServerConfig();
    shard_index_t shard_idx = header_.shard;
    ShardID shard(from.id_.node_.index(), shard_idx);
    it->second->onReply(shard, header_.status);
  }

  return Disposition::NORMAL;
}

}} // namespace facebook::logdevice
