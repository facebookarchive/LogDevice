/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/common/protocol/FINDKEY_REPLY_Message.h"

#include "logdevice/common/FindKeyRequest.h"
#include "logdevice/common/Sender.h"
#include "logdevice/common/ShardID.h"
#include "logdevice/common/Worker.h"

namespace facebook { namespace logdevice {

void FINDKEY_REPLY_Message::serialize(ProtocolWriter& writer) const {
  writer.write(header_);
}

MessageReadResult FINDKEY_REPLY_Message::deserialize(ProtocolReader& reader) {
  FINDKEY_REPLY_Header header;
  // Defaults for older protocols
  header.shard = -1;
  reader.read(&header);
  return reader.result([&] { return new FINDKEY_REPLY_Message(header); });
}

Message::Disposition FINDKEY_REPLY_Message::onReceived(const Address& from) {
  if (from.isClientAddress()) {
    ld_error("got FINDKEY_REPLY message from client %s",
             Sender::describeConnection(from).c_str());
    err = E::PROTO;
    return Disposition::ERROR;
  }

  // Status gets validated in FindTimeRequest::onReply() where we switch on it

  Worker* worker = Worker::onThisThread();
  auto& rqmap = worker->runningFindKey().map;
  auto it = rqmap.find(header_.client_rqid);
  if (it != rqmap.end()) {
    auto scfg = worker->getServerConfig();
    shard_index_t shard_idx = header_.shard;
    ShardID shard(from.id_.node_.index(), shard_idx);
    it->second->onReply(
        shard, header_.status, header_.result_lo, header_.result_hi);
  }
  return Disposition::NORMAL;
}

}} // namespace facebook::logdevice
