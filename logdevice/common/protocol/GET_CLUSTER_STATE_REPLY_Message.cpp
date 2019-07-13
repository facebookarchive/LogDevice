/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/common/protocol/GET_CLUSTER_STATE_REPLY_Message.h"

#include "logdevice/common/GetClusterStateRequest.h"
#include "logdevice/common/Worker.h"
#include "logdevice/common/protocol/ProtocolReader.h"
#include "logdevice/common/protocol/ProtocolWriter.h"

namespace facebook { namespace logdevice {

Message::Disposition
GET_CLUSTER_STATE_REPLY_Message::onReceived(const Address& from) {
  auto& rqmap = Worker::onThisThread()->runningGetClusterState().map;
  auto it = rqmap.find(header_.client_rqid);
  if (it != rqmap.end()) {
    it->second->onReply(from,
                        header_.status,
                        std::move(nodes_state_),
                        std::move(boycotted_nodes_));
  }

  return Disposition::NORMAL;
}

MessageReadResult
GET_CLUSTER_STATE_REPLY_Message::deserialize(ProtocolReader& reader) {
  auto msg = std::make_unique<GET_CLUSTER_STATE_REPLY_Message>();
  reader.read(&msg->header_);
  if (msg->header_.status == E::OK) {
    reader.readLengthPrefixedVector(&msg->nodes_state_);
    reader.readLengthPrefixedVector(&msg->boycotted_nodes_);
  }
  return reader.resultMsg(std::move(msg));
}

void GET_CLUSTER_STATE_REPLY_Message::serialize(ProtocolWriter& writer) const {
  writer.write(header_);
  if (header_.status == E::OK) {
    auto nodes_state(nodes_state_);
    writer.writeLengthPrefixedVector(nodes_state);
    writer.writeLengthPrefixedVector(boycotted_nodes_);
  }
}

}} // namespace facebook::logdevice
