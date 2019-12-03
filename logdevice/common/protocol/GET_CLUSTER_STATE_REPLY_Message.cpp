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
#include "logdevice/common/protocol/Compatibility.h"
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
                        std::move(boycotted_nodes_),
                        std::move(nodes_status_));
  }

  return Disposition::NORMAL;
}

MessageReadResult
GET_CLUSTER_STATE_REPLY_Message::deserialize(ProtocolReader& reader) {
  auto msg = std::make_unique<GET_CLUSTER_STATE_REPLY_Message>();
  reader.read(&msg->header_);
  if (msg->header_.status == E::OK) {
    if (reader.proto() < Compatibility::ProtocolVersion::
                             NODE_STATUS_AND_HASHMAP_SUPPORT_IN_CLUSTER_STATE) {
      // Read vector of states
      std::vector<uint8_t> node_state_vector;
      reader.readLengthPrefixedVector(&node_state_vector);
      for (int i = 0; i < node_state_vector.size(); i++) {
        std::pair<node_index_t, uint16_t> node_state(i, node_state_vector[i]);
        msg->nodes_state_.emplace_back(node_state);
      }
    } else {
      // reader.proto() >=
      // Compatibility::ProtocolVersion::NODE_STATUS_AND_HASHMAP_SUPPORT_IN_CLUSTER_STATE
      reader.readLengthPrefixedVector(&msg->nodes_state_);
    }
    reader.readLengthPrefixedVector(&msg->boycotted_nodes_);
    if (reader.proto() >=
        Compatibility::ProtocolVersion::
            NODE_STATUS_AND_HASHMAP_SUPPORT_IN_CLUSTER_STATE) {
      reader.readLengthPrefixedVector(&msg->nodes_status_);
    }
  }
  return reader.resultMsg(std::move(msg));
}

void GET_CLUSTER_STATE_REPLY_Message::serialize(ProtocolWriter& writer) const {
  writer.write(header_);
  if (header_.status == E::OK) {
    if (writer.proto() < Compatibility::ProtocolVersion::
                             NODE_STATUS_AND_HASHMAP_SUPPORT_IN_CLUSTER_STATE) {
      // Write vector of states
      // pair vector is sorted by node ids, so max node id can be easily
      // located (+1 assuming node ids start from 0)
      std::vector<uint8_t> node_state_vector(
          std::get<0>(nodes_state_[nodes_state_.size() - 1]) + 1,
          ClusterStateNodeState::DEAD);
      for (auto& [node_idx, state] : nodes_state_) {
        if (node_idx < node_state_vector.size()) {
          node_state_vector[node_idx] = static_cast<uint8_t>(state);
        }
      }
      writer.writeLengthPrefixedVector(node_state_vector);
    } else {
      // writer.proto() >=
      // Compatibility::ProtocolVersion::NODE_STATUS_AND_HASHMAP_SUPPORT_IN_CLUSTER_STATE
      writer.writeLengthPrefixedVector(nodes_state_);
    }
    writer.writeLengthPrefixedVector(boycotted_nodes_);
    if (writer.proto() >=
        Compatibility::ProtocolVersion::
            NODE_STATUS_AND_HASHMAP_SUPPORT_IN_CLUSTER_STATE) {
      writer.writeLengthPrefixedVector(nodes_status_);
    }
  }
}

}} // namespace facebook::logdevice
