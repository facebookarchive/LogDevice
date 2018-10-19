/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <vector>

#include "logdevice/common/Request.h"
#include "logdevice/common/protocol/Message.h"

namespace facebook { namespace logdevice {

struct GET_CLUSTER_STATE_REPLY_Header {
  request_id_t client_rqid;

  /**
   * GET_CLUSTER_STATE response status can be one of
   * E::OK - the server was able to retrieve the list of dead nodes and
   *         the client may extract it from the content of the message.
   * E::NOTSUPPORTED - the failure detector is not available (not enabled or
   *                   not initialized).
   * E::NOTREADY - the state of the failure detector is likely not up to date
   *               to be shared with clients.
   */
  Status status;
} __attribute__((__packed__));

class GET_CLUSTER_STATE_REPLY_Message : public Message {
 public:
  GET_CLUSTER_STATE_REPLY_Message(const GET_CLUSTER_STATE_REPLY_Header& hdr,
                                  std::vector<uint8_t> nodes_state,
                                  std::vector<node_index_t> boycotted_nodes)
      : Message(MessageType::GET_CLUSTER_STATE_REPLY,
                TrafficClass::FAILURE_DETECTOR),
        header_(hdr),
        nodes_state_(nodes_state),
        boycotted_nodes_(boycotted_nodes) {}

  GET_CLUSTER_STATE_REPLY_Message()
      : Message(MessageType::GET_CLUSTER_STATE_REPLY,
                TrafficClass::FAILURE_DETECTOR),
        header_({}),
        nodes_state_() {}

  void serialize(ProtocolWriter&) const override;
  static Message::deserializer_t deserialize;

  Message::Disposition onReceived(const Address& from) override;

 private:
  GET_CLUSTER_STATE_REPLY_Header header_;
  std::vector<uint8_t> nodes_state_;
  std::vector<node_index_t> boycotted_nodes_;
};

}} // namespace facebook::logdevice
