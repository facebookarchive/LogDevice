/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include "logdevice/common/Request.h"
#include "logdevice/common/protocol/FixedSizeMessage.h"
#include "logdevice/common/protocol/Message.h"

namespace facebook { namespace logdevice {

/**
 * @file HELLO is the first message that LogDevice clients and nodes
 *       send into a new connection that they just actively opened. Its
 *       purpose is to inform the other end of the LD protocol version that
 *       this end speaks and supply credentials that the other end may use
 *       to authenticate us. The other end replies with an ACK.
 */

typedef uint64_t HELLO_flags_t;

struct HELLO_Header {
  // Min protocol version supported by the client.
  uint16_t proto_min;
  // Max protocol version supported by the client.
  uint16_t proto_max;

  // a bitset of flags
  HELLO_flags_t flags;

  // optional id of Request that initiated this connection. This value is
  // echoed back in the corresponding ACK. Intended use case is chained
  // sends with direct replies where all connections to recipients must be
  // positively ACK'ed before the message can be sent through a chain
  request_id_t rqid;

  static const size_t CREDS_SIZE_V1 = 128;
  // The buffer is used to store the authentication data needed when extracting
  // the principal for the connection. This buffer can be updated depending on
  // the authentication scheme that is being used.
  mutable char credentials[CREDS_SIZE_V1];
  // If set, this client is one of the nodes in the cluster. HELLO message
  // will include its NodeID.
  static constexpr HELLO_flags_t SOURCE_NODE = 1ul << 0;

  // If set, HELLO message will include the destination NodeID
  static constexpr HELLO_flags_t DESTINATION_NODE = 1ul << 1;

  // If set, the server will check that the client cluster name matches the
  // servers cluster name. The variable length cluster name will be sent over
  // the wire along side the length of the cluster name
  // (2 bytes + variable length data). Refer to serialize() for more
  // information.
  static constexpr HELLO_flags_t CLUSTER_NAME = 1ul << 2;

  // If set, the server will use CSID (Client Session ID)
  static constexpr HELLO_flags_t CSID = 1ul << 3;

  // If set, extra client build info is added to identify which client
  // we are talking to. 2 bytes length header + dynamic json string
  static constexpr HELLO_flags_t BUILD_INFO = 1ul << 4;

  // If set, HELLO message will include the client location
  static constexpr HELLO_flags_t CLIENT_LOCATION = 1ul << 5;
} __attribute__((__packed__));

/**
 * HELLO is a message composed of a fixed-size header optionally followed
 * by extra bits of information (depending on the flags set).  It is sent by
 * clients with protocol > 5.
 */
class HELLO_Message : public Message {
 public:
  explicit HELLO_Message(const HELLO_Header& header)
      : Message(MessageType::HELLO, TrafficClass::HANDSHAKE), header_(header) {}

  // implementation of the Message interface
  void serialize(ProtocolWriter&) const override;
  static Message::deserializer_t deserialize;
  Disposition onReceived(const Address& from) override;

  // fixed-size header
  const HELLO_Header header_;

  // if this message is sent by one of the nodes in the cluster, this will
  // store its id (see HELLO_Header::SOURCE_NODE)
  NodeID source_node_id_;

  // this will store the node id of the desired destination node
  // (see HELLO_Header::SOURCE_NODE)
  NodeID destination_node_id_;

  // if the HELLOv2 message has the CLUSTER_NODE_NAME flag set in the header,
  // this will store the name of the cluster that sent the message.
  std::string cluster_name_;

  // if the HELLOv2 message has the CSID flag set in the header,
  // this will store CSID (Client Session ID).
  std::string csid_;

  // contains extra client information in json when
  // the BUILD_INFO flag has been set
  std::string build_info_;

  // Location of the client (currently used for local SCD reading)
  std::string client_location_;

 private:
  HELLO_Message()
      : Message(MessageType::HELLO, TrafficClass::HANDSHAKE), header_() {}
};

}} // namespace facebook::logdevice
