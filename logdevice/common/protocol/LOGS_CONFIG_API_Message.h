/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include "logdevice/common/LogsConfigRequestOrigin.h"
#include "logdevice/common/Request.h"
#include "logdevice/common/configuration/TrafficClass.h"
#include "logdevice/common/protocol/FixedSizeMessage.h"

namespace facebook { namespace logdevice {

/**
 * @file Message sent by the client to a node in the cluster to mutate or get a
 * node in the LogsConfigTree managed by LogsConfigManager.
 */

struct LOGS_CONFIG_API_Header {
  enum class Type : uint8_t {
    /*
     * MUTATION_REQUEST means that this request has a payload that is a child of
     * the Delta class.
     */
    MUTATION_REQUEST = 0,
    GET_DIRECTORY,         // payload is path string
    GET_LOG_GROUP_BY_NAME, // payload is path string
    GET_LOG_GROUP_BY_ID,   // payload is log id as string
  };

  request_id_t client_rqid;
  Type request_type;

  // When set to true, the sender of this message will be notified
  // of any log config changes
  bool subscribe_to_config_;
  LogsConfigRequestOrigin origin;

  static size_t headerSize(uint16_t proto) {
    return sizeof(LOGS_CONFIG_API_Header);
  }
} __attribute__((__packed__));

static_assert((uint8_t)LOGS_CONFIG_API_Header::Type::GET_LOG_GROUP_BY_NAME == 2,
              "");

class LOGS_CONFIG_API_Message : public Message {
  using blob_size_t = uint32_t;

 public:
  explicit LOGS_CONFIG_API_Message(const LOGS_CONFIG_API_Header& header)
      : Message(MessageType::LOGS_CONFIG_API, TrafficClass::HANDSHAKE),
        header_(header) {}

  explicit LOGS_CONFIG_API_Message(const LOGS_CONFIG_API_Header& header,
                                   std::string blob)
      : Message(MessageType::LOGS_CONFIG_API, TrafficClass::HANDSHAKE),
        header_(header),
        blob_(std::move(blob)) {}

  void serialize(ProtocolWriter& writer) const override;

  static MessageReadResult deserialize(ProtocolReader& reader);

  Disposition onReceived(const Address&) override {
    // Receipt handler lives in server/LOGS_CONFIG_API_onReceived.cpp;
    std::abort();
  }

  void onSent(Status st, const Address& to) const override;

  std::vector<std::pair<std::string, folly::dynamic>>
  getDebugInfo() const override;

  LOGS_CONFIG_API_Header header_;
  std::string blob_;

 protected:
  // Used by deserialization
  LOGS_CONFIG_API_Message()
      : Message(MessageType::LOGS_CONFIG_API, TrafficClass::HANDSHAKE),
        header_() {}
};

}} // namespace facebook::logdevice
