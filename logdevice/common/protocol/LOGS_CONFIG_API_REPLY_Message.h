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
#include "logdevice/common/protocol/FixedSizeMessage.h"

namespace facebook { namespace logdevice {

struct LOGS_CONFIG_API_REPLY_Header {
  request_id_t client_rqid;
  uint64_t config_version;
  Status status;
  uint32_t total_payload_size; // total size on first chunk, 0 otherwise

  LogsConfigRequestOrigin origin;

  static size_t headerSize(uint16_t proto) {
    return sizeof(LOGS_CONFIG_API_REPLY_Header);
  }
} __attribute__((__packed__));

class LOGS_CONFIG_API_REPLY_Message : public Message {
 public:
  using blob_size_t = uint32_t;

  static const size_t HEADER_SIZE = sizeof(LOGS_CONFIG_API_REPLY_Header);

  explicit LOGS_CONFIG_API_REPLY_Message(
      const LOGS_CONFIG_API_REPLY_Header& header,
      std::string&& payload);

  explicit LOGS_CONFIG_API_REPLY_Message();

  LOGS_CONFIG_API_REPLY_Message(LOGS_CONFIG_API_REPLY_Message&&) noexcept =
      delete;
  LOGS_CONFIG_API_REPLY_Message(const LOGS_CONFIG_API_REPLY_Message&) noexcept =
      delete;
  LOGS_CONFIG_API_REPLY_Message&
  operator=(const LOGS_CONFIG_API_REPLY_Message&) = delete;
  LOGS_CONFIG_API_REPLY_Message& operator=(LOGS_CONFIG_API_REPLY_Message&&) =
      delete;

  // see Message.h
  void serialize(ProtocolWriter&) const override;
  Disposition onReceived(const Address& from) override;
  void onSent(Status st, const Address& to) const override;
  static Message::deserializer_t deserialize;

  std::vector<std::pair<std::string, folly::dynamic>>
  getDebugInfo() const override;

 private:
  LOGS_CONFIG_API_REPLY_Header header_;
  std::string blob_;
};

}} // namespace facebook::logdevice
