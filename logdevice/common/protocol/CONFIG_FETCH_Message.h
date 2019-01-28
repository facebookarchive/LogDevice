/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include "logdevice/common/protocol/ProtocolReader.h"

namespace facebook { namespace logdevice {

/**
 * @file Used to fetch a config from another host. Receiver should send a
 *       CONFIG_CHANGED message in response.
 */

struct CONFIG_FETCH_Header {
  enum class ConfigType : uint8_t { MAIN_CONFIG = 0, LOGS_CONFIG = 1 };

  CONFIG_FETCH_Header() = default;
  CONFIG_FETCH_Header(request_id_t rid, ConfigType config_type)
      : rid(rid), config_type(config_type) {}
  explicit CONFIG_FETCH_Header(ConfigType config_type)
      : rid(REQUEST_ID_INVALID), config_type(config_type) {}

  void serialize(ProtocolWriter&) const;
  static CONFIG_FETCH_Header deserialize(ProtocolReader& reader);

  request_id_t rid{REQUEST_ID_INVALID};
  ConfigType config_type;
} __attribute__((__packed__));

static_assert(sizeof(CONFIG_FETCH_Header) == 9,
              "CONFIG_FETCH_Header is expected to be 9 byte");

class CONFIG_FETCH_Message : public Message {
 public:
  CONFIG_FETCH_Message() : CONFIG_FETCH_Message(CONFIG_FETCH_Header()) {}

  explicit CONFIG_FETCH_Message(const CONFIG_FETCH_Header& header)
      : Message(MessageType::CONFIG_FETCH, TrafficClass::RECOVERY),
        header_(header) {}

  void serialize(ProtocolWriter&) const override;
  static Message::deserializer_t deserialize;

  Disposition onReceived(const Address& from) override;

  const CONFIG_FETCH_Header& getHeader() const {
    return header_;
  }

 private:
  CONFIG_FETCH_Header header_;
};

}} // namespace facebook::logdevice
