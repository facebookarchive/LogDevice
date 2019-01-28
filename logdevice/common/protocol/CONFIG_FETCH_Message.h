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
  ConfigType config_type;

  void serialize(ProtocolWriter&) const;
  static CONFIG_FETCH_Header deserialize(ProtocolReader& reader);
} __attribute__((__packed__));

static_assert(sizeof(CONFIG_FETCH_Header) == 1,
              "CONFIG_FETCH_Header is expected to be 1 byte");

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
