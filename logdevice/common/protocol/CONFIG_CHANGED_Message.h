/**
 * Copyright (c) 2017-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include "logdevice/common/Worker.h"
#include "logdevice/common/protocol/ProtocolReader.h"
#include "logdevice/common/protocol/SimpleMessage.h"

namespace facebook { namespace logdevice {

/**
 * @file Notification of configuration change. Forces the receiver to either
 * reload or update the config.
 */

struct CONFIG_CHANGED_Header {
  enum class ConfigType : uint8_t { MAIN_CONFIG = 0, LOGS_CONFIG = 1 };
  enum class Action : uint8_t {
    // Used by RemoteLogsConfig to signal that current config should be
    // invalidated
    RELOAD = 0,
    // Used by config synchronization to provide the new config
    UPDATE = 1
  };
  uint64_t modified_time;
  config_version_t version;
  // Used to determine whether the config in the message body can be trusted.
  // If the config was originally sent from a server, it will have a valid
  // server_origin. Otherwise, it will be invalid.
  NodeID server_origin;
  ConfigType config_type;
  Action action;
  char hash[10];
};

static_assert(sizeof(CONFIG_CHANGED_Header) == 32,
              "CONFIG_CHANGED_Header is expected to be 32 bytes");

class CONFIG_CHANGED_Message : public Message {
 public:
  explicit CONFIG_CHANGED_Message(const CONFIG_CHANGED_Header& header)
      : // At most one CONFIG_CHANGED message is sent per config change, per
        // socket, so using a high priority TrafficClass here shouldn't be an
        // issue.
        Message(MessageType::CONFIG_CHANGED, TrafficClass::RECOVERY),
        header_(header) {}

  explicit CONFIG_CHANGED_Message(const CONFIG_CHANGED_Header& header,
                                  const std::string& config_str)
      : Message(MessageType::CONFIG_CHANGED, TrafficClass::RECOVERY),
        header_(header),
        config_str_(config_str) {}

  void serialize(ProtocolWriter&) const override;
  static Message::deserializer_t deserialize;

  Disposition onReceived(const Address& from) override;

 private:
  CONFIG_CHANGED_Header header_;
  std::string config_str_;
};

}} // namespace facebook::logdevice
