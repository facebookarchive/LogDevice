/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
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
    UPDATE = 1,
    // Used when there's some request that's waiting for this response.
    // rid should never be invalid in this case
    CALLBACK = 2,
  };

  CONFIG_CHANGED_Header() = default;

  explicit CONFIG_CHANGED_Header(Status status) : status(status) {}

  explicit CONFIG_CHANGED_Header(Status status,
                                 request_id_t rid,
                                 uint64_t modified_time,
                                 config_version_t version,
                                 NodeID server_origin,
                                 ConfigType config_type,
                                 Action action)
      : status(status),
        rid(rid),
        modified_time(modified_time),
        version(version),
        server_origin(server_origin),
        config_type(config_type),
        action(action) {}

  explicit CONFIG_CHANGED_Header(uint64_t modified_time,
                                 config_version_t version,
                                 NodeID server_origin,
                                 ConfigType config_type,
                                 Action action)
      : modified_time(modified_time),
        version(version),
        server_origin(server_origin),
        config_type(config_type),
        action(action) {}

  static CONFIG_CHANGED_Header deserialize(ProtocolReader& reader);

  void serialize(ProtocolWriter& writer) const;

  Status status{Status::OK};
  request_id_t rid{REQUEST_ID_INVALID};
  uint64_t modified_time;
  config_version_t version;
  // Used to determine whether the config in the message body can be trusted.
  // If the config was originally sent from a server, it will have a valid
  // server_origin. Otherwise, it will be invalid.
  NodeID server_origin;
  ConfigType config_type;
  Action action;
  char hash[10] = {0};

} __attribute__((__packed__));

static_assert(sizeof(CONFIG_CHANGED_Header) == 38,
              "CONFIG_CHANGED_Header is expected to be 38 bytes");

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

  const CONFIG_CHANGED_Header& getHeader() const {
    return header_;
  }

  const std::string& getConfigStr() const {
    return config_str_;
  }

 private:
  Disposition handleCallbackAction(const Address& from);
  Disposition handleReloadAction(const Address& from);
  Disposition handleUpdateAction(const Address& from);

 private:
  CONFIG_CHANGED_Header header_;
  std::string config_str_;
};

}} // namespace facebook::logdevice
