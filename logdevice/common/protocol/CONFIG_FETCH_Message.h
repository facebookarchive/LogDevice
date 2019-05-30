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

namespace configuration { namespace nodes {
class NodesConfiguration;
}} // namespace configuration::nodes

class Configuration;
class CONFIG_CHANGED_Message;

/**
 * @file Used to fetch a config from another host. Receiver should send a
 *       CONFIG_CHANGED message in response.
 */

struct CONFIG_FETCH_Header {
  enum class ConfigType : uint8_t {
    MAIN_CONFIG = 0,
    LOGS_CONFIG = 1,
    NODES_CONFIGURATION = 2
  };

  CONFIG_FETCH_Header() = default;
  CONFIG_FETCH_Header(request_id_t rid,
                      ConfigType config_type,
                      uint64_t my_version = 0)
      : rid(rid), my_version(my_version), config_type(config_type) {}

  explicit CONFIG_FETCH_Header(ConfigType config_type, uint64_t my_version = 0)
      : CONFIG_FETCH_Header(REQUEST_ID_INVALID, config_type, my_version) {}

  void serialize(ProtocolWriter&) const;
  static CONFIG_FETCH_Header deserialize(ProtocolReader& reader);

  // If the request ID is set, it means that whoever sent this message is
  // interested in the reply so the response action should be CALLBACK.
  // Otherwise, the caller is only interested in the side effects.
  request_id_t rid{REQUEST_ID_INVALID};

  // Used to do conditionall fetch. If the version of config being fetched is
  // smaller than or equal this version, the CONFIG_CHANGED response will have
  // a UPTODATE status. If it's set to zero (default), a config will always be
  // sent.
  uint64_t my_version;
  ConfigType config_type;
} __attribute__((__packed__));

static_assert(sizeof(CONFIG_FETCH_Header) == 17,
              "CONFIG_FETCH_Header is expected to be 17 byte");

class CONFIG_FETCH_Message : public Message {
 public:
  CONFIG_FETCH_Message() : CONFIG_FETCH_Message(CONFIG_FETCH_Header()) {}

  explicit CONFIG_FETCH_Message(const CONFIG_FETCH_Header& header)
      : Message(MessageType::CONFIG_FETCH, TrafficClass::RECOVERY),
        header_(header) {}

  virtual ~CONFIG_FETCH_Message() override {}

  void serialize(ProtocolWriter&) const override;
  static Message::deserializer_t deserialize;

  Disposition onReceived(const Address& from) override;

  const CONFIG_FETCH_Header& getHeader() const {
    return header_;
  }

  bool isCallerWaitingForCallback() const;

 protected:
  // To be overridden in tests
  virtual std::shared_ptr<Configuration> getConfig();
  virtual NodeID getMyNodeID() const;
  virtual std::shared_ptr<const configuration::nodes::NodesConfiguration>
  getNodesConfiguration();

  virtual int sendMessage(std::unique_ptr<CONFIG_CHANGED_Message> msg,
                          const Address& to);

 private:
  Disposition handleMainConfigRequest(const Address& from);
  Disposition handleLogsConfigRequest(const Address& from);
  Disposition handleNodesConfigurationRequest(const Address& from);

 private:
  CONFIG_FETCH_Header header_;
};

}} // namespace facebook::logdevice
