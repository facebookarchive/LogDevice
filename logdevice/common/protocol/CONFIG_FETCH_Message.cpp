/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/common/protocol/CONFIG_FETCH_Message.h"

#include <folly/Memory.h>

#include "logdevice/common/Processor.h"
#include "logdevice/common/Sender.h"
#include "logdevice/common/Worker.h"
#include "logdevice/common/configuration/Configuration.h"
#include "logdevice/common/configuration/nodes/NodesConfigurationCodec.h"
#include "logdevice/common/protocol/CONFIG_CHANGED_Message.h"

namespace facebook { namespace logdevice {

void CONFIG_FETCH_Header::serialize(ProtocolWriter& writer) const {
  if (writer.proto() >=
      Compatibility::ProtocolVersion::RID_IN_CONFIG_MESSAGES) {
    writer.write(rid);
    writer.write(my_version);
  }
  writer.write(config_type);
}

CONFIG_FETCH_Header CONFIG_FETCH_Header::deserialize(ProtocolReader& reader) {
  request_id_t rid;
  uint64_t my_version = 0;
  CONFIG_FETCH_Header::ConfigType config_type;

  if (reader.proto() >=
      Compatibility::ProtocolVersion::RID_IN_CONFIG_MESSAGES) {
    reader.read(&rid);
    reader.read(&my_version);
  }

  reader.read(&config_type);

  return CONFIG_FETCH_Header{
      rid,
      config_type,
      my_version,
  };
}

void CONFIG_FETCH_Message::serialize(ProtocolWriter& writer) const {
  header_.serialize(writer);
}

MessageReadResult CONFIG_FETCH_Message::deserialize(ProtocolReader& reader) {
  CONFIG_FETCH_Message message;
  message.header_ = CONFIG_FETCH_Header::deserialize(reader);
  return reader.result([message = std::move(message)] {
    return new CONFIG_FETCH_Message(message);
  });
}

bool CONFIG_FETCH_Message::isCallerWaitingForCallback() const {
  return header_.rid != REQUEST_ID_INVALID;
}

Message::Disposition CONFIG_FETCH_Message::onReceived(const Address& from) {
  RATELIMIT_INFO(std::chrono::seconds(20),
                 1,
                 "CONFIG_FETCH messages received. E.g., from %s",
                 Sender::describeConnection(from).c_str());

  switch (header_.config_type) {
    case CONFIG_FETCH_Header::ConfigType::LOGS_CONFIG:
      return handleLogsConfigRequest(from);
    case CONFIG_FETCH_Header::ConfigType::MAIN_CONFIG:
      return handleMainConfigRequest(from);
    case CONFIG_FETCH_Header::ConfigType::NODES_CONFIGURATION:
      return handleNodesConfigurationRequest(from);
  }

  ld_error("Received a CONFIG_FETCH message with an unknown ConfigType: %u. "
           "Ignoring it.",
           static_cast<uint8_t>(header_.config_type));
  return Disposition::ERROR;
}

Message::Disposition
CONFIG_FETCH_Message::handleMainConfigRequest(const Address& from) {
  auto config = getConfig();
  auto server_config = config->serverConfig();
  auto zk_config = config->zookeeperConfig();

  ServerConfig::ConfigMetadata metadata =
      server_config->getMainConfigMetadata();
  CONFIG_CHANGED_Header hdr{
      Status::OK,
      header_.rid,
      static_cast<uint64_t>(metadata.modified_time.count()),
      server_config->getVersion(),
      server_config->getServerOrigin(),
      CONFIG_CHANGED_Header::ConfigType::MAIN_CONFIG,
      isCallerWaitingForCallback() ? CONFIG_CHANGED_Header::Action::CALLBACK
                                   : CONFIG_CHANGED_Header::Action::UPDATE};
  metadata.hash.copy(hdr.hash, sizeof hdr.hash);

  std::unique_ptr<CONFIG_CHANGED_Message> msg;
  if (server_config->getVersion().val() <= header_.my_version) {
    // The requester already have an up to date version.
    hdr.status = Status::UPTODATE;
    msg = std::make_unique<CONFIG_CHANGED_Message>(hdr, "");
  } else {
    // We still send the Zookeeper section for backwards compatibility on
    // older servers, but on newer servers this is ignored
    // Clients already ignore / don't use the Zookeeper section
    // TODO deprecate in T32793726
    msg = std::make_unique<CONFIG_CHANGED_Message>(
        hdr, server_config->toString(nullptr, zk_config.get(), true));
  }

  int rv = sendMessage(std::move(msg), from);
  if (rv != 0) {
    ld_error("Sending CONFIG_CHANGED_Message to %s failed with error %s",
             from.toString().c_str(),
             error_description(err));
  }
  return Disposition::NORMAL;
}

Message::Disposition
CONFIG_FETCH_Message::handleLogsConfigRequest(const Address& from) {
  ld_error("CONFIG_FETCH for logs config is currently not supported.");
  err = E::BADMSG;
  auto bad_msg =
      std::make_unique<CONFIG_CHANGED_Message>(CONFIG_CHANGED_Header(err));

  int rv = sendMessage(std::move(bad_msg), from);
  if (rv != 0) {
    ld_error("Sending CONFIG_CHANGED_Message to %s failed with error %s",
             from.toString().c_str(),
             error_description(err));
  }
  return Disposition::ERROR;
}

Message::Disposition
CONFIG_FETCH_Message::handleNodesConfigurationRequest(const Address& from) {
  auto nodes_cfg = getNodesConfiguration();

  CONFIG_CHANGED_Header hdr{
      Status::OK,
      header_.rid,
      static_cast<uint64_t>(
          nodes_cfg->getLastChangeTimestamp().time_since_epoch().count()),
      nodes_cfg->getVersion(),
      getMyNodeID(),
      CONFIG_CHANGED_Header::ConfigType::NODES_CONFIGURATION,
      isCallerWaitingForCallback() ? CONFIG_CHANGED_Header::Action::CALLBACK
                                   : CONFIG_CHANGED_Header::Action::UPDATE};

  std::unique_ptr<CONFIG_CHANGED_Message> msg;
  if (nodes_cfg->getVersion().val() <= header_.my_version) {
    // The requester already have an up to date version.
    hdr.status = Status::UPTODATE;
    msg = std::make_unique<CONFIG_CHANGED_Message>(hdr, "");
  } else {
    auto serialized = configuration::nodes::NodesConfigurationCodec::serialize(
        *nodes_cfg, {/*compression=*/true});
    if (serialized == "" && err != Status::OK) {
      ld_error("Failed to serialize the NodesConfiguration with error %s",
               error_description(err));
      return Disposition::NORMAL;
    }
    msg = std::make_unique<CONFIG_CHANGED_Message>(hdr, serialized);
  }

  int rv = sendMessage(std::move(msg), from);
  if (rv != 0) {
    ld_error("Sending CONFIG_CHANGED_Message to %s failed with error %s",
             from.toString().c_str(),
             error_description(err));
  }
  return Disposition::NORMAL;
}

std::shared_ptr<Configuration> CONFIG_FETCH_Message::getConfig() {
  return Worker::onThisThread()->getConfig();
}

NodeID CONFIG_FETCH_Message::getMyNodeID() const {
  return Worker::onThisThread()->processor_->getMyNodeID();
}

std::shared_ptr<const configuration::nodes::NodesConfiguration>
CONFIG_FETCH_Message::getNodesConfiguration() {
  // always use the NCM based NC
  return Worker::onThisThread()->getNodesConfigurationFromNCMSource();
}

int CONFIG_FETCH_Message::sendMessage(
    std::unique_ptr<CONFIG_CHANGED_Message> msg,
    const Address& to) {
  return Worker::onThisThread()->sender().sendMessage(std::move(msg), to);
}

}} // namespace facebook::logdevice
