/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/common/protocol/CONFIG_FETCH_Message.h"

#include <folly/Memory.h>

#include "logdevice/common/Sender.h"
#include "logdevice/common/Worker.h"
#include "logdevice/common/configuration/Configuration.h"
#include "logdevice/common/protocol/CONFIG_CHANGED_Message.h"

namespace facebook { namespace logdevice {

void CONFIG_FETCH_Header::serialize(ProtocolWriter& writer) const {
  if (writer.proto() >=
      Compatibility::ProtocolVersion::RID_IN_CONFIG_MESSAGES) {
    writer.write(rid);
  }
  writer.write(config_type);
}

CONFIG_FETCH_Header CONFIG_FETCH_Header::deserialize(ProtocolReader& reader) {
  request_id_t rid;
  CONFIG_FETCH_Header::ConfigType config_type;

  if (reader.proto() >=
      Compatibility::ProtocolVersion::RID_IN_CONFIG_MESSAGES) {
    reader.read(&rid);
  }

  reader.read(&config_type);

  return CONFIG_FETCH_Header{
      rid,
      config_type,
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

Message::Disposition CONFIG_FETCH_Message::onReceived(const Address& from) {
  ld_info("CONFIG_FETCH received from %s",
          Sender::describeConnection(from).c_str());

  if (header_.config_type == CONFIG_FETCH_Header::ConfigType::LOGS_CONFIG) {
    ld_error("CONFIG_FETCH for logs config is currently not supported.");
    err = E::BADMSG;
    // TODO send a response here in the next diff.
    return Disposition::ERROR;
  }

  Worker* worker = Worker::onThisThread();
  auto config = worker->getConfig();
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
      CONFIG_CHANGED_Header::Action::UPDATE};
  metadata.hash.copy(hdr.hash, sizeof hdr.hash);

  // We still send the Zookeeper section for backwards compatibility on
  // older servers, but on newer servers this is ignored
  // Clients already ignore / don't use the Zookeeper section
  // TODO deprecate in T32793726
  std::unique_ptr<CONFIG_CHANGED_Message> msg =
      std::make_unique<CONFIG_CHANGED_Message>(
          hdr, server_config->toString(nullptr, zk_config.get(), true));

  int rv = worker->sender().sendMessage(std::move(msg), from);
  if (rv != 0) {
    ld_error("Sending CONFIG_CHANGED_Message to %s failed with error %s",
             from.toString().c_str(),
             error_description(err));
  }
  return Disposition::NORMAL;
}

}} // namespace facebook::logdevice
