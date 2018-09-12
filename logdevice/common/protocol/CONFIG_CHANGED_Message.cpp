/**
 * Copyright (c) 2017-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "CONFIG_CHANGED_Message.h"

#include <folly/compression/Compression.h>

#include "logdevice/common/configuration/Configuration.h"
#include "logdevice/common/Sender.h"
#include "logdevice/common/Socket.h"
#include "logdevice/common/configuration/UpdateableConfig.h"
#include "logdevice/common/Worker.h"
#include "logdevice/common/configuration/ServerConfig.h"
#include "logdevice/common/protocol/ProtocolReader.h"
#include "logdevice/common/protocol/ProtocolWriter.h"
#include "logdevice/common/stats/Stats.h"

namespace facebook { namespace logdevice {

using config_str_size_t = uint32_t;

void CONFIG_CHANGED_Message::serialize(ProtocolWriter& writer) const {
  writer.write(header_);

  if (header_.action == CONFIG_CHANGED_Header::Action::UPDATE) {
    ld_check(config_str_.size() <=
             Message::MAX_LEN - sizeof(header_) - sizeof(config_str_size_t));
    config_str_size_t size = config_str_.size();
    writer.write(size);
    writer.writeVector(config_str_);
  }
}

MessageReadResult CONFIG_CHANGED_Message::deserialize(ProtocolReader& reader) {
  CONFIG_CHANGED_Header hdr;
  reader.read(&hdr);
  std::string config_str;
  if (hdr.action == CONFIG_CHANGED_Header::Action::UPDATE) {
    config_str_size_t size = 0;
    reader.read(&size);
    config_str.resize(size);
    reader.read(const_cast<char*>(config_str.data()), config_str.size());
  }
  return reader.result(
      [&] { return new CONFIG_CHANGED_Message(hdr, config_str); });
}

Message::Disposition CONFIG_CHANGED_Message::onReceived(const Address& from) {
  RATELIMIT_INFO(std::chrono::seconds(10),
                 10,
                 "CONFIG_CHANGED received from %s",
                 Sender::describeConnection(from).c_str());

  Worker* worker = Worker::onThisThread();
  if (header_.action == CONFIG_CHANGED_Header::Action::RELOAD) {
    // this should only happen if we're using RemoteLogsConfig
    auto logs_config = worker->getLogsConfig();
    if (logs_config->isLocal()) {
      RATELIMIT_ERROR(std::chrono::seconds(10),
                      10,
                      "Received CONFIG_CHANGED with RELOAD action from %s "
                      "but the LogsConfig is local!",
                      from.toString().c_str());
      return Disposition::NORMAL;
    }
    if (from.isClientAddress()) {
      RATELIMIT_CRITICAL(std::chrono::seconds(10),
                         10,
                         "Received CONFIG_CHANGED with RELOAD action from %s "
                         "but should only be sent by servers",
                         from.toString().c_str());
      return Disposition::NORMAL;
    }
    if (!logs_config->useConfigChangedMessageFrom(from.asNodeID())) {
      // this is a stale reply - we don't care about this node's view of the
      // config anymore
      return Disposition::NORMAL;
    }

    int rv =
        worker->getUpdateableConfig()->updateableLogsConfig()->invalidate();
    if (rv != 0) {
      ld_error("Config reload failed with error %s", error_description(err));
      return Disposition::NORMAL;
    }
    WORKER_STAT_INCR(config_changed_reload);
    return Disposition::NORMAL;
  }

  if (header_.action != CONFIG_CHANGED_Header::Action::UPDATE) {
    ld_error("Invalid CONFIG_CHANGED_header_::Action received");
    err = E::BADMSG;
    return Disposition::ERROR;
  }

  if (header_.config_type != CONFIG_CHANGED_Header::ConfigType::MAIN_CONFIG) {
    ld_error("Only CONFIG_CHANGED_header_::ConfigType::MAIN_CONFIG is "
             "supported for CONFIG_CHANGED updates");
    err = E::BADMSG;
    return Disposition::ERROR;
  }

  auto current_server_config = worker->getServerConfig();
  auto updateable_server_config =
      worker->getUpdateableConfig()->updateableServerConfig();

  // Update last known version if needed, so that any CONFIG_ADVISORY coming
  // after that will not trigger fetching the config, in case this message
  // already contains the most recent config.
  // Note we don't care if the update succeeded. if it failed, it means that
  // either we have the most recent config, or we are waiting for it.
  updateable_server_config->updateLastKnownVersion(header_.version);

  // Now check if we already have the latest config
  if (header_.version <= current_server_config->getVersion()) {
    // Config version is already up to date, so we ignore the update
    WORKER_STAT_INCR(config_changed_ignored_uptodate);
    ld_debug("CONFIG_CHANGED_Message from %s ignored",
             Sender::describeConnection(from).c_str());
    return Disposition::NORMAL;
  }

  // Check if we have already received a more recent config.
  // At this point, if that condition fails, it means that we are already
  // processing a previously received CONFIG_CHANGED, but haven't updated the
  // config yet. We can ignore this message to avoid wasting resources.
  if (!updateable_server_config->updateLastReceivedVersion(header_.version)) {
    // A CONFIG_UPDATE message for the same version or more recent, has already
    // been received.
    WORKER_STAT_INCR(config_changed_ignored_update_in_progress);
    ld_debug("CONFIG_CHANGED_Message from %s ignored since another message for "
             "same version or newer has already been received.",
             Sender::describeConnection(from).c_str());
    return Disposition::NORMAL;
  }

  using folly::IOBuf;
  std::unique_ptr<IOBuf> input =
      IOBuf::wrapBuffer(config_str_.data(), config_str_.size());
  auto codec = folly::io::getCodec(folly::io::CodecType::GZIP);
  std::unique_ptr<IOBuf> uncompressed;
  try {
    uncompressed = codec->uncompress(input.get());
  } catch (const std::runtime_error& ex) {
    ld_error("gzip decompression of CONFIG_CHANGED_Message body failed");
    err = E::BADMSG;
    return Disposition::ERROR;
  }
  std::string config_json = uncompressed->moveToFbString().toStdString();

  // Parse configuration with empty logs config
  std::unique_ptr<ServerConfig> server_config =
      ServerConfig::fromJson(config_json);
  if (!server_config) {
    ld_error("Parsing CONFIG_CHANGED_Message body failed with error %s",
             error_description(err));
    err = E::BADMSG;
    return Disposition::ERROR;
  }

  // Update config metadata
  ServerConfig::ConfigMetadata metadata;
  metadata.hash = std::string(header_.hash, sizeof header_.hash);
  metadata.uri = "CONFIG_CHANGED_Message:" + from.toString();
  using namespace std::chrono;
  metadata.modified_time = milliseconds(header_.modified_time);
  metadata.loaded_time =
      duration_cast<milliseconds>(system_clock::now().time_since_epoch());
  server_config->setMainConfigMetadata(metadata);

  server_config->setServerOrigin(header_.server_origin);

  if (from.isClientAddress()) { // Check is needed for the ServerConfigSource
    if (!header_.server_origin.isNodeID() ||
        !current_server_config->getNode(header_.server_origin) ||
        current_server_config->getClusterCreationTime() !=
            server_config->getClusterCreationTime() ||
        current_server_config->getClusterName() !=
            server_config->getClusterName()) {
      ld_info("Untrustworthy CONFIG_CHANGED_Message received. Fetching from "
              "source instead.");
      WORKER_STAT_INCR(config_changed_ignored_not_trusted);
      int rv = updateable_server_config->getUpdater()->fetchFromSource();
      if (rv != 0) {
        ld_warning("Failed to fetch config from source with error %s",
                   error_description(err));
      }
      return Disposition::NORMAL;
    }
  }

  updateable_server_config->update(std::move(server_config));
  WORKER_STAT_INCR(config_changed_update);
  ld_info("Config updated to version %u by CONFIG_CHANGED_Message from %s",
          header_.version.val(),
          Sender::describeConnection(from).c_str());

  worker->sender().setPeerConfigVersion(from, *this, header_.version);

  return Disposition::NORMAL;
}

}} // namespace facebook::logdevice
