/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/common/protocol/CONFIG_CHANGED_Message.h"

#include <algorithm>
#include <iterator>

#include <folly/compression/Compression.h>

#include "logdevice/common/ConfigurationFetchRequest.h"
#include "logdevice/common/Sender.h"
#include "logdevice/common/Worker.h"
#include "logdevice/common/configuration/Configuration.h"
#include "logdevice/common/configuration/ServerConfig.h"
#include "logdevice/common/configuration/UpdateableConfig.h"
#include "logdevice/common/protocol/Compatibility.h"
#include "logdevice/common/protocol/ProtocolReader.h"
#include "logdevice/common/protocol/ProtocolWriter.h"
#include "logdevice/common/stats/Stats.h"

namespace facebook { namespace logdevice {

// TODO: Remove this when the min protocol version is >=
// Comptability::ProtocolVersion::RID_IN_CONFIG_MESSAGES.
// It's deprecated because the struct is not packed making it super hard to
// extend it or add new fields.
namespace {
struct CONFIG_CHANGED_Header_DEPRECATED {
  enum class ConfigType : uint8_t { MAIN_CONFIG = 0, LOGS_CONFIG = 1 };
  enum class Action : uint8_t { RELOAD = 0, UPDATE = 1 };
  uint64_t modified_time;
  config_version_t version;
  NodeID server_origin;
  ConfigType config_type;
  Action action;
  char hash[10];
  char padding[4] = {0};
};
static_assert(sizeof(CONFIG_CHANGED_Header_DEPRECATED) == 32,
              "CONFIG_CHANGED_Header_DEPRECATED is expected to be 32 bytes");
} // namespace

using config_str_size_t = uint32_t;

void CONFIG_CHANGED_Header::serialize(ProtocolWriter& writer) const {
  if (writer.proto() < Compatibility::ProtocolVersion::RID_IN_CONFIG_MESSAGES) {
    CONFIG_CHANGED_Header_DEPRECATED old_hdr{
        modified_time,
        getServerConfigVersion(),
        server_origin,
        static_cast<CONFIG_CHANGED_Header_DEPRECATED::ConfigType>(config_type),
        static_cast<CONFIG_CHANGED_Header_DEPRECATED::Action>(action),
        {}};
    std::copy(std::begin(hash), std::end(hash), std::begin(old_hdr.hash));
    writer.write(old_hdr);
  } else {
    writer.write(status);
    writer.write(rid);
    writer.write(modified_time);
    writer.write(server_origin);
    writer.write(config_type);
    writer.write(action);
    writer.write(hash);
    writer.write(version);
  }
}

CONFIG_CHANGED_Header
CONFIG_CHANGED_Header::deserialize(ProtocolReader& reader) {
  CONFIG_CHANGED_Header header;
  if (reader.proto() < Compatibility::ProtocolVersion::RID_IN_CONFIG_MESSAGES) {
    CONFIG_CHANGED_Header_DEPRECATED old_hdr;
    reader.read(&old_hdr);
    header = CONFIG_CHANGED_Header{
        old_hdr.modified_time,
        old_hdr.version,
        old_hdr.server_origin,
        static_cast<CONFIG_CHANGED_Header::ConfigType>(old_hdr.config_type),
        static_cast<CONFIG_CHANGED_Header::Action>(old_hdr.action)};
    std::copy(std::begin(old_hdr.hash),
              std::end(old_hdr.hash),
              std::begin(header.hash));
  } else {
    // Can't directly read into the struct as it's packed and the linter
    // complains. We need to use temp variables;
    Status status;
    request_id_t rid;
    uint64_t modified_time;
    NodeID server_origin;
    ConfigType config_type;
    Action action;
    std::array<char, 10> hash;
    uint64_t version;

    reader.read(&status);
    reader.read(&rid);
    reader.read(&modified_time);
    reader.read(&server_origin);
    reader.read(&config_type);
    reader.read(&action);
    reader.read(&hash);
    reader.read(&version);

    header = CONFIG_CHANGED_Header{status,
                                   rid,
                                   modified_time,
                                   version,
                                   server_origin,
                                   config_type,
                                   action};
    std::copy(std::begin(hash), std::end(hash), std::begin(header.hash));
  }
  return header;
}

void CONFIG_CHANGED_Message::serialize(ProtocolWriter& writer) const {
  header_.serialize(writer);

  if (writer.proto() < Compatibility::ProtocolVersion::RID_IN_CONFIG_MESSAGES) {
    if (header_.action == CONFIG_CHANGED_Header::Action::UPDATE) {
      ld_check(config_str_.size() <=
               Message::MAX_LEN - sizeof(header_) - sizeof(config_str_size_t));
      config_str_size_t size = config_str_.size();
      writer.write(size);
      writer.writeVector(config_str_);
    }
  } else {
    config_str_size_t size = config_str_.size();
    writer.write(size);
    writer.writeVector(config_str_);
  }
}

config_version_t CONFIG_CHANGED_Header::getServerConfigVersion() const {
  ld_check(version <= std::numeric_limits<uint32_t>::max());
  return config_version_t(version);
}

vcs_config_version_t CONFIG_CHANGED_Header::getVCSConfigVersion() const {
  return vcs_config_version_t(version);
}

MessageReadResult CONFIG_CHANGED_Message::deserialize(ProtocolReader& reader) {
  CONFIG_CHANGED_Header hdr = CONFIG_CHANGED_Header::deserialize(reader);
  std::string config_str;

  if (reader.proto() < Compatibility::ProtocolVersion::RID_IN_CONFIG_MESSAGES) {
    if (hdr.action == CONFIG_CHANGED_Header::Action::UPDATE) {
      config_str_size_t size = 0;
      reader.read(&size);
      config_str.resize(size);
      reader.read(const_cast<char*>(config_str.data()), config_str.size());
    }
  } else {
    config_str_size_t size;
    reader.read(&size);
    config_str.resize(size);
    reader.read(const_cast<char*>(config_str.data()), config_str.size());
  }
  return reader.result(
      [&] { return new CONFIG_CHANGED_Message(hdr, config_str); });
}

std::string CONFIG_CHANGED_Header::actionToString(Action a) {
  switch (a) {
    case Action::RELOAD:
      return "RELOAD";
    case Action::UPDATE:
      return "UPDATE";
    case Action::CALLBACK:
      return "CALLBACK";
  }
  return "(unknown)";
}

Message::Disposition CONFIG_CHANGED_Message::onReceived(const Address& from) {
  // If it's a CALLBACK message, it means that we asked for it before. So
  // not really interesting to log.
  if (header_.action != CONFIG_CHANGED_Header::Action::CALLBACK) {
    RATELIMIT_INFO(
        std::chrono::seconds(10),
        10,
        "CONFIG_CHANGED received from %s, action %s, version %u, status %s",
        Sender::describeConnection(from).c_str(),
        CONFIG_CHANGED_Header::actionToString(header_.action).c_str(),
        header_.getServerConfigVersion().val(),
        error_name(header_.status));
  }

  switch (header_.action) {
    case CONFIG_CHANGED_Header::Action::CALLBACK:
      return handleCallbackAction(from);
    case CONFIG_CHANGED_Header::Action::RELOAD:
      return handleReloadAction(from);
    case CONFIG_CHANGED_Header::Action::UPDATE:
      return handleUpdateAction(from);
    default:
      ld_error("Invalid CONFIG_CHANGED_header_::Action received");
      err = E::BADMSG;
      return Disposition::ERROR;
  }
}

Message::Disposition
CONFIG_CHANGED_Message::handleCallbackAction(const Address& from) {
  Worker* worker = Worker::onThisThread();
  auto& map = worker->runningConfigurationFetches().map;
  auto it = map.find(header_.rid);
  if (it == map.end()) {
    RATELIMIT_INFO(std::chrono::seconds(10),
                   10,
                   "CONFIG_CHANGED of action CALLBACK received from %s "
                   "but couldn't find the corresponding request. Ignoring.",
                   Sender::describeConnection(from).c_str());
    return Disposition::NORMAL;
  }
  it->second->onReply(header_, config_str_);
  return Disposition::NORMAL;
}

Message::Disposition
CONFIG_CHANGED_Message::handleReloadAction(const Address& from) {
  Worker* worker = Worker::onThisThread();
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

  int rv = worker->getUpdateableConfig()->updateableLogsConfig()->invalidate();
  if (rv != 0) {
    ld_error("Config reload failed with error %s", error_description(err));
    return Disposition::NORMAL;
  }
  WORKER_STAT_INCR(config_changed_reload);
  return Disposition::NORMAL;
}

Message::Disposition
CONFIG_CHANGED_Message::handleUpdateAction(const Address& from) {
  RATELIMIT_ERROR(
      std::chrono::seconds(10),
      1,
      "CONFIG_CHANGED message with action UPDATE is no longer supported");
  return Disposition::ERROR;
}

}} // namespace facebook::logdevice
