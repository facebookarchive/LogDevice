/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/common/protocol/LOGS_CONFIG_API_REPLY_Message.h"

#include "logdevice/common/GetLogInfoRequest.h"
#include "logdevice/common/LogsConfigApiRequest.h"
#include "logdevice/common/Sender.h"
#include "logdevice/common/Worker.h"

namespace facebook { namespace logdevice {

LOGS_CONFIG_API_REPLY_Message::LOGS_CONFIG_API_REPLY_Message(
    const LOGS_CONFIG_API_REPLY_Header& header,
    std::string&& payload)
    : Message(MessageType::LOGS_CONFIG_API_REPLY, TrafficClass::HANDSHAKE),
      header_(header),
      blob_(payload) {}

LOGS_CONFIG_API_REPLY_Message::LOGS_CONFIG_API_REPLY_Message()
    : Message(MessageType::LOGS_CONFIG_API_REPLY, TrafficClass::HANDSHAKE),
      header_() {}

void LOGS_CONFIG_API_REPLY_Message::serialize(ProtocolWriter& writer) const {
  blob_size_t size = blob_.size();

  ld_debug("Using new LOGS_CONFIG_API_REPLY_Message protocol");
  writer.write(
      &header_, LOGS_CONFIG_API_REPLY_Header::headerSize(writer.proto()));
  writer.write(size);
  if (size > 0) {
    writer.write(blob_.data(), size);
  }
}

MessageReadResult
LOGS_CONFIG_API_REPLY_Message::deserialize(ProtocolReader& reader) {
  blob_size_t blob_length;
  auto m = std::make_unique<LOGS_CONFIG_API_REPLY_Message>();

  // Set fields for older protocol
  m->header_.origin = LogsConfigRequestOrigin::LOGS_CONFIG_API_REQUEST;

  reader.read(
      &m->header_, LOGS_CONFIG_API_REPLY_Header::headerSize(reader.proto()));
  reader.read(&blob_length);
  reader.readVector(&(m->blob_), blob_length);
  reader.allowTrailingBytes();
  return reader.resultMsg(std::move(m));
}

std::vector<std::pair<std::string, folly::dynamic>>
LOGS_CONFIG_API_REPLY_Message::getDebugInfo() const {
  std::vector<std::pair<std::string, folly::dynamic>> res;

  res.emplace_back("client_rqid", header_.client_rqid.val());
  res.emplace_back("config_version", header_.config_version);
  res.emplace_back("status", error_name(header_.status));
  res.emplace_back("total_payload_size", header_.total_payload_size);
  res.emplace_back("origin", toString(header_.origin));
  res.emplace_back("blob_size", blob_.size());

  return res;
}

void LOGS_CONFIG_API_REPLY_Message::onSent(Status st, const Address& to) const {
  Message::onSent(st, to);
}

Message::Disposition
LOGS_CONFIG_API_REPLY_Message::onReceived(const Address& from) {
  if (from.isClientAddress()) {
    ld_error("got LOGS_CONFIG_API_REPLY message from client %s",
             Sender::describeConnection(from).c_str());
    err = E::PROTO;
    return Disposition::ERROR;
  }

  switch (header_.origin) {
    case LogsConfigRequestOrigin::LOGS_CONFIG_API_REQUEST: {
      auto& rqmap = Worker::onThisThread()->runningLogsConfigApiRequests().map;
      auto it = rqmap.find(header_.client_rqid);
      if (it != rqmap.end()) {
        it->second->onReply(from.id_.node_,
                            header_.status,
                            header_.config_version,
                            std::move(blob_),
                            header_.total_payload_size);
      }
      break;
    }
    case LogsConfigRequestOrigin::REMOTE_LOGS_CONFIG_REQUEST: {
      auto& rqmap = Worker::onThisThread()->runningGetLogInfo().per_node_map;
      auto it = rqmap.find(header_.client_rqid);
      if (it != rqmap.end()) {
        it->second->onReply(from.id_.node_,
                            header_.status,
                            header_.config_version,
                            std::move(blob_),
                            header_.total_payload_size);
      }
      break;
    }
  }

  return Disposition::NORMAL;
}
}} // namespace facebook::logdevice
