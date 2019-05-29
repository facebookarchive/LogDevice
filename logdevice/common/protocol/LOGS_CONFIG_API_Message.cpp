/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/common/protocol/LOGS_CONFIG_API_Message.h"

#include <folly/Memory.h>

#include "logdevice/common/GetLogInfoRequest.h"
#include "logdevice/common/LogsConfigApiRequest.h"
#include "logdevice/common/Processor.h"
#include "logdevice/common/Sender.h"
#include "logdevice/common/Worker.h"
#include "logdevice/common/configuration/logs/LogsConfigManager.h"
#include "logdevice/common/protocol/LOGS_CONFIG_API_REPLY_Message.h"

namespace facebook { namespace logdevice {

void LOGS_CONFIG_API_Message::serialize(ProtocolWriter& writer) const {
  writer.write(&header_, LOGS_CONFIG_API_Header::headerSize(writer.proto()));
  const blob_size_t size = blob_.size();
  ld_check(blob_.size() <= Message::MAX_LEN -
               LOGS_CONFIG_API_Header::headerSize(writer.proto()) -
               sizeof(size));
  writer.write(size);
  if (size > 0) {
    writer.write(blob_.data(), size);
  }
}

MessageReadResult LOGS_CONFIG_API_Message::deserialize(ProtocolReader& reader) {
  std::unique_ptr<LOGS_CONFIG_API_Message> m(new LOGS_CONFIG_API_Message());

  // Set properties for old protocol (LOGS_CONFIG_API_SUBSCRIPTIONS)
  m->header_.origin = LogsConfigRequestOrigin::LOGS_CONFIG_API_REQUEST;
  m->header_.subscribe_to_config_ = false;

  reader.read(&m->header_, LOGS_CONFIG_API_Header::headerSize(reader.proto()));
  blob_size_t blob_length = 0;
  reader.read(&blob_length);
  if (blob_length) {
    m->blob_.resize(blob_length);
    reader.read(&m->blob_.front(), blob_length);
  }

  reader.allowTrailingBytes();
  return reader.resultMsg(std::move(m));
}

std::vector<std::pair<std::string, folly::dynamic>>
LOGS_CONFIG_API_Message::getDebugInfo() const {
  std::vector<std::pair<std::string, folly::dynamic>> res;

  res.emplace_back("client_rqid", header_.client_rqid.val());

  using Type = LOGS_CONFIG_API_Header::Type;
  switch (header_.request_type) {
    case Type::MUTATION_REQUEST:
      res.emplace_back("type", "mutation");
      res.emplace_back("delta_size", blob_.size());
      break;
    case Type::GET_DIRECTORY:
      res.emplace_back("type", "get_directory");
      res.emplace_back("path", blob_);
      break;
    case Type::GET_LOG_GROUP_BY_NAME:
      res.emplace_back("type", "get_log_group_by_name");
      res.emplace_back("path", blob_);
      break;
    case Type::GET_LOG_GROUP_BY_ID:
      res.emplace_back("type", "get_log_group_by_id");
      res.emplace_back("log_id", blob_);
      break;
  }

  return res;
}

void LOGS_CONFIG_API_Message::onSent(Status status, const Address& to) const {
  switch (header_.origin) {
    case LogsConfigRequestOrigin::LOGS_CONFIG_API_REQUEST: {
      if (status != E::OK) {
        auto& rqmap =
            Worker::onThisThread()->runningLogsConfigApiRequests().map;
        auto it = rqmap.find(header_.client_rqid);
        if (it != rqmap.end()) {
          it->second->onError(status);
        }
      }
      break;
    }
    case LogsConfigRequestOrigin::REMOTE_LOGS_CONFIG_REQUEST: {
      // Report to GetLogInfoFromNodeRequest of the send result
      auto& rqmap = Worker::onThisThread()->runningGetLogInfo().per_node_map;
      auto it = rqmap.find(header_.client_rqid);
      if (it != rqmap.end()) {
        it->second->onMessageSent(to.id_.node_, status);
      }
      break;
    }
  }
}

}} // namespace facebook::logdevice
