/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/common/protocol/STARTED_Message.h"

#include "logdevice/common/EpochRecovery.h"
#include "logdevice/common/Processor.h"
#include "logdevice/common/Sender.h"
#include "logdevice/common/Worker.h"
#include "logdevice/common/client_read_stream/AllClientReadStreams.h"
#include "logdevice/common/debug.h"
#include "logdevice/common/protocol/ProtocolReader.h"
#include "logdevice/common/protocol/ProtocolWriter.h"

namespace facebook { namespace logdevice {

STARTED_Message::STARTED_Message(const STARTED_Header& header,
                                 TrafficClass tc,
                                 Source source,
                                 lsn_t starting_read_ptr)
    : Message(MessageType::STARTED, tc),
      header_(header),
      source_(source),
      starting_read_ptr_(starting_read_ptr) {}

Message::Disposition STARTED_Message::onReceived(const Address& from) {
  if (from.isClientAddress()) {
    ld_error("got STARTED message from client %s",
             Sender::describeConnection(from).c_str());
    err = E::PROTO;
    return Disposition::ERROR;
  }

  if (header_.log_id == LOGID_INVALID) {
    RATELIMIT_ERROR(std::chrono::seconds(1),
                    10,
                    "PROTOCOL ERROR: got STARTED message with an invalid "
                    "log id:%lu, and read stream id:%lu from %s",
                    header_.log_id.val(),
                    header_.read_stream_id.val_,
                    Sender::describeConnection(from).c_str());
    err = E::PROTO;
    return Disposition::ERROR;
  }

  auto w = Worker::onThisThread();
  const auto& nc = w->getNodesConfiguration();
  if (!nc->isNodeInServiceDiscoveryConfig(from.asNodeID().index())) {
    ld_check(false);
    err = E::PROTO;
    return Disposition::ERROR;
  }

  shard_size_t n_shards = nc->getNumShards(from.asNodeID().index());
  if (n_shards <= 0) {
    RATELIMIT_ERROR(std::chrono::seconds(1),
                    10,
                    "got STARTED message from %s which is not "
                    "a storage node. Discarding.",
                    Sender::describeConnection(from).c_str());
    return Disposition::NORMAL;
  }

  shard_index_t shard = header_.shard;
  ld_check(shard != -1);
  if (shard >= n_shards) {
    RATELIMIT_ERROR(std::chrono::seconds(1),
                    10,
                    "PROTOCOL ERROR: got STARTED message from %s with an "
                    "invalid shard %d (this node only has %d shards). "
                    "log id:%lu, read stream id:%lu.",
                    Sender::describeConnection(from).c_str(),
                    shard,
                    n_shards,
                    header_.log_id.val(),
                    header_.read_stream_id.val_);
    err = E::PROTO;
    return Disposition::ERROR;
  }

  ShardID shard_id(from.id_.node_.index(), shard);

  Worker* worker = Worker::onThisThread();
  EpochRecovery* recovery =
      Worker::onThisThread()->findActiveEpochRecovery(header_.log_id);

  if (recovery &&
      recovery->digestingReadStream(shard_id, header_.read_stream_id)) {
    recovery->onDigestStreamStarted(shard_id,
                                    header_.read_stream_id,
                                    // if not LSN_INVALID, this is the lng of
                                    // the digesting epoch, see STARTED_Header
                                    header_.last_released_lsn,
                                    header_.status);
  } else {
    worker->clientReadStreams().onStarted(shard_id, *this);
  }

  return Disposition::NORMAL;
}

void STARTED_Message::serialize(ProtocolWriter& writer) const {
  writer.write(header_);
}

MessageReadResult STARTED_Message::deserialize(ProtocolReader& reader) {
  STARTED_Header hdr;
  reader.read(&hdr);
  return reader.result(
      [&] { return new STARTED_Message(hdr, TrafficClass::READ_BACKLOG); });
}

std::string STARTED_Message::toString() const {
  std::string result;
  bool first = true;
  for (auto& field : getDebugInfo()) {
    if (!first) {
      result += ",";
    }
    result += field.first + "=" + logdevice::toString(field.second);
    first = false;
  }
  return result;
}

std::vector<std::pair<std::string, folly::dynamic>>
STARTED_Message::getDebugInfo() const {
  std::vector<std::pair<std::string, folly::dynamic>> res;
  auto add = [&](const char* key, folly::dynamic val) {
    res.push_back(
        std::make_pair<std::string, folly::dynamic>(key, std::move(val)));
  };
  add("log_id", logdevice::toString(header_.log_id));
  add("shard", header_.shard);
  add("status", error_name(header_.status));
  add("read_stream_id", header_.read_stream_id.val());
  add("filter_version", header_.filter_version.val());
  add("last_released_lsn", lsn_to_string(header_.last_released_lsn));
  return res;
}

}} // namespace facebook::logdevice
