/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/common/protocol/SEALED_Message.h"

#include <folly/Memory.h>

#include "logdevice/common/LogRecoveryRequest.h"
#include "logdevice/common/Worker.h"
#include "logdevice/common/debug.h"
#include "logdevice/common/protocol/Compatibility.h"
#include "logdevice/common/protocol/ProtocolReader.h"
#include "logdevice/common/protocol/ProtocolWriter.h"
#include "logdevice/common/stats/Stats.h"

namespace facebook { namespace logdevice {

size_t SEALED_Header::getExpectedSize(uint16_t proto) {
  if (proto < Compatibility::TRIM_POINT_IN_SEALED) {
    return offsetof(SEALED_Header, trim_point);
  }
  return sizeof(SEALED_Header);
}

void SEALED_Message::serialize(ProtocolWriter& writer) const {
  ld_check(header_.lng_list_size == epoch_lng_.size());
  ld_check(header_.lng_list_size == epoch_offset_map_.size());
  ld_check(header_.lng_list_size == last_timestamp_.size());
  ld_check(header_.lng_list_size == max_seen_lsn_.size());
  writer.write(&header_, SEALED_Header::getExpectedSize(writer.proto()));
  writer.writeVector(epoch_lng_);
  writer.write(seal_);
  writer.writeVectorOfSerializable(epoch_offset_map_);
  writer.writeVector(last_timestamp_);
  writer.writeVector(max_seen_lsn_);

  for (const auto& tr : tail_records_) {
    tr.serialize(writer);
  }
}

MessageReadResult SEALED_Message::deserialize(ProtocolReader& reader) {
  SEALED_Header header;
  // Defaults for old protocols
  header.shard = -1;
  header.num_tail_records = -1;
  header.trim_point = LSN_INVALID;
  reader.read(&header, SEALED_Header::getExpectedSize(reader.proto()));

  std::vector<lsn_t> epoch_lng(header.lng_list_size);
  Seal seal;
  std::vector<OffsetMap> epoch_offset_map(header.lng_list_size);
  std::vector<uint64_t> last_timestamp(header.lng_list_size, 0);
  std::vector<lsn_t> max_seen_lsn(header.lng_list_size, LSN_INVALID);

  reader.readVector(&epoch_lng);
  reader.read(&seal);
  reader.readVectorOfSerializable(&epoch_offset_map, header.lng_list_size);
  reader.readVector(&last_timestamp);
  reader.readVector(&max_seen_lsn);

  std::vector<TailRecord> tail_records;
  if (header.num_tail_records > 0) {
    tail_records.resize(header.num_tail_records);
    for (auto& tr : tail_records) {
      // currently we don't send payloads with SEALED message, so zero
      // copy here is not relevant
      tr.deserialize(reader, /*zero_copy*/ true);
    }
  }

  return reader.result([&] {
    return std::make_unique<SEALED_Message>(header,
                                            std::move(epoch_lng),
                                            seal,
                                            std::move(last_timestamp),
                                            std::move(epoch_offset_map),
                                            std::move(max_seen_lsn),
                                            std::move(tail_records));
  });
}

Message::Disposition SEALED_Message::onReceived(const Address& from) {
  Worker* worker = Worker::onThisThread();

  if (header_.status == E::PREEMPTED && !seal_.valid()) {
    RATELIMIT_ERROR(
        std::chrono::seconds(10),
        10,
        "Got a SEALED message for log %lu from %s with status PREEMPTED but "
        "invalid seal %s. This is unexpected, please investigate.",
        header_.log_id.val_,
        Sender::describeConnection(from).c_str(),
        seal_.toString().c_str());
    err = E::PROTO;
    return Disposition::ERROR;
  }

  auto& rqmap = worker->runningLogRecoveries().map;
  auto it = rqmap.find(header_.log_id);
  if (it == rqmap.end()) {
    return Disposition::NORMAL;
  }

  const auto& nodes_configuration = worker->getNodesConfiguration();
  const node_index_t node = from.id_.node_.index();
  if (!nodes_configuration->getStorageMembership()->hasNode(node)) {
    RATELIMIT_INFO(std::chrono::seconds(10),
                   10,
                   "Got a SEALED message for log %lu from %s but this node is "
                   "not a storage node. Ignoring.",
                   header_.log_id.val_,
                   Sender::describeConnection(from).c_str());
    return Disposition::NORMAL;
  }

  const auto* node_attr = nodes_configuration->getNodeStorageAttribute(node);
  ld_check(node_attr != nullptr);
  const shard_size_t n_shards = node_attr->num_shards;
  ld_check(n_shards > 0); // We already checked we are a storage node.
  shard_index_t shard_idx = header_.shard;
  if (shard_idx >= n_shards) {
    RATELIMIT_ERROR(std::chrono::seconds(10),
                    10,
                    "Got SEALED message from client %s with invalid shard %u, "
                    "this node only has %u shards",
                    Sender::describeConnection(from).c_str(),
                    shard_idx,
                    n_shards);
    return Message::Disposition::NORMAL;
  }

  it->second->onSealReply(ShardID(from.id_.node_.index(), shard_idx), *this);

  return Disposition::NORMAL;
}

void SEALED_Message::createAndSend(const Address& to,
                                   logid_t log_id,
                                   shard_index_t shard_idx,
                                   epoch_t seal_epoch,
                                   Status status,
                                   lsn_t trim_point,
                                   std::vector<lsn_t> lng_list,
                                   Seal seal,
                                   std::vector<OffsetMap> epoch_offset_map,
                                   std::vector<uint64_t> last_timestamp,
                                   std::vector<lsn_t> max_seen_lsn,
                                   std::vector<TailRecord> tail_records) {
  ld_check(lng_list.size() == last_timestamp.size());
  ld_check(lng_list.size() == epoch_offset_map.size());

  if (status == E::PREEMPTED) {
    ld_check(seal.valid());
  }

  SEALED_Header header;
  header.log_id = log_id;
  header.shard = shard_idx;
  header.seal_epoch = seal_epoch;
  header.status = status;
  header.trim_point = trim_point;

  // header.num_tail_records set by constructor

  int rv = Worker::onThisThread()->sender().sendMessage(
      std::make_unique<SEALED_Message>(header,
                                       std::move(lng_list),
                                       seal,
                                       std::move(last_timestamp),
                                       std::move(epoch_offset_map),
                                       std::move(max_seen_lsn),
                                       std::move(tail_records)),
      to);

  if (rv != 0) {
    RATELIMIT_WARNING(std::chrono::seconds(10),
                      10,
                      "Failed to send SEALED reply for log id %lu, epoch %u, "
                      "seal status %s to %s: %s.",
                      header.log_id.val_,
                      header.seal_epoch.val_,
                      error_name(header.status),
                      Sender::describeConnection(to).c_str(),
                      error_name(err));
    WORKER_STAT_INCR(sealed_reply_failed_to_send);
  }
}

void SEALED_Message::onSent(Status status, const Address& to) const {
  if (status != E::OK) {
    RATELIMIT_WARNING(std::chrono::seconds(10),
                      10,
                      "Failed to send SEALED reply for log id %lu, epoch %u, "
                      "seal status %s to %s: %s",
                      header_.log_id.val_,
                      header_.seal_epoch.val_,
                      error_name(header_.status),
                      Sender::describeConnection(to).c_str(),
                      error_name(status));
    WORKER_STAT_INCR(sealed_reply_failed_to_send);
  }
}

std::string SEALED_Message::toString() const {
  std::string ret = folly::sformat("log_id: {}, seal_epoch: {}, status: {}, "
                                   "lng_list_size: {}, seal_: {}, epoch_lng_:",
                                   header_.log_id.val_,
                                   header_.seal_epoch.val_,
                                   error_name(header_.status),
                                   header_.lng_list_size,
                                   seal_.toString());
  for (const lsn_t lsn : epoch_lng_) {
    ret += " " + lsn_to_string(lsn);
  }
  // TODO: Add last_timestamp_ and epoch_offset_map_.
  return ret;
}
}} // namespace facebook::logdevice
