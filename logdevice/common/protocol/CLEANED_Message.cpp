/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/common/protocol/CLEANED_Message.h"

#include "logdevice/common/EpochRecovery.h"
#include "logdevice/common/Sender.h"
#include "logdevice/common/Worker.h"
#include "logdevice/common/debug.h"
#include "logdevice/common/protocol/ProtocolReader.h"
#include "logdevice/common/protocol/ProtocolWriter.h"

namespace facebook { namespace logdevice {

CLEANED_Message::CLEANED_Message(const CLEANED_Header& header)
    : Message(MessageType::CLEANED, TrafficClass::RECOVERY), header_(header) {}

void CLEANED_Message::serialize(ProtocolWriter& writer) const {
  writer.write(&header_, CLEANED_Header::headerSize(writer.proto()));
}

MessageReadResult CLEANED_Message::deserialize(ProtocolReader& reader) {
  CLEANED_Header hdr{};
  // Defaults for old protocols
  hdr.shard = -1;
  reader.read(&hdr, CLEANED_Header::headerSize(reader.proto()));
  return reader.result([&] { return new CLEANED_Message(hdr); });
}

Message::Disposition CLEANED_Message::onReceived(const Address& from) {
  Worker* w = Worker::onThisThread();

  if (from.isClientAddress()) {
    RATELIMIT_ERROR(std::chrono::seconds(1),
                    10,
                    "PROTOCOL ERROR: got CLEANED message from client %s",
                    Sender::describeConnection(from).c_str());
    err = E::PROTO;
    return Disposition::ERROR;
  }

  if (header_.log_id == LOGID_INVALID) {
    RATELIMIT_ERROR(std::chrono::seconds(1),
                    10,
                    "PROTOCOL ERROR: got CLEANED message with an invalid "
                    "log id (recovery id %lu) from %s",
                    header_.recovery_id.val_,
                    Sender::describeConnection(from).c_str());
    err = E::PROTO;
    return Disposition::ERROR;
  }

  if (header_.recovery_id == recovery_id_t(0)) {
    RATELIMIT_ERROR(std::chrono::seconds(1),
                    10,
                    "PROTOCOL ERROR: got CLEANED message with invalid "
                    "recovery id %lu (log %lu) from %s",
                    header_.recovery_id.val_,
                    header_.log_id.val_,
                    Sender::describeConnection(from).c_str());
    err = E::PROTO;
    return Disposition::ERROR;
  }

  EpochRecovery* recovery = w->findActiveEpochRecovery(header_.log_id);

  if (!recovery || recovery->id_ != header_.recovery_id) {
    RATELIMIT_INFO(
        std::chrono::seconds(10),
        10,
        "Got a stale CLEANED message for log %lu epoch %u id %lu from %s. "
        "%s. Ignoring.",
        header_.log_id.val_,
        header_.epoch.val_,
        header_.recovery_id.val_,
        Sender::describeConnection(from).c_str(),
        recovery ? ("Active recovery has id " + toString(recovery->id_)).c_str()
                 : "No epoch recovery machine is active for log");
    return Disposition::NORMAL;
  }

  if (recovery->epoch_ != header_.epoch) {
    // If recovery IDs match, the epochs should match as well.
    RATELIMIT_ERROR(
        std::chrono::seconds(10),
        10,
        "Got a CLEANED message for log %lu epoch %u id %lu.  The EpochRecovery "
        "instance with that ID was found but it is working on epoch %u.  This "
        "should never happen!",
        header_.log_id.val_,
        header_.epoch.val_,
        header_.recovery_id.val_,
        recovery->epoch_.val_);
    err = E::PROTO;
    return Disposition::ERROR;
  }

  ld_check(recovery->getLogID() == header_.log_id);

  const shard_size_t n_shards = w->getNodesConfiguration()->getNumShards();
  shard_index_t shard = header_.shard;
  ld_check(shard != -1);

  if (shard >= n_shards) {
    RATELIMIT_ERROR(std::chrono::seconds(10),
                    10,
                    "Got CLEANED message from %s with invalid shard %u, "
                    "this node only has %u shards",
                    Sender::describeConnection(from).c_str(),
                    shard,
                    n_shards);
    return Message::Disposition::NORMAL;
  }

  recovery->onCleaned(
      ShardID(from.id_.node_.index(), shard), header_.status, header_.seal);

  return Disposition::NORMAL;
}

}} // namespace facebook::logdevice
