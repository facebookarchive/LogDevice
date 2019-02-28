/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/common/protocol/MUTATED_Message.h"

#include <memory>

#include "logdevice/common/EpochRecovery.h"
#include "logdevice/common/Sender.h"
#include "logdevice/common/Worker.h"
#include "logdevice/common/debug.h"
#include "logdevice/common/protocol/ProtocolReader.h"
#include "logdevice/common/protocol/ProtocolWriter.h"

namespace facebook { namespace logdevice {

MUTATED_Message::MUTATED_Message(const MUTATED_Header& header)
    : Message(MessageType::MUTATED, TrafficClass::RECOVERY), header_(header) {}

Message::Disposition MUTATED_Message::onReceived(const Address& from) {
  if (from.isClientAddress()) {
    RATELIMIT_ERROR(std::chrono::seconds(10),
                    10,
                    "PROTOCOL ERROR: got a MUTATED message for %s from client "
                    "%s. MUTATED can only arrive from servers.",
                    header_.rid.toString().c_str(),
                    Sender::describeConnection(from).c_str());
    err = E::PROTO;
    return Disposition::ERROR;
  }

  Worker* w = Worker::onThisThread();

  ld_check(header_.shard != -1);
  ShardID shard(from.id_.node_.index(), header_.shard);

  // If the mutation was initiated by something other than EpochRecovery,
  // route the response to the callback.
  auto it = w->customMutationCallbacks_.find(header_.recovery_id);
  if (it != w->customMutationCallbacks_.end()) {
    auto cb = it->second;
    cb(header_, shard);
    return Disposition::NORMAL;
  }

  EpochRecovery* recovery = w->findActiveEpochRecovery(header_.rid.logid);

  if (!recovery || recovery->id_ != header_.recovery_id) {
    RATELIMIT_INFO(std::chrono::seconds(10),
                   10,
                   "Got a MUTATED message for record %s (recovery id %lu) "
                   "from %s, but no EpochRecovery machine with the given id "
                   "is active. Probably from a previous mutation attempt. "
                   "Ignoring.",
                   header_.rid.toString().c_str(),
                   header_.recovery_id.val_,
                   shard.toString().c_str());
    return Disposition::NORMAL;
  }

  recovery->onMutated(shard, header_);
  return Disposition::NORMAL;
}

void MUTATED_Message::serialize(ProtocolWriter& writer) const {
  writer.write(&header_, MUTATED_Header::headerSize(writer.proto()));
}

MessageReadResult MUTATED_Message::deserialize(ProtocolReader& reader) {
  MUTATED_Header hdr;
  // Defaults for old protocols
  hdr.seal = Seal();
  hdr.shard = -1;
  hdr.wave = 0;
  reader.read(&hdr, MUTATED_Header::headerSize(reader.proto()));
  return reader.result([&] { return std::make_unique<MUTATED_Message>(hdr); });
}

void MUTATED_Message::createAndSend(const MUTATED_Header& header, ClientID to) {
  ld_check(to.valid());

  // Mutations are always sent directly (i.e. not through chains), so replies
  // are sent by workers which received the STORE.

  auto msg = std::make_unique<MUTATED_Message>(header);
  int rv = Worker::onThisThread()->sender().sendMessage(std::move(msg), to);
  if (rv != 0) {
    RATELIMIT_ERROR(std::chrono::seconds(10),
                    10,
                    "Failed to send MUTATED for %s (recovery id %lu) to %s: %s",
                    header.rid.toString().c_str(),
                    header.recovery_id.val_,
                    Sender::describeConnection(Address(to)).c_str(),
                    error_description(err));
  }
}

}} // namespace facebook::logdevice
