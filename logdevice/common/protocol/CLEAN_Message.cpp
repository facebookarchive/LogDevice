/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/common/protocol/CLEAN_Message.h"

#include "logdevice/common/EpochRecovery.h"
#include "logdevice/common/Sender.h"
#include "logdevice/common/Worker.h"
#include "logdevice/common/debug.h"
#include "logdevice/common/protocol/CLEANED_Message.h"
#include "logdevice/common/protocol/ProtocolReader.h"
#include "logdevice/common/protocol/ProtocolWriter.h"

namespace facebook { namespace logdevice {

CLEAN_Message::CLEAN_Message(const CLEAN_Header& header,
                             TailRecord tail_record,
                             OffsetMap epoch_size_map,
                             StorageSet absent_nodes)
    : Message(MessageType::CLEAN, TrafficClass::RECOVERY),
      header_(header),
      tail_record_(std::move(tail_record)),
      epoch_size_map_(std::move(epoch_size_map)),
      absent_nodes_(std::move(absent_nodes)) {}

void CLEAN_Message::serialize(ProtocolWriter& writer) const {
  ld_check(header_.num_absent_nodes == absent_nodes_.size());
  CLEAN_Header write_header = header_;
  write_header.epoch_end_offset_DEPRECATED =
      tail_record_.offsets_map_.getCounter(BYTE_OFFSET);
  write_header.epoch_size_DEPRECATED = epoch_size_map_.getCounter(BYTE_OFFSET);
  write_header.flags |= CLEAN_Header::INCLUDE_TAIL_RECORD;
  writer.write(write_header);
  writer.writeVector(absent_nodes_);
  ld_check(tail_record_.isValid());
  tail_record_.serialize(writer);
  epoch_size_map_.serialize(writer);
}

StorageSet CLEAN_Message::readAbsentNodes(ProtocolReader& reader,
                                          const CLEAN_Header& hdr) {
  StorageSet absent_nodes;
  if (hdr.num_absent_nodes == 0) {
    return absent_nodes;
  }

  reader.readVector(&absent_nodes, hdr.num_absent_nodes);
  return absent_nodes;
}

MessageReadResult CLEAN_Message::deserialize(ProtocolReader& reader) {
  CLEAN_Header hdr{};
  reader.read(&hdr);

  StorageSet absent_nodes;
  if (reader.ok()) {
    absent_nodes = readAbsentNodes(reader, hdr);
  }

  TailRecord tail_record;
  OffsetMap epoch_size_map;
  if (hdr.flags & CLEAN_Header::INCLUDE_TAIL_RECORD) {
    tail_record.deserialize(reader, true);
  }
  epoch_size_map.deserialize(reader, false /* unused */);
  hdr.epoch_size_DEPRECATED = BYTE_OFFSET_INVALID;
  hdr.epoch_end_offset_DEPRECATED = BYTE_OFFSET_INVALID;

  return reader.result([&] {
    return std::make_unique<CLEAN_Message>(hdr,
                                           std::move(tail_record),
                                           std::move(epoch_size_map),
                                           std::move(absent_nodes));
  });
}

void CLEAN_Message::onSent(Status st, const Address& to) const {
  ld_debug(": message=CLEAN st=%s to=%s",
           error_name(st),
           Sender::describeConnection(to).c_str());

  Worker* w = Worker::onThisThread();

  EpochRecovery* active_recovery = w->findActiveEpochRecovery(header_.log_id);

  if (!active_recovery || active_recovery->epoch_ != header_.epoch ||
      active_recovery->id_ != header_.recovery_id) {
    // if there is an active EpochRecovery for this log, the epoch number
    // in CLEAN must never exceed the epoch number of the active epoch --
    // otherwise the CLEAN would not have been sent.
    char current_buf[128];
    if (active_recovery) {
      snprintf(current_buf,
               sizeof current_buf,
               "Current epoch recovery is for epoch %u (recovery id %lu).",
               active_recovery->epoch_.val_,
               active_recovery->id_.val_);
    } else {
      snprintf(current_buf,
               sizeof current_buf,
               "There is no active epoch recovery for this log.");
    }

    RATELIMIT_WARNING(
        std::chrono::seconds(10),
        10,
        "Stale onSent() call for CLEAN message for log %lu.  Sent by epoch "
        "recovery machine for epoch %u (recovery id %lu). %s",
        header_.log_id.val_,
        header_.epoch.val_,
        header_.recovery_id.val_,
        current_buf);
    return;
  }

  ld_check(header_.shard >= 0);
  active_recovery->onMessageSent(
      ShardID(to.id_.node_.index(), header_.shard), type_, st);
}

}} // namespace facebook::logdevice
