/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/common/protocol/GAP_Message.h"

#include "logdevice/common/EpochRecovery.h"
#include "logdevice/common/Sender.h"
#include "logdevice/common/Worker.h"
#include "logdevice/common/client_read_stream/AllClientReadStreams.h"
#include "logdevice/common/debug.h"
#include "logdevice/common/protocol/ProtocolReader.h"
#include "logdevice/common/protocol/ProtocolWriter.h"

namespace facebook { namespace logdevice {

template <>
/* static */
const std::string& EnumMap<GapReason, std::string>::invalidValue() {
  static const std::string invalidGapName("");
  return invalidGapName;
}

template <>
void EnumMap<GapReason, std::string>::setValues() {
  static_assert(static_cast<int>(GapReason::MAX) == 6,
                "Please update GAP_Message.cpp after modifying GapReason");
  set(GapReason::TRIM, "TRIM");
  set(GapReason::NO_RECORDS, "NO_RECORDS");
  set(GapReason::CHECKSUM_FAIL, "CHECKSUM_FAIL");
  set(GapReason::EPOCH_NOT_CLEAN_DEPRECATED, "EPOCH_NOT_CLEAN_DEPRECATED");
  set(GapReason::UNDER_REPLICATED, "UNDER_REPLICATED");
  set(GapReason::FILTERED_OUT, "FILTERED_OUT");
}

EnumMap<GapReason, std::string> gap_reason_names;

GAP_Message::GAP_Message(const GAP_Header& header,
                         TrafficClass tc,
                         Source source)
    : Message(MessageType::GAP, tc), header_(header), source_(source) {}

Message::Disposition GAP_Message::onReceived(Address const& from) {
  if (from.isClientAddress()) {
    RATELIMIT_ERROR(std::chrono::seconds(1),
                    10,
                    "Received GAP message %s from client %s",
                    header_.identify().c_str(),
                    Sender::describeConnection(from).c_str());
    err = E::PROTO;
    return Disposition::ERROR;
  }

  if (header_.read_stream_id == READ_STREAM_ID_INVALID) {
    RATELIMIT_ERROR(std::chrono::seconds(1),
                    10,
                    "Invalid read stream id 0 in GAP message %s from %s.",
                    header_.identify().c_str(),
                    Sender::describeConnection(from).c_str());
    err = E::PROTO;
    return Disposition::ERROR;
  }

  if (header_.start_lsn > header_.end_lsn) {
    RATELIMIT_ERROR(std::chrono::seconds(1),
                    10,
                    "Invalid GAP message %s from %s. Start LSN must not be "
                    "greater than end LSN.",
                    header_.identify().c_str(),
                    Sender::describeConnection(from).c_str());
    err = E::PROTO;
    return Disposition::ERROR;
  }

  Worker* w = Worker::onThisThread();

  shard_index_t shard = header_.shard;
  ld_check(shard != -1);

  EpochRecovery* recovery = nullptr;
  if (header_.flags & GAP_Header::DIGEST) {
    // this is a digest GAP, route to an EpochRecovery object
    recovery = w->findActiveEpochRecovery(header_.log_id);
    if (!recovery) {
      RATELIMIT_INFO(std::chrono::seconds(1),
                     10,
                     "Got an invalid or stale digest GAP message %s from %s "
                     "No epoch recovery machine is active for log. Ignoring.",
                     header_.identify().c_str(),
                     Sender::describeConnection(from).c_str());
      return Disposition::NORMAL;
    }
    if (lsn_to_epoch(header_.start_lsn) != lsn_to_epoch(header_.end_lsn)) {
      RATELIMIT_ERROR(std::chrono::seconds(1),
                      10,
                      "PROTOCOL ERROR: got an invalid digest GAP %s from %s. "
                      "A digest gap must not cross epochs",
                      header_.identify().c_str(),
                      Sender::describeConnection(from).c_str());
      err = E::PROTO;
      return Disposition::ERROR;
    }
  }

  ShardID shard_id(from.id_.node_.index(), shard);

  if (recovery) {
    recovery->onDigestGap(shard_id, header_);
  } else {
    w->clientReadStreams().onGap(shard_id, *this);
  }

  return Disposition::NORMAL;
}

void GAP_Message::serialize(ProtocolWriter& writer) const {
  writer.write(header_);
}

MessageReadResult GAP_Message::deserialize(ProtocolReader& reader) {
  GAP_Header hdr;
  reader.read(&hdr);
  return reader.result(
      [&] { return new GAP_Message(hdr, TrafficClass::READ_BACKLOG); });
}

std::string GAP_Header::identify() const {
  return "Log=" + std::to_string(log_id.val_) + ",[" +
      lsn_to_string(start_lsn) + "," + lsn_to_string(end_lsn) +
      "],reason=" + gap_reason_names[reason];
}

std::vector<std::pair<std::string, folly::dynamic>>
GAP_Message::getDebugInfo() const {
  std::vector<std::pair<std::string, folly::dynamic>> res;
  auto add = [&](const char* key, folly::dynamic val) {
    res.push_back(
        std::make_pair<std::string, folly::dynamic>(key, std::move(val)));
  };
  add("log_id", toString(header_.log_id));
  add("shard", header_.shard);
  add("start_lsn", lsn_to_string(header_.start_lsn));
  add("end_lsn", lsn_to_string(header_.end_lsn));
  add("reason", gap_reason_names[header_.reason]);
  add("flags", header_.flags);
  return res;
}

}} // namespace facebook::logdevice
