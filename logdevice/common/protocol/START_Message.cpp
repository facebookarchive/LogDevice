/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#define __STDC_FORMAT_MACROS
#include "logdevice/common/protocol/START_Message.h"

#include <memory>
#include <random>

#include "logdevice/common/Address.h"
#include "logdevice/common/EpochRecovery.h"
#include "logdevice/common/Worker.h"
#include "logdevice/common/client_read_stream/AllClientReadStreams.h"
#include "logdevice/common/debug.h"
#include "logdevice/common/protocol/ProtocolReader.h"
#include "logdevice/common/protocol/ProtocolWriter.h"
#include "logdevice/include/Err.h"

namespace facebook { namespace logdevice {

void START_Message::onSent(Status st, const Address& to) const {
  ld_debug(": message=START st=%s to=%s log_id=%lu",
           error_name(st),
           Sender::describeConnection(to).c_str(),
           header_.log_id.val_);

  Worker* w = Worker::onThisThread();

  ShardID shard(to.id_.node_.index(), header_.shard);

  if (header_.flags & START_Header::DIGEST) {
    // This is a request for recovery digest records. Route to the
    // EpochRecovery machine and on to the RecoveryNode that sent the message,
    // if they are still around.
    const epoch_t recovering_epoch = lsn_to_epoch(header_.start_lsn);
    EpochRecovery* active_recovery = w->findActiveEpochRecovery(header_.log_id);

    if (!active_recovery || active_recovery->epoch_ != recovering_epoch) {
      RATELIMIT_WARNING(std::chrono::seconds(1),
                        10,
                        "Got a stale onSent() for a START message sent by an "
                        "epoch recovery machine for log %lu epoch %u.",
                        header_.log_id.val_,
                        lsn_to_epoch(header_.start_lsn).val_);
    } else {
      active_recovery->onMessageSent(shard, type_, st, header_.read_stream_id);
    }
  } else {
    w->clientReadStreams().onStartSent(header_.read_stream_id, shard, st);
  }
}

START_Message::START_Message(const START_Header& header,
                             small_shardset_t filtered_out,
                             const ReadStreamAttributes* attrs,
                             std::string client_session_id)
    : Message(MessageType::START, TrafficClass::HANDSHAKE),
      header_(header),
      filtered_out_(std::move(filtered_out)),
      client_session_id_(std::move(client_session_id)) {
  if (attrs != nullptr) {
    attrs_ = *attrs;
  }
  if (folly::kIsDebug && !filtered_out.empty()) {
    ld_check(header_.flags & START_Header::SINGLE_COPY_DELIVERY);
    // in debug mode assert that all node indexes in filtered_out are distinct
    small_shardset_t copy(filtered_out);
    std::sort(copy.begin(), copy.end());
    ld_assert(std::unique(copy.begin(), copy.end()) == copy.end());
  }
}

void START_Message::serialize(ProtocolWriter& writer) const {
  writer.write(header_);
  writer.writeLengthPrefixedVector(filtered_out_);
  writer.write(static_cast<uint8_t>(attrs_.filter_type));
  writer.writeLengthPrefixedVector(attrs_.filter_key1);
  writer.writeLengthPrefixedVector(attrs_.filter_key2);

  if (header_.scd_copyset_reordering ==
      SCDCopysetReordering::HASH_SHUFFLE_CLIENT_SEED) {
    // Randomly generated seeds for SpookyHash
    // same as in LocalLogStoreReadFilter::applyCopysetReordering
    uint64_t h1 = 0x59d2d101d78f02ad, h2 = 0x430bfb34b1cd41e1;

    folly::hash::SpookyHashV2::Hash128(
        client_session_id_.c_str(),
        client_session_id_.length() * sizeof(char),
        &h1,
        &h2);
    writer.write(h1);
    writer.write(h2);
  }
}

MessageReadResult START_Message::deserialize(ProtocolReader& reader) {
  const auto proto = reader.proto();

  START_Header hdr;
  // Defaults for old protocols
  hdr.filter_version.val_ = 1;
  hdr.num_filtered_out = 0;
  hdr.replication = 0;
  hdr.scd_copyset_reordering = SCDCopysetReordering::NONE;
  hdr.shard = -1;
  reader.read(&hdr);

  auto m = std::make_unique<START_Message>(hdr);
  m->proto_ = proto;

  if (reader.ok()) {
    reader.readLengthPrefixedVector(&m->filtered_out_);

    uint8_t temp;
    reader.read(&temp, sizeof(temp));
    m->attrs_.filter_type = static_cast<ServerRecordFilterType>(temp);
    if (m->attrs_.filter_type != ServerRecordFilterType::EQUALITY &&
        m->attrs_.filter_type != ServerRecordFilterType::RANGE &&
        m->attrs_.filter_type != ServerRecordFilterType::NOFILTER) {
      ld_error("Bad START message, unknown ServerRecordFilterType: %d",
               static_cast<int>(m->attrs_.filter_type));
      return reader.errorResult(E::BADMSG);
    }
    reader.readLengthPrefixedVector(&m->attrs_.filter_key1);
    reader.readLengthPrefixedVector(&m->attrs_.filter_key2);

    if (m->header_.scd_copyset_reordering ==
        SCDCopysetReordering::HASH_SHUFFLE_CLIENT_SEED) {
      reader.read(&m->csid_hash_pt1, sizeof(m->csid_hash_pt1));
      reader.read(&m->csid_hash_pt2, sizeof(m->csid_hash_pt2));
    }
  }

  return reader.resultMsg(std::move(m));
}

bool START_Message::allowUnencrypted() const {
  return MetaDataLog::isMetaDataLog(header_.log_id) &&
      Worker::settings().read_streams_use_metadata_log_only;
}

std::vector<std::pair<std::string, folly::dynamic>>
START_Message::getDebugInfo() const {
  std::vector<std::pair<std::string, folly::dynamic>> res;
  auto add = [&](const char* key, folly::dynamic val) {
    res.push_back(
        std::make_pair<std::string, folly::dynamic>(key, std::move(val)));
  };
  add("log_id", toString(header_.log_id));
  add("shard", header_.shard);
  add("read_stream_id", header_.read_stream_id.val());
  add("start_lsn", lsn_to_string(header_.start_lsn));
  add("until_lsn", lsn_to_string(header_.until_lsn));
  add("window_high", lsn_to_string(header_.window_high));
  add("filter_version", header_.filter_version.val());
  add("num_filtered_out", header_.num_filtered_out);
  add("replication", header_.replication);
  add("scd_copyset_reordering", int(header_.scd_copyset_reordering));
  add("flags", header_.flags);
  return res;
}

}} // namespace facebook::logdevice
