/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/common/protocol/RECORD_Message.h"

#include <cstdlib>

#include <folly/Memory.h>

#include "logdevice/common/Checksum.h"
#include "logdevice/common/EpochRecovery.h"
#include "logdevice/common/Sender.h"
#include "logdevice/common/Worker.h"
#include "logdevice/common/client_read_stream/AllClientReadStreams.h"
#include "logdevice/common/debug.h"
#include "logdevice/common/protocol/GAP_Message.h"
#include "logdevice/common/protocol/MessageType.h"
#include "logdevice/common/protocol/ProtocolReader.h"
#include "logdevice/common/protocol/ProtocolWriter.h"
#include "logdevice/common/settings/Settings.h"

namespace facebook { namespace logdevice {

static std::unique_ptr<ExtraMetadata>
read_extra_metadata(ProtocolReader& reader,
                    logid_t logid,
                    RECORD_flags_t flags);

static void write_extra_metadata(ProtocolWriter& writer,
                                 const ExtraMetadata& metadata,
                                 const RECORD_Header& header);

RECORD_Message::RECORD_Message(const RECORD_Header& header,
                               TrafficClass tc,
                               Payload&& payload,
                               std::unique_ptr<ExtraMetadata> extra_metadata,
                               Source source,
                               OffsetMap offsets,
                               std::shared_ptr<std::string> log_group_path)
    :

      Message(MessageType::RECORD, tc),
      header_(header),
      payload_(std::move(payload)),
      extra_metadata_(std::move(extra_metadata)),
      source_(source),
      offsets_(std::move(offsets)),
      log_group_path_(std::move(log_group_path)) {}

RECORD_Message::~RECORD_Message() {
  free(const_cast<void*>(payload_.data()));
}

void RECORD_Message::serialize(ProtocolWriter& writer) const {
  writer.write(header_);

  // Note: this method needs to be kept at least approximately in sync with
  // expectedSize().

  if (extra_metadata_) {
    write_extra_metadata(writer, *extra_metadata_, header_);
  }

  if (offsets_.isValid()) {
    ld_check(header_.flags & RECORD_Header::INCLUDE_BYTE_OFFSET);
    offsets_.serialize(writer);
  }

  ld_check(payload_.size() < Message::MAX_LEN); // must have been checked
                                                // by upper layers
  if (payload_.size() <= MAX_COPY_TO_EVBUFFER_PAYLOAD_SIZE) {
    writer.write(payload_.data(), payload_.size());
  } else {
    writer.writeWithoutCopy(payload_.data(), payload_.size());
  }
}

MessageReadResult RECORD_Message::deserialize(ProtocolReader& reader) {
  TrafficClass tc = TrafficClass::READ_TAIL;
  RECORD_Header header;
  reader.read(&header);

  std::unique_ptr<ExtraMetadata> extra_metadata;

  if (header.flags & RECORD_Header::INCLUDES_EXTRA_METADATA) {
    extra_metadata = read_extra_metadata(reader, header.log_id, header.flags);
  }

  OffsetMap offsets;
  if (header.flags & RECORD_Header::INCLUDE_BYTE_OFFSET) {
    offsets.deserialize(reader, false /* unused */);
  }

  if (header.flags & RECORD_Header::DIGEST) {
    tc = TrafficClass::RECOVERY;
  } else if (header.flags & RECORD_Header::INCLUDES_EXTRA_METADATA) {
    tc = TrafficClass::REBUILD;
  }

  // If flags indicate that the payload includes a checksum, strip it now.
  // The payload size reported to the client will be just the actual client
  // payload.
  uint64_t expected_checksum = 0;
  if (reader.ok() && (header.flags & RECORD_Header::CHECKSUM)) {
    union {
      uint64_t c64;
      uint32_t c32;
    } u;
    void* ptr; // will be either &u.c64 or &u.c32
    size_t checksum_size;
    if (header.flags & RECORD_Header::CHECKSUM_64BIT) {
      ptr = &u.c64;
      checksum_size = sizeof u.c64;
    } else {
      ptr = &u.c32;
      checksum_size = sizeof u.c32;
    }

    if (reader.bytesRemaining() < checksum_size) {
      RATELIMIT_ERROR(
          std::chrono::seconds(10),
          10,
          "Malformed RECORD message: ran out of bytes while reading "
          "checksum (expected %zu, got %zu); log: %lu lsn: %s rsid: %lu",
          checksum_size,
          reader.bytesRemaining(),
          header.log_id.val_,
          lsn_to_string(header.lsn).c_str(),
          header.read_stream_id.val_);
      // Put garbage in place of checksum, so that onReceived() reports checksum
      // mismatch. Don't raise a protocol error because the bad payload probably
      // comes from sender's local log store, without sender checking it.
      expected_checksum = 0x5000b4df00f00f00ul;
    } else {
      reader.read(ptr, checksum_size);
      expected_checksum =
          (header.flags & RECORD_Header::CHECKSUM_64BIT) ? u.c64 : u.c32;
    }
  }

  size_t payload_size = reader.ok() ? reader.bytesRemaining() : 0;
  ld_check(payload_size < Message::MAX_LEN);

  void* payload = nullptr;
  if (payload_size > 0) {
    payload = malloc(payload_size);
    if (!payload) { // unlikely
      throw std::bad_alloc();
    }
    reader.read(payload, payload_size);
  }

  return reader.result([&] {
    auto m = std::make_unique<RECORD_Message>(
        header, tc, Payload(payload, payload_size), std::move(extra_metadata));
    m->expected_checksum_ = expected_checksum;
    m->offsets_ = std::move(offsets);
    return m;
  });
}

std::string RECORD_Message::identify() const {
  RecordID rid{
      lsn_to_esn(header_.lsn), lsn_to_epoch(header_.lsn), header_.log_id};
  return rid.toString();
}

Message::Disposition RECORD_Message::onReceived(const Address& from) {
  if (from.isClientAddress()) {
    RATELIMIT_ERROR(std::chrono::seconds(1),
                    10,
                    "Received RECORD message %s from client %s",
                    identify().c_str(),
                    Sender::describeConnection(from).c_str());
    err = E::PROTO;
    return Disposition::ERROR;
  }

  if (header_.read_stream_id == READ_STREAM_ID_INVALID) {
    RATELIMIT_ERROR(std::chrono::seconds(1),
                    10,
                    "Invalid read stream id 0 in a RECORD message %s from %s.",
                    identify().c_str(),
                    Sender::describeConnection(from).c_str());
    err = E::PROTO;
    return Disposition::ERROR;
  }

  Worker* w = Worker::onThisThread();
  EpochRecovery* recovery = nullptr;

  ld_check(header_.shard != -1);

  if (header_.flags & RECORD_Header::DIGEST) {
    // this is a digest record, route to an EpochRecovery object
    recovery = w->findActiveEpochRecovery(header_.log_id);
    if (!recovery) {
      RATELIMIT_INFO(std::chrono::seconds(1),
                     10,
                     "Got an invalid or stale digest record %s from %s for "
                     "read stream %lu. No epoch recovery machine is active "
                     "for log. Ignoring.",
                     identify().c_str(),
                     Sender::describeConnection(from).c_str(),
                     header_.read_stream_id.val_);
      return Disposition::NORMAL;
    }

    if (!extra_metadata_) {
      RATELIMIT_WARNING(std::chrono::seconds(10),
                        2,
                        "Received a RECORD message %s from %s with DIGEST flag "
                        "set, but no extra_metadata. The storage node might "
                        "be running with an old logdevice version.",
                        identify().c_str(),
                        Sender::describeConnection(from).c_str());
      // TODO 11866467: enable the protocol check once every deployment has been
      // upgraded.
      // err = E::PROTO;
      // return Disposition::ERROR;
    }
  }

  bool invalid_checksum = (verifyChecksum() != 0);

  std::unique_ptr<DataRecordOwnsPayload> record(
      new DataRecordOwnsPayload(header_.log_id,
                                std::move(payload_),
                                header_.lsn,
                                std::chrono::milliseconds(header_.timestamp),
                                header_.flags,
                                std::move(extra_metadata_),
                                std::shared_ptr<BufferedWriteDecoder>(),
                                0, // batch_offset
                                OffsetMap::toRecord(offsets_),
                                invalid_checksum));
  // We have transferred ownership of the payload.
  ld_check(!payload_.data());

  ShardID shard(from.id_.node_.index(), header_.shard);
  if (recovery) {
    recovery->onDigestRecord(shard, header_.read_stream_id, std::move(record));
  } else {
    w->clientReadStreams().onDataRecord(
        shard, header_.log_id, header_.read_stream_id, std::move(record));
  }

  return Disposition::NORMAL;
}

size_t RECORD_Message::expectedSize(size_t payload_size) {
  // TODO: Also account for extra metadata and byte offset.
  return ProtocolHeader::bytesNeeded(
             MessageType::RECORD, Compatibility::MAX_PROTOCOL_SUPPORTED) +
      sizeof(RECORD_Header) + payload_size;
}

std::unique_ptr<ExtraMetadata> read_extra_metadata(ProtocolReader& reader,
                                                   logid_t logid,
                                                   RECORD_flags_t flags) {
  auto result = std::make_unique<ExtraMetadata>();
  reader.read(&result->header);

  reader.readVector(&result->copyset, result->header.copyset_size);

  if (flags & RECORD_Header::INCLUDE_OFFSET_WITHIN_EPOCH) {
    OffsetMap offsets_within_epoch;
    offsets_within_epoch.deserialize(reader, false /* unused */);
    result->offsets_within_epoch = std::move(offsets_within_epoch);
  }
  return result;
}

void write_extra_metadata(ProtocolWriter& writer,
                          const ExtraMetadata& metadata,
                          const RECORD_Header& header) {
  ld_check(header.flags & RECORD_Header::INCLUDES_EXTRA_METADATA);
  ld_check(metadata.header.copyset_size == metadata.copyset.size());
  writer.write(metadata.header);
  // Only servers should pass INCLUDES_EXTRA_METADATA, and servers should have
  // all been upgraded to a recent enough protocol version.
  writer.writeVector(metadata.copyset);
  if (metadata.offsets_within_epoch.isValid()) {
    ld_check(header.flags & RECORD_Header::INCLUDE_OFFSET_WITHIN_EPOCH);
    metadata.offsets_within_epoch.serialize(writer);
  }
}

int RECORD_Message::verifyChecksum() const {
  // Verify the integrity of the checksum bits: CHECKSUM_PARITY should be the
  // XNOR of the other two.
  bool expected_parity = bool(header_.flags & RECORD_Header::CHECKSUM) ==
      bool(header_.flags & RECORD_Header::CHECKSUM_64BIT);
  if (expected_parity != bool(header_.flags & RECORD_Header::CHECKSUM_PARITY)) {
    RecordID rid = {
        lsn_to_esn(header_.lsn), lsn_to_epoch(header_.lsn), header_.log_id};
    RATELIMIT_ERROR(std::chrono::seconds(1),
                    100,
                    "Checksum flag parity check failed for record %s",
                    rid.toString().c_str());
    return -1;
  }

  // If the message came with a checksum, verify it
  if (header_.flags & RECORD_Header::CHECKSUM) {
    Slice slice{payload_};
    uint64_t payload_checksum = (header_.flags & RECORD_Header::CHECKSUM_64BIT)
        ? checksum_64bit(slice)
        : checksum_32bit(slice);

    if (payload_checksum != expected_checksum_) {
      RecordID rid = {
          lsn_to_esn(header_.lsn), lsn_to_epoch(header_.lsn), header_.log_id};
      RATELIMIT_ERROR(
          std::chrono::seconds(1),
          100,
          "Checksum verification failed for record %s: "
          "expected %lx, calculated %lx, payload: %s%s",
          rid.toString().c_str(),
          expected_checksum_,
          payload_checksum,
          hexdump_buf(payload_.data(), std::min(payload_.size(), 200lu))
              .c_str(),
          payload_.size() > 200lu ? "... (truncated)" : "");
      return -1;
    }
  }

  return 0;
}

std::string RECORD_Message::flagsToString(RECORD_flags_t flags) {
  std::string s;

#define FLAG(f)                   \
  if (flags & RECORD_Header::f) { \
    if (!s.empty()) {             \
      s += "|";                   \
    }                             \
    s += #f;                      \
  }

  FLAG(INCLUDES_EXTRA_METADATA)
  FLAG(CHECKSUM)
  FLAG(CHECKSUM_64BIT)
  FLAG(CHECKSUM_PARITY)
  FLAG(DIGEST)
  FLAG(HOLE)
  FLAG(BUFFERED_WRITER_BLOB)
  FLAG(INCLUDE_BYTE_OFFSET)
  FLAG(INCLUDE_OFFSET_WITHIN_EPOCH)
  FLAG(WRITTEN_BY_RECOVERY)
  FLAG(CSI_DATA_ONLY)
  FLAG(BRIDGE)
  FLAG(EPOCH_BEGIN)
  FLAG(UNDER_REPLICATED_REGION)
  FLAG(DRAINED)

#undef FLAG

  return s;
}

std::vector<std::pair<std::string, folly::dynamic>>
RECORD_Message::getDebugInfo() const {
  std::vector<std::pair<std::string, folly::dynamic>> res;
  auto add = [&](const char* key, folly::dynamic val) {
    res.push_back(
        std::make_pair<std::string, folly::dynamic>(key, std::move(val)));
  };
  add("log_id", toString(header_.log_id));
  add("shard", header_.shard);
  add("lsn", lsn_to_string(header_.lsn));
  add("timestamp", header_.timestamp);
  add("read_stream_id", header_.read_stream_id.val());
  add("flags", flagsToString(header_.flags));
  add("payload size", payload_.size());
  add("expected checksum", expected_checksum_);
  if (source_ != RECORD_Message::Source::UNKNOWN) {
    add("source", sourceToString(source_));
  }
  add("offsets", offsets_.toString().c_str());
  if (extra_metadata_) {
    folly::dynamic map = folly::dynamic::object;
    map["last_known_good"] = extra_metadata_->header.last_known_good.val();
    map["wave"] = extra_metadata_->header.wave;
    map["copyset_size"] = extra_metadata_->header.copyset_size;
    map["copyset"] = toString(
        extra_metadata_->copyset.data(), extra_metadata_->copyset.size());
    map["offsets_within_epoch_"] =
        extra_metadata_->offsets_within_epoch.toString().c_str();
    add("extra_metadata", std::move(map));
  }
  return res;
}

}} // namespace facebook::logdevice
