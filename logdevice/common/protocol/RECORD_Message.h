/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <memory>

#include <boost/noncopyable.hpp>

#include "logdevice/common/CopySet.h"
#include "logdevice/common/OffsetMap.h"
#include "logdevice/common/protocol/Message.h"
#include "logdevice/common/types_internal.h"
#include "logdevice/include/Record.h"
#include "logdevice/include/types.h"

namespace facebook { namespace logdevice {

typedef uint32_t RECORD_flags_t;

struct RECORD_Header {
  logid_t log_id;                  // log id
  read_stream_id_t read_stream_id; // client-issued identifier for the stream
  lsn_t lsn;
  uint64_t timestamp;
  RECORD_flags_t flags; // bitset of flag constants defined below
  shard_index_t shard;

  // Header is followed by:
  // - optionally a variable-sized ExtraMetadata record (see flags)
  // - variable-sized payload

  // If set in .flags, indicates that following the record header and payload,
  // there is a ExtraMetadata structure.

  static const RECORD_flags_t INCLUDES_EXTRA_METADATA = 1u << 0; //=1

  // Value 2 (bit 1) is available for use

  // If set in .flags, the payload is prefixed with a checksum.  The length of
  // the checksum depends on the CHECKSUM_64BIT flag; if it is set, the
  // checksum is 64 bits, otherwise 32 bits.
  static const RECORD_flags_t CHECKSUM = 1u << 2;       //=4
  static const RECORD_flags_t CHECKSUM_64BIT = 1u << 3; //=8

  // XNOR of the other two checksum bits.  Ensures that single-bit corruptions
  // in the checksum bits (or flags getting zeroed out) get detected.
  static const RECORD_flags_t CHECKSUM_PARITY = 1u << 4; //=16

  // This is a digest record. It is a part of a read stream that was
  // created by a START message with the DIGEST flag set. Digest
  // records are used in epoch recovery.
  static const RECORD_flags_t DIGEST = 1u << 5; //=32

  // The record is a plug for a hole in the numbering sequence.
  // RECORD_Message::payload_ must be empty.
  static const RECORD_flags_t HOLE = 1u << 6; //=64

  // Record contains a blob composed by BufferedWriter
  static const RECORD_flags_t BUFFERED_WRITER_BLOB = 1u << 7; //=128

  static const RECORD_flags_t INCLUDE_BYTE_OFFSET = 1u << 8; //=256

  // If set, offsets within epoch value is going to be included into
  // rebuilding metadata.
  static const RECORD_flags_t INCLUDE_OFFSET_WITHIN_EPOCH = 1u << 9; //=4096

  // the record was written or overwritten by epoch recovery
  static const RECORD_flags_t WRITTEN_BY_RECOVERY = 1u << 13; //=8192

  // record header was constructed with data from copy set index
  static const RECORD_flags_t CSI_DATA_ONLY = 1u << 14; //=16384

  // The record being stored is a special hole indicating the end of an epoch.
  // There is no payload. WRITTEN_BY_RECOVERY and HOLE must also be set.
  static const RECORD_flags_t BRIDGE = 1u << 15; //=32768

  // The record is the first record in the epoch. It is safe to assume that
  // no record was stored before the ESN of this record in the epoch of the
  // record.
  static const RECORD_flags_t EPOCH_BEGIN = 1u << 16; //=65536

  // The record is within a region of the log that is under replicated.
  // Readers will exclude nodes that set this from from the f-majority
  // calculation used to report a data loss gap.
  static const RECORD_flags_t UNDER_REPLICATED_REGION = 1u << 17; //=131072

  // the record was explicitly drained from this node and amended to have
  // a new copyset. The new copyset should not include "my node id". If
  // this flag is clear, the copyset should include "my node id".
  static const RECORD_flags_t DRAINED = 1u << 19; //=524288

  // Please update RECORD_Message::flagsToString() when adding flags.

} __attribute__((__packed__));

/**
 * Additional metadata that can be sent alongside RECORD messages.
 * This is used by state machines such as:
 * - The digest phase of recovery which needs the wave and offset_within_epoch;
 * - The replication checker tool (see logdevice/replication_checker/ which
 *   needs to know the wave and copyset of each record;
 * - The meta-fixer tool (see logdevice/scripts/meta-fixer/).
 */
struct ExtraMetadata {
  struct Header {
    esn_t last_known_good;
    uint32_t wave;
    copyset_size_t copyset_size;
    // On the wire, header is followed by the copyset consisting of copyset_size
    // node indexes
  } __attribute__((__packed__));
  Header header;
  copyset_t copyset;
  OffsetMap offsets_within_epoch;
};

class RECORD_Message : public Message, boost::noncopyable {
 public:
  // identifies the origin of the record
  enum class Source { LOCAL_LOG_STORE, CACHED_DIGEST, UNKNOWN };

  static const char* sourceToString(Source val) {
    switch (val) {
      case Source::LOCAL_LOG_STORE:
        return "LOCAL_LOG_STORE";
      case Source::CACHED_DIGEST:
        return "CACHED_DIGEST";
      case Source::UNKNOWN:
        return "UNKNOWN";
    }
    ld_check(false);
    return "";
  }

  // NOTE: populates header flags and header payload_size based on
  // extra_metadata parameter
  RECORD_Message(const RECORD_Header& header,
                 TrafficClass tc,
                 Payload&& payload,
                 std::unique_ptr<ExtraMetadata> extra_metadata,
                 Source source = Source::LOCAL_LOG_STORE,
                 OffsetMap offsets = OffsetMap(),
                 std::shared_ptr<std::string> log_group_path = nullptr);

  /**
   * Convenience method that calculates how much space a RECORD message
   * containing the given payload size would take in the output socket.
   * Approximately equivalent to creating the RECORD_Message and calling size()
   * on it.
   *
   * Note that this is only an estimate, it doesn't take into account the
   * extra metadata or byte_offset.
   */
  static size_t expectedSize(size_t payload_size);

  // see Message.h
  void serialize(ProtocolWriter&) const override;
  Disposition onReceived(const Address& from) override;
  static Message::deserializer_t deserialize;
  // onSent() handler lives in server/RECORD_onSent.cpp

  /**
   * @return a human-readable string with the record's log id, epoch, and ESN
   *         for use in error messages
   */
  std::string identify() const;

  ~RECORD_Message() override;

  /**
   * Pretty-prints flags.
   */
  static std::string flagsToString(RECORD_flags_t flags);

  RECORD_Header header_;

  // RECORD messages own the memory pointed to by their payload:
  // - On the send path, transmission can be deferred due to traffic shaping.
  //   Senders "freeze the payload" by copying it to a dedicated buffer for
  //   the RECORD message.  This ensures the payload is still valid at the time
  //   the message is finally sent. To avoid another copy for payloads larger
  //   than MAX_COPY_TO_EVBUFFER_PAYLOAD_SIZE, the payload memory is added to
  //   the evbuffer by reference. The payload is freed, along with the message,
  //   once transfer into the socket is acknowledged.
  // - On the read path, the payload gets malloc-d in deserialize().
  //   onReceived() then passes its ownership to the client library where the
  //   memory will get freed later when it is no longer needed.
  Payload payload_;

  // If non-null:
  // - On the send path, the structure will be embedded in the RECORD
  //   message
  // - On the read path, the structure came with the record
  std::unique_ptr<ExtraMetadata> extra_metadata_;

  // On the receiving end, this contains the checksum if it was prepended to
  // the payload (determined by .flags).  onReceived() will verify it.
  uint64_t expected_checksum_;

  // used only on the sending end
  Source source_{Source::UNKNOWN};

  OffsetMap offsets_;

  // used for per-log-group stats on the server side, can be nullptr if log
  // group is unknown. Doesn't get serialized into the message.
  std::shared_ptr<std::string> log_group_path_;

  virtual std::vector<std::pair<std::string, folly::dynamic>>
  getDebugInfo() const override;

 private:
  // Verifies the integrity of checksum flags and the checksum if the message
  // came with one
  int verifyChecksum() const;

  friend class CachedDigestTest;
  friend class CatchupQueueTest;
  friend class ChecksumTest;
  friend class RECORD_MessageTest;
  friend class MessageSerializationTest;
};

}} // namespace facebook::logdevice
