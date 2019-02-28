/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include "logdevice/common/SerializableData.h"
#include "logdevice/common/Worker.h"
#include "logdevice/include/Err.h"
#include "logdevice/include/Record.h"
#include "logdevice/include/types.h"

namespace facebook { namespace logdevice {

// Header of a snapshot record.
struct RSMSnapshotHeader : public SerializableData {
  enum Version { BASE_VERSION = 0, CONTAINS_DELTA_LOG_READ_PTR_AND_LENGTH = 1 };

  uint32_t format_version;  // current snapshot header version
  uint32_t flags;           // unused, might be handy in the future.
  uint64_t byte_offset;     // byte offset of last considered delta.
  uint64_t offset;          // offset of last considered delta.
  lsn_t base_version;       // version of last applied delta.
  size_t length;            // length of this header in bytes (for future use).
  lsn_t delta_log_read_ptr; // reader pointer of the delta log reader at the
                            // time of this snapshot.

  RSMSnapshotHeader() = default;

  RSMSnapshotHeader(uint32_t format_version,
                    uint32_t flags,
                    uint64_t byte_offset,
                    uint64_t offset,
                    lsn_t base_version,
                    lsn_t delta_log_read_ptr = 0)
      : format_version(format_version),
        flags(flags),
        byte_offset(byte_offset),
        offset(offset),
        base_version(base_version),
        length(computeLengthInBytes(*this)),
        delta_log_read_ptr(delta_log_read_ptr) {}

  // If this flag is set, use ZSTD to compress / decompress the snapshot
  // payload.
  static const uint32_t ZSTD_COMPRESSION = 1 << 0; //=1

  /**
   * Deserialize a RSMSnapshotHeader from a payload.
   *
   * @param payload Payload to read from;
   * @param out     Header to populate.
   *
   * @return how many bytes in `p` were consumed to read the header or -1 on
   *         error and err is set to E::BADMSG.
   */
  static int deserialize(Payload payload, RSMSnapshotHeader& out);

  void
  deserialize(ProtocolReader& reader,
              bool evbuffer_zero_copy,
              folly::Optional<size_t> expected_size = folly::none) override;

  /**
   * Serialize a RSMSnapshotHeader onto a payload.
   *
   * @param payload  Buffer to write to (if nullptr, we don't write anything);
   * @param buf_size Size of that buffer;
   *
   * @return How many bytes were written if payload was not nullptr or -1 if
   *         buf_size is too small, and err is set to E::NOBUFS.
   */
  static int serialize(const RSMSnapshotHeader& hdr,
                       void* payload,
                       size_t buf_size);

  void serialize(ProtocolWriter& writer) const override;

  inline static int computeLengthInBytes(const RSMSnapshotHeader& hdr) {
    // attempt to serialize
    return serialize(hdr, nullptr, /*buf_size=*/0);
  }

  const char* name() const override {
    return "RSMSnapshotHeader";
  }

  bool operator==(const RSMSnapshotHeader&) const;
};

}} // namespace facebook::logdevice
