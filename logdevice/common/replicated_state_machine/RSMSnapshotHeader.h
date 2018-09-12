/**
 * Copyright (c) 2017-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include "logdevice/include/Err.h"
#include "logdevice/include/Record.h"
#include "logdevice/include/types.h"

namespace facebook { namespace logdevice {

// Header of a snapshot record.
struct RSMSnapshotHeader {
  uint32_t format_version; // unused, might be handy in the future.
  uint32_t flags;          // unused, might be handy in the future.
  uint64_t byte_offset;    // byte offset of last considered delta.
  uint64_t offset;         // offset of last considered delta.
  lsn_t base_version;      // version of last applied delta.

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
};

// Prefer a static assert rather than using __attribute__((__packed__)) to
// prevent inadvertent creation of unaligned fields that trigger undefined
// behavior.
static_assert(sizeof(RSMSnapshotHeader) == 32, "wrong expected size");

}} // namespace facebook::logdevice
