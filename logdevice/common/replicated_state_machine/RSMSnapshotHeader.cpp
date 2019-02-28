/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/common/replicated_state_machine/RSMSnapshotHeader.h"

#include "logdevice/common/Worker.h"
#include "logdevice/common/protocol/ProtocolReader.h"
#include "logdevice/common/protocol/ProtocolWriter.h"

namespace facebook { namespace logdevice {

int RSMSnapshotHeader::serialize(const RSMSnapshotHeader& hdr,
                                 void* payload,
                                 size_t size) {
  ProtocolWriter writer({payload, size}, "RSMSnapshotHeader::serialize");
  hdr.serialize(writer);

  if (writer.isBlackHole()) {
    return writer.result();
  } else if (writer.error()) {
    err = writer.status();
    return -1;
  } else if (hdr.format_version >= CONTAINS_DELTA_LOG_READ_PTR_AND_LENGTH) {
    if (hdr.length < 0 || hdr.length > size || hdr.length < writer.result()) {
      err = E::PROTO;
      return -1;
    } else {
      // Return the length of the header so extra bytes can be skipped.
      return hdr.length;
    }
  } else {
    return writer.result();
  }
}

int RSMSnapshotHeader::deserialize(Payload payload, RSMSnapshotHeader& out) {
  ProtocolReader reader(
      {payload.data(), payload.size()}, "RSMSnapshotHeader::deserialize");
  out.deserialize(reader, /*zero_copy=*/false);

  if (reader.error()) {
    err = reader.status();
    return -1;
  } else if (out.format_version >= CONTAINS_DELTA_LOG_READ_PTR_AND_LENGTH) {
    if (out.length < 0 || out.length > payload.size() ||
        out.length < reader.bytesRead()) {
      err = E::PROTO;
      return -1;
    } else {
      // Return the length of the header so extra bytes can be skipped.
      return out.length;
    }
  } else {
    return reader.bytesRead();
  }
}

void RSMSnapshotHeader::deserialize(ProtocolReader& reader,
                                    bool /* unused */,
                                    folly::Optional<size_t> /* unused */) {
  reader.readVersion(&format_version);
  reader.read(&flags);
  reader.read(&byte_offset);
  reader.read(&offset);
  reader.read(&base_version);
  reader.protoGate(CONTAINS_DELTA_LOG_READ_PTR_AND_LENGTH);
  reader.read(&length);
  reader.read(&delta_log_read_ptr);
}

void RSMSnapshotHeader::serialize(ProtocolWriter& writer) const {
  writer.writeVersion(format_version);
  writer.write(flags);
  writer.write(byte_offset);
  writer.write(offset);
  writer.write(base_version);
  writer.protoGate(CONTAINS_DELTA_LOG_READ_PTR_AND_LENGTH);
  writer.write(length);
  writer.write(delta_log_read_ptr);
}

bool RSMSnapshotHeader::operator==(const RSMSnapshotHeader& out) const {
  return format_version == out.format_version && flags == out.flags &&
      byte_offset == out.byte_offset && offset == out.offset &&
      base_version == out.base_version &&
      (format_version < CONTAINS_DELTA_LOG_READ_PTR_AND_LENGTH ||
       (delta_log_read_ptr == out.delta_log_read_ptr && length == out.length));
}

}} // namespace facebook::logdevice
