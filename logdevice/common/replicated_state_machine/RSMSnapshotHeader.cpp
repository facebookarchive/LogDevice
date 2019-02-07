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
  return writer.result();
}

int RSMSnapshotHeader::deserialize(Payload payload, RSMSnapshotHeader& out) {
  ProtocolReader reader(
      {payload.data(), payload.size()}, "RSMSnapshotHeader::deserialize");
  out.deserialize(reader, /*zero_copy=*/false);

  if (reader.error()) {
    err = reader.status();
    return -1;
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
}

void RSMSnapshotHeader::serialize(ProtocolWriter& writer) const {
  writer.writeVersion(format_version);
  writer.write(flags);
  writer.write(byte_offset);
  writer.write(offset);
  writer.write(base_version);
}

bool RSMSnapshotHeader::operator==(const RSMSnapshotHeader& out) const {
  return format_version == out.format_version && flags == out.flags &&
      byte_offset == out.byte_offset && offset == out.offset &&
      base_version == out.base_version;
}

}} // namespace facebook::logdevice
