/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/common/replicated_state_machine/RSMSnapshotHeader.h"

namespace facebook { namespace logdevice {

int RSMSnapshotHeader::deserialize(Payload payload, RSMSnapshotHeader& out) {
  if (payload.size() < sizeof(out)) {
    err = E::BADMSG;
    return -1;
  }

  const uint8_t* ptr = reinterpret_cast<const uint8_t*>(payload.data());
  memcpy(&out, ptr, sizeof(out));

  return sizeof(out);
}

int RSMSnapshotHeader::serialize(const RSMSnapshotHeader& hdr,
                                 void* payload,
                                 size_t size) {
  if (payload) {
    if (size < sizeof(hdr)) {
      err = E::NOBUFS;
      return -1;
    }
    uint8_t* ptr = reinterpret_cast<uint8_t*>(payload);
    memcpy(ptr, &hdr, sizeof(hdr));
  }

  return sizeof(hdr);
}

}} // namespace facebook::logdevice
