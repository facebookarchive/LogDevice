/**
 * Copyright (c) 2019-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "logdevice/test/ldbench/worker/RecordWriterInfo.h"

#include <cstring>

#include "logdevice/include/Err.h"

namespace facebook { namespace logdevice {

// If payload starts with these 8 bytes, we assume it has RecordWriterInfo.
static const uint64_t MAGIC_NUMBER = 0xf506aab41f587bb5ul;

size_t RecordWriterInfo::serializedSize() const {
  return sizeof(Header) + sizeof(RecordWriterInfo);
}

void RecordWriterInfo::serialize(char* out) const {
  Header h;
  h.magic_number = MAGIC_NUMBER;
  h.flags = 0;
  h.payload_offset = serializedSize();
  memcpy(out, &h, sizeof(h));
  int64_t ts = client_timestamp.count();
  memcpy(out + sizeof(h), &ts, sizeof(ts));
}

int RecordWriterInfo::deserialize(Payload& payload) {
  const char* data = reinterpret_cast<const char*>(payload.data());
  Header h;

  if (payload.size() < sizeof(Header) + sizeof(RecordWriterInfo)) {
    memcpy(&h.magic_number, data, sizeof(h.magic_number));
    err = h.magic_number == MAGIC_NUMBER ? E::MALFORMED_RECORD : E::NOTFOUND;
    return -1;
  }

  memcpy(&h, data, sizeof(h));
  if (h.magic_number != MAGIC_NUMBER) {
    err = E::NOTFOUND;
    return -1;
  }

  if (h.payload_offset > payload.size()) {
    err = E::MALFORMED_RECORD;
    return -1;
  }

  int64_t ts;
  memcpy(&ts, data + sizeof(h), sizeof(ts));
  client_timestamp = std::chrono::microseconds(ts);
  size_t size = payload.size() - h.payload_offset;
  payload = size ? Payload(data + h.payload_offset, size) : Payload();

  return 0;
}

}} // namespace facebook::logdevice
