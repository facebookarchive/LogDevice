/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "logdevice/common/SerializableData.h"

#include "logdevice/common/protocol/ProtocolReader.h"
#include "logdevice/common/protocol/ProtocolWriter.h"

namespace facebook { namespace logdevice {

int SerializableData::serialize(void* buffer, size_t size) const {
  ProtocolWriter w({buffer, size}, name(), 0);
  this->serialize(w);
  if (w.error()) {
    err = w.status();
    return -1;
  }
  return w.result();
}

int SerializableData::deserialize(Slice buffer,
                                  folly::Optional<size_t> expected_size) {
  ProtocolReader reader(buffer, name(), 0);
  this->deserialize(reader, false, expected_size);
  if (reader.error()) {
    err = reader.status();
    return -1;
  }
  return reader.bytesRead();
}

}} // namespace facebook::logdevice
