/**
 * Copyright (c) 2019-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <chrono>

#include "logdevice/include/types.h"

namespace facebook { namespace logdevice {

// This is similar to lib/verifier/VerificationDataStructures.h, but much
// simpler. We're using this instead of verifier because verifier implementation
// is not good enough for real use yet.
// Eventually we should make verifier good and throw this file away.

struct RecordWriterInfo {
  struct Header {
    uint64_t magic_number;
    uint32_t flags;
    uint32_t payload_offset;
  };

  // Time since epoch when the writer started the append.
  std::chrono::microseconds client_timestamp;

  // Exact number of bytes serialize() will need.
  size_t serializedSize() const;

  // *out must be at least serializedSize() long.
  void serialize(char* out) const;

  // If `payload` contains RecordWriterInfo, deserializes it into *this,
  // updates `payload` to point to only the user part of the payload,
  // and returns 0.
  // If `payload` doesn't seem to contain RecordWriterInfo, sets err to
  // E::NOTFOUND and returns -1.
  // If `payload` seems to contain malformed RecordWriterInfo, sets err to
  // E::MALFORMED_RECORD and returns -1.
  int deserialize(Payload& payload);
};

}} // namespace facebook::logdevice
