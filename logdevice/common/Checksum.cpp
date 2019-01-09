/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/common/Checksum.h"

#include <folly/hash/Checksum.h>
#include <folly/hash/Hash.h>

namespace facebook { namespace logdevice {

uint32_t checksum_32bit(Slice slice) {
  return folly::crc32c((const uint8_t*)slice.data, slice.size);
}

uint64_t checksum_64bit(Slice slice) {
  const uint64_t seed = 0x5715d9be01f6a3f8ULL; // randomly generated
  return folly::hash::SpookyHashV2::Hash64(slice.data, slice.size, seed);
}

Slice checksum_bytes(Slice blob, int nbits, char* buf_out) {
  ld_check(nbits == 32 || nbits == 64);
  if (nbits == 64) {
    uint64_t c64 = checksum_64bit(blob);
    memcpy(buf_out, &c64, sizeof c64);
  } else {
    uint32_t c32 = checksum_32bit(blob);
    memcpy(buf_out, &c32, sizeof c32);
  }
  return Slice(buf_out, nbits / 8);
}

}} // namespace facebook::logdevice
