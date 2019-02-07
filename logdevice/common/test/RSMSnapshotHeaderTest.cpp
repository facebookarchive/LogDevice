/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/common/replicated_state_machine/RSMSnapshotHeader.h"

#include <gtest/gtest.h>

using namespace facebook::logdevice;
using namespace testing;

struct LegacyRSMSnapshotHeader {
  uint32_t format_version;
  uint32_t flags;
  uint64_t byte_offset;
  uint64_t offset;
  lsn_t base_version;
};

static_assert(sizeof(LegacyRSMSnapshotHeader) == 32);

const uint32_t format_version = 0;
const uint32_t flags = 0x12345678u;
const uint64_t byte_offset = 0x123456789ABCDEF0u;
const uint64_t offset = 0x23456789ABCDEF01u;
const lsn_t base_version = 0x3456789ABCDEF012u;
const RSMSnapshotHeader default_header{format_version,
                                       flags,
                                       byte_offset,
                                       offset,
                                       base_version};

const LegacyRSMSnapshotHeader default_header_legacy{format_version,
                                                    flags,
                                                    byte_offset,
                                                    offset,
                                                    base_version};

TEST(RSMSnapshotHeaderTest, SerializationAndDeserialization) {
  RSMSnapshotHeader header = default_header;

  uint8_t buf[sizeof(header)];
  memset(buf, 0xFF, sizeof(buf));

  /* serialize onto buf */
  auto length = RSMSnapshotHeader::serialize(header, nullptr, 0);
  ASSERT_LE(length, sizeof(header));
  RSMSnapshotHeader::serialize(header, &buf, sizeof(header));

  RSMSnapshotHeader output;

  RSMSnapshotHeader::deserialize(Payload(buf, sizeof(buf)), output);

  EXPECT_EQ(output, header);
}

TEST(RSMSnapshotHeaderTest, BackwardsCompatibility) {
  uint8_t buf1[sizeof(default_header_legacy)];
  uint8_t buf2[sizeof(default_header_legacy)];
  memset(buf1, 0xFF, sizeof(buf1));
  memset(buf2, 0xFF, sizeof(buf2));

  /* serialize onto buf1 */
  auto length = RSMSnapshotHeader::serialize(default_header, nullptr, 0);
  ASSERT_LE(length, sizeof(default_header));
  RSMSnapshotHeader::serialize(default_header, &buf1, sizeof(default_header));

  memcpy(buf2, &default_header_legacy, sizeof(default_header_legacy));

  EXPECT_EQ(memcmp(buf1, buf2, sizeof(buf1)), 0);
}
