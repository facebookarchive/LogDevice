/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/common/replicated_state_machine/RSMSnapshotHeader.h"

#include <gtest/gtest.h>

#include "logdevice/common/protocol/ProtocolWriter.h"

using namespace facebook::logdevice;
using namespace testing;

struct LegacyRSMSnapshotHeader {
  uint32_t format_version;
  uint32_t flags;
  uint64_t byte_offset;
  uint64_t offset;
  lsn_t base_version;
};

static_assert(sizeof(LegacyRSMSnapshotHeader) == 32, "");

const uint32_t flags = 0x12345678u;
const uint64_t byte_offset = 0x123456789ABCDEF0u;
const uint64_t offset = 0x23456789ABCDEF01u;
const lsn_t base_version = 0x3456789ABCDEF012u;
const lsn_t last_read_lsn = 0x456789ABCDEF0123u;

const RSMSnapshotHeader headerv0{/*format_version=*/0,
                                 flags,
                                 byte_offset,
                                 offset,
                                 base_version};

const RSMSnapshotHeader headerv1{/*format_version=*/1,
                                 flags,
                                 byte_offset,
                                 offset,
                                 base_version,
                                 last_read_lsn};

const LegacyRSMSnapshotHeader headerv0_legacy{/*format_version=*/0,
                                              flags,
                                              byte_offset,
                                              offset,
                                              base_version};

TEST(RSMSnapshotHeaderTest, SerializationAndDeserializationv0) {
  RSMSnapshotHeader header = headerv0;

  std::array<uint8_t, sizeof(header)> buf;
  memset(buf.data(), 0xFF, buf.size());

  /* serialize onto buf */
  auto length = RSMSnapshotHeader::serialize(header, nullptr, 0);
  ASSERT_LE(length, sizeof(header));
  RSMSnapshotHeader::serialize(header, &buf, sizeof(header));

  RSMSnapshotHeader output;

  RSMSnapshotHeader::deserialize(Payload(buf.data(), buf.size()), output);

  EXPECT_EQ(output, header);
}

TEST(RSMSnapshotHeaderTest, SerializationAndDeserializationv1) {
  RSMSnapshotHeader header = headerv1;

  std::array<uint8_t, sizeof(header)> buf;
  memset(buf.data(), 0xFF, buf.size());

  /* serialize onto buf */
  auto length = RSMSnapshotHeader::serialize(header, nullptr, 0);
  ASSERT_LE(length, sizeof(header));
  RSMSnapshotHeader::serialize(header, &buf, sizeof(header));

  RSMSnapshotHeader output;

  RSMSnapshotHeader::deserialize(Payload(buf.data(), buf.size()), output);

  EXPECT_EQ(output, header);
}

TEST(RSMSnapshotHeaderTest, BackwardsCompatibility) {
  std::array<uint8_t, sizeof(headerv0_legacy)> buf1;
  std::array<uint8_t, sizeof(headerv0_legacy)> buf2;
  memset(buf1.data(), 0xFF, buf1.size());
  memset(buf2.data(), 0xFF, buf2.size());

  /* serialize onto buf1 */
  auto length = RSMSnapshotHeader::serialize(headerv0, nullptr, 0);
  ASSERT_LE(length, sizeof(headerv0));
  RSMSnapshotHeader::serialize(headerv0, &buf1, sizeof(headerv0));

  memcpy(buf2.data(), &headerv0_legacy, sizeof(headerv0_legacy));

  EXPECT_EQ(memcmp(buf1.data(), buf2.data(), buf1.size()), 0);
}

TEST(RSMSnapshotHeaderTest, ForwardsCompatibility) {
  auto header = headerv1;

  // header now comes from the future.
  header.format_version = std::numeric_limits<uint16_t>::max();
  constexpr int len_from_future_version = sizeof(header) + 100;
  header.length = len_from_future_version;

  std::array<uint8_t, len_from_future_version> buf;

  auto out1 = RSMSnapshotHeader::serialize(header, buf.data(), buf.size());
  ASSERT_GT(out1, 0);

  decltype(header) header_out;

  // we should fail if we the length in the header is too big.
  auto outf = RSMSnapshotHeader::deserialize(
      Payload(buf.data(), sizeof(header)), header_out);
  ASSERT_LE(outf, 0);

  // length is ok
  auto out2 = RSMSnapshotHeader::deserialize(
      Payload(buf.data(), buf.size()), header_out);
  ASSERT_EQ(out2, len_from_future_version);

  // set the length in the header to be smaller than the number of bytes read
  header.length = 1;
  ASSERT_EQ(RSMSnapshotHeader::serialize(header, buf.data(), buf.size()), -1);

  // we now skip serialization length checks to test deserialization length
  // checks
  {
    ProtocolWriter writer({buf.data(), buf.size()}, "");
    header.serialize(writer);
    ASSERT_FALSE(writer.error());
    ASSERT_LE(RSMSnapshotHeader::deserialize(
                  Payload(buf.data(), buf.size()), header_out),
              0);
  }
}
