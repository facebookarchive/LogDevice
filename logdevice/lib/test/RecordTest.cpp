/**
 * Copyright (c) 2020-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/include/Record.h"

#include <gtest/gtest.h>

using namespace ::testing;

namespace facebook { namespace logdevice {

TEST(DataRecordTest, EmptyPayloadGroup) {
  DataRecord data_record{logid_t{1}, PayloadGroup()};
  EXPECT_EQ(data_record.payload.data(), nullptr);
  EXPECT_EQ(data_record.payload.size(), 0);
}

TEST(DataRecordTest, EmptyPrimaryPayload) {
  // Payload requires data to be nullptr in case sice is 0
  // Test ensures that different empty IOBufs are properly handled, when
  // converting PayloadGroup to Payload

  folly::IOBuf empty_iobuf1(folly::IOBuf::COPY_BUFFER, "");
  DataRecord data_record1{logid_t{1}, PayloadGroup{{0, empty_iobuf1}}};
  EXPECT_EQ(data_record1.payload.data(), nullptr);
  EXPECT_EQ(data_record1.payload.size(), 0);

  folly::IOBuf empty_iobuf2;
  DataRecord data_record2{logid_t{1}, PayloadGroup{{0, empty_iobuf2}}};
  EXPECT_EQ(data_record2.payload.data(), nullptr);
  EXPECT_EQ(data_record2.payload.size(), 0);
}

TEST(DataRecordTest, PayloadMapsToPayloadGroup) {
  const std::string data = "payload";
  DataRecord data_record{logid_t{1}, Payload{data.data(), data.size()}};

  // When record is created with Payload, it's expected to appear in payloads as
  // 0th payload.
  ASSERT_EQ(data_record.payloads.size(), 1);
  ASSERT_EQ(data_record.payloads.count(0), 1);

  folly::IOBuf& iobuf = data_record.payloads.at(0);
  EXPECT_EQ(folly::StringPiece(
                reinterpret_cast<const char*>(iobuf.data()), iobuf.length()),
            data);
}

TEST(DataRecordTest, PayloadGroupMapsToPayload) {
  const std::string data0_1 = "pay";
  const std::string data0_2 = "load[0]";
  const std::string data1 = "payload[1]";

  // Create IOBuf for 0th payload, which is a chain of two pieces.
  auto iobuf0 = folly::IOBuf::wrapBufferAsValue(data0_1.data(), data0_1.size());
  iobuf0.prependChain(folly::IOBuf::wrapBuffer(data0_2.data(), data0_2.size()));
  ASSERT_TRUE(iobuf0.isChained());

  auto iobuf1 = folly::IOBuf::wrapBufferAsValue(data1.data(), data1.size());

  PayloadGroup payload_group({{0, std::move(iobuf0)}, {1, std::move(iobuf1)}});
  DataRecord data_record{logid_t{1}, std::move(payload_group)};

  // Chained IOBuf should be coalesced to make sure it's convertible to Payload
  EXPECT_EQ(folly::StringPiece(
                reinterpret_cast<const char*>(data_record.payload.data()),
                data_record.payload.size()),
            data0_1 + data0_2);
}

}} // namespace facebook::logdevice
