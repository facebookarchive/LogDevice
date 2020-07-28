/**
 * Copyright (c) 2020-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "logdevice/common/buffered_writer/BufferedWriteCodec.h"

#include <unordered_map>
#include <variant>

#include <folly/Overload.h>
#include <gtest/gtest.h>

namespace facebook { namespace logdevice {

namespace {

using PayloadVariant = std::variant<std::string, PayloadGroup>;

class BufferedWriteEstimatorTest
    : public ::testing::TestWithParam<
          std::pair<int, std::vector<PayloadVariant>>> {};

template <typename F>
void withPayload(const PayloadVariant& payload_variant, F&& func) {
  std::visit(
      folly::overload(
          [&](const std::string& payload) {
            auto iobuf =
                folly::IOBuf::wrapBufferAsValue(payload.data(), payload.size());
            func(std::move(iobuf));
          },
          [&](const PayloadGroup& payload_group) { func(payload_group); }),
      payload_variant);
}

template <typename T>
folly::IOBuf encode(int checksum_bits,
                    size_t size,
                    const std::vector<PayloadVariant>& payloads) {
  BufferedWriteCodec::Encoder<T> encoder(checksum_bits, payloads.size(), size);
  for (const auto& payload : payloads) {
    withPayload(payload, [&](auto&& p) { encoder.append(std::move(p)); });
  }
  folly::IOBufQueue queue;
  encoder.encode(queue, Compression::NONE);
  return queue.moveAsValue();
}

} // namespace

TEST_F(BufferedWriteEstimatorTest, FormatChange) {
  const folly::IOBuf payload1 =
      folly::IOBuf::wrapBufferAsValue(folly::StringPiece("payload1"));
  const folly::IOBuf payload2 =
      folly::IOBuf::wrapBufferAsValue(folly::StringPiece("payload2"));

  PayloadGroup group{{0, payload1}};

  BufferedWriteCodec::Estimator estimator;
  EXPECT_EQ(estimator.getFormat(), BufferedWriteCodec::Format::SINGLE_PAYLOADS);

  estimator.append(payload1);
  EXPECT_EQ(estimator.getFormat(), BufferedWriteCodec::Format::SINGLE_PAYLOADS);
  estimator.append(payload2);
  EXPECT_EQ(estimator.getFormat(), BufferedWriteCodec::Format::SINGLE_PAYLOADS);
  estimator.append(group);
  EXPECT_EQ(estimator.getFormat(), BufferedWriteCodec::Format::PAYLOAD_GROUPS);
}

TEST_P(BufferedWriteEstimatorTest, EstimateMatch) {
  const auto& [checksum_bits, payloads] = GetParam();

  BufferedWriteCodec::Estimator estimator;
  for (const auto& payload : payloads) {
    withPayload(payload, [&](const auto& p) { estimator.append(p); });
  }
  const size_t size = estimator.calculateSize(checksum_bits);

  folly::IOBuf encoded;
  switch (estimator.getFormat()) {
    case BufferedWriteCodec::Format::SINGLE_PAYLOADS:
      encoded = encode<BufferedWriteSinglePayloadsCodec::Encoder>(
          checksum_bits, size, payloads);
      break;
    case BufferedWriteCodec::Format::PAYLOAD_GROUPS:
      encoded =
          encode<PayloadGroupCodec::Encoder>(checksum_bits, size, payloads);
      break;
  }
  EXPECT_EQ(size, encoded.computeChainDataLength());
}

namespace {

folly::IOBuf iobuf_empty;
folly::IOBuf iobuf_payload =
    *folly::IOBuf::copyBuffer(folly::StringPiece("payload1"));

PayloadGroup group_empty;
PayloadGroup group1 = {{1, iobuf_empty}};
PayloadGroup group2 = {{2, iobuf_payload}};

const std::vector<std::pair<int, std::vector<PayloadVariant>>> payloads{
    // {checksum_bits, payloads}
    {0, {}},
    {32, {}},
    {64, {}},
    {0, {""}},
    {32, {"", ""}},
    {64, {"payload"}},
    {0, {"", "payload"}},
    {32, {"payload", ""}},
    {64, {std::string(1024, '\0'), "()", std::string(1234, '?')}},
    {32, std::vector<PayloadVariant>(10, "-")},
    {32, std::vector<PayloadVariant>(100, "-")},
    {32, std::vector<PayloadVariant>(1000, "-")},
    {32, std::vector<PayloadVariant>(10000, "-")},
    {32, std::vector<PayloadVariant>(100000, "-")},

    {0, {group_empty}},
    {32, {"", group_empty}},
    {64, {"payload", group1}},
    {0, {group1, group2}},
    {32, {group1, group2, ""}},
    {64, {group1, group1, "", "payload"}},
};

INSTANTIATE_TEST_CASE_P(EstimateMatch,
                        BufferedWriteEstimatorTest,
                        ::testing::ValuesIn(payloads));
} // namespace

}} // namespace facebook::logdevice
