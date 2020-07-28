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
#include <folly/Varint.h>
#include <gmock/gmock.h>
#include <gtest/gtest.h>

namespace facebook { namespace logdevice {

namespace {

using PayloadVariant = std::variant<std::string, PayloadGroup>;

class BufferedWriteSinglePayloadEncoderTest
    : public ::testing::TestWithParam<std::vector<std::string>> {};

class BufferedWriteCodecTest
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

std::vector<PayloadVariant>
convert(const std::vector<std::string>& payloads_in) {
  std::vector<PayloadVariant> payloads;
  for (const auto& payload : payloads_in) {
    payloads.push_back(payload);
  }
  return payloads;
}

std::unordered_map<PayloadKey, std::string>
convert(const PayloadGroup& payload_group) {
  std::unordered_map<PayloadKey, std::string> result;
  for (auto& [key, payload] : payload_group) {
    result[key] = payload.cloneAsValue().moveToFbString().toStdString();
  }
  return result;
}

BufferedWriteCodec::Estimator
estimate(const std::vector<PayloadVariant>& payloads) {
  BufferedWriteCodec::Estimator estimator;
  for (const auto& payload : payloads) {
    withPayload(payload, [&](auto&& p) { estimator.append(std::move(p)); });
  }
  return estimator;
}

MATCHER_P(PayloadGroupEq, p1, "") {
  const PayloadGroup& p2 = arg;

  testing::Matcher<std::unordered_map<PayloadKey, std::string>> matcher =
      testing::ContainerEq(convert(p2));
  return matcher.MatchAndExplain(convert(p1), result_listener);
}

} // namespace

TEST(BufferedWriteCodecTest, FailDecodePayloadGroupFormat) {
  const int checksum_bits = 0;
  const size_t capacity = 0; // unused for PayloadGroupCodec, so use 0
  std::vector<PayloadVariant> payloads{PayloadGroup{}};
  auto encoded =
      encode<PayloadGroupCodec::Encoder>(checksum_bits, capacity, payloads);
  encoded.coalesce();

  // this API cannot decode Format::PAYLOAD_GROUPS, and should fail
  std::vector<folly::IOBuf> decoded;
  size_t consumed =
      BufferedWriteCodec::decode(Slice(encoded.data(), encoded.length()),
                                 decoded,
                                 /* allow_buffer_sharing */ true);
  EXPECT_EQ(consumed, 0);
  EXPECT_TRUE(decoded.empty());
}

TEST(BufferedWriteCodecTest, FailDecodeTrimmedData) {
  const int checksum_bits = 0;

  const std::string payload1 = "p1";
  // this should be big enough to take more than 1 byte to encode varint size
  const std::string payload2(1024, '-');
  ASSERT_GT(folly::encodeVarintSize(payload2.size()), 1);

  // prepare encoded data
  std::vector<PayloadVariant> payloads{payload1, payload2};
  auto estimator = estimate(payloads);
  ASSERT_EQ(estimator.getFormat(), BufferedWriteCodec::Format::SINGLE_PAYLOADS);
  auto encoded = encode<BufferedWriteSinglePayloadsCodec::Encoder>(
      checksum_bits, estimator.calculateSize(checksum_bits), payloads);
  encoded.coalesce();

  // trim encoded data to trigger different decoding failures
  std::vector<folly::IOBuf> decoded;

  auto checkDecode = [&] {
    size_t consumed =
        BufferedWriteCodec::decode(Slice(encoded.data(), encoded.length()),
                                   decoded,
                                   /* allow_buffer_sharing */ true);
    EXPECT_EQ(consumed, 0);
    EXPECT_TRUE(decoded.empty());
  };

  // too short payload (1 bytes missing)
  encoded.trimEnd(1);
  checkDecode();

  // no payload
  encoded.trimEnd(payload2.size() - 1);
  checkDecode();

  // invalid payload size varint
  encoded.trimEnd(1);
  checkDecode();
}

TEST(BufferedWriteCodecTest, FailDecodeTrimmedHeader) {
  const int checksum_bits = 0;

  // prepare encoded data
  std::vector<PayloadVariant> payloads;
  auto estimator = estimate(payloads);
  ASSERT_EQ(estimator.getFormat(), BufferedWriteCodec::Format::SINGLE_PAYLOADS);
  auto encoded = encode<BufferedWriteSinglePayloadsCodec::Encoder>(
      checksum_bits, estimator.calculateSize(checksum_bits), payloads);
  encoded.coalesce();

  // check that untouched data is decodable
  std::vector<folly::IOBuf> decoded;
  size_t consumed =
      BufferedWriteCodec::decode(Slice(encoded.data(), encoded.length()),
                                 decoded,
                                 /* allow_buffer_sharing */ true);
  EXPECT_EQ(consumed, encoded.length());
  EXPECT_TRUE(decoded.empty());

  // now trim bytes one by one and check that decoding fails
  while (!encoded.empty()) {
    encoded.trimEnd(1);

    consumed =
        BufferedWriteCodec::decode(Slice(encoded.data(), encoded.length()),
                                   decoded,
                                   /* allow_buffer_sharing */ true);
    EXPECT_EQ(consumed, 0);
    EXPECT_TRUE(decoded.empty());
  }
}

TEST(BufferedWriteCodecTest, FailDecodeCorruptedHeader) {
  const int checksum_bits = 0;

  // prepare encoded data
  std::vector<PayloadVariant> payloads;
  auto estimator = estimate(payloads);
  ASSERT_EQ(estimator.getFormat(), BufferedWriteCodec::Format::SINGLE_PAYLOADS);
  auto encoded = encode<BufferedWriteSinglePayloadsCodec::Encoder>(
      checksum_bits, estimator.calculateSize(checksum_bits), payloads);
  encoded.coalesce();

  // first byte stores encoding format, replace it with invalid format
  encoded.writableBuffer()[0] = 0;

  // check that decoding fails
  std::vector<folly::IOBuf> decoded;
  size_t consumed =
      BufferedWriteCodec::decode(Slice(encoded.data(), encoded.length()),
                                 decoded,
                                 /* allow_buffer_sharing */ true);
  EXPECT_EQ(consumed, 0);
  EXPECT_TRUE(decoded.empty());
}

TEST_P(BufferedWriteSinglePayloadEncoderTest, EncodeDecodeMatch) {
  const auto& payloads_in = GetParam();

  const auto converted = convert(payloads_in);

  // Use estimator to calculate capacity
  BufferedWriteCodec::Estimator estimator = estimate(converted);
  ASSERT_EQ(estimator.getFormat(), BufferedWriteCodec::Format::SINGLE_PAYLOADS);

  // Encode payloads
  const int checksum_bits = 0;
  auto encoded = encode<BufferedWriteSinglePayloadsCodec::Encoder>(
      checksum_bits, estimator.calculateSize(checksum_bits), converted);
  encoded.coalesce();

  // Check encoded batch size is correct
  size_t batch_size = 0;
  EXPECT_TRUE(BufferedWriteCodec::decodeBatchSize(
      Slice(encoded.data(), encoded.length()), &batch_size));
  EXPECT_EQ(batch_size, payloads_in.size());

  // Decode payloads
  std::vector<folly::IOBuf> decoded;
  size_t consumed =
      BufferedWriteCodec::decode(Slice(encoded.data(), encoded.length()),
                                 decoded,
                                 /* allow_buffer_sharing */ true);

  EXPECT_EQ(consumed, encoded.length());

  // Payloads should match
  std::vector<std::string> payloads_out;
  for (auto& payload : decoded) {
    payloads_out.push_back(payload.moveToFbString().toStdString());
  }

  EXPECT_EQ(payloads_in, payloads_out);
}

INSTANTIATE_TEST_CASE_P(EncodeDecodeMatch,
                        BufferedWriteSinglePayloadEncoderTest,
                        ::testing::ValuesIn<std::vector<std::string>>({
                            {},
                            {""},
                            {"", ""},
                            {"payload"},
                            {"", "payload"},
                            {"payload", ""},
                            {"", "payload", ""},
                            std::vector<std::string>(10, "p10"),
                            std::vector<std::string>(100, "p100"),
                            std::vector<std::string>(1000, "p1000"),
                            std::vector<std::string>(10000, "p10000"),
                            std::vector<std::string>(100000, "p100000"),
                        }));

TEST(BufferedWriteEstimatorTest, FormatChange) {
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

TEST_P(BufferedWriteCodecTest, EncodeDecodeMatch) {
  const auto& [checksum_bits, payloads_in] = GetParam();

  // estimate payloads size
  BufferedWriteCodec::Estimator estimator = estimate(payloads_in);
  size_t size = estimator.calculateSize(checksum_bits);

  // encode payloads
  folly::IOBuf encoded;
  switch (estimator.getFormat()) {
    case BufferedWriteCodec::Format::SINGLE_PAYLOADS:
      encoded = encode<BufferedWriteSinglePayloadsCodec::Encoder>(
          checksum_bits, size, payloads_in);
      break;
    case BufferedWriteCodec::Format::PAYLOAD_GROUPS:
      encoded =
          encode<PayloadGroupCodec::Encoder>(checksum_bits, size, payloads_in);
      break;
  }

  // check estimator provides correct estimate
  EXPECT_EQ(size, encoded.computeChainDataLength());

  encoded.coalesce();

  // trim checksum since it's always trimmed before passing data to decoder
  encoded.trimStart(checksum_bits / 8);

  std::vector<PayloadGroup> payloads_out;
  size_t consumed =
      BufferedWriteCodec::decode(Slice(encoded.data(), encoded.length()),
                                 payloads_out,
                                 /* allow_buffer_sharing */ true);
  EXPECT_EQ(consumed, encoded.length());

  // check decoded payloads match
  ASSERT_EQ(payloads_out.size(), payloads_in.size());
  for (int i = 0; i < payloads_out.size(); i++) {
    std::visit(
        folly::overload(
            [&](const PayloadGroup& payload_group) {
              EXPECT_THAT(payloads_out[i], PayloadGroupEq(payload_group));
            },
            [&](const std::string& payload) {
              ASSERT_EQ(payloads_out[i].size(), 1);
              EXPECT_EQ(payloads_out[i].at(0).moveToFbString().toStdString(),
                        payload);
            }),
        payloads_in[i]);
  }
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

INSTANTIATE_TEST_CASE_P(EncodeDecodeMatch,
                        BufferedWriteCodecTest,
                        ::testing::ValuesIn(payloads));
} // namespace

}} // namespace facebook::logdevice
