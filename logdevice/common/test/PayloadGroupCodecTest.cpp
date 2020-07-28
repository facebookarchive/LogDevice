/**
 * Copyright (c) 2020-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "logdevice/common/PayloadGroupCodec.h"

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "logdevice/common/ThriftCodec.h"
#include "logdevice/common/if/gen-cpp2/payload_types.h"

namespace facebook::logdevice {

namespace {

std::string to_string(const folly::IOBuf& buf) {
  std::string res;
  res.reserve(buf.computeChainDataLength());
  for (auto range : buf) {
    res.append(reinterpret_cast<const char*>(range.data()), range.size());
  }
  return res;
}

PayloadGroup
from_map(const std::unordered_map<PayloadKey, std::string>& payloads) {
  PayloadGroup result;
  for (const auto& [key, payload] : payloads) {
    result[key] = *folly::IOBuf::copyBuffer(payload);
  }
  return result;
}

std::unordered_map<PayloadKey, std::string>
to_map(const PayloadGroup& payload_group) {
  std::unordered_map<PayloadKey, std::string> result;
  for (auto& [key, payload] : payload_group) {
    result[key] = to_string(payload);
  }
  return result;
}

folly::IOBuf encode(const PayloadGroup& payload_group) {
  folly::IOBufQueue queue;
  PayloadGroupCodec::encode(payload_group, queue);
  return queue.moveAsValue();
}

using ThriftSerializer = apache::thrift::BinarySerializer;

void to_thrift(
    const folly::IOBuf& encoded,
    std::map<PayloadKey, thrift::CompressedPayloadsMetadata>& metadata_out,
    std::map<PayloadKey, folly::IOBuf>& payloads_out) {
  thrift::CompressedPayloadGroups compressed_payload_groups;
  size_t size = ThriftCodec::deserialize<ThriftSerializer>(
      &encoded, compressed_payload_groups);
  CHECK_NE(size, 0);

  for (auto& [key, compressed_payloads] :
       *compressed_payload_groups.payloads_ref()) {
    CHECK_EQ(*compressed_payloads.metadata_compression_ref(),
             static_cast<int8_t>(Compression::NONE));

    size = ThriftCodec::deserialize<ThriftSerializer>(
        &compressed_payloads.compressed_metadata_ref().value(),
        metadata_out[key]);
    CHECK_NE(size, 0);
    payloads_out[key] =
        std::move(*compressed_payloads.compressed_payloads_ref());
  }
}

void to_thrift(
    const PayloadGroup& payload_group,
    std::map<PayloadKey, thrift::CompressedPayloadsMetadata>& metadata_out,
    std::map<PayloadKey, folly::IOBuf>& payloads_out) {
  auto encoded = encode(payload_group);
  encoded.coalesce();
  to_thrift(encoded, metadata_out, payloads_out);
}

size_t from_thrift(
    const std::map<PayloadKey, thrift::CompressedPayloadsMetadata>& metadata,
    const std::map<PayloadKey, folly::IOBuf>& payloads,
    PayloadGroup& payload_group_out) {
  thrift::CompressedPayloadGroups compressed_payload_groups;

  for (auto& [key, payload] : payloads) {
    folly::IOBufQueue queue;

    ThriftCodec::serialize<ThriftSerializer>(metadata.at(key), &queue);

    auto& compressed_payloads = compressed_payload_groups.payloads_ref()[key];
    compressed_payloads.metadata_compression_ref() =
        static_cast<int8_t>(Compression::NONE);
    compressed_payloads.compressed_metadata_ref() = queue.moveAsValue();
    compressed_payloads.metadata_uncompressed_size_ref() =
        compressed_payloads.compressed_metadata_ref()->computeChainDataLength();
    compressed_payloads.payloads_compression_ref() =
        static_cast<int8_t>(Compression::NONE);
    compressed_payloads.compressed_payloads_ref() = payload;
  }

  folly::IOBufQueue queue;
  ThriftCodec::serialize<ThriftSerializer>(compressed_payload_groups, &queue);
  auto encoded = queue.move();
  encoded->coalesce();
  return PayloadGroupCodec::decode(Slice(encoded->data(), encoded->length()),
                                   payload_group_out,
                                   /* allow_buffer_sharing */ true);
}

} // namespace

class PayloadGroupCodecTest : public ::testing::TestWithParam<PayloadGroup> {};

TEST_P(PayloadGroupCodecTest, EncodeDecodeMatch) {
  PayloadGroup payload_group_in = GetParam();

  auto encoded = encode(payload_group_in);
  encoded.coalesce();

  PayloadGroup payload_group_out;
  size_t consumed =
      PayloadGroupCodec::decode(Slice(encoded.data(), encoded.length()),
                                payload_group_out,
                                /* allow_buffer_sharing */ true);

  EXPECT_EQ(consumed, encoded.length());
  EXPECT_EQ(to_map(payload_group_out), to_map(payload_group_in));
}

INSTANTIATE_TEST_CASE_P(EncodeDecodeMatch,
                        PayloadGroupCodecTest,
                        ::testing::Values(from_map({}),
                                          from_map({{7, "payload7"}}),
                                          from_map({{1, "payload1"},
                                                    {42, "payload42"}})));

TEST_F(PayloadGroupCodecTest, FailDecodeCorruptDescriptors) {
  const PayloadKey key = 1;
  PayloadGroup payload_group_in =
      from_map({{key, "payload1"}, {42, "payload42"}});
  std::map<PayloadKey, thrift::CompressedPayloadsMetadata> metadata;
  std::map<PayloadKey, folly::IOBuf> payloads;
  to_thrift(payload_group_in, metadata, payloads);

  // make sure thrift is correct
  PayloadGroup payload_group_out;
  EXPECT_NE(from_thrift(metadata, payloads, payload_group_out), 0);

  // add extra descriptor - should fail decoding
  err = E::OK;
  metadata[key].descriptors_ref()->emplace_back();
  EXPECT_EQ(from_thrift(metadata, payloads, payload_group_out), 0);
  EXPECT_EQ(err, E::BADMSG);

  metadata[key].descriptors_ref()->pop_back();

  // negative payload size
  err = E::OK;
  metadata[key]
      .descriptors_ref()
      ->back()
      .descriptor_ref()
      ->uncompressed_size_ref() = -5;
  EXPECT_EQ(from_thrift(metadata, payloads, payload_group_out), 0);
  EXPECT_EQ(err, E::BADMSG);

  // remove descriptors - should fail decoding
  err = E::OK;
  metadata[key].descriptors_ref()->pop_back();
  EXPECT_EQ(from_thrift(metadata, payloads, payload_group_out), 0);
  EXPECT_EQ(err, E::BADMSG);
}

TEST_F(PayloadGroupCodecTest, FailDecodeCorruptPayload) {
  const PayloadKey key = 1;
  PayloadGroup payload_group_in = from_map({{key, "payload1"}});
  std::map<PayloadKey, thrift::CompressedPayloadsMetadata> metadata;
  std::map<PayloadKey, folly::IOBuf> payloads;
  to_thrift(payload_group_in, metadata, payloads);

  // make sure thrift is correct
  PayloadGroup payload_group_out;
  EXPECT_NE(from_thrift(metadata, payloads, payload_group_out), 0);

  // extra data in payload - should fail decoding
  err = E::OK;
  payloads[key].prependChain(folly::IOBuf::copyBuffer("extra"));
  EXPECT_EQ(from_thrift(metadata, payloads, payload_group_out), 0);
  EXPECT_EQ(err, E::BADMSG);

  // not enough data in payload - should fail decoding
  err = E::OK;
  payloads[key] = *folly::IOBuf::copyBuffer("damaged");
  EXPECT_EQ(from_thrift(metadata, payloads, payload_group_out), 0);
  EXPECT_EQ(err, E::BADMSG);
}

TEST_F(PayloadGroupCodecTest, FailDecodeCorruptBinary) {
  std::string data = "";
  PayloadGroup payload_group;

  err = E::OK;
  EXPECT_EQ(PayloadGroupCodec::decode(Slice(data.data(), data.size()),
                                      payload_group,
                                      /* allow_buffer_sharing */ true),
            0);
  EXPECT_EQ(err, E::BADMSG);

  data = "damaged";
  err = E::OK;
  EXPECT_EQ(PayloadGroupCodec::decode(Slice(data.data(), data.size()),
                                      payload_group,
                                      /* allow_buffer_sharing */ true),
            0);
  EXPECT_EQ(err, E::BADMSG);
}

class PayloadGroupEncoderTest
    : public ::testing::TestWithParam<std::vector<PayloadGroup>> {};

MATCHER(PayloadGroupEq, "") {
  const PayloadGroup& p1 = std::get<0>(arg);
  const PayloadGroup& p2 = std::get<1>(arg);

  testing::Matcher<std::unordered_map<PayloadKey, std::string>> matcher =
      testing::ContainerEq(to_map(p2));
  return matcher.MatchAndExplain(to_map(p1), result_listener);
}

TEST_P(PayloadGroupEncoderTest, EncodeDecodeMatch) {
  const auto& payload_groups = GetParam();

  // Loop to test intermediate results
  for (int i = 1; i <= payload_groups.size(); i++) {
    PayloadGroupCodec::Encoder encoder(i);

    std::vector<PayloadGroup> payload_groups_in;
    for (const auto& payload_group : payload_groups) {
      payload_groups_in.push_back(payload_group);
      encoder.append(payload_group);
      if (payload_groups_in.size() == i) {
        break;
      }
    }

    folly::IOBufQueue queue;
    encoder.encode(queue, Compression::NONE);
    folly::IOBuf encoded = queue.moveAsValue();
    encoded.coalesce();

    std::vector<PayloadGroup> payload_groups_out;
    size_t consumed =
        PayloadGroupCodec::decode(Slice(encoded.data(), encoded.length()),
                                  payload_groups_out,
                                  /* allow_buffer_sharing */ true);
    EXPECT_EQ(consumed, encoded.length());

    EXPECT_THAT(payload_groups_out,
                testing::Pointwise(PayloadGroupEq(), payload_groups_in));
  }
}

TEST_F(PayloadGroupEncoderTest, NoAppends) {
  folly::IOBufQueue queue;
  PayloadGroupCodec::Encoder encoder(0);
  encoder.encode(queue, Compression::NONE);
  folly::IOBuf encoded = queue.moveAsValue();
  encoded.coalesce();
  std::vector<PayloadGroup> decoded;
  size_t consumed =
      PayloadGroupCodec::decode(Slice(encoded.data(), encoded.length()),
                                decoded,
                                /* allow_buffer_sharing */ true);
  EXPECT_EQ(consumed, encoded.length());
  EXPECT_TRUE(decoded.empty());
}

TEST(PayloadGroupDecoderTest, BufferSharing) {
  // Encode payload group
  PayloadGroupCodec::Encoder encoder(1);
  const PayloadKey key = 1;
  std::string payload = "payload";
  encoder.append(PayloadGroup{
      {key, folly::IOBuf::wrapBufferAsValue(payload.data(), payload.size())}});
  folly::IOBufQueue queue;
  encoder.encode(queue, Compression::NONE);

  folly::IOBuf encoded = queue.moveAsValue();
  encoded.coalesce();

  // Now decode it with sharing disabled and enabled
  PayloadGroup payload_group_no_sharing;
  ASSERT_GT(PayloadGroupCodec::decode(Slice(encoded.data(), encoded.length()),
                                      payload_group_no_sharing,
                                      /* allow_buffer_sharing */ false),
            0);
  EXPECT_TRUE(payload_group_no_sharing.at(key).isManaged());
  EXPECT_FALSE(payload_group_no_sharing.at(key).isChained());

  PayloadGroup payload_group_sharing;
  ASSERT_GT(
      PayloadGroupCodec::decode(
          Slice(encoded.data(), encoded.length()), payload_group_sharing, true),
      0);
  // IOBuf will be unnmanaged, since it points to slice fragment
  EXPECT_FALSE(payload_group_sharing.at(key).isManaged());
  EXPECT_FALSE(payload_group_sharing.at(key).isChained());

  // Change payload in encoded buffer
  size_t index = folly::qfind(
      folly::StringPiece(
          reinterpret_cast<const char*>(encoded.data()), encoded.length()),
      folly::StringPiece(payload));
  encoded.writableData()[index] = 'P';

  // Decoded group, sharing buffer, should see the change
  EXPECT_EQ(std::string(reinterpret_cast<const char*>(
                            payload_group_sharing.at(key).data()),
                        payload_group_sharing.at(key).length()),
            "Payload");

  // Decoded group, not sharing buffer, should remain unchanged
  EXPECT_EQ(std::string(reinterpret_cast<const char*>(
                            payload_group_no_sharing.at(key).data()),
                        payload_group_no_sharing.at(key).length()),
            "payload");
}

class PayloadGroupEncoderCompressionTest
    : public ::testing::TestWithParam<Compression> {};

TEST_P(PayloadGroupEncoderCompressionTest, CompressUncompress) {
  const PayloadKey compressible_key = 0;
  const PayloadKey incompressible_key = 1;
  std::vector<PayloadGroup> payload_groups_in = {
      from_map({{compressible_key, std::string(1024, 'a')}}),
      from_map({{compressible_key, std::string(1024, 'b')},
                {incompressible_key, "A"}}),
      from_map({{incompressible_key, "b"}}),
  };

  PayloadGroupCodec::Encoder encoder(payload_groups_in.size());

  for (const auto& payload_group : payload_groups_in) {
    encoder.append(payload_group);
  }
  const Compression compression = GetParam();
  folly::IOBufQueue queue;
  encoder.encode(queue, compression, 1);

  folly::IOBuf encoded = queue.moveAsValue();
  encoded.coalesce();

  CompressedPayloadGroups compressed_payload_groups;
  size_t consumed;
  consumed = PayloadGroupCodec::decode(
      encoded, compressed_payload_groups, /* allow_buffer_sharing */ false);
  EXPECT_EQ(consumed, encoded.length());
  EXPECT_EQ(
      compressed_payload_groups.at(compressible_key).compression, compression);
  EXPECT_EQ(compressed_payload_groups.at(incompressible_key).compression,
            Compression::NONE);

  std::vector<PayloadGroup> payload_groups_out;
  consumed = PayloadGroupCodec::decode(Slice(encoded.data(), encoded.length()),
                                       payload_groups_out,
                                       /* allow_buffer_sharing */ true);
  EXPECT_EQ(consumed, encoded.length());

  EXPECT_THAT(payload_groups_out,
              testing::Pointwise(PayloadGroupEq(), payload_groups_in));
}

INSTANTIATE_TEST_CASE_P(CompressUncompress,
                        PayloadGroupEncoderCompressionTest,
                        ::testing::Values(Compression::LZ4,
                                          Compression::LZ4_HC,
                                          Compression::ZSTD));

class PayloadGroupEstimatorTest
    : public ::testing::TestWithParam<std::vector<PayloadGroup>> {};

TEST_P(PayloadGroupEstimatorTest, EstimateMatch) {
  const auto& payload_groups = GetParam();

  // Loop to test intermediate results
  for (int i = 1; i <= payload_groups.size(); i++) {
    PayloadGroupCodec::Estimator estimator;
    PayloadGroupCodec::Encoder encoder(i);

    std::vector<PayloadGroup> payload_groups_in;
    for (const auto& payload_group : payload_groups) {
      payload_groups_in.push_back(payload_group);
      estimator.append(payload_group);
      encoder.append(payload_group);
      if (payload_groups_in.size() == i) {
        break;
      }
    }

    folly::IOBufQueue encoded{folly::IOBufQueue::cacheChainLength()};
    encoder.encode(encoded, Compression::NONE);
    EXPECT_EQ(estimator.calculateSize(), encoded.chainLength());
  }
}

TEST_P(PayloadGroupEstimatorTest, NoAppends) {
  folly::IOBufQueue encoded{folly::IOBufQueue::cacheChainLength()};
  PayloadGroupCodec::Encoder encoder(0);
  encoder.encode(encoded, Compression::NONE);
  PayloadGroupCodec::Estimator estimator;
  EXPECT_EQ(estimator.calculateSize(), encoded.chainLength());
  encoded.move().reset();
}

namespace {
const PayloadGroup empty = from_map({});
const PayloadGroup group1 = from_map({{1, "payload1"}});
const PayloadGroup group2 = from_map({{2, "data2"}});
const PayloadGroup group12 = from_map({{1, "p1"}, {2, "p22"}});

const auto test_groups = ::testing::ValuesIn<std::vector<PayloadGroup>>(
    {{empty, empty, empty, empty, empty},
     {empty, group1, group2, empty},
     {empty, group1, group1, empty},
     {empty, group1, group12, empty},
     {empty, group12, group1, group2, empty},

     {group1},
     {group2},
     {group12},

     {group1, group1},
     {group1, group2},
     {group1, group12},
     {group1, empty, group1, empty},
     {group1, empty, group2, empty},
     {group1, empty, group12, empty},
     {group1, group1, group2, group1, group12, group1},

     {group12, group1, group2, empty},
     {group12, group12, group2, group12, empty},

     {group1, group2, group12},
     {group1, group12, group2},
     {group2, group1, group12},
     {group2, group12, group1},
     {group12, group1, group12},
     {group12, group2, group1}});

} // namespace

INSTANTIATE_TEST_CASE_P(EncodeDecodeMatch,
                        PayloadGroupEncoderTest,
                        test_groups);

INSTANTIATE_TEST_CASE_P(EstimateMatch, PayloadGroupEstimatorTest, test_groups);

} // namespace facebook::logdevice
