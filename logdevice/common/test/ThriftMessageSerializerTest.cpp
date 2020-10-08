/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "logdevice/common/thrift/compat/ThriftMessageSerializer.h"

#include <gtest/gtest.h>
#include <thrift/lib/cpp2/protocol/Serializer.h>

#include "logdevice/common/ThriftCodec.h"
#include "logdevice/common/protocol/ACK_Message.h"
#include "logdevice/common/protocol/APPEND_Message.h"

using namespace ::testing;
using apache::thrift::CompactSerializer;

namespace facebook { namespace logdevice {

class ThriftMessageSerializerTest : public ::testing::Test {
 public:
  void checkAppendMessage(const APPEND_Message& m, const APPEND_Message& m2) {
    EXPECT_EQ(m.header_.rqid, m2.header_.rqid);
    EXPECT_EQ(m.header_.logid, m2.header_.logid);
    EXPECT_EQ(m.header_.seen, m2.header_.seen);
    EXPECT_EQ(m.header_.timeout_ms, m2.header_.timeout_ms);
    EXPECT_EQ(m.header_.flags, m2.header_.flags);
    EXPECT_EQ(m.payload_.getPayload().toString(),
              m2.payload_.getPayload().toString());
  }

  std::unique_ptr<APPEND_Message> createSampleAppend() {
    APPEND_flags_t flags = APPEND_Header::CHECKSUM_64BIT |
        APPEND_Header::REACTIVATE_IF_PREEMPTED |
        APPEND_Header::BUFFERED_WRITER_BLOB;
    APPEND_Header h = {request_id_t(0xb64a0f255e281e45),
                       logid_t(0x3c3b4fa7a1299851),
                       epoch_t(0xee396b50),
                       0xed5b3efc,
                       flags};
    AppendAttributes attrs;
    return std::make_unique<APPEND_Message>(
        h, LSN_INVALID, attrs, PayloadHolder::copyString("hello"));
  }

  thrift::Message serder(thrift::Message& source) {
    auto serialized = CompactSerializer::serialize<std::string>(source);
    return CompactSerializer::deserialize<thrift::Message>(serialized);
  }

  ThriftMessageSerializer serializer{"test"};
};

// Checks serialized message can be deserialized w/o loss
TEST_F(ThriftMessageSerializerTest, Basic) {
  auto proto = Compatibility::MIN_PROTOCOL_SUPPORTED;
  auto before = createSampleAppend();
  auto converted = serializer.toThrift(*before, proto);
  thrift::Message deserialized = serder(*converted);
  auto message = serializer.fromThrift(std::move(deserialized), proto);
  ASSERT_NE(message, nullptr);
  const auto* after = dynamic_cast<APPEND_Message*>(message.get());
  checkAppendMessage(*before, *after);
}

// Checks serialized message can be deserialized w/o loss
TEST_F(ThriftMessageSerializerTest, Ack) {
  auto proto = Compatibility::MIN_PROTOCOL_SUPPORTED;
  ACK_Header ackhdr{0, request_id_t(1), 1, proto, E::OK};
  ACK_Message before{ackhdr};
  auto converted = serializer.toThrift(before, proto);
  thrift::Message deserialized = serder(*converted);
  auto message = serializer.fromThrift(std::move(deserialized), proto);
  ASSERT_NE(message, nullptr);
  const auto* after = dynamic_cast<ACK_Message*>(message.get());
  ASSERT_NE(after, nullptr);
  EXPECT_EQ(after->getHeader().options, before.getHeader().options);
  EXPECT_EQ(after->getHeader().rqid, before.getHeader().rqid);
  EXPECT_EQ(after->getHeader().client_idx, before.getHeader().client_idx);
  EXPECT_EQ(after->getHeader().proto, before.getHeader().proto);
}

// Create well-formed Thrift message with some garbage in wrapper
TEST_F(ThriftMessageSerializerTest, Failure) {
  auto proto = Compatibility::MIN_PROTOCOL_SUPPORTED;
  thrift::Message deserialized;
  std::string buffer = "\x01\x01\x41\xFF\x11\xCA\xFE\x0F\x01\x23\x11\xEE\xFF";
  auto io_buf = folly::IOBuf::wrapBuffer(buffer.c_str(), buffer.size());
  thrift::LegacyMessageWrapper wrapper;
  wrapper.set_payload(std::move(io_buf));
  deserialized.set_legacyMessage(wrapper);
  auto message = serializer.fromThrift(std::move(deserialized), proto);
  ASSERT_EQ(message, nullptr);
  ASSERT_EQ(err, E::BADMSG);
}
}} // namespace facebook::logdevice
