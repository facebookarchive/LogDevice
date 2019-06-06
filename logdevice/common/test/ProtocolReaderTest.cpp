/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/common/protocol/ProtocolReader.h"

#include <deque>

#include <gtest/gtest.h>

#include "event2/buffer.h"
#include "logdevice/common/libevent/compat.h"
#include "logdevice/common/protocol/Compatibility.h"
#include "logdevice/common/protocol/Message.h"

using namespace facebook::logdevice;

class ProtocolReaderTest : public ::testing::Test {
 protected:
  void TearDown() override {
    for (evbuffer* evbuf : evbufs_) {
      LD_EV(evbuffer_free)(evbuf);
    }
    evbufs_.clear();
  }

 private:
  std::deque<evbuffer*> evbufs_;
};

TEST_F(ProtocolReaderTest, Basic) {
  const int orig = 0x12345678;
  int read_back = 0;
  bool cb_invoked = false;
  ProtocolReader reader(
      Slice(&orig, sizeof orig), "test", Compatibility::MAX_PROTOCOL_SUPPORTED);
  reader.read(&read_back);
  reader.result([&]() {
    cb_invoked = true;
    return nullptr;
  });
  // No errors so callback should get invoked
  ASSERT_TRUE(cb_invoked);
  ASSERT_EQ(orig, read_back);
}

TEST_F(ProtocolReaderTest, TooSmall) {
  const int8_t orig = 0x99;
  int16_t read_back = 0xff;
  bool cb_invoked = false;
  ProtocolReader reader(
      Slice(&orig, sizeof orig), "test", Compatibility::MAX_PROTOCOL_SUPPORTED);
  reader.read(&read_back);
  reader.result([&]() {
    cb_invoked = true;
    return nullptr;
  });
  // Because we tried to read two bytes but the evbuffer only has one, the
  // callback should never get invoked
  ASSERT_FALSE(cb_invoked);
  ASSERT_EQ(E::BADMSG, err);
  ASSERT_EQ(0, read_back);
}

TEST_F(ProtocolReaderTest, TooBig) {
  const int16_t orig = 0x9988;
  int8_t read_back = 0;
  bool cb_invoked = false;
  ProtocolReader reader(
      Slice(&orig, sizeof orig), "test", Compatibility::MAX_PROTOCOL_SUPPORTED);
  reader.read(&read_back);
  reader.result([&]() {
    cb_invoked = true;
    return nullptr;
  });
  // The evbuffer has leftover bytes, should generate error by default
  ASSERT_FALSE(cb_invoked);
  ASSERT_EQ(E::TOOBIG, err);
}

TEST_F(ProtocolReaderTest, LengthPrefixedVectorSomeBytes) {
  const int64_t data[2] = {0x08, 0x02};
  std::vector<int64_t> read_back;
  bool cb_invoked = false;
  ProtocolReader reader(
      Slice(&data, sizeof data), "test", Compatibility::MAX_PROTOCOL_SUPPORTED);
  reader.readLengthPrefixedVector(&read_back);
  reader.result([&]() {
    cb_invoked = true;
    return nullptr;
  });
  ASSERT_TRUE(cb_invoked);
  ASSERT_EQ(1, read_back.size());
  ASSERT_EQ(data[1], read_back[0]);
}

TEST_F(ProtocolReaderTest, LengthPrefixedVectorZeroBytes) {
  const int64_t orig = 0x00; // 0-length vector
  std::vector<int64_t> read_back = {0xff};
  bool cb_invoked = false;
  ProtocolReader reader(
      Slice(&orig, sizeof orig), "test", Compatibility::MAX_PROTOCOL_SUPPORTED);
  reader.readLengthPrefixedVector(&read_back);
  reader.result([&]() {
    cb_invoked = true;
    return nullptr;
  });
  ASSERT_TRUE(cb_invoked);
  ASSERT_EQ(0, read_back.size());
}

TEST_F(ProtocolReaderTest, LengthPrefixedVectorTooSmall) {
  const int64_t orig = 0x08; // vector with 1 element but no data
  std::vector<int64_t> read_back = {0xff};
  bool cb_invoked = false;
  ProtocolReader reader(
      Slice(&orig, sizeof orig), "test", Compatibility::MAX_PROTOCOL_SUPPORTED);
  reader.readLengthPrefixedVector(&read_back);
  reader.result([&]() {
    cb_invoked = true;
    return nullptr;
  });
  // Because we tried to read two int64_t's but the evbuffer only has one, the
  // callback should never get invoked
  ASSERT_FALSE(cb_invoked);
  ASSERT_EQ(0, read_back.size());
  ASSERT_EQ(E::BADMSG, err);
}

// Indicates that a vector of int64_t's is 1 byte long, i.e. the size isn't
// divisible by the type size
TEST_F(ProtocolReaderTest, LengthPrefixedVectorInvalidBytes) {
  const int64_t orig = 0x01;
  std::vector<int64_t> read_back = {0xff};
  bool cb_invoked = false;
  ProtocolReader reader(
      Slice(&orig, sizeof orig), "test", Compatibility::MAX_PROTOCOL_SUPPORTED);
  reader.readLengthPrefixedVector(&read_back);
  reader.result([&]() {
    cb_invoked = true;
    return nullptr;
  });
  // Because the encoding was invalid, the callback should never get invoked
  ASSERT_FALSE(cb_invoked);
  ASSERT_EQ(0, read_back.size());
  ASSERT_EQ(E::BADMSG, err);
}

TEST_F(ProtocolReaderTest, Proto) {
  const uint8_t input_bytes[] = {0x11, 0x22};
  const uint16_t SOCKET_PROTO = 2;
  ProtocolReader reader(
      Slice(&input_bytes, sizeof(input_bytes)), "test", SOCKET_PROTO);
  ASSERT_EQ(2, reader.bytesRemaining());

  {
    uint8_t b = 0;
    reader.protoGate(SOCKET_PROTO);
    reader.read(&b);
    // SOCKET_PROTO is high enough so this should get read
    ASSERT_EQ(0x11, b);
    ASSERT_EQ(1, reader.bytesRemaining());
  }
  {
    uint8_t b = 0;
    reader.protoGate(SOCKET_PROTO + 1);
    reader.read(&b);
    // Because SOCKET_PROTO is older than the gate, the read() call should
    // have been a no-op
    ASSERT_EQ(0x0, b);
    ASSERT_EQ(1, reader.bytesRemaining());
  }
}

TEST_F(ProtocolReaderTest, ProtoInField) {
  const uint8_t input_bytes[] = {0x01};
  ProtocolReader reader(
      Slice(&input_bytes, sizeof(input_bytes)), "test", folly::none);

  ASSERT_FALSE(reader.isProtoSet());
  {
    uint8_t b = 0;
    reader.readVersion(&b);
    ASSERT_EQ(0x1, b);
    ASSERT_EQ(0x1, reader.proto());
    ASSERT_TRUE(reader.isProtoSet());
  }
}

TEST_F(ProtocolReaderTest, ProtoMustBeSetToRead) {
  const uint8_t input_bytes[] = {0xfa, 0xce};
  ProtocolReader reader(
      Slice(&input_bytes, sizeof(input_bytes)), "test", folly::none);

  uint16_t val = 0;
  ASSERT_FALSE(reader.isProtoSet());
  reader.read(&val);
  ASSERT_NE(0xfaceu, val);
  ASSERT_EQ(reader.status(), E::PROTO);
}
