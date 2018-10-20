/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/common/Checksum.h"

#include <memory>

#include <folly/ScopeGuard.h>
#include <gtest/gtest.h>

#include "event2/buffer.h"
#include "logdevice/common/libevent/compat.h"
#include "logdevice/common/protocol/APPEND_Message.h"
#include "logdevice/common/protocol/ProtocolReader.h"
#include "logdevice/common/protocol/ProtocolWriter.h"
#include "logdevice/common/protocol/RECORD_Message.h"
#include "logdevice/common/util.h"

namespace facebook { namespace logdevice {

class ChecksumTest : public ::testing::Test {
 protected:
  // Takes advantage of friend declaration in RECORD_Message to expose
  // verifyChecksum() and payload_
  int verifyChecksum(const RECORD_Message& msg) {
    return msg.verifyChecksum();
  }
  const Payload& getPayload(const RECORD_Message& msg) {
    return msg.payload_;
  }

  // Simulates part of a record's lifecycle that is useful for testing
  // checksums:
  // (1) An APPEND message is created on the client with the client's payload
  //     ("123456789").
  // (2) The APPEND message is serialized into an evbuffer, at which time the
  //     checksum (if requested) is injected at the front of the payload.
  // (3) The APPEND message makes it to the server cluster, record gets stored.
  // (4) Later on, the client tries to reads the record.
  // (5) An optional mutation is applied to the flags and payload, to make
  //     it possible to test checksum failures.
  // (6) A RECORD message is created on the server, serialized and
  //     deserialized on the client.
  // (7) The "received" RECORD message is returned by this method.
  std::unique_ptr<RECORD_Message> roundTrip(
      APPEND_flags_t checksum_flags,
      std::function<void(RECORD_flags_t& checksum_flags, Payload& payload)>
          mutation = nullptr);
};

// This test guards against the underlying checksum implementations changing
// (unlikely), which would be disastrous since we persist checksums on disk.
TEST_F(ChecksumTest, Invariable) {
  const Slice data("123456789", 9);
  EXPECT_EQ(~0xe3069283, checksum_32bit(data));
  EXPECT_EQ(0xf8e4f0d10bd88705, checksum_64bit(data));
}

std::unique_ptr<RECORD_Message> ChecksumTest::roundTrip(
    APPEND_flags_t checksum_flags,
    std::function<void(RECORD_flags_t&, Payload&)> mutation) {
  // Assert that parity of outgoing flags checks out
  bool expected_parity = bool(checksum_flags & APPEND_Header::CHECKSUM) ==
      bool(checksum_flags & APPEND_Header::CHECKSUM_64BIT);
  bool actual_parity = bool(checksum_flags & APPEND_Header::CHECKSUM_PARITY);
  ld_check(expected_parity == actual_parity);

  std::unique_ptr<APPEND_Message> ap_recv_msg;

  {
    APPEND_Header ap_send_hdr = {
        request_id_t(1), logid_t(1), EPOCH_INVALID, 0, checksum_flags};
    PayloadHolder ph(Payload("123456789", 9), PayloadHolder::UNOWNED);
    AppendAttributes attrs;
    attrs.optional_keys[KeyType::FINDKEY] = std::string("abcdefgh");
    ap_send_hdr.flags |= APPEND_Header::CUSTOM_KEY;
    APPEND_Message ap_send_msg(ap_send_hdr, LSN_INVALID, attrs, std::move(ph));
    struct evbuffer* ap_send_evbuf = LD_EV(evbuffer_new)();
    SCOPE_EXIT {
      LD_EV(evbuffer_free)(ap_send_evbuf);
    };

    ProtocolWriter writer(ap_send_msg.type_,
                          ap_send_evbuf,
                          Compatibility::MAX_PROTOCOL_SUPPORTED);
    ap_send_msg.serialize(writer);
    ssize_t ap_send_size = writer.result();
    ld_check(ap_send_size > 0);

    ProtocolReader reader(MessageType::APPEND,
                          ap_send_evbuf,
                          ap_send_size,
                          Compatibility::MAX_PROTOCOL_SUPPORTED);
    ap_recv_msg = checked_downcast<std::unique_ptr<APPEND_Message>>(
        APPEND_Message::deserialize(reader, 1024).msg);
    ld_check(ap_recv_msg);
  }

  const Payload& ap_recv_payload = ap_recv_msg->payload_.getPayload();

  // Make a copy of the payload and apply the caller-provided mutation callback
  RECORD_flags_t flags = ap_recv_msg->header_.flags &
      (APPEND_Header::CHECKSUM | APPEND_Header::CHECKSUM_64BIT |
       APPEND_Header::CHECKSUM_PARITY);
  Payload payload = ap_recv_payload.dup();
  if (mutation) {
    mutation(flags, payload);
  }

  RECORD_Header record_send_hdr = {
      logid_t(1), read_stream_id_t(1), LSN_OLDEST, 0, flags};

  RECORD_Message record_send_msg(
      record_send_hdr, TrafficClass::READ_TAIL, std::move(payload), nullptr);
  struct evbuffer* record_send_evbuf = LD_EV(evbuffer_new)();
  SCOPE_EXIT {
    LD_EV(evbuffer_free)(record_send_evbuf);
  };

  ProtocolWriter writer(record_send_msg.type_,
                        record_send_evbuf,
                        Compatibility::MAX_PROTOCOL_SUPPORTED);
  record_send_msg.serialize(writer);
  ssize_t record_send_size = writer.result();
  ld_check(record_send_size > 0);

  ProtocolReader reader(MessageType::RECORD,
                        record_send_evbuf,
                        record_send_size,
                        Compatibility::MAX_PROTOCOL_SUPPORTED);
  return checked_downcast<std::unique_ptr<RECORD_Message>>(
      RECORD_Message::deserialize(reader).msg);
}

// Valid combinations of checksum flags (satisfying parity)
const APPEND_flags_t FLAGS_32 = APPEND_Header::CHECKSUM;
const APPEND_flags_t FLAGS_64 = APPEND_Header::CHECKSUM |
    APPEND_Header::CHECKSUM_64BIT | APPEND_Header::CHECKSUM_PARITY;
const APPEND_flags_t FLAGS_NONE = APPEND_Header::CHECKSUM_PARITY;

// Test that, with no corruption, the original 9-byte payload comes through

TEST_F(ChecksumTest, RoundTrip32Success) {
  std::unique_ptr<RECORD_Message> recv = roundTrip(FLAGS_32);
  ASSERT_EQ(0, verifyChecksum(*recv));
  ASSERT_EQ(9, getPayload(*recv).size());
}

TEST_F(ChecksumTest, RoundTrip64Success) {
  std::unique_ptr<RECORD_Message> recv = roundTrip(FLAGS_64);
  ASSERT_EQ(0, verifyChecksum(*recv));
  ASSERT_EQ(9, getPayload(*recv).size());
}

TEST_F(ChecksumTest, RoundTripNoChecksumSuccess) {
  std::unique_ptr<RECORD_Message> recv = roundTrip(FLAGS_NONE);
  ASSERT_EQ(0, verifyChecksum(*recv));
  ASSERT_EQ(9, getPayload(*recv).size());
}

// If a bit is flipped in the payload, we should be able to detect it when
// there was a checksum

static void payload_bit_flip(RECORD_flags_t& /*checksum_flags*/,
                             Payload& payload) {
  ((char*)payload.data())[payload.size() - 1] ^= 1 << 5;
}

TEST_F(ChecksumTest, RoundTrip32PayloadBitFlip) {
  std::unique_ptr<RECORD_Message> recv = roundTrip(FLAGS_32, payload_bit_flip);
  ASSERT_EQ(-1, verifyChecksum(*recv));
}

TEST_F(ChecksumTest, RoundTrip64PayloadBitFlip) {
  std::unique_ptr<RECORD_Message> recv = roundTrip(FLAGS_64, payload_bit_flip);
  ASSERT_EQ(-1, verifyChecksum(*recv));
}

TEST_F(ChecksumTest, RoundTripNoChecksumPayloadBitFlip) {
  std::unique_ptr<RECORD_Message> recv =
      roundTrip(FLAGS_NONE, payload_bit_flip);
  ASSERT_EQ(0, verifyChecksum(*recv)); // no protection
}

// If flags get corrupted (zeroed out), we should be able to detect that

static void flags_zeroed(RECORD_flags_t& checksum_flags, Payload& /*payload*/) {
  checksum_flags &= ~(APPEND_Header::CHECKSUM | APPEND_Header::CHECKSUM_64BIT |
                      APPEND_Header::CHECKSUM_PARITY);
}

TEST_F(ChecksumTest, RoundTrip32FlagsZeroed) {
  std::unique_ptr<RECORD_Message> recv = roundTrip(FLAGS_32, flags_zeroed);
  ASSERT_EQ(-1, verifyChecksum(*recv));
}

TEST_F(ChecksumTest, RoundTrip64FlagsZeroed) {
  std::unique_ptr<RECORD_Message> recv = roundTrip(FLAGS_64, flags_zeroed);
  ASSERT_EQ(-1, verifyChecksum(*recv));
}

TEST_F(ChecksumTest, RoundTripNoChecksumFlagsZeroed) {
  std::unique_ptr<RECORD_Message> recv = roundTrip(FLAGS_NONE, flags_zeroed);
  ASSERT_EQ(-1, verifyChecksum(*recv));
}

}} // namespace facebook::logdevice
