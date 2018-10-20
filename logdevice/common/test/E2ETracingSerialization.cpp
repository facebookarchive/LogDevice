/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include <memory>

#include <gtest/gtest.h>

#include "event2/buffer.h"
#include "logdevice/common/libevent/compat.h"
#include "logdevice/common/protocol/APPEND_Message.h"
#include "logdevice/common/protocol/ProtocolReader.h"
#include "logdevice/common/protocol/ProtocolWriter.h"
#include "logdevice/common/protocol/STORE_Message.h"
#include "logdevice/common/util.h"

namespace facebook { namespace logdevice {

class E2ETracingSerializationTest : public ::testing::Test {
 protected:
  std::unique_ptr<APPEND_Message> buildAppendMessage(APPEND_flags_t flags,
                                                     std::string tracing_info);

  std::unique_ptr<STORE_Message> buildStoreMessage(STORE_flags_t flags,
                                                   std::string tracing_info);
  // Simulate the serialization and deserialization of the message
  // The original message is passed as parameter
  // This function serializes the message into an evbuffer, uses a
  // ProtocolWriter to serialize the message and then the result is deserialized
  // using a ProtocolReader and returned so that it can be checked against the
  // original message. MessageType needed for the deserialization part
  std::unique_ptr<Message> simpleRoundTrip(Message* original_msg,
                                           MessageType type);

  std::string getTracingContext(APPEND_Message* msg) {
    return msg->e2e_tracing_context_;
  }

  std::string getTracingContext(STORE_Message* msg) {
    return msg->e2e_tracing_context_;
  }
};

std::unique_ptr<APPEND_Message>
E2ETracingSerializationTest::buildAppendMessage(APPEND_flags_t flags,
                                                std::string tracing_info) {
  APPEND_Header ap_send_hdr = {
      request_id_t(1), logid_t(1), EPOCH_INVALID, 0, flags};

  PayloadHolder ph(Payload("123456789", 9), PayloadHolder::UNOWNED);

  AppendAttributes attrs;
  attrs.optional_keys[KeyType::FINDKEY] = std::string("abcdefgh");
  ap_send_hdr.flags |= APPEND_Header::CUSTOM_KEY;

  return std::make_unique<APPEND_Message>(
      ap_send_hdr, LSN_INVALID, attrs, std::move(ph), tracing_info);
}

std::unique_ptr<STORE_Message>
E2ETracingSerializationTest::buildStoreMessage(STORE_flags_t flags,
                                               std::string tracing_info) {
  auto hdr = STORE_Header{
      RecordID(0xc5e58b03be6504ce, logid_t(0x0fbc3a81c75251e2)),
      0xeb4925bfde9dec8e,
      esn_t(0xebecb8bb),
      0x00000001, // wave
      flags,      // flags received as parameter
      1,
      2,
      1, // copyset size
      0xb9b421a5,
      NodeID(0x508b, 0xa554),
  };

  std::vector<StoreChainLink> cs;
  cs.assign({{ShardID(1, 0), ClientID(1)}});

  return std::make_unique<STORE_Message>(
      hdr,
      cs.data(),
      hdr.copyset_offset,
      0,
      STORE_Extra{},
      std::map<KeyType, std::string>(),
      std::make_shared<PayloadHolder>(
          Payload("123456789", 9), PayloadHolder::UNOWNED),
      false,
      tracing_info);
}

// simulate serialization and deserialization
std::unique_ptr<Message>
E2ETracingSerializationTest::simpleRoundTrip(Message* msg_to_send,
                                             MessageType type) {
  // simple fields to complete serialization and deserialization calls
  struct evbuffer* ap_to_send_evbuf = LD_EV(evbuffer_new)();
  SCOPE_EXIT {
    LD_EV(evbuffer_free)(ap_to_send_evbuf);
  };

  ProtocolWriter writer(msg_to_send->type_,
                        ap_to_send_evbuf,
                        Compatibility::MAX_PROTOCOL_SUPPORTED);
  msg_to_send->serialize(writer);
  ssize_t ap_to_send_size = writer.result();
  ld_check(ap_to_send_size > 0);

  ProtocolReader reader(type,
                        ap_to_send_evbuf,
                        ap_to_send_size,
                        Compatibility::MAX_PROTOCOL_SUPPORTED);

  if (type == MessageType::APPEND) {
    return APPEND_Message::deserialize(reader, 1024).msg;
  }

  if (type == MessageType::STORE) {
    return STORE_Message::deserialize(reader, 1024).msg;
  }

  return nullptr;
}

const APPEND_flags_t APPEND_FLAGS_E2E_TRACING_ON =
    APPEND_Header::E2E_TRACING_ON;
const STORE_flags_t STORE_FLAGS_E2E_TRACING_ON = STORE_Header::E2E_TRACING_ON;
const std::string simple_string = "TRACING_INFORMATION";

TEST_F(E2ETracingSerializationTest, APPEND_Message_Serialization) {
  auto original_msg =
      buildAppendMessage(APPEND_FLAGS_E2E_TRACING_ON, simple_string);

  std::unique_ptr<Message> msg_recv =
      simpleRoundTrip(original_msg.get(), MessageType::APPEND);
  std::unique_ptr<APPEND_Message> append_msg_recv =
      checked_downcast<std::unique_ptr<APPEND_Message>>(std::move(msg_recv));
  std::string deserialized_tracing_context =
      getTracingContext(append_msg_recv.get());

  ASSERT_EQ(simple_string, deserialized_tracing_context);
}

TEST_F(E2ETracingSerializationTest, APPEND_Message_NoSerialization) {
  // no flags, tracing information not serialized
  auto original_msg = buildAppendMessage(0, simple_string);

  std::unique_ptr<Message> msg_recv =
      simpleRoundTrip(original_msg.get(), MessageType::APPEND);
  std::unique_ptr<APPEND_Message> append_msg_recv =
      checked_downcast<std::unique_ptr<APPEND_Message>>(std::move(msg_recv));

  std::string deserialized_tracing_context =
      getTracingContext(append_msg_recv.get());

  // the deserialized tracing_info should be empty
  ASSERT_TRUE(deserialized_tracing_context.empty());
}

TEST_F(E2ETracingSerializationTest, STORE_Message_Serialization) {
  auto original_msg =
      buildStoreMessage(STORE_FLAGS_E2E_TRACING_ON, simple_string);
  std::unique_ptr<Message> msg_recv =
      simpleRoundTrip(original_msg.get(), MessageType::STORE);
  std::unique_ptr<STORE_Message> store_msg_recv =
      checked_downcast<std::unique_ptr<STORE_Message>>(std::move(msg_recv));

  std::string deserialized_tracing_context =
      getTracingContext(store_msg_recv.get());

  ASSERT_EQ(simple_string, deserialized_tracing_context);
}
}} // namespace facebook::logdevice
