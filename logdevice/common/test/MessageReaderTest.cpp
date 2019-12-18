/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "logdevice/common/network/MessageReader.h"

#include <gtest/gtest.h>

#include "logdevice/common/protocol/Compatibility.h"
#include "logdevice/common/test/MockProtocolHandler.h"

using namespace ::testing;
using ::testing::_;
using ::testing::Return;
using namespace facebook::logdevice;

TEST(MessageReaderTest, EntireMessageReceivedTest) {
  MockProtocolHandler mock_conn;
  MessageReader read_cb(mock_conn, Compatibility::MAX_PROTOCOL_SUPPORTED);
  EXPECT_CALL(mock_conn, validateProtocolHeader(_))
      .Times(2)
      .WillRepeatedly(Return(true));
  EXPECT_CALL(mock_conn, dispatchMessageBody(_, _)).Times(1);
  ON_CALL(mock_conn, good()).WillByDefault(Return(true));
  void* bufReturn = nullptr;
  size_t lenReturn = 0;
  read_cb.getReadBuffer(&bufReturn, &lenReturn);
  ASSERT_NE(nullptr, bufReturn);
  ASSERT_EQ(sizeof(ProtocolHeader), lenReturn);
  ProtocolHeader* hdr = (ProtocolHeader*)bufReturn;
  const size_t store_msg_size = 100;
  hdr->type = MessageType::STORE;
  hdr->len = store_msg_size + sizeof(ProtocolHeader);
  hdr->cksum = 0;
  read_cb.readDataAvailable(lenReturn);
  read_cb.getReadBuffer(&bufReturn, &lenReturn);
  ASSERT_NE(nullptr, bufReturn);
  ASSERT_EQ(store_msg_size, lenReturn);
  read_cb.readDataAvailable(store_msg_size);
  // This test knowns something about the implementation and would need to
  // change once implementation changes. Don't like it but it's ok I guess.
  void* prev_buf_ptr = bufReturn;
  read_cb.getReadBuffer(&bufReturn, &lenReturn);
  ASSERT_NE(nullptr, bufReturn);
  ASSERT_EQ(sizeof(ProtocolHeader), lenReturn);
  // Calling it again should get the same buffer.
  prev_buf_ptr = bufReturn;
  read_cb.getReadBuffer(&bufReturn, &lenReturn);
  ASSERT_EQ(bufReturn, prev_buf_ptr);
  ASSERT_EQ(sizeof(ProtocolHeader), lenReturn);
  // Add another store message header of store_msg_size
  hdr = (ProtocolHeader*)bufReturn;
  hdr->type = MessageType::STORE;
  hdr->len = store_msg_size + sizeof(ProtocolHeader);
  hdr->cksum = 0;
  read_cb.readDataAvailable(lenReturn);
  // Calling getReadBuffer again should get buffer of  msg_size +
  // sizeof(ProtocolHeader).
  read_cb.getReadBuffer(&bufReturn, &lenReturn);
  ASSERT_NE(nullptr, bufReturn);
  ASSERT_EQ(store_msg_size, lenReturn);
}

TEST(MessageReaderTest, PartialHeaderReceived) {
  MockProtocolHandler mock_conn;
  MessageReader read_cb(mock_conn, Compatibility::MAX_PROTOCOL_SUPPORTED);
  EXPECT_CALL(mock_conn, validateProtocolHeader(_))
      .Times(1)
      .WillRepeatedly(Return(true));
  ON_CALL(mock_conn, good()).WillByDefault(Return(true));
  void* bufReturn = nullptr;
  size_t lenReturn = 0;
  void* prev_buf_ptr = bufReturn;
  for (size_t total_buffer_size = sizeof(ProtocolHeader); total_buffer_size > 1;
       total_buffer_size--) {
    read_cb.getReadBuffer(&bufReturn, &lenReturn);
    if (prev_buf_ptr != nullptr) {
      ASSERT_EQ((uint8_t*)prev_buf_ptr + 1, (uint8_t*)bufReturn);
    }
    ASSERT_EQ(lenReturn, total_buffer_size);
    read_cb.readDataAvailable(1);
    prev_buf_ptr = bufReturn;
  }
  // Get the last byte so that we can write the header contents.
  read_cb.getReadBuffer(&bufReturn, &lenReturn);
  ASSERT_EQ((uint8_t*)prev_buf_ptr + 1, (uint8_t*)bufReturn);
  ASSERT_EQ(lenReturn, 1);
  ProtocolHeader* hdr =
      (ProtocolHeader*)((uint8_t*)bufReturn - sizeof(ProtocolHeader) + 1);
  const size_t store_msg_size = 100;
  hdr->type = MessageType::STORE;
  hdr->len = store_msg_size + sizeof(ProtocolHeader);
  hdr->cksum = 0;
  read_cb.readDataAvailable(1);

  read_cb.getReadBuffer(&bufReturn, &lenReturn);
  ASSERT_NE(nullptr, bufReturn);
  ASSERT_EQ(store_msg_size, lenReturn);
}

TEST(MessageReaderTest, PartialMessageReceived) {
  MockProtocolHandler mock_conn;
  MessageReader read_cb(mock_conn, Compatibility::MAX_PROTOCOL_SUPPORTED);
  EXPECT_CALL(mock_conn, validateProtocolHeader(_))
      .Times(2)
      .WillRepeatedly(Return(true));
  ON_CALL(mock_conn, good()).WillByDefault(Return(true));
  void* bufReturn = nullptr;
  size_t lenReturn = 0;
  read_cb.getReadBuffer(&bufReturn, &lenReturn);
  ASSERT_NE(nullptr, bufReturn);
  ASSERT_EQ(sizeof(ProtocolHeader), lenReturn);
  ProtocolHeader* hdr = (ProtocolHeader*)bufReturn;
  const size_t store_msg_size = 100;
  hdr->type = MessageType::STORE;
  hdr->len = store_msg_size + sizeof(ProtocolHeader);
  hdr->cksum = 0;
  read_cb.readDataAvailable(lenReturn);
  bufReturn = nullptr;
  void* prev_buf_ptr = bufReturn;
  EXPECT_CALL(mock_conn, dispatchMessageBody(_, _)).Times(1);
  for (size_t total_buffer_size = store_msg_size; total_buffer_size > 0;
       total_buffer_size--) {
    read_cb.getReadBuffer(&bufReturn, &lenReturn);
    if (prev_buf_ptr != nullptr) {
      ASSERT_EQ((uint8_t*)prev_buf_ptr + 1, (uint8_t*)bufReturn);
    }
    ASSERT_EQ(lenReturn, total_buffer_size);
    read_cb.readDataAvailable(1);
    prev_buf_ptr = bufReturn;
  }
  // Get the last byte so that we can write the header contents.
  read_cb.getReadBuffer(&bufReturn, &lenReturn);
  ASSERT_NE(nullptr, bufReturn);
  ASSERT_EQ(lenReturn, sizeof(ProtocolHeader));
  hdr = (ProtocolHeader*)bufReturn;
  hdr->type = MessageType::STORE;
  hdr->len = store_msg_size + sizeof(ProtocolHeader);
  hdr->cksum = 0;
  read_cb.readDataAvailable(sizeof(ProtocolHeader));
  read_cb.getReadBuffer(&bufReturn, &lenReturn);
  ASSERT_NE(nullptr, bufReturn);
  ASSERT_EQ(store_msg_size, lenReturn);
}

TEST(MessageReaderTest, HitErrorWhenProcessingMessageOrHeader) {
  MockProtocolHandler mock_conn;
  MessageReader read_cb(mock_conn, Compatibility::MAX_PROTOCOL_SUPPORTED);
  EXPECT_CALL(mock_conn, validateProtocolHeader(_))
      .Times(1)
      .WillRepeatedly(Return(true));
  // Will return valid buffer once.
  EXPECT_CALL(mock_conn, good())
      .Times(3)
      .WillOnce(Return(true))
      .WillOnce(Return(true))
      .WillOnce(Return(false));

  void* bufReturn = nullptr;
  size_t lenReturn = 0;
  read_cb.getReadBuffer(&bufReturn, &lenReturn);
  ASSERT_NE(nullptr, bufReturn);
  ASSERT_EQ(sizeof(ProtocolHeader), lenReturn);
  ProtocolHeader* hdr = (ProtocolHeader*)bufReturn;
  const size_t store_msg_size = 100;
  hdr->type = MessageType::STORE;
  hdr->len = store_msg_size + sizeof(ProtocolHeader);
  hdr->cksum = 0;
  read_cb.readDataAvailable(lenReturn);
  read_cb.getReadBuffer(&bufReturn, &lenReturn);
  ASSERT_EQ(nullptr, bufReturn);
  ASSERT_EQ(0, lenReturn);
}

TEST(MessageReaderTest, FailProtocolHeaderValidation) {
  MockProtocolHandler mock_conn;
  MessageReader read_cb(mock_conn, Compatibility::MAX_PROTOCOL_SUPPORTED);
  EXPECT_CALL(mock_conn, validateProtocolHeader(_))
      .Times(1)
      .WillRepeatedly(Return(false));
  EXPECT_CALL(mock_conn, good())
      .Times(3)
      .WillOnce(Return(true))
      .WillOnce(Return(true))
      .WillOnce(Return(false));
  EXPECT_CALL(mock_conn, notifyErrorOnSocket(_)).Times(1);
  void* bufReturn = nullptr;
  size_t lenReturn = 0;
  read_cb.getReadBuffer(&bufReturn, &lenReturn);
  ASSERT_NE(nullptr, bufReturn);
  ASSERT_EQ(sizeof(ProtocolHeader), lenReturn);
  ProtocolHeader* hdr = (ProtocolHeader*)bufReturn;
  const size_t store_msg_size = 100;
  hdr->type = MessageType::STORE;
  hdr->len = store_msg_size + sizeof(ProtocolHeader);
  hdr->cksum = 0;
  read_cb.readDataAvailable(lenReturn);
  read_cb.getReadBuffer(&bufReturn, &lenReturn);
  ASSERT_EQ(nullptr, bufReturn);
  ASSERT_EQ(0, lenReturn);
}
